import Foundation
import GRDB

extension ReplicaEngine {
    /// Save a row action locally and queue it as one server transaction.
    /// Every member must pass its gates. A held member, an unfrozen dependency,
    /// or an oversized group throws and rolls back the entire local action.
    /// Once committed, the immutable group survives retries and process restart.
    @discardableResult
    public nonisolated func writeAtomically<T>(_ body: (ReplicaTransaction) throws -> T) throws -> T {
        guard ReplicaTransaction.open(on: self) == nil, Self.currentDraft == nil else {
            throw ReplicaError.atomicWriteBlocked("Atomic writes cannot be nested or captured inside a draft")
        }
        return try write { tx in
            tx.atomicEntries = []
            let value = try body(tx)
            try tx.store.freezeAtomicWrite(tx.db, ids: tx.atomicEntries ?? [])
            return value
        }
    }

    nonisolated func validateAtomicAddress(
        _ db: Database, stream: String, id: String, store: ReplicaStateStore
    ) throws {
        guard let captured = ReplicaTransaction.open(on: self)?.atomicEntries else { return }
        let earlier = try store.entriesAddressing(db, stream: stream, rowId: id).contains {
            $0.parked == nil && !$0.sent && !captured.contains($0.id)
        }
        let held = try store.hold(db, stream: stream, rowId: id)
        if earlier || held != nil {
            throw ReplicaError.atomicWriteBlocked("Unsubmitted dependency: \(stream)/\(id)")
        }
    }

    nonisolated func validateAtomicAdmission(
        _ db: Database, op: ReplicaOp, preimage: Data?, store: ReplicaStateStore
    ) throws {
        try validateAtomicAddress(db, stream: op.stream, id: op.rowId, store: store)
        for reference in op.references {
            try validateAtomicAddress(db, stream: reference.stream, id: reference.id, store: store)
        }
        switch syncGates.judge(try Self.change(op, preimage: preimage)) {
        case .push:
            return
        case .hold(let gate, let reason):
            throw ReplicaError.atomicWriteBlocked("\(gate): \(reason)")
        case .discard:
            throw ReplicaError.atomicWriteBlocked("A gate would discard \(op.stream)/\(op.rowId)")
        }
    }
}

extension ReplicaStateStore {
    /// The action freezes as one submission whose operations share a group.
    func freezeAtomicWrite(_ db: Database, ids: [String]) throws {
        guard !ids.isEmpty else { return }
        guard ids.count <= ReplicaProtocol.maxOperations else {
            throw ReplicaError.atomicWriteBlocked("An atomic write supports at most 100 operations")
        }
        let rows = try Row.fetchAll(db, sql: """
            SELECT id, payload FROM intents
            WHERE id IN (\(databaseQuestionMarks(count: ids.count))) ORDER BY rowid
            """, arguments: StatementArguments(ids))
        guard !rows.isEmpty else { return }
        let operations = try wireOperations(rows)
        let content = try ReplicaJSON.encoder().encode(operations)
        guard content.count <= ReplicaProtocol.operationBytes else {
            throw ReplicaError.atomicWriteBlocked("An atomic write supports at most 100 operations and 32 MiB")
        }
        try insertSubmission(db, content: content, entries: rows, operations: operations)
    }
}
