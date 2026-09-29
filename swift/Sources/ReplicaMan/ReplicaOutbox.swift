import Foundation
import GRDB

/// One immutable submission: its frozen wire operations, their ids in order,
/// and the frozen intents they answer for, position by position.
struct ReplicaSubmission: Sendable {
    let sequence: Int64
    let content: Data
    let ids: [String]
    let entries: [ReplicaStateStore.JournalRow]
}

extension ReplicaStateStore {
    /// The oldest frozen submissions one push may carry. When nothing is
    /// frozen, the lane's owed intents freeze first, each its own submission.
    func freezeSubmissions(_ db: Database, lane: ReplicaLane?, limit: Int) throws -> [ReplicaSubmission] {
        let frozen = try frozenSubmissions(db)
        if !frozen.isEmpty { return frozen }

        let entries = try Row.fetchAll(db, sql: """
            SELECT id, payload FROM intents
            WHERE state = 'owed' AND (? IS NULL OR lane = ?)
            ORDER BY rowid LIMIT ?
            """, arguments: [lane?.rawValue, lane?.rawValue, limit])

        var bytes = 0
        for entry in entries {
            let operations = try wireOperations([entry])
            let content = try ReplicaJSON.encoder().encode(operations)
            guard content.count <= ReplicaProtocol.operationBytes else {
                throw ReplicaError.storage("Mutation exceeds the request size limit; its local bytes are retained")
            }
            if bytes + content.count > ReplicaProtocol.operationBytes { break }
            try insertSubmission(db, content: content, entries: [entry], operations: operations)
            bytes += content.count
        }
        return try frozenSubmissions(db)
    }

    /// Intents as the operations one submission sends: each with a fresh
    /// UUID, and a shared group when there is more than one.
    func wireOperations(_ entries: [Row]) throws -> [ReplicaOp] {
        let group = entries.count > 1 ? ReplicaID.uuidV7() : nil
        return try entries.map { entry in
            var operation = try JournalRow.decodeOperation(Data((entry["payload"] as String).utf8))
            guard operation.incarnation != nil else {
                throw ReplicaError.storage("Journal entry has no entity incarnation")
            }
            operation.id = ReplicaID.uuidV7()
            operation.group = group
            return operation
        }
    }

    /// Freeze: the bytes go to `submissions`, each intent keeps its row and
    /// its queue place and learns the operation it now answers to.
    func insertSubmission(_ db: Database, content: Data, entries: [Row], operations: [ReplicaOp]) throws {
        let sequence = try meta(db).nextSequence
        guard sequence < Int64.max else {
            throw ReplicaError.storage("Submission sequence exhausted; recover this store")
        }
        try db.execute(sql: "INSERT INTO submissions (sequence, content) VALUES (?, ?)", arguments: [sequence, content])
        for (entry, operation) in zip(entries, operations) {
            try db.execute(sql: "UPDATE intents SET state = 'frozen', sequence = ?, operation = ? WHERE id = ?",
                           arguments: [sequence, operation.id, entry["id"] as String])
        }
        try db.execute(sql: "UPDATE meta SET next_sequence = ? WHERE id = 1", arguments: [sequence + 1])
    }

    /// Oldest first, at most one push's operations, never splitting a submission.
    func frozenSubmissions(_ db: Database) throws -> [ReplicaSubmission] {
        let rows = try Row.fetchCursor(db, sql: "SELECT sequence, content FROM submissions ORDER BY sequence")
        var submissions: [ReplicaSubmission] = []
        var count = 0
        var bytes = 0

        while let row = try rows.next() {
            let sequence: Int64 = row["sequence"]
            let content: Data = row["content"]
            let ids = try ReplicaJSON.decoder().decode([ReplicaOp].self, from: content).map(\.id)
            if !submissions.isEmpty,
               count + ids.count > ReplicaProtocol.maxOperations || bytes + content.count > ReplicaProtocol.operationBytes {
                break
            }
            count += ids.count
            bytes += content.count

            let entries = try ids.map { operation in
                guard let entry = try Row.fetchOne(db, sql: """
                    SELECT \(Self.journalColumns) FROM intents WHERE operation = ? AND sequence = ?
                    """, arguments: [operation, sequence]).map(Self.journalRow) else {
                    throw ReplicaError.storage("A frozen submission and its intents disagree")
                }
                return entry
            }

            submissions.append(ReplicaSubmission(sequence: sequence, content: content, ids: ids, entries: entries))
        }

        return submissions
    }

    /// An accepted intent stays visible over the base until a round that
    /// started after its acceptance publishes: its sequence is kept.
    func accept(_ db: Database, id: String) throws {
        try db.execute(sql: "UPDATE intents SET state = 'accepted', operation = NULL WHERE id = ?", arguments: [id])
    }

    /// The server's reason, kept until the application dismisses it.
    func refuse(_ db: Database, id: String, reason: String) throws {
        try db.execute(sql: """
            UPDATE intents SET state = 'refused', reason = ?, sequence = NULL, operation = NULL WHERE id = ?
            """, arguments: [reason, id])
    }

    /// A verdict that settles its intent without leaving anything behind.
    func consume(_ db: Database, id: String) throws {
        try db.execute(sql: "DELETE FROM intents WHERE id = ?", arguments: [id])
    }

    func finishSubmission(_ db: Database, sequence: Int64) throws {
        try db.execute(sql: "DELETE FROM submissions WHERE sequence = ?", arguments: [sequence])
    }

    func overlays(_ db: Database, stream: String, id: String) throws -> [ReplicaOp] {
        let content = try String.fetchAll(db, sql: """
            SELECT payload FROM intents WHERE row_id = ? AND stream = ? AND state = 'accepted'
            ORDER BY sequence, id
            """, arguments: [id, stream])

        return try content.map { try ReplicaJSON.decoder().decode(ReplicaOp.self, from: Data($0.utf8)) }
    }
}
