import Foundation
import GRDB

/// One local transaction — `ReplicaEngine.write`. What it reads it reads under
/// the writer: the latest committed state plus its own writes. What it writes
/// commits together or not at all. A pull applies before it or after it, never
/// between its read and its write, so a decision made on a read cannot go
/// stale before it lands.
///
/// Row lane only; a document moves through `updateDoc`.
public final class ReplicaTransaction {
    let engine: ReplicaEngine
    let db: Database
    let store: ReplicaStateStore
    let lane: ReplicaLane
    let draft: String?
    /// A journal entry was written: a push is owed once this commits.
    private(set) var journaled = false
    var atomicEntries: [String]?

    init(engine: ReplicaEngine, db: Database, store: ReplicaStateStore, lane: ReplicaLane, draft: String?) {
        self.engine = engine
        self.db = db
        self.store = store
        self.lane = lane
        self.draft = draft
    }

    // MARK: - Reads

    /// The row as this transaction sees it. Nil only when the store does not
    /// hold it — a row the model cannot read is `undecodableRow`, never absent.
    public func find<Model: ReplicaRowModel>(_ type: Model.Type, _ id: String) throws -> Model? {
        guard let row = try store.snapshot(db, stream: Model.streamName, rowId: id) else { return nil }
        guard let model = Model(id: id, type: row.type, data: row.data) else {
            throw ReplicaError.undecodableRow(stream: Model.streamName, id: id)
        }
        return model
    }

    /// The scoped read under the writer. A row the model cannot read is left
    /// out, as in every list.
    public func list<Model: ReplicaRowModel>(
        _ type: Model.Type,
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil
    ) throws -> [Model] {
        let compiled = predicate?.compile(stream: Model.streamName, indexes: store.indexes)
        return try store.snapshots(
            db, stream: Model.streamName, where: compiled?.sql ?? "1", arguments: compiled?.arguments ?? [],
            orderBy: ReplicaOrder.clause(order, stream: Model.streamName, indexes: store.indexes), limit: limit
        ).compactMap { Model(id: $0.rowId, type: $0.type, data: $0.data) }
    }

    // MARK: - Writes

    /// Mint a row: absent, or `rowExists`.
    public func create<Model: ReplicaWritableRowModel>(_ model: Model) throws {
        let spec = try rowSpec(Model.streamName)
        guard try store.snapshot(db, stream: Model.streamName, rowId: model.id) == nil else {
            throw ReplicaError.rowExists(stream: Model.streamName, id: model.id)
        }
        let wrote = try engine.applyRowWrite(
            db, store: store, spec: spec, stream: Model.streamName, id: model.id, type: model.typeName,
            data: model.encode(), existing: nil, lane: lane, draft: draft
        )
        journaled = journaled || wrote
    }

    /// Edit the row this transaction reads: present, or `unknownRow`. Only the
    /// fields `edit` changed are owed — a field another writer moved keeps its
    /// value, and so does one the model cannot represent.
    @discardableResult
    public func update<Model: ReplicaWritableRowModel>(
        _ type: Model.Type, _ id: String, _ edit: (inout Model) throws -> Void
    ) throws -> Model {
        let spec = try rowSpec(Model.streamName)
        guard let existing = try store.snapshot(db, stream: Model.streamName, rowId: id) else {
            throw ReplicaError.unknownRow(stream: Model.streamName, id: id)
        }
        guard var model = Model(id: id, type: existing.type, data: existing.data) else {
            throw ReplicaError.undecodableRow(stream: Model.streamName, id: id)
        }
        let before = model.encode()
        try edit(&model)
        let changed = model.encode().filter { (before[$0.key] ?? .null) != $0.value }
        let wrote = try engine.applyRowWrite(
            db, store: store, spec: spec, stream: Model.streamName, id: id, type: existing.type ?? model.typeName,
            data: changed, existing: existing, lane: lane, draft: draft
        )
        journaled = journaled || wrote
        return model
    }

    /// True when a `row.delete` is owed; false when there was nothing to
    /// delete or the row's unsent birth collapsed with it.
    @discardableResult
    public func delete<Model: ReplicaWritableRowModel>(_ type: Model.Type, _ id: String) throws -> Bool {
        let spec = try rowSpec(Model.streamName)
        let queued = try engine.applyRowDelete(
            db, store: store, spec: spec, stream: Model.streamName, id: id, lane: lane, draft: draft
        )
        journaled = journaled || queued
        return queued
    }

    public func delete<Model: ReplicaWritableRowModel>(_ type: Model.Type, ids: [String]) throws {
        for id in ids { try delete(type, id) }
    }

    private func rowSpec(_ stream: String) throws -> ReplicaStreamSpec {
        let spec = try engine.writableSpec(stream)
        guard spec.lane == .row else { throw ReplicaError.laneMismatch(stream) }
        return spec
    }

    // MARK: - Typed stream views

    public func rows<Model: ReplicaWritableRowModel>(_ type: Model.Type) -> TransactionRows<Model> {
        TransactionRows(tx: self)
    }

    public func readonlyRows<Model: ReplicaRowModel>(_ type: Model.Type) -> TransactionReadonlyRows<Model> {
        TransactionReadonlyRows(tx: self)
    }

    // MARK: - The open transaction on this thread

    private static let threadKey = "io.replicaman.transaction"

    static func open(on engine: ReplicaEngine) -> ReplicaTransaction? {
        guard let open = Thread.current.threadDictionary[threadKey] as? ReplicaTransaction,
              open.engine === engine
        else { return nil }
        return open
    }

    static func running<T>(_ tx: ReplicaTransaction, _ body: () throws -> T) rethrows -> T {
        let dictionary = Thread.current.threadDictionary
        let previous = dictionary[threadKey]
        dictionary[threadKey] = tx
        defer { dictionary[threadKey] = previous }
        return try body()
    }
}

/// One writable stream inside a transaction — what `tx.<stream>` is.
public struct TransactionRows<Model: ReplicaWritableRowModel> {
    let tx: ReplicaTransaction

    public func find(_ id: String) throws -> Model? {
        try tx.find(Model.self, id)
    }

    public func list(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil
    ) throws -> [Model] {
        try tx.list(Model.self, predicate, order: order, limit: limit)
    }

    public func create(_ model: Model) throws {
        try tx.create(model)
    }

    @discardableResult
    public func update(_ id: String, _ edit: (inout Model) throws -> Void) throws -> Model {
        try tx.update(Model.self, id, edit)
    }

    @discardableResult
    public func delete(_ id: String) throws -> Bool {
        try tx.delete(Model.self, id)
    }

    public func delete(ids: [String]) throws {
        try tx.delete(Model.self, ids: ids)
    }
}

/// A readonly stream inside a transaction: reads under the writer, no verbs.
public struct TransactionReadonlyRows<Model: ReplicaRowModel> {
    let tx: ReplicaTransaction

    public func find(_ id: String) throws -> Model? {
        try tx.find(Model.self, id)
    }

    public func list(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil
    ) throws -> [Model] {
        try tx.list(Model.self, predicate, order: order, limit: limit)
    }
}

extension ReplicaEngine {
    /// One local transaction, on the caller's thread — the main actor
    /// included; nothing inside may `await`. A `write` inside a `write` joins
    /// the open one. The lane and the draft are the caller's.
    @discardableResult
    public nonisolated func write<T>(_ body: (ReplicaTransaction) throws -> T) throws -> T {
        if let open = ReplicaTransaction.open(on: self) { return try body(open) }
        let lane = ReplicaEngine.currentLane
        let draft = ReplicaEngine.currentDraft
        guard binding.store != nil else { throw ReplicaError.noOwner }
        try writeGate.enter()
        defer { writeGate.leave() }
        guard let store = binding.store else { throw ReplicaError.noOwner }
        var journaled = false
        let value = try store.pool.write { db -> T in
            let tx = ReplicaTransaction(engine: self, db: db, store: store, lane: lane, draft: draft)
            let value = try ReplicaTransaction.running(tx) { try body(tx) }
            journaled = tx.journaled
            return value
        }
        if journaled { Task { await self.schedulePush() } }
        return value
    }

    /// The same transaction without holding the caller's thread while the
    /// writer is busy — for a heavy write, or a caller that must not block.
    /// Not cancellable: a write is a fact, and a cancel handler's cleanup is
    /// written from the very task that was cancelled.
    @discardableResult
    public nonisolated func write<T: Sendable>(
        _ body: @escaping @Sendable (ReplicaTransaction) throws -> T
    ) async throws -> T {
        let lane = ReplicaEngine.currentLane
        let draft = ReplicaEngine.currentDraft
        guard binding.store != nil else { throw ReplicaError.noOwner }
        try writeGate.enter()
        defer { writeGate.leave() }
        guard let store = binding.store else { throw ReplicaError.noOwner }
        let (value, journaled) = try await withCheckedThrowingContinuation { continuation in
            store.pool.asyncWrite({ db -> (T, Bool) in
                let tx = ReplicaTransaction(engine: self, db: db, store: store, lane: lane, draft: draft)
                let value = try ReplicaTransaction.running(tx) { try body(tx) }
                return (value, tx.journaled)
            }, completion: { _, result in
                continuation.resume(with: result)
            })
        }
        if journaled { Task { await self.schedulePush() } }
        return value
    }
}
