import Foundation
import GRDB

/// The runtime under the generated verb surface. Codegen emits one model
/// type per stream (STI base + subclasses, decode switch on `type`) and one
/// stream handle per declaration. A row stream's handle reads; its writes are
/// a transaction's verbs (`ReplicaEngine.write`), and a readonly stream has
/// none there to call — the mistake is inexpressible. Document streams get
/// `DocumentStream` (their content rides the doc, not row ops).

// MARK: - Model protocols

/// A model whose wire columns are named by a generated enum — a row model
/// or an STI variant — so a host names a column by type, never by string.
public protocol ReplicaColumns: Sendable {
    associatedtype Column: RawRepresentable, Sendable, Hashable where Column.RawValue == String
    static var streamName: String { get }
}

public protocol ReplicaRowModel: Identifiable, Sendable where ID == String {
    /// The stream's indexed fields (generated). Models without indexes keep
    /// the default: nothing to scope on.
    associatedtype Field: ReplicaIndexedField = ReplicaNoField
    static var streamName: String { get }
    /// Best-effort typed projection of a raw snapshot row: an unknown STI
    /// `type` or a missing required field returns nil — the raw row stays in
    /// the store either way (decode tolerance, ARCHITECTURE §4.2).
    init?(id: String, type: String?, data: [String: ReplicaValue])
    var id: String { get }
    var typeName: String? { get }
    func encode() -> [String: ReplicaValue]
}

/// The marker split that makes readonly compile-time: a writable model has
/// write verbs inside a transaction (`TransactionRows`); a readonly model
/// never conforms.
public protocol ReplicaWritableRowModel: ReplicaRowModel {}

public protocol ReplicaDocModel: Identifiable, Sendable where ID == String {
    associatedtype Field: ReplicaIndexedField = ReplicaNoField
    /// What `findDoc` / `watchDoc` read of the document (`NoDocState` for a
    /// model that only carries its projection row).
    associatedtype State: ReplicaDocState = NoDocState
    static var streamName: String { get }
    init?(id: String, data: [String: ReplicaValue])
    var id: String { get }
}

/// Columns whose values come from the authenticated engine session rather
/// than from a CRUD caller: the owner and both clocks at a document's birth,
/// and `updatedAt` again whenever the document moves on this device. A
/// stream spec declares it; it is runtime metadata only and never changes
/// the replica wire grammar.
public struct ReplicaStamp: Sendable, Equatable {
    public var userId: String?
    public var createdAt: String?
    public var updatedAt: String?

    public init(userId: String? = nil, createdAt: String? = nil, updatedAt: String? = nil) {
        self.userId = userId
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    public static let standard = ReplicaStamp(
        userId: "userId",
        createdAt: "createdAt",
        updatedAt: "updatedAt"
    )
}

// MARK: - Shared reads

enum ReplicaReads {
    /// A find is a POINT read: the warm model when this type is materialized
    /// at the live sequence, else one index-served row that reuses the
    /// stream's decoded record — never a materialization of the stream. A
    /// find that rebuilt the picture walked every row on the caller's
    /// thread after each write, and under the device pipeline's write
    /// cadence that was every find.
    static func find<Model>(
        _ store: ReplicaStateStore?, stream: String, id: String,
        decode: @escaping @Sendable (String, String?, [String: ReplicaValue]) -> Model?
    ) throws -> Model? where Model: Sendable {
        guard let store else { return nil }
        if let warm: Model = store.warmModel(stream: stream, id: id) { return warm }
        return try store.scopedRows(
            stream: stream, where: "row_id = ?", arguments: [id], decode: decode
        ).first
    }

    /// The whole stream, through the materialization cache.
    static func materialized<Model>(
        _ store: ReplicaStateStore, stream: String, minimumSequence: Int64? = nil,
        decode: @escaping @Sendable (String, String?, [String: ReplicaValue]) -> Model?
    ) throws -> [Model] where Model: Sendable {
        let materialization: ReplicaStateStore.RowMaterialization<Model> = try store.materializedRows(
            stream: stream,
            minimumSequence: minimumSequence,
            decode: decode
        )
        return materialization.rows.map { $0.model }
    }

    /// The scoped read: a predicate compiles to an indexed WHERE over
    /// `snapshots`, an order to an ORDER BY over the same columns; only the
    /// matching rows are fetched, and each one reuses the stream's decoded
    /// record / model when its raw text is unchanged. No predicate, order or
    /// limit = the whole stream through the materialization cache.
    static func list<Model: Sendable, Field: ReplicaIndexedField>(
        _ store: ReplicaStateStore?, stream: String, predicate: ReplicaPredicate<Field>?,
        order: [ReplicaOrder<Field>] = [], limit: Int? = nil,
        minimumSequence: Int64? = nil,
        decode: @escaping @Sendable (String, String?, [String: ReplicaValue]) -> Model?
    ) throws -> [Model] {
        guard let store else { return [] }
        guard predicate != nil || !order.isEmpty || limit != nil else {
            return try materialized(store, stream: stream, minimumSequence: minimumSequence, decode: decode)
        }
        let compiled = predicate?.compile(stream: stream, indexes: store.indexes)
        return try store.scopedRows(
            stream: stream, where: compiled?.sql ?? "1", arguments: compiled?.arguments ?? [],
            orderBy: ReplicaOrder.clause(order, stream: stream, indexes: store.indexes), limit: limit, decode: decode
        )
    }

    /// The scoped live query (the Cachebay shape): the sync `list` gave the
    /// first picture, so the watch delivers CHANGES only — each stream
    /// commit re-runs the scoped read off-main and delivers on the main
    /// actor when the picture differs from the last one delivered. A commit
    /// outside the scope is silent. `includeInitial` opts a background
    /// consumer into the baseline. Same owner-rebinding loop as `watch`.
    static func watch<Model: Sendable & Equatable, Field: ReplicaIndexedField>(
        _ binding: ReplicaBinding, health: ReplicaHealth, stream: String, predicate: ReplicaPredicate<Field>?,
        order: [ReplicaOrder<Field>] = [], limit: Int? = nil, includeInitial: Bool,
        decode: @escaping @Sendable (String, String?, [String: ReplicaValue]) -> Model?,
        deliver: @escaping @MainActor @Sendable ([Model]) -> Void
    ) -> ReplicaWatch {
        let last = LastPicture<Model>()
        let task = Task {
            var armed = false
            while !Task.isCancelled {
                let (bound, generation) = binding.snapshot()
                guard let store = bound?.store else {
                    let wasArmed = armed
                    await MainActor.run {
                        guard !Task.isCancelled, binding.snapshot().generation == generation else { return }
                        guard (includeInitial && !wasArmed) || (wasArmed && last.value != []) else { return }
                        last.value = []
                        deliver([])
                    }
                    armed = true
                    await binding.waitForChange(after: generation)
                    continue
                }
                // The picture is read AT the sequence the observation saw:
                // a cache entry older than it is bypassed. Without the
                // floor the read could land before `commitMutation` evicts
                // the stale entry and deliver the OLD picture — equal to
                // the last one, so nothing reaches the consumer, and the
                // stream is silent until its next commit: a consumer
                // kept showing a finished run as still running.
                let observation = ValueObservation
                    .tracking { db in
                        try store.changeSequence(db, stream: stream)
                    }
                    .removeDuplicates()
                    .map { sequence in
                        try list(
                            store, stream: stream, predicate: predicate, order: order, limit: limit,
                            minimumSequence: sequence, decode: decode
                        )
                    }
                let wasArmed = armed
                armed = true
                await withTaskGroup(of: Void.self) { group in
                    group.addTask {
                        var baseline = true
                        do {
                            for try await picture in observation.values(in: store.pool) {
                                let isBaseline = baseline
                                baseline = false
                                await MainActor.run {
                                    guard !Task.isCancelled, binding.snapshot().generation == generation else { return }
                                    if isBaseline, !wasArmed, !includeInitial {
                                        last.value = picture
                                        return
                                    }
                                    guard last.value != picture else { return }
                                    last.value = picture
                                    deliver(picture)
                                }
                            }
                        } catch {
                            // A closing outgoing store may interrupt its observation.
                            // Active-store failures remain visible through health.
                            if !Task.isCancelled, binding.snapshot().generation == generation {
                                health.record(error, operation: "watch rows \(stream)")
                            }
                        }
                    }
                    group.addTask { await binding.waitForChange(after: generation) }
                    await group.next()
                    group.cancelAll()
                    await group.waitForAll()
                }
                await binding.waitForChange(after: generation)
            }
        }
        return ReplicaWatch(task: task)
    }

    private final class LastPicture<Model: Sendable & Equatable>: @unchecked Sendable {
        private let lock = NSLock()
        private var stored: [Model]?

        var value: [Model]? {
            get { lock.withLock { stored } }
            set { lock.withLock { stored = newValue } }
        }
    }
}

// MARK: - Stream handles

public struct RowStream<Model: ReplicaRowModel>: Sendable {
    public let engine: ReplicaEngine

    public init(engine: ReplicaEngine) {
        self.engine = engine
    }

    /// A point read on the warm store — a picture to show. A read a write
    /// decides on is taken inside `ReplicaEngine.write`.
    public func find(_ id: String) throws -> Model? {
        try ReplicaReads.find(engine.store, stream: Model.streamName, id: id, decode: Model.init)
    }

    /// Sync read on the warm store — the first picture. Only indexed
    /// predicates exist; bare `list()` is the whole stream.
    public func list(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil
    ) throws -> [Model] {
        try ReplicaReads.list(
            engine.store, stream: Model.streamName, predicate: predicate, order: order, limit: limit, decode: Model.init
        )
    }

    /// Changes after `list`, delivered on the main actor; hold the handle.
    public func watch(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil,
        includeInitial: Bool = false,
        _ deliver: @escaping @MainActor @Sendable ([Model]) -> Void
    ) -> ReplicaWatch where Model: Equatable {
        ReplicaReads.watch(
            engine.binding, health: engine.health, stream: Model.streamName, predicate: predicate, order: order, limit: limit,
            includeInitial: includeInitial, decode: Model.init, deliver: deliver
        )
    }
}

/// A server-authored document stream: readable fold and projection, no
/// write verbs at all — the readonly counterpart of `DocumentStream`.
public struct ReadonlyDocumentStream<Model: ReplicaDocModel>: Sendable {
    public let engine: ReplicaEngine

    public init(engine: ReplicaEngine) {
        self.engine = engine
    }

    public func fold(id: String) throws -> Data? {
        try engine.docFold(stream: Model.streamName, id: id)
    }

    public func find(_ id: String) throws -> Model? {
        try ReplicaReads.find(engine.store, stream: Model.streamName, id: id) { id, _, data in
            Model(id: id, data: data)
        }
    }

    public func list(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil
    ) throws -> [Model] {
        try ReplicaReads.list(engine.store, stream: Model.streamName, predicate: predicate, order: order, limit: limit) { id, _, data in
            Model(id: id, data: data)
        }
    }

    public func watch(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil,
        includeInitial: Bool = false,
        _ deliver: @escaping @MainActor @Sendable ([Model]) -> Void
    ) -> ReplicaWatch where Model: Equatable {
        ReplicaReads.watch(
            engine.binding, health: engine.health, stream: Model.streamName, predicate: predicate, order: order, limit: limit,
            includeInitial: includeInitial, decode: { id, _, data in Model(id: id, data: data) }, deliver: deliver
        )
    }
}

public struct DocumentStream<Model: ReplicaDocModel>: Sendable {
    public let engine: ReplicaEngine

    public init(engine: ReplicaEngine) {
        self.engine = engine
    }

    /// Birth the document: `row.create` carrying codec + seed (creation is
    /// gated server-side; the row exists only after the verdict). `peer` is
    /// the loro peer the seed was authored under — the engine records it and
    /// rotates it if the fold is ever lost.
    @discardableResult
    public func create(
        id: String,
        seed: Data,
        peer: UInt64,
        data: [String: ReplicaValue] = [:]
    ) async throws -> Bool {
        try await engine.createDoc(stream: Model.streamName, id: id, seed: seed, peer: peer, data: data)
    }

    public func delete(id: String) async throws {
        _ = try await engine.deleteRow(stream: Model.streamName, id: id)
    }

    /// Fold a local edit in: merged into the fold, superseded into the ONE
    /// pending `doc.delta` for this document.
    public func delta(id: String, payload: Data) async throws {
        try await engine.recordDocDelta(stream: Model.streamName, id: id, payload: payload)
    }

    // MARK: - The document's own doors (the rows' law: sync first read, then watch)

    /// Birth — the seed is the whole document; a present id is refused
    /// (false), like `create` on a row.
    @discardableResult
    public func createDoc(id: String, seed: Data, peer: UInt64, data: [String: ReplicaValue] = [:]) async throws -> Bool {
        try await create(id: id, seed: seed, peer: peer, data: data)
    }

    /// The document's state, read where the caller stands — SYNC. Nil
    /// when the store holds no document under `id`.
    public func findDoc(_ id: String) throws -> Model.State? {
        try engine.documentState(stream: Model.streamName, id: id, as: Model.State.self)
    }

    /// The documents' stored fingerprints (fold length + acked version) —
    /// what a memo over closed documents keys on, one query for all ids.
    public func docFingerprints(_ ids: [String]) throws -> [String: Data] {
        try engine.documentFingerprints(stream: Model.streamName, ids: ids)
    }

    /// The state of a HELD document, nil otherwise — never opens one. A
    /// reader over many documents asks this first and `findDoc` only what
    /// it must open.
    public func heldDoc(_ id: String) throws -> Model.State? {
        try engine.documentHeldState(stream: Model.streamName, id: id, as: Model.State.self)
    }

    /// A read of the held document — SYNC; nil when the store holds none
    /// under `id`. The body must not move it.
    public func readDoc<T>(_ id: String, _ body: (Model.State.Codec.Document) throws -> T) throws -> T? {
        try engine.readDocument(stream: Model.streamName, id: id, codec: Model.State.Codec.self, body)
    }

    /// Changes of the document's state — local edits and pulled deltas
    /// through the one door. `includeInitial` opts into the baseline.
    public func watchDoc(
        _ id: String,
        includeInitial: Bool = false,
        _ deliver: @escaping @MainActor @Sendable (Model.State?) -> Void
    ) -> ReplicaWatch {
        let engine = engine
        let stream = Model.streamName
        return ReplicaReads.watchDocument(
            engine.binding, health: engine.health, stream: stream, includeInitial: includeInitial,
            read: { _ in try engine.documentState(stream: stream, id: id, as: Model.State.self) },
            deliver: deliver
        )
    }

    /// A local edit: `body` moves the held document, the delta is
    /// journaled, `watchDoc` delivers. False when nothing moved; throws
    /// `unknownDocument` for an id the store does not hold.
    @discardableResult
    public func updateDoc(
        _ id: String,
        _ body: @Sendable (Model.State.Codec.Document) throws -> Void
    ) async throws -> Bool {
        try await engine.updateDocument(stream: Model.streamName, id: id, codec: Model.State.Codec.self, body)
    }

    @discardableResult
    public func undoDoc(_ id: String) async throws -> Bool {
        try await engine.undoDocument(stream: Model.streamName, id: id, codec: Model.State.Codec.self)
    }

    @discardableResult
    public func redoDoc(_ id: String) async throws -> Bool {
        try await engine.redoDocument(stream: Model.streamName, id: id, codec: Model.State.Codec.self)
    }

    /// A session's hold: the document stays held while pinned.
    public func pinDoc(_ id: String) {
        engine.pinDocument(stream: Model.streamName, id: id)
    }

    public func unpinDoc(_ id: String) {
        engine.unpinDocument(stream: Model.streamName, id: id)
    }

    public func fold(id: String) throws -> Data? {
        try engine.docFold(stream: Model.streamName, id: id)
    }

    public func peer(id: String) throws -> UInt64? {
        try engine.docPeer(stream: Model.streamName, id: id)
    }

    public func find(_ id: String) throws -> Model? {
        try ReplicaReads.find(engine.store, stream: Model.streamName, id: id) { id, _, data in
            Model(id: id, data: data)
        }
    }

    public func list(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil
    ) throws -> [Model] {
        try ReplicaReads.list(engine.store, stream: Model.streamName, predicate: predicate, order: order, limit: limit) { id, _, data in
            Model(id: id, data: data)
        }
    }

    public func watch(
        _ predicate: ReplicaPredicate<Model.Field>? = nil,
        order: [ReplicaOrder<Model.Field>] = [],
        limit: Int? = nil,
        includeInitial: Bool = false,
        _ deliver: @escaping @MainActor @Sendable ([Model]) -> Void
    ) -> ReplicaWatch where Model: Equatable {
        ReplicaReads.watch(
            engine.binding, health: engine.health, stream: Model.streamName, predicate: predicate, order: order, limit: limit,
            includeInitial: includeInitial, decode: { id, _, data in Model(id: id, data: data) }, deliver: deliver
        )
    }
}

/// A draft's handle — the key its held journal entries carry (`ReplicaEngine.beginDraft`).
public struct ReplicaDraft: Sendable, Hashable {
    public let key: String

    public init(key: String) {
        self.key = key
    }
}
