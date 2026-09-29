import Foundation
import GRDB

/// The live half of a document codec. `ReplicaCodec` merges FOLDS — bytes
/// the store keeps; this opens a fold as a document the engine holds,
/// reads a state off it, edits it, and hands back the delta the fold lane
/// journals. Rows-only consumers never see it.
public protocol DocumentCodec: ReplicaCodec {
    associatedtype Document: AnyObject

    /// The wire name the schema registers the codec under (`loro@1`).
    static var codecName: String { get }

    /// A document from a fold (nil = a blank one), authored as `peer`.
    func open(fold: Data?, peer: UInt64) throws -> Document
    func snapshot(_ document: Document) throws -> Data
    /// The document's own version, encoded — the cursor every state and
    /// delta is stamped with.
    func version(_ document: Document) -> Data
    func exportDelta(_ document: Document, since version: Data) throws -> Data
    func importDeltas(_ document: Document, _ payloads: [Data]) throws
    func peer(of document: Document) -> UInt64
    func canUndo(_ document: Document) -> Bool
    func canRedo(_ document: Document) -> Bool
    /// False when there was no step to take; a step the document refuses throws.
    func undo(_ document: Document) throws -> Bool
    func redo(_ document: Document) throws -> Bool
    /// `value` at `path`, the maps on the way made when absent.
    func write(_ value: ReplicaValue, at path: [String], in document: Document) throws
}

/// What a consumer READS of a document: a value minted from the held
/// document at a version. The generated document model names its state
/// (`Project.State`); the engine mints one per version and hands the same
/// value out until the document moves.
public protocol ReplicaDocState: Sendable, Equatable {
    associatedtype Codec: DocumentCodec
    static func state(of document: Codec.Document, version: Data, canUndo: Bool, canRedo: Bool) -> Self
}

/// The default for a document model that reads no state.
public struct NoDocState: ReplicaDocState {
    public typealias Codec = NoDocumentCodec
    public init() {}
    public static func state(of document: NoDocumentCodec.Document, version: Data, canUndo: Bool, canRedo: Bool) -> NoDocState {
        NoDocState()
    }
}

/// The codec behind `NoDocState` — never registered, never opened.
public struct NoDocumentCodec: DocumentCodec {
    public final class Document {}
    public static let codecName = "none"
    public var name: String { Self.codecName }
    public init() {}
    public func merge(fold: Data?, payload: Data, reflecting reflections: [ReplicaReflection]) throws -> ReplicaMerge {
        throw ReplicaError.codec("no document codec")
    }
    public func diff(fold: Data, since version: Data?) throws -> Data { throw ReplicaError.codec("no document codec") }
    public func version(fold: Data) throws -> Data { throw ReplicaError.codec("no document codec") }
    public func payloadVersion(_ payload: Data) throws -> Data { throw ReplicaError.codec("no document codec") }
    public func mergeVersions(_ a: Data?, _ b: Data) throws -> Data { throw ReplicaError.codec("no document codec") }
    public func isEmptyDiff(_ payload: Data) -> Bool { true }
    public func open(fold: Data?, peer: UInt64) throws -> Document { throw ReplicaError.codec("no document codec") }
    public func snapshot(_ document: Document) throws -> Data { throw ReplicaError.codec("no document codec") }
    public func version(_ document: Document) -> Data { Data() }
    public func exportDelta(_ document: Document, since version: Data) throws -> Data { throw ReplicaError.codec("no document codec") }
    public func importDeltas(_ document: Document, _ payloads: [Data]) throws {}
    public func peer(of document: Document) -> UInt64 { 0 }
    public func canUndo(_ document: Document) -> Bool { false }
    public func canRedo(_ document: Document) -> Bool { false }
    public func undo(_ document: Document) -> Bool { false }
    public func redo(_ document: Document) -> Bool { false }
    public func write(_ value: ReplicaValue, at path: [String], in document: Document) throws {
        throw ReplicaError.codec("no document codec")
    }
}

extension ReplicaEngine {
    /// The document's stored row — fold, peer, codec, acked — read where
    /// the caller stands, like `docFold`.
    public nonisolated func docRow(stream: String, id: String) throws -> ReplicaStateStore.DocRow? {
        guard let store else { return nil }
        return try store.pool.read { db in try store.doc(db, stream: stream, rowId: id) }
    }

    /// The documents' stored fingerprints — fold length + acked version —
    /// for a reader that memoizes over many documents and must notice one
    /// moving without opening any. Documents the store does not hold are
    /// absent from the result.
    public nonisolated func documentFingerprints(stream: String, ids: [String]) throws -> [String: Data] {
        guard let store else { return [:] }
        return try store.pool.read { db in try store.docFingerprints(db, stream: stream, rowIds: ids) }
    }

    nonisolated func documentCodec<C: DocumentCodec>(_ type: C.Type) throws -> C {
        guard let codec = codecs[C.codecName] as? C else {
            throw ReplicaError.codec("no document codec registered as \(C.codecName)")
        }
        return codec
    }

    /// The document's state, read where the caller stands — SYNC, the
    /// rows' law: the held document's state, minted once per version; a
    /// document not held yet opens from its fold here. Nil when the store
    /// holds no document under `id`.
    public nonisolated func documentState<S: ReplicaDocState>(stream: String, id: String, as type: S.Type) throws -> S? {
        let codec = try documentCodec(S.Codec.self)
        return try liveDocuments.publishing {
            guard let row = try docRow(stream: stream, id: id) else { return nil }
            return try liveDocuments.state(LiveDocuments.Key(stream: stream, id: id), codec: codec) {
                try openDocument(codec, row: row, stream: stream, id: id)
            }
        }
    }

    /// The state of a document the engine already HOLDS — nil when it is
    /// not held, and the document is left closed. The reader over many
    /// documents (a grid) asks this way: a warm document answers, a cold
    /// one costs nothing.
    public nonisolated func documentHeldState<S: ReplicaDocState>(stream: String, id: String, as type: S.Type) throws -> S? {
        let codec = try documentCodec(S.Codec.self)
        return try liveDocuments.heldState(LiveDocuments.Key(stream: stream, id: id), codec: codec)
    }

    /// A corrupt fold is an error. Reading never substitutes empty history or
    /// schedules a later write against a potentially different owner.
    private nonisolated func openDocument<C: DocumentCodec>(
        _ codec: C, row: ReplicaStateStore.DocRow, stream: String, id: String
    ) throws -> C.Document {
        try codec.open(fold: row.fold, peer: row.peer)
    }

    /// A local edit on the held document: `body` moves it, the movement
    /// since before is the delta the fold lane journals (`recordDocDelta`),
    /// the stream's change sequence bumps and every `watchDoc` re-reads.
    /// False when the body moved nothing.
    public func updateDocument<C: DocumentCodec>(
        stream: String, id: String, codec type: C.Type, _ body: (C.Document) throws -> Void
    ) throws -> Bool {
        _ = try writableStore()
        let codec = try documentCodec(C.self)
        guard let row = try docRow(stream: stream, id: id) else {
            throw ReplicaError.unknownDocument(stream: stream, id: id)
        }
        let key = LiveDocuments.Key(stream: stream, id: id)
        return try liveDocuments.with(key, codec: codec, open: {
            try openDocument(codec, row: row, stream: stream, id: id)
        }) { document, held in
            do {
                let before = codec.version(document)
                try body(document)
                let after = codec.version(document)
                guard after != before else { return false }
                let delta = try codec.exportDelta(document, since: before)
                try recordDocDelta(stream: stream, id: id, payload: delta)
                held.version = after
                held.state = nil
                return true
            } catch {
                liveDocuments.evict(key)
                throw error
            }
        }
    }

    /// A read of a detached durable snapshot, including past versions. A read
    /// cannot mutate the engine's authoring object, even if its handle escapes.
    /// Use documentState for the cached current projection.
    public nonisolated func readDocument<C: DocumentCodec, T>(
        stream: String, id: String, codec type: C.Type, _ body: (C.Document) throws -> T
    ) throws -> T? {
        let codec = try documentCodec(C.self)
        guard let row = try docRow(stream: stream, id: id) else { return nil }
        let document = try codec.open(fold: row.fold, peer: peerMinter())
        let before = codec.version(document)
        let result = try body(document)
        guard codec.version(document) == before else {
            throw ReplicaError.codec("a read of \(stream)/\(id) moved the document")
        }
        return result
    }

    /// A session's hold on a document: pinned, it never leaves the LRU.
    #if DEBUG
    /// Test seam: drop a held copy so the next read opens from the fold —
    /// what the LRU does to an unpinned document under pressure.
    public nonisolated func evictDocumentForTesting(stream: String, id: String) {
        liveDocuments.evict(LiveDocuments.Key(stream: stream, id: id))
    }
    #endif

    public nonisolated func pinDocument(stream: String, id: String) {
        liveDocuments.pin(LiveDocuments.Key(stream: stream, id: id))
    }

    public nonisolated func unpinDocument(stream: String, id: String) {
        liveDocuments.unpin(LiveDocuments.Key(stream: stream, id: id))
    }

    public func undoDocument<C: DocumentCodec>(stream: String, id: String, codec type: C.Type) throws -> Bool {
        try updateDocument(stream: stream, id: id, codec: type) { document in
            let codec = try documentCodec(C.self)
            _ = try codec.undo(document)
        }
    }

    public func redoDocument<C: DocumentCodec>(stream: String, id: String, codec type: C.Type) throws -> Bool {
        try updateDocument(stream: stream, id: id, codec: type) { document in
            let codec = try documentCodec(C.self)
            _ = try codec.redo(document)
        }
    }
}

extension ReplicaReads {
    /// The document's live state: the sync `findDoc` gave the first
    /// picture, the watch delivers CHANGES — every commit of the stream
    /// re-reads the state off-main and delivers on the main actor when it
    /// differs from the last delivered. Local edits and pulled deltas both
    /// bump the sequence, so both arrive through this one door.
    static func watchDocument<State: ReplicaDocState>(
        _ binding: ReplicaBinding, health: ReplicaHealth, stream: String, includeInitial: Bool,
        read: @escaping @Sendable (ReplicaStateStore) throws -> State?,
        deliver: @escaping @MainActor @Sendable (State?) -> Void
    ) -> ReplicaWatch {
        let last = LastDocState<State>()
        let task = Task {
            var armed = false
            while !Task.isCancelled {
                let (bound, generation) = binding.snapshot()
                guard let store = bound?.store else {
                    let wasArmed = armed
                    await MainActor.run {
                        guard !Task.isCancelled, binding.snapshot().generation == generation else { return }
                        guard (includeInitial && !wasArmed) || (wasArmed && last.value != nil) else { return }
                        last.value = nil
                        deliver(nil)
                    }
                    armed = true
                    await binding.waitForChange(after: generation)
                    continue
                }
                let observation = ValueObservation
                    .tracking { db in try store.changeSequence(db, stream: stream) }
                    .removeDuplicates()
                    .map { _ in try read(store) }
                let wasArmed = armed
                armed = true
                await withTaskGroup(of: Void.self) { group in
                    group.addTask {
                        var baseline = true
                        do {
                            for try await state in observation.values(in: store.pool) {
                                let isBaseline = baseline
                                baseline = false
                                await MainActor.run {
                                    guard !Task.isCancelled, binding.snapshot().generation == generation else { return }
                                    if isBaseline, !wasArmed, !includeInitial {
                                        last.value = state
                                        return
                                    }
                                    guard last.value != state else { return }
                                    last.value = state
                                    deliver(state)
                                }
                            }
                        } catch {
                            // A canceled observation belongs to the outgoing
                            // store. Report any failure while this store is active.
                            if !Task.isCancelled, binding.snapshot().generation == generation {
                                health.record(error, operation: "observe document \(stream)")
                            }
                            Log.logger.info("[watch] stream=\(stream, privacy: .public) document observation ended — waiting for the next owner")
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
}

private final class LastDocState<State: ReplicaDocState>: @unchecked Sendable {
    private let lock = NSLock()
    private var stored: State?
    var value: State? {
        get { lock.withLock { stored } }
        set { lock.withLock { stored = newValue } }
    }
}
