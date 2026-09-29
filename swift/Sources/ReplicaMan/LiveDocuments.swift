import Foundation

/// The documents the engine HOLDS: the codec's live object per (stream, id)
/// with its state minted once per version — the rows' warm law for
/// documents (an unmoved document decodes nothing). A held document is
/// single-threaded by nature, so every touch — open, read, edit, absorb —
/// runs under the one lock, and a read never sees a half-edit.
///
/// The fold in the store stays the durable truth: a pulled payload merges
/// into the fold inside the pull's transaction and reaches the held copy
/// right after commit (`absorb`); a held copy that cannot take a payload is
/// dropped, and the next open reads the fold. Unpinned documents live in an
/// LRU of `capacity`; a session pins the one it edits — a declaration on
/// the key, so a pin placed before the open holds the document once it is.
///
/// Lock order: this lock BEFORE the store's writer, never inside a write — a
/// write that waits here while a holder of this lock waits on the writer (or
/// on a reader the writer's observers keep pinned) freezes the process.
final class LiveDocuments: @unchecked Sendable {
    struct Key: Hashable, Sendable {
        let stream: String
        let id: String
    }

    final class Held {
        let codecName: String
        let document: AnyObject
        let currentVersion: () -> Data
        let importing: ([Data]) throws -> Void
        var version: Data
        var state: (type: ObjectIdentifier, value: any Sendable)?
        var lastUse: UInt64 = 0

        init(codecName: String, document: AnyObject, version: Data,
             currentVersion: @escaping () -> Data, importing: @escaping ([Data]) throws -> Void) {
            self.codecName = codecName
            self.document = document
            self.version = version
            self.currentVersion = currentVersion
            self.importing = importing
        }
    }

    private let lock = NSRecursiveLock()
    private var held: [Key: Held] = [:]
    private var pins: [Key: Int] = [:]
    private var tick: UInt64 = 0
    let capacity: Int

    init(capacity: Int = 32) {
        self.capacity = capacity
    }

    /// Keep a durable checkpoint and its held copies one publication.
    /// Recursive because the guarded operation uses the ordinary cache doors.
    func publishing<T>(_ body: () throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body()
    }

    /// `body` on the held document, opened through `open` when it is not
    /// held yet (or held under another codec).
    func with<C: DocumentCodec, T>(
        _ key: Key, codec: C, open: () throws -> C.Document, _ body: (C.Document, Held) throws -> T
    ) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        let entry: Held
        tick &+= 1
        if let existing = held[key], existing.codecName == C.codecName, existing.document is C.Document {
            try validate(existing, at: key)
            entry = existing
            entry.lastUse = tick
        } else {
            let document = try open()
            entry = Held(
                codecName: C.codecName, document: document, version: codec.version(document),
                currentVersion: { codec.version(document) },
                importing: { try codec.importDeltas(document, $0) }
            )
            entry.lastUse = tick
            held[key] = entry
            evictOverflow()
        }
        guard let document = entry.document as? C.Document else {
            throw ReplicaError.codec("held document under \(key.stream)/\(key.id) is not \(C.codecName)")
        }
        return try body(document, entry)
    }

    /// The state at the document's current version — the memo when the
    /// document has not moved, minted otherwise.
    func state<S: ReplicaDocState>(_ key: Key, codec: S.Codec, open: () throws -> S.Codec.Document) throws -> S {
        try with(key, codec: codec, open: open) { document, entry in
            state(of: document, entry, codec: codec)
        }
    }

    /// The state of a HELD document — nil when the document is not held.
    /// Never opens one: a peek, for a reader that must not pay for a
    /// document it does not need (the grid over every listed project).
    func heldState<S: ReplicaDocState>(_ key: Key, codec: S.Codec) throws -> S? {
        lock.lock()
        defer { lock.unlock() }
        guard let entry = held[key], entry.codecName == S.Codec.codecName,
              let document = entry.document as? S.Codec.Document
        else { return nil }
        try validate(entry, at: key)
        tick &+= 1
        entry.lastUse = tick
        return state(of: document, entry, codec: codec)
    }

    /// An escaped edit handle may not become an unjournaled source of truth.
    private func validate(_ entry: Held, at key: Key) throws {
        guard entry.currentVersion() == entry.version else {
            held[key] = nil
            throw ReplicaError.codec("Document was mutated outside its edit transaction")
        }
    }

    /// lock held.
    private func state<S: ReplicaDocState>(of document: S.Codec.Document, _ entry: Held, codec: S.Codec) -> S {
        let version = codec.version(document)
        if let memo = entry.state, memo.type == ObjectIdentifier(S.self), entry.version == version,
           let value = memo.value as? S {
            return value
        }
        let state = S.state(
            of: document, version: version,
            canUndo: codec.canUndo(document), canRedo: codec.canRedo(document)
        )
        entry.version = version
        entry.state = (ObjectIdentifier(S.self), state)
        return state
    }

    /// A pulled payload into the held copy, if there is one. A copy that
    /// cannot take it is dropped — the fold already merged it, the next open
    /// reads that.
    func absorb(_ key: Key, payloads: [Data]) {
        lock.lock()
        defer { lock.unlock() }
        guard let entry = held[key] else { return }
        do {
            try validate(entry, at: key)
            try entry.importing(payloads)
            entry.version = entry.currentVersion()
            entry.state = nil
        } catch {
            Log.logger.error("[docs] \(key.stream, privacy: .public)/\(key.id, privacy: .public) held copy refused a pulled payload — dropped, the fold stands: \(error.localizedDescription, privacy: .public)")
            held[key] = nil
        }
    }

    func evict(_ key: Key) {
        lock.lock()
        defer { lock.unlock() }
        held[key] = nil
    }

    func evictAll() {
        lock.lock()
        defer { lock.unlock() }
        held.removeAll()
    }

    func evictAll(except kept: Set<Key>) {
        lock.lock()
        defer { lock.unlock() }
        held = held.filter { kept.contains($0.key) }
    }

    /// A session's hold: pinned keys never leave the LRU.
    func pin(_ key: Key) {
        lock.lock()
        defer { lock.unlock() }
        pins[key, default: 0] += 1
    }

    func unpin(_ key: Key) {
        lock.lock()
        defer { lock.unlock() }
        guard let count = pins[key] else { return }
        pins[key] = count > 1 ? count - 1 : nil
        evictOverflow()
    }

    var heldCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return held.count
    }

    /// lock held.
    private func evictOverflow() {
        var unpinned = held.filter { pins[$0.key] == nil }.sorted { $0.value.lastUse < $1.value.lastUse }
        while unpinned.count > capacity, let oldest = unpinned.first {
            held[oldest.key] = nil
            unpinned.removeFirst()
        }
    }
}
