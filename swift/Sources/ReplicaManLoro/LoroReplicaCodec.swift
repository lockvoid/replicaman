import Foundation
import Loro
import ReplicaMan

/// The loro codec plugin — the ONLY target in the package that imports Loro
/// (module-graph boundary). Same crate pin as the
/// server's `vendor/loro-ruby`; the cross-platform golden fixtures are what
/// prove the bindings agree.
///
/// Folds and payloads stay opaque `Data` at the seam: every operation loads,
/// works, exports. Loro PARKS an update whose causal deps are missing
/// instead of failing — unguarded that reports success and silently never
/// applies the edit, so `merge` refuses it (`missingCausalDeps`), matching
/// the server codec's refusal.
public struct LoroReplicaCodec: ReplicaCodec {
    public static let codecName = "loro@1"

    public var name: String { Self.codecName }

    public init() {}

    public func merge(fold: Data?, payload: Data, reflecting reflections: [ReplicaReflection]) throws -> ReplicaMerge {
        let doc = try load(fold)
        let status: ImportStatus
        do {
            status = try doc.import(bytes: payload)
        } catch {
            throw ReplicaError.codec("import failed: \(error)")
        }
        if let pending = status.pending, !pending.isEmpty {
            throw ReplicaError.missingCausalDeps
        }
        return ReplicaMerge(
            fold: try export(doc),
            reflected: Dictionary(uniqueKeysWithValues: reflections.map { ($0.field, value(at: $0.path, in: doc)) })
        )
    }

    public func diff(fold: Data, since version: Data?) throws -> Data {
        let doc = try load(fold)
        let from = try version.flatMap(Self.versionVector) ?? VersionVector()
        do {
            return try doc.export(mode: .updates(from: from))
        } catch {
            throw ReplicaError.codec("export updates failed: \(error)")
        }
    }

    public func version(fold: Data) throws -> Data {
        try load(fold).oplogVv().encode()
    }

    public func payloadVersion(_ payload: Data) throws -> Data {
        do {
            return try decodeImportBlobMeta(bytes: payload, checkChecksum: false).partialEndVv.encode()
        } catch {
            throw ReplicaError.codec("blob meta unreadable: \(error)")
        }
    }

    public func mergeVersions(_ a: Data?, _ b: Data) throws -> Data {
        let merged = try a.flatMap(Self.versionVector) ?? VersionVector()
        do {
            merged.extendToIncludeVv(other: try VersionVector.decode(bytes: b))
        } catch {
            throw ReplicaError.codec("version vector unreadable: \(error)")
        }
        return merged.encode()
    }

    public func isEmptyDiff(_ payload: Data) -> Bool {
        do {
            return try decodeImportBlobMeta(bytes: payload, checkChecksum: false).changeNum == 0
        } catch {
            ReplicaMan.logger.error("[loro] a payload's meta will not read — judged by its size: \(String(describing: error), privacy: .public)")
            return payload.isEmpty
        }
    }

    /// A version vector the codec wrote, or nil — logged — for bytes that
    /// will not decode; the caller then works from the empty vector, which
    /// costs a longer diff and never a lost edit.
    static func versionVector(_ bytes: Data) throws -> VersionVector? {
        do {
            return try VersionVector.decode(bytes: bytes)
        } catch LoroError.DecodeVersionVectorError {
            ReplicaMan.logger.error("[loro] a version vector will not decode — diffing from empty: invalid encoded vector")
            return nil
        }
    }

    // MARK: - Plumbing

    private func load(_ fold: Data?) throws -> LoroDoc {
        let doc = LoroDoc()
        // Merge order is peer/lamport, never a wall clock; recording
        // timestamps would also make fold bytes non-deterministic.
        doc.setRecordTimestamp(record: false)
        guard let fold, !fold.isEmpty else { return doc }
        do {
            let status = try doc.import(bytes: fold)
            if let pending = status.pending, !pending.isEmpty {
                throw ReplicaError.missingCausalDeps
            }
        } catch let error as ReplicaError {
            throw error
        } catch {
            throw ReplicaError.codec("fold unreadable: \(error)")
        }
        return doc
    }

    private func export(_ doc: LoroDoc) throws -> Data {
        do {
            return try doc.export(mode: .snapshot)
        } catch {
            throw ReplicaError.codec("export snapshot failed: \(error)")
        }
    }

    /// The value at `path`: a root map, the maps under it, the key last.
    private func value(at path: [String], in doc: LoroDoc) -> ReplicaValue {
        guard let root = path.first, let key = path.last else { return .null }
        var map = doc.getMap(id: root)
        for name in path.dropFirst().dropLast() {
            guard let child = map.get(key: name)?.asLoroMap() else { return .null }
            map = child
        }
        return map.get(key: key)?.asValue().map { ReplicaValue(loro: $0).row } ?? .null
    }
}

extension ReplicaValue {
    /// A document value read as the document holds it: `i64` is `.integer`,
    /// `double` is `.number` — the server's binding keeps the two apart too.
    init(loro value: LoroValue) {
        switch value {
        case .null, .binary, .container: self = .null
        case .bool(let flag): self = .bool(flag)
        case .double(let number): self = .number(number)
        case .i64(let number): self = .integer(number)
        case .string(let text): self = .string(text)
        case .list(let items): self = .array(items.map(ReplicaValue.init(loro:)))
        case .map(let fields): self = .object(fields.mapValues(ReplicaValue.init(loro:)))
        }
    }

    /// The same value in a row's vocabulary: the JSON wire has one number.
    var row: ReplicaValue {
        switch self {
        case .integer(let integer): .signedInteger(integer)
        case .array(let items): .array(items.map(\.row))
        case .object(let fields): .object(fields.mapValues(\.row))
        case .string, .number, .bool, .null: self
        }
    }

    var loro: LoroValue {
        switch self {
        case .null: .null
        case .bool(let flag): .bool(value: flag)
        case .number(let number): .double(value: number)
        case .integer(let integer): .i64(value: integer)
        case .string(let text): .string(value: text)
        case .array(let items): .list(value: items.map(\.loro))
        case .object(let fields): .map(value: fields.mapValues(\.loro))
        }
    }
}

/// A path the document cannot write through: a key on the way holds a plain
/// value where a map would be — a peer put it there, since the adapter only
/// ever makes mergeable maps on a path.
public enum LoroDocumentError: Error, Equatable {
    case notAMap(path: [String])
}

/// The document the engine holds for `loro@1`: the loro doc plus its
/// collaborative undo — Loro's own manager, which inverts only THIS peer's
/// ops and rebases the inversion over whatever arrived meanwhile.
public final class LoroDocument {
    let doc: LoroDoc
    let undo: Loro.UndoManager

    /// The peer this document authors as.
    public var peer: UInt64 { doc.peerId() }

    /// The whole document, materialized — every root map by name.
    public var value: ReplicaValue { ReplicaValue(loro: doc.getDeepValue()) }

    /// This document's place in its own history — the address a version row
    /// keeps. Committed first, so the value names everything written so far.
    public var frontiers: Data {
        doc.commit()
        return doc.oplogFrontiers().encode()
    }

    /// Back to the state `frontiers` names, as NEW ops: the history stays
    /// linear and the revert syncs like any other edit. A frontiers value
    /// this document has never seen is a `ReplicaError.codec`.
    public func revert(to frontiers: Data) throws {
        doc.commit()
        do {
            try doc.revertTo(version: try Frontiers.decode(bytes: frontiers))
        } catch {
            throw ReplicaError.codec("revert failed: \(error)")
        }
    }

    /// The first of `addresses` the document reads the same as now. By what
    /// a reader reaches, not by op diff: a revert lands the old state under
    /// a new address, and containers it detached still exist in the history
    /// though nothing reaches them. The present is materialized once however
    /// many addresses are asked, and standing ON an address needs no fork.
    public func firstMatch(among addresses: [Data]) throws -> Int? {
        let here = frontiers
        let now = doc.getDeepValue()
        return try addresses.firstIndex { address in
            if address == here { return true }
            return try fork(at: address)?.getDeepValue() == now
        }
    }

    /// Nil means a valid point outside retained history. Malformed addresses
    /// and unexpected native failures throw without changing this document.
    public func value(at frontiers: Data) throws -> ReplicaValue? {
        try fork(at: frontiers).map { ReplicaValue(loro: $0.getDeepValue()) }
    }

    private func fork(at frontiers: Data) throws -> LoroDoc? {
        doc.commit()
        let point = try Frontiers.decode(bytes: frontiers)
        guard doc.frontiersToVv(frontiers: point) != nil else { return nil }
        do {
            return try doc.forkAt(frontiers: point)
        } catch LoroError.FrontiersNotFound, LoroError.SwitchToVersionBeforeShallowRoot {
            // A valid version can belong to another branch or pruned history.
            // Those two native outcomes mean absence; all other failures escape.
            return nil
        }
    }

    public func differingRoots(from frontiers: Data) throws -> [String]? {
        guard let past = try fork(at: frontiers) else { return nil }
        guard case let .map(now) = doc.getDeepValue(), case let .map(was) = past.getDeepValue() else {
            return past.getDeepValue() == doc.getDeepValue() ? [] : ["(root)"]
        }
        return Set(now.keys).union(was.keys).filter { now[$0] != was[$0] }.sorted()
    }

    // MARK: - Writing

    /// `value` at `path`: a root map, the maps on the way, the key last. The
    /// maps on the way are MERGEABLE children — every peer makes the same one
    /// for the same key, so two first writes of one entry merge field-wise
    /// where an op-id child would fork and drop one peer's whole entry. A value
    /// the key already holds, or a null where the key never was, writes
    /// nothing. The edit holding the write commits it: one edit, one undo step.
    public func write(_ value: ReplicaValue, at path: [String]) throws {
        let (map, key) = try slot(path)
        let current = map.get(key: key)
        if value == .null, current == nil { return }
        if let held = current?.asValue(), ReplicaValue(loro: held) == value { return }
        do {
            try map.insert(key: key, v: value.loro)
        } catch {
            throw ReplicaError.codec("write failed: \(error)")
        }
    }

    /// A birth's write: the value as given, a null included — a born document
    /// names every field it has.
    public func seed(_ value: ReplicaValue, at path: [String]) throws {
        let (map, key) = try slot(path)
        do {
            try map.insert(key: key, v: value.loro)
        } catch {
            throw ReplicaError.codec("seed failed: \(error)")
        }
    }

    /// The keys of the map at `path` — a root map, the maps under it — or none
    /// when there is no map there. Reads without making anything.
    public func keys(at path: [String]) -> [String] {
        guard let root = path.first else { return [] }
        var map = doc.getMap(id: root)
        for name in path.dropFirst() {
            guard let child = map.get(key: name)?.asLoroMap() else { return [] }
            map = child
        }
        return map.keys()
    }

    /// The key at the end of `path`, gone.
    public func delete(at path: [String]) throws {
        let (map, key) = try slot(path)
        do {
            try map.delete(key: key)
        } catch {
            throw ReplicaError.codec("delete failed: \(error)")
        }
    }

    /// The writes `body` makes are authored by `peer` — undo then inverts only
    /// this document's own peer's edits. Loro commits what is pending before
    /// the switch, so no edit straddles two peers.
    public func asPeer(_ peer: UInt64, _ body: () throws -> Void) throws {
        let own = doc.peerId()
        do {
            try doc.setPeerId(peer: peer)
            try body()
            try doc.setPeerId(peer: own)
        } catch {
            do {
                try doc.setPeerId(peer: own)
            } catch let restore {
                throw ReplicaError.codec("peer \(own) could not be restored after \(error): \(restore)")
            }
            throw error
        }
    }

    private func slot(_ path: [String]) throws -> (map: LoroMap, key: String) {
        guard path.count >= 2, let root = path.first, let key = path.last else {
            throw ReplicaError.codec("a document path names a root map and a key: \(path)")
        }
        var map = doc.getMap(id: root)
        for (depth, name) in path.dropFirst().dropLast().enumerated() {
            do {
                map = try map.ensureMergeableMap(key: name)
            } catch LoroError.ArgErr {
                throw LoroDocumentError.notAMap(path: Array(path.prefix(depth + 2)))
            } catch {
                throw ReplicaError.codec("path \(path) failed at \(name): \(error)")
            }
        }
        return (map, key)
    }

    init(doc: LoroDoc) {
        self.doc = doc
        let manager = Loro.UndoManager(doc: doc)
        // One undo step per commit: a commit IS the boundary of one user
        // action, so coalescing by time would merge two distinct actions.
        manager.setMergeInterval(interval: 0)
        manager.setMaxUndoSteps(size: 100)
        undo = manager
    }
}

extension LoroReplicaCodec: DocumentCodec {
    public typealias Document = LoroDocument

    public func open(fold: Data?, peer: UInt64) throws -> LoroDocument {
        let doc = try load(fold)
        try doc.setPeerId(peer: peer)
        return LoroDocument(doc: doc)
    }

    public func snapshot(_ document: LoroDocument) throws -> Data {
        document.doc.commit()
        return try export(document.doc)
    }

    public func version(_ document: LoroDocument) -> Data {
        document.doc.commit()
        return document.doc.oplogVv().encode()
    }

    public func exportDelta(_ document: LoroDocument, since version: Data) throws -> Data {
        let from = try Self.versionVector(version) ?? VersionVector()
        do {
            return try document.doc.export(mode: .updates(from: from))
        } catch {
            throw ReplicaError.codec("export updates failed: \(error)")
        }
    }

    public func importDeltas(_ document: LoroDocument, _ payloads: [Data]) throws {
        let blobs = payloads.filter { !$0.isEmpty }
        guard !blobs.isEmpty else { return }
        let status: ImportStatus
        do {
            status = try document.doc.importBatch(bytes: blobs)
        } catch {
            throw ReplicaError.codec("import failed: \(error)")
        }
        if let pending = status.pending, !pending.isEmpty {
            throw ReplicaError.missingCausalDeps
        }
    }

    public func peer(of document: LoroDocument) -> UInt64 {
        document.doc.peerId()
    }

    public func canUndo(_ document: LoroDocument) -> Bool { document.undo.canUndo() }
    public func canRedo(_ document: LoroDocument) -> Bool { document.undo.canRedo() }

    public func undo(_ document: LoroDocument) throws -> Bool {
        document.doc.commit()
        do {
            return try document.undo.undo()
        } catch {
            throw ReplicaError.codec("undo failed: \(error)")
        }
    }

    public func redo(_ document: LoroDocument) throws -> Bool {
        document.doc.commit()
        do {
            return try document.undo.redo()
        } catch {
            throw ReplicaError.codec("redo failed: \(error)")
        }
    }

    public func write(_ value: ReplicaValue, at path: [String], in document: LoroDocument) throws {
        try document.write(value, at: path)
    }
}
