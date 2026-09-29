import ReplicaManTestProtocol
import Foundation
import GRDB
import Loro
@testable import ReplicaManLoro
import XCTest
@testable import ReplicaMan

/// Loro-side fixtures: the same stub transport discipline as the core
/// suite, plus loro authoring helpers standing in for the app's document
/// layer — a client doc is born FROM the store's
/// fold under the store's peer, edits export as update payloads.
actor LoroStubTransport: FixtureTransport {
    let protocolFixture = ProtocolFixture()
    private(set) var pushedBatches: [[ReplicaOp]] = []
    private(set) var pushCount = 0
    private var pullQueues: [String: [ReplicaPullResponse]] = [:]
    private var pushScript: (@Sendable ([ReplicaOp]) -> [ReplicaVerdict])?
    private var pushFails = false

    func queuePull(shard: String, _ response: ReplicaPullResponse) {
        pullQueues[shard, default: []].append(response)
    }

    func scriptPush(_ script: @escaping @Sendable ([ReplicaOp]) -> [ReplicaVerdict]) {
        pushScript = script
    }

    func failPushes(_ fail: Bool) {
        pushFails = fail
    }

    func pull(shard: String, cursor: String?, limit: Int) async throws -> ReplicaPullResponse {
        if var queue = pullQueues[shard], !queue.isEmpty {
            let response = queue.removeFirst()
            pullQueues[shard] = queue
            return response
        }
        return ReplicaPullResponse(frames: [], cursor: cursor ?? "0:", more: false)
    }

    func push(_ ops: [ReplicaOp]) async throws -> [ReplicaVerdict] {
        pushCount += 1
        if pushFails { throw ReplicaError.transport("push refused (stub)") }
        pushedBatches.append(ops)
        if let pushScript { return pushScript(ops) }
        return ops.map { ReplicaVerdict(id: $0.id, outcome: .accepted) }
    }
}

enum LoroFixture {
    static let serverPeer: UInt64 = 1

    static func schema(stamp: ReplicaStamp? = nil) -> ReplicaSchema {
        ReplicaSchema(streams: [
            ReplicaStreamSpec(
                name: "boards", lane: .document, shard: "user", codec: LoroReplicaCodec.codecName,
                reflections: [ReplicaReflection(field: "name", path: ["meta", "name"])], stamp: stamp
            ),
            ReplicaStreamSpec(name: "notes", lane: .row, shard: "user"),
        ])
    }

    static func store(_ name: String = #function) throws -> ReplicaStateStore {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("replica-man-loro-tests", isDirectory: true)
            .appendingPathComponent("\(name)-\(UUID().uuidString).sqlite").path
        return try ReplicaStateStore(path: path)
    }

    static func minter(from start: UInt64 = 100) -> @Sendable () -> UInt64 {
        let lock = NSLock()
        nonisolated(unsafe) var value = start
        return {
            lock.lock()
            defer { lock.unlock() }
            defer { value += 1 }
            return value
        }
    }

    static func engine(
        store: ReplicaStateStore,
        transport: LoroStubTransport,
        minterStart: UInt64 = 100,
        schema: ReplicaSchema = LoroFixture.schema(),
        clock: @escaping @Sendable () -> Date = Date.init,
        syncGates: [any SyncGate] = []
    ) -> ReplicaEngine {
        ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: schema,
            codecs: [LoroReplicaCodec()],
            peerMinter: minter(from: minterStart),
            clock: clock,
            automaticallyPushWrites: false,
            syncGates: syncGates
        )
    }

    // MARK: - Loro authoring (the app layer's half, in miniature)

    static func doc(peer: UInt64, fold: Data? = nil) throws -> LoroDoc {
        let doc = LoroDoc()
        doc.setRecordTimestamp(record: false)
        try doc.setPeerId(peer: peer)
        if let fold, !fold.isEmpty {
            _ = try doc.import(bytes: fold)
        }
        return doc
    }

    static func setMeta(_ doc: LoroDoc, _ key: String, _ value: String) throws {
        try doc.getMap(id: "meta").insert(key: key, v: value)
        doc.commit()
    }

    /// Edit-and-export: the payload a real client would journal for this
    /// mutation (updates since the doc's version before the edit).
    static func editPayload(_ doc: LoroDoc, _ key: String, _ value: String) throws -> Data {
        let before = doc.oplogVv()
        try setMeta(doc, key, value)
        return try doc.export(mode: .updates(from: before))
    }

    static func meta(_ doc: LoroDoc, _ key: String) -> String? {
        guard case .map(let root) = doc.getDeepValue(),
              case .map(let meta)? = root["meta"],
              case .string(let value)? = meta[key]
        else { return nil }
        return value
    }

    static func meta(fold: Data, _ key: String) throws -> String? {
        meta(try doc(peer: 999_999, fold: fold), key)
    }
}

// MARK: - Store peeks

extension ReplicaStateStore {
    func peekDoc(_ stream: String, _ rowId: String) throws -> DocRow? {
        try pool.read { try self.doc($0, stream: stream, rowId: rowId) }
    }

    func peekSnapshot(_ stream: String, _ rowId: String) throws -> SnapshotRow? {
        try pool.read { try self.snapshot($0, stream: stream, rowId: rowId) }
    }

    func peekPending() throws -> [JournalRow] {
        try pool.read { try self.pending($0) }
    }

    func peekParked() throws -> [JournalRow] {
        try pool.read { try self.parked($0) }
    }
}

/// Test export through the public bounded reader, including chunk boundaries.
func recoveryBytes(_ store: ReplicaStateStore, record: ReplicaRecoveryRecord, kind: String) throws -> Data {
    let part = try XCTUnwrap(store.recoveryParts(id: record.id).first { $0.kind == kind })
    var bytes = Data()
    while bytes.count < part.byteCount {
        let chunk = try store.recoveryChunk(id: record.id, part: part, offset: Int64(bytes.count))
        guard !chunk.isEmpty else { throw ReplicaError.storage("Truncated recovery export") }
        bytes.append(chunk)
    }
    return bytes
}
