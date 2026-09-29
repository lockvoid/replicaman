import CryptoKit
import Foundation
import GRDB

struct ReplicaIntegrityHash {
    private var hash = SHA256()

    init(_ domain: String) { hash.update(data: Data((domain + "\0").utf8)) }

    mutating func append(_ bytes: Data?) {
        var length = (bytes.map { UInt64($0.count) } ?? UInt64.max).bigEndian
        withUnsafeBytes(of: &length) { hash.update(data: Data($0)) }
        if let bytes { hash.update(data: bytes) }
    }

    mutating func append(_ text: String?) { append(text.map { Data($0.utf8) }) }

    func finish() -> String {
        let digits = Array("0123456789abcdef".utf8)
        var encoded = [UInt8](repeating: 0, count: 64)
        for (index, byte) in hash.finalize().enumerated() {
            encoded[index * 2] = digits[Int(byte >> 4)]
            encoded[index * 2 + 1] = digits[Int(byte & 15)]
        }
        return String(decoding: encoded, as: UTF8.self)
    }

    static func base(
        stream: String, id: String, shard: String, incarnation: String, revision: Int64,
        type: String?, data: String, codec: String?, fold: Data?
    ) -> String {
        var hash = Self("replicaman-base")
        for field in [stream, id, shard, incarnation, String(revision), type, data, codec] { hash.append(field) }
        hash.append(fold)
        return hash.finish()
    }
}

struct ReplicaIntegritySnapshot {
    let cursor: String
    let generation: Int64
    let digest: String
    let count: Int64
}

extension ReplicaStateStore {
    /// One bounded SQLite read snapshot; never include optimistic local authoring.
    func integritySnapshot(_ db: Database, shard: String) throws -> ReplicaIntegritySnapshot {
        guard let cursor = try cursor(db, shard: shard) else {
            throw ReplicaError.protocolFailure(code: "CheckpointRequired", message: "Synchronize before verifying the replica")
        }
        var hash = ReplicaIntegrityHash("replicaman-view")
        var count: Int64 = 0
        let rows = try Row.fetchCursor(db, sql: """
            SELECT * FROM base WHERE shard = ? ORDER BY stream COLLATE BINARY, row_id COLLATE BINARY
            """, arguments: [shard])
        while let row = try rows.next() {
            let stream: String = row["stream"]
            let id: String = row["row_id"]
            let incarnation: String = row["incarnation"]
            let revision: Int64 = row["revision"]
            let actual = ReplicaIntegrityHash.base(
                stream: stream, id: id, shard: shard, incarnation: incarnation, revision: revision,
                type: row["type"], data: row["data"], codec: row["codec"], fold: row["fold"])
            guard let expected: String = row["integrity"], actual == expected else {
                throw ReplicaError.storage("Authoritative row integrity failed: \(stream)/\(id); local work is retained")
            }
            for field in [stream, id, incarnation, String(revision)] { hash.append(field) }
            count += 1
        }
        return ReplicaIntegritySnapshot(cursor: cursor, generation: try readGeneration(db, shard: shard),
                                        digest: hash.finish(), count: count)
    }
}

extension ReplicaEngine {
    /// Compare the stored authoritative base with the server at its published
    /// cursor. The server answers only for its current heads: `CursorBehind`
    /// means pull the shard, then verify again.
    /// This reads every authoritative row; schedule it periodically, not per edit.
    /// A mismatch preserves all data. Explicit recovery/rebuild remains the caller's decision.
    public func verifyIntegrity(shard: String = "user") async throws {
        let store = try beginWireOperation()
        defer { endWireOperation() }
        let connection = ReplicaConnection(transport: transport, schema: schema)
        let (proof, dataset) = try await store.pool.read { db in
            (try store.integritySnapshot(db, shard: shard), try store.meta(db).dataset)
        }
        guard let dataset else {
            throw ReplicaError.protocolFailure(code: "CheckpointRequired", message: "Synchronize before verifying the replica")
        }
        let answer = try await connection.verify(shard: shard, cursor: proof.cursor, dataset: dataset)
        let count = try ReplicaProtocol.counter(answer.count)
        let current = try await store.pool.read { db in
            try store.cursor(db, shard: shard) == proof.cursor && store.readGeneration(db, shard: shard) == proof.generation
        }
        guard current else {
            throw ReplicaError.protocolFailure(code: "CheckpointChanged", message: "The published cursor changed during verification; retry")
        }
        guard answer.digest == proof.digest, count == proof.count else {
            throw ReplicaError.protocolFailure(code: "ReplicaDiverged", message: "Authoritative membership differs from the server; local work is retained")
        }
    }
}
