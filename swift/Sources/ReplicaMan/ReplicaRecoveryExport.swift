import Foundation
import GRDB

extension ReplicaStateStore {
    /// Stream a complete recovery branch as JSON Lines from one SQLite snapshot.
    /// The sink must only write bytes, without reentering this store. Failures
    /// propagate and the archive remains retained. A complete export ends with
    /// a `complete` record; callers must not publish a partially written file.
    public func exportRecovery(id: String, write: @escaping (Data) throws -> Void) throws {
        try pool.read { db in
            try ReplicaRecoveryExporter(db: db, write: write).export(id: id)
        }
    }
}

private struct ReplicaRecoveryExporter {
    let db: Database
    let write: (Data) throws -> Void

    func emit(_ fields: [String: ReplicaValue]) throws {
        var line = try ReplicaJSON.encoder().encode(fields)
        line.append(0x0a)
        try write(line)
    }

    func export(id: String) throws {
        guard let record = try Row.fetchOne(db, sql: "SELECT * FROM recoveries WHERE id = ?", arguments: [id]) else {
            throw ReplicaError.storage("Recovery record does not exist")
        }
        try emit([
            "type": .string("record"), "format": .string("replicaman-recovery"), "version": .integer(1),
            "id": .string(id), "stream": .string(record["stream"]), "row_id": .string(record["row_id"]),
            "incarnation": .string(record["incarnation"]), "reason": .string(record["reason"]),
            "created_at": .number(record["created_at"]),
        ])

        let parts = try Row.fetchCursor(db, sql: """
            SELECT kind, part_key, length(content) AS bytes FROM recovery_parts
            WHERE recovery_id = ? ORDER BY kind, part_key
            """, arguments: [id])
        var count: Int64 = 0
        var bytes: Int64 = 0
        while let row = try parts.next() {
            let size: Int64 = row["bytes"]
            try exportPart(id: id, kind: row["kind"], key: row["part_key"], size: size)
            count += 1
            bytes += size
        }
        try emit(["type": .string("complete"), "parts": .integer(count), "bytes": .string(String(bytes))])
    }

    func exportPart(id: String, kind: String, key: String, size: Int64) throws {
        try emit(["type": .string("part"), "kind": .string(kind), "key": .string(key), "bytes": .string(String(size))])
        var offset: Int64 = 0
        while offset < size {
            guard let chunk = try Data.fetchOne(db, sql: """
                SELECT substr(content, ?, 262144) FROM recovery_parts
                WHERE recovery_id = ? AND kind = ? AND part_key = ?
                """, arguments: [offset + 1, id, kind, key]), !chunk.isEmpty else {
                throw ReplicaError.storage("Recovery part is incomplete")
            }
            try emit([
                "type": .string("chunk"), "offset": .string(String(offset)),
                "sha256": .string(ReplicaProtocol.digest(chunk)), "content": .string(chunk.base64EncodedString()),
            ])
            offset += Int64(chunk.count)
        }
    }
}
