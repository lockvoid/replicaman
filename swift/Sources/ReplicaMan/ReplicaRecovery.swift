import Foundation
import GRDB

/// Metadata for a locally archived branch. Its bytes are exported in bounded
/// parts and never retried automatically against a different entity lifetime.
public struct ReplicaRecoveryRecord: Sendable {
    public let id: String
    public let stream: String
    public let rowId: String
    public let incarnation: String
    public let reason: String
    public let createdAt: Date
}

public struct ReplicaRecoveryPart: Sendable {
    public let kind: String
    public let key: String
    public let byteCount: Int64
}

extension ReplicaStateStore {
    func archiveDocument(_ db: Database, stream: String, rowId: String, reason: String) throws {
        try archiveEntity(db, stream: stream, id: rowId, reason: reason, force: true)
    }

    /// SQL copies raw state inside the caller's transaction. Damaged JSON remains
    /// recoverable, and a large pending history never becomes one in-memory blob.
    func archiveEntity(
        _ db: Database, stream: String, id: String, reason: String, force: Bool = false
    ) throws {
        let hasAuthoring = try Bool.fetchOne(db, sql: """
            SELECT EXISTS(SELECT 1 FROM intents WHERE row_id = ? AND stream = ? AND state <> 'refused')
                OR EXISTS(SELECT 1 FROM holds WHERE stream = ? AND row_id = ?)
            """, arguments: [id, stream, stream, id]) ?? false
        guard force || hasAuthoring else { return }
        guard let identity = try incarnation(db, stream: stream, id: id) else {
            throw ReplicaError.storage("Cannot archive an entity without its incarnation")
        }

        let recoveryID = ReplicaID.ulid()
        try db.execute(sql: """
            INSERT INTO recoveries VALUES (?, ?, ?, ?, ?, CAST('{"format":1}' AS BLOB), ?)
            """, arguments: [recoveryID, stream, id, identity, reason, Date().timeIntervalSince1970])
        for statement in ReplicaRecoverySQL.sql.components(separatedBy: ";") {
            guard !statement.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { continue }
            try db.execute(sql: statement, arguments: [recoveryID, stream, id])
        }
    }

    /// Paginate by the last returned id. Rescan from the start to discover
    /// archives added while traversing a previously opened page.
    public func recoveryRecords(after id: String? = nil, limit: Int = 100) throws -> [ReplicaRecoveryRecord] {
        guard (1...1000).contains(limit) else { throw ReplicaError.storage("Recovery page limit must be 1...1000") }
        return try pool.read { db in
            try Row.fetchAll(db, sql: """
                SELECT id, stream, row_id, incarnation, reason, created_at FROM recoveries
                WHERE (? IS NULL OR id > ?) ORDER BY id LIMIT ?
                """, arguments: [id, id, limit]).map { row in
                .init(id: row["id"], stream: row["stream"], rowId: row["row_id"], incarnation: row["incarnation"],
                      reason: row["reason"], createdAt: Date(timeIntervalSince1970: row["created_at"]))
            }
        }
    }

    public func recoveryParts(
        id: String, after part: ReplicaRecoveryPart? = nil, limit: Int = 100
    ) throws -> [ReplicaRecoveryPart] {
        guard (1...1000).contains(limit) else { throw ReplicaError.storage("Recovery page limit must be 1...1000") }
        return try pool.read { db in
            try Row.fetchAll(db, sql: """
                SELECT kind, part_key, length(content) AS bytes FROM recovery_parts
                WHERE recovery_id = ? AND (? IS NULL OR (kind, part_key) > (?, ?))
                ORDER BY kind, part_key LIMIT ?
                """, arguments: [id, part?.kind, part?.kind, part?.key, limit]).map {
                .init(kind: $0["kind"], key: $0["part_key"], byteCount: $0["bytes"])
            }
        }
    }

    public func recoveryChunk(
        id: String, part: ReplicaRecoveryPart, offset: Int64 = 0, limit: Int = 256 * 1024
    ) throws -> Data {
        guard offset >= 0, offset < Int64.max, (1...262144).contains(limit) else {
            throw ReplicaError.storage("Invalid recovery chunk range")
        }
        return try pool.read { db in
            guard let bytes = try Data.fetchOne(db, sql: """
                SELECT substr(content, ?, ?) FROM recovery_parts
                WHERE recovery_id = ? AND kind = ? AND part_key = ?
                """, arguments: [offset + 1, limit, id, part.kind, part.key]) else {
                throw ReplicaError.storage("Recovery part does not exist")
            }
            return bytes
        }
    }

    /// Explicitly forget an exported or dismissed branch, including its bytes.
    public func removeRecoveryRecord(id: String) throws {
        try pool.write { db in
            try db.execute(sql: "DELETE FROM recovery_parts WHERE recovery_id = ?", arguments: [id])
            try db.execute(sql: "DELETE FROM recoveries WHERE id = ?", arguments: [id])
        }
    }
}
