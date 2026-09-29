import Foundation
import GRDB

struct ReplicaBaseRow {
    let incarnation: String
    let revision: Int64
    let type: String?
    let data: [String: ReplicaValue]
    let codec: String?
    let fold: Data?
}

extension ReplicaStateStore {
    func baseRow(_ db: Database, stream: String, id: String) throws -> ReplicaBaseRow? {
        guard let row = try Row.fetchOne(db,
            sql: "SELECT * FROM base WHERE stream = ? AND row_id = ?", arguments: [stream, id]) else { return nil }
        let data: String = row["data"]
        return ReplicaBaseRow(incarnation: row["incarnation"], revision: row["revision"], type: row["type"],
            data: try ReplicaJSON.decoder().decode([String: ReplicaValue].self, from: Data(data.utf8)),
            codec: row["codec"], fold: row["fold"])
    }

    func saveBase(_ db: Database, stream: String, id: String, shard: String, row: ReplicaBaseRow) throws {
        let data = String(decoding: try ReplicaJSON.encoder().encode(row.data), as: UTF8.self)
        let integrity = ReplicaIntegrityHash.base(
            stream: stream, id: id, shard: shard, incarnation: row.incarnation, revision: row.revision,
            type: row.type, data: data, codec: row.codec, fold: row.fold)
        try db.execute(sql: """
            INSERT INTO base (stream, row_id, shard, incarnation, revision, type, data, codec, fold, integrity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard,
                incarnation = excluded.incarnation, revision = excluded.revision, type = excluded.type,
                data = excluded.data, codec = excluded.codec, fold = excluded.fold, integrity = excluded.integrity
            """, arguments: [stream, id, shard, row.incarnation, row.revision, row.type, data, row.codec, row.fold, integrity])
    }

    func hasLocalBirth(_ db: Database, stream: String, id: String, incarnation: String) throws -> Bool {
        let pending = try entriesAddressing(db, stream: stream, rowId: id).filter { $0.parked == nil }
        let operations = try pending.map { try $0.op() } + overlays(db, stream: stream, id: id)
        return operations.contains { $0.verb == ReplicaOp.Verb.rowCreate && $0.incarnation == incarnation }
    }
}
