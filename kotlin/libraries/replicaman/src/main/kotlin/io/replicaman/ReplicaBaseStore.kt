package io.replicaman

import androidx.sqlite.SQLiteConnection

internal data class ReplicaBaseRow(
    val incarnation: String, val revision: Long, val type: String?, val data: Map<String, ReplicaValue>,
    val codec: String?, val fold: ByteArray?,
)

internal fun ReplicaStateStore.baseRow(db: SQLiteConnection, stream: String, id: String): ReplicaBaseRow? = db.queryOne("""
    SELECT incarnation, revision, type, data, codec, fold FROM base WHERE stream = ? AND row_id = ?
    """.trimIndent(), listOf(stream, id)) {
    val data = (ReplicaJSON.decodeValue(it.getText(3)) as? ReplicaValue.Obj)?.fields
        ?: throw ReplicaError.Storage("Authoritative row is not an object")
    ReplicaBaseRow(it.getText(0), it.getLong(1), it.textOrNull(2), data, it.textOrNull(4), it.blobOrNull(5))
}

internal fun ReplicaStateStore.saveBase(db: SQLiteConnection, stream: String, id: String, shard: String, row: ReplicaBaseRow) {
    val data = ReplicaJSON.encodeToString(ReplicaValue.Obj(row.data))
    val integrity = ReplicaIntegrityHash.base(stream, id, shard, row.incarnation, row.revision, row.type, data, row.codec, row.fold)
    db.exec("""
        INSERT INTO base (stream, row_id, shard, incarnation, revision, type, data, codec, fold, integrity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard,
            incarnation = excluded.incarnation, revision = excluded.revision, type = excluded.type,
            data = excluded.data, codec = excluded.codec, fold = excluded.fold, integrity = excluded.integrity
        """.trimIndent(), listOf(stream, id, shard, row.incarnation, row.revision, row.type,
            data, row.codec, row.fold, integrity))
}

internal fun ReplicaStateStore.hasLocalBirth(db: SQLiteConnection, stream: String, id: String, incarnation: String): Boolean {
    val pending = entriesAddressing(db, stream, id).filter { it.parked == null }.map { it.op() }
    return (pending + overlays(db, stream, id)).any { it.verb == ReplicaOp.Verb.ROW_CREATE && it.incarnation == incarnation }
}
