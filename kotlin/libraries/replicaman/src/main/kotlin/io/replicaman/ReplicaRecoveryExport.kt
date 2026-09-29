package io.replicaman

import androidx.sqlite.SQLiteConnection
import java.util.Base64

/**
 * Stream one recovery branch as JSON Lines from a consistent SQLite snapshot.
 * The sink must only write bytes, without reentering this store. Failures
 * propagate and retain the archive. Publish the file only after this returns;
 * a complete export ends with a `complete` record.
 */
public fun ReplicaStateStore.exportRecovery(id: String, write: (ByteArray) -> Unit) {
    read { db -> RecoveryExporter(db, write).export(id) }
}

private class RecoveryExporter(val db: SQLiteConnection, val write: (ByteArray) -> Unit) {
    fun emit(vararg fields: Pair<String, ReplicaValue>) {
        write(ReplicaJSON.encodeToBytes(ReplicaValue.Obj(mapOf(*fields))) + byteArrayOf(10))
    }

    fun export(id: String) {
        val record = db.queryOne("SELECT stream, row_id, incarnation, reason, created_at FROM recoveries WHERE id = ?", listOf(id)) {
            ReplicaRecoveryRecord(id, it.getText(0), it.getText(1), it.getText(2), it.getText(3), it.getDouble(4))
        } ?: throw ReplicaError.Storage("Recovery record does not exist")
        emit(
            "type" to ReplicaValue.Str("record"), "format" to ReplicaValue.Str("replicaman-recovery"),
            "version" to ReplicaValue.Integer(1), "id" to ReplicaValue.Str(id),
            "stream" to ReplicaValue.Str(record.stream), "row_id" to ReplicaValue.Str(record.rowId),
            "incarnation" to ReplicaValue.Str(record.incarnation), "reason" to ReplicaValue.Str(record.reason),
            "created_at" to ReplicaValue.Num(record.createdAt),
        )

        var count = 0L
        var bytes = 0L
        db.prepare("""
            SELECT kind, part_key, length(content) FROM recovery_parts
            WHERE recovery_id = ? ORDER BY kind, part_key
        """.trimIndent()).use { parts ->
            parts.bindText(1, id)
            while (parts.step()) {
                val size = parts.getLong(2)
                exportPart(id, parts.getText(0), parts.getText(1), size)
                count++
                bytes += size
            }
        }
        emit("type" to ReplicaValue.Str("complete"), "parts" to ReplicaValue.Integer(count), "bytes" to ReplicaValue.Str(bytes.toString()))
    }

    fun exportPart(id: String, kind: String, key: String, size: Long) {
        emit("type" to ReplicaValue.Str("part"), "kind" to ReplicaValue.Str(kind),
            "key" to ReplicaValue.Str(key), "bytes" to ReplicaValue.Str(size.toString()))
        var offset = 0L
        while (offset < size) {
            val chunk = db.queryOne("""
                SELECT substr(content, ?, 262144) FROM recovery_parts
                WHERE recovery_id = ? AND kind = ? AND part_key = ?
            """.trimIndent(), listOf(offset + 1, id, kind, key)) { it.getBlob(0) }
                ?: throw ReplicaError.Storage("Recovery part does not exist")
            if (chunk.isEmpty()) throw ReplicaError.Storage("Recovery part is incomplete")
            emit("type" to ReplicaValue.Str("chunk"), "offset" to ReplicaValue.Str(offset.toString()),
                "sha256" to ReplicaValue.Str(ReplicaProtocol.digest(chunk)),
                "content" to ReplicaValue.Str(Base64.getEncoder().encodeToString(chunk)))
            offset += chunk.size
        }
    }
}
