package io.replicaman

import androidx.sqlite.SQLiteConnection

/** Archived authoring is evidence, never an automatic retry against a new lifetime. */
public data class ReplicaRecoveryRecord(
    val id: String, val stream: String, val rowId: String, val incarnation: String,
    val reason: String, val createdAt: Double,
)

public data class ReplicaRecoveryPart(val kind: String, val key: String, val byteCount: Long)

internal fun ReplicaStateStore.archiveDocument(db: SQLiteConnection, stream: String, rowId: String, reason: String) {
    archiveEntity(db, stream, rowId, reason, force = true)
}

/** Copy raw bytes in SQLite; damaged JSON and large histories remain exportable. */
internal fun ReplicaStateStore.archiveEntity(
    db: SQLiteConnection, stream: String, id: String, reason: String, force: Boolean = false,
) {
    val hasAuthoring = db.queryLong("""
        SELECT EXISTS(SELECT 1 FROM intents WHERE row_id = ? AND stream = ? AND state <> 'refused')
            OR EXISTS(SELECT 1 FROM holds WHERE stream = ? AND row_id = ?)
        """.trimIndent(), listOf(id, stream, stream, id)) == 1L
    if (!force && !hasAuthoring) return
    val identity = incarnation(db, stream, id)
        ?: throw ReplicaError.Storage("Cannot archive an entity without its incarnation")
    val recoveryID = ReplicaID.ulid()
    db.exec("""
        INSERT INTO recoveries VALUES (?, ?, ?, ?, ?, CAST('{"format":1}' AS BLOB), ?)
        """.trimIndent(), listOf(recoveryID, stream, id, identity, reason, System.currentTimeMillis() / 1000.0))
    for (statement in sqlStatements(ReplicaRecoverySQL.sql)) {
        db.exec(statement, listOf(recoveryID, stream, id))
    }
}

/** Rescan from the start to discover archives added during pagination. */
public fun ReplicaStateStore.recoveryRecords(after: String? = null, limit: Int = 100): List<ReplicaRecoveryRecord> {
    require(limit in 1..1000) { "Recovery page limit must be 1..1000" }
    return read { db ->
        db.query("""
            SELECT id, stream, row_id, incarnation, reason, created_at FROM recoveries
            WHERE (? IS NULL OR id > ?) ORDER BY id LIMIT ?
            """.trimIndent(), listOf(after, after, limit)) {
            ReplicaRecoveryRecord(it.getText(0), it.getText(1), it.getText(2), it.getText(3), it.getText(4), it.getDouble(5))
        }
    }
}

public fun ReplicaStateStore.recoveryParts(
    id: String, after: ReplicaRecoveryPart? = null, limit: Int = 100,
): List<ReplicaRecoveryPart> {
    require(limit in 1..1000) { "Recovery page limit must be 1..1000" }
    return read { db ->
        db.query("""
            SELECT kind, part_key, length(content) FROM recovery_parts
            WHERE recovery_id = ? AND (? IS NULL OR (kind, part_key) > (?, ?))
            ORDER BY kind, part_key LIMIT ?
            """.trimIndent(), listOf(id, after?.kind, after?.kind, after?.key, limit)) {
            ReplicaRecoveryPart(it.getText(0), it.getText(1), it.getLong(2))
        }
    }
}

public fun ReplicaStateStore.recoveryChunk(
    id: String, part: ReplicaRecoveryPart, offset: Long = 0, limit: Int = 256 * 1024,
): ByteArray {
    require(offset >= 0 && offset < Long.MAX_VALUE && limit in 1..262144) { "Invalid recovery chunk range" }
    return read { db ->
        db.queryOne("""
            SELECT substr(content, ?, ?) FROM recovery_parts
            WHERE recovery_id = ? AND kind = ? AND part_key = ?
            """.trimIndent(), listOf(offset + 1, limit, id, part.kind, part.key)) { it.getBlob(0) }
            ?: throw ReplicaError.Storage("Recovery part does not exist")
    }
}

public fun ReplicaStateStore.removeRecoveryRecord(id: String) {
    write { db ->
        db.exec("DELETE FROM recovery_parts WHERE recovery_id = ?", listOf(id))
        db.exec("DELETE FROM recoveries WHERE id = ?", listOf(id))
    }
}
