package io.replicaman

import androidx.sqlite.SQLiteConnection

/**
 * A pull round: the cursor its next request continues from (null for a
 * baseline), whether publishing it replaces the shard's base, and the highest
 * accepted local sequence when it started.
 */
internal data class ReplicaDownload(val cursor: String?, val reset: Boolean, val visible: Long)

/** The server's opaque cursor of the last published round. */
internal fun ReplicaStateStore.cursor(db: SQLiteConnection, shard: String): String? =
    db.queryString("SELECT cursor FROM checkpoints WHERE shard = ?", listOf(shard))

internal fun ReplicaStateStore.setCursor(db: SQLiteConnection, value: String, shard: String) {
    db.exec("""
        INSERT INTO checkpoints (shard, cursor) VALUES (?, ?)
        ON CONFLICT(shard) DO UPDATE SET cursor = excluded.cursor
        """.trimIndent(), listOf(shard, value))
}

/** The next round of this shard is a baseline; a round in flight or staged is abandoned. */
internal fun ReplicaStateStore.clearCursor(db: SQLiteConnection, shard: String) {
    invalidateDownload(db, shard)
    db.exec("UPDATE checkpoints SET cursor = NULL WHERE shard = ?", listOf(shard))
}

internal fun ReplicaStateStore.readGeneration(db: SQLiteConnection, shard: String): Long =
    db.queryLong("SELECT generation FROM checkpoints WHERE shard = ?", listOf(shard)) ?: 0

internal fun ReplicaStateStore.invalidateDownload(db: SQLiteConnection, shard: String) {
    db.exec("""
        INSERT INTO checkpoints (shard, generation) VALUES (?, 1)
        ON CONFLICT(shard) DO UPDATE SET generation = generation + 1
        """.trimIndent(), listOf(shard))
    discardDownload(db, shard)
}

/** The round this shard's next request belongs to: the staged one, or a new one from the published cursor. */
internal fun ReplicaStateStore.round(db: SQLiteConnection, shard: String): ReplicaDownload =
    db.queryOne("SELECT cursor, reset, visible FROM downloads WHERE shard = ?", listOf(shard)) {
        ReplicaDownload(it.textOrNull(0), it.getLong(1) != 0L, it.getLong(2))
    } ?: cursor(db, shard).let { ReplicaDownload(it, it == null, acceptedThrough(db)) }

/** Stages one answer of a round that continues; the round's next request uses `cursor`. */
internal fun ReplicaStateStore.stage(db: SQLiteConnection, shard: String, round: ReplicaDownload, frames: List<ReplicaValue>, cursor: String) {
    db.exec("""
        INSERT INTO downloads (shard, cursor, reset, visible, started_at) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(shard) DO UPDATE SET cursor = excluded.cursor
        """.trimIndent(), listOf(shard, cursor, round.reset, round.visible, System.currentTimeMillis() / 1000.0))
    db.exec("INSERT INTO download_pages (shard, page, content) VALUES (?, ?, ?)",
        listOf(shard, stagedPages(db, shard).size, ReplicaJSON.encodeToBytes(ReplicaValue.Arr(frames))))
}

internal fun ReplicaStateStore.stagedPages(db: SQLiteConnection, shard: String): List<Int> {
    val pages = db.query("SELECT page FROM download_pages WHERE shard = ? ORDER BY page", listOf(shard)) { it.getLong(0).toInt() }
    if (pages != pages.indices.toList()) throw ReplicaError.Storage("Staged page gap")
    return pages
}

internal fun ReplicaStateStore.stagedFrames(db: SQLiteConnection, shard: String, page: Int): List<ReplicaPulledFrame> {
    val content = db.queryOne("SELECT content FROM download_pages WHERE shard = ? AND page = ?", listOf(shard, page)) { it.getBlob(0) }
        ?: throw ReplicaError.Storage("Staged page is missing")
    return ReplicaPullPage.decodeFrames(ReplicaProtocol.decode(content).items ?: throw ReplicaError.Storage("Staged page is not an array"))
}

internal fun ReplicaStateStore.discardDownload(db: SQLiteConnection, shard: String) {
    db.exec("DELETE FROM download_pages WHERE shard = ?", listOf(shard))
    db.exec("DELETE FROM downloads WHERE shard = ?", listOf(shard))
}
