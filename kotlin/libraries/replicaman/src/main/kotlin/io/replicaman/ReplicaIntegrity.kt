package io.replicaman

import androidx.sqlite.SQLiteConnection
import java.nio.ByteBuffer
import java.security.MessageDigest
import kotlinx.coroutines.withContext

internal class ReplicaIntegrityHash(domain: String) {
    private val hash = MessageDigest.getInstance("SHA-256")

    init { hash.update((domain + '\u0000').toByteArray(Charsets.UTF_8)) }

    fun append(bytes: ByteArray?) {
        hash.update(ByteBuffer.allocate(8).putLong(bytes?.size?.toLong() ?: -1L).array())
        if (bytes != null) hash.update(bytes)
    }

    fun append(text: String?) = append(text?.toByteArray(Charsets.UTF_8))

    fun finish(): String {
        val digits = "0123456789abcdef"
        val bytes = hash.digest()
        val hex = CharArray(bytes.size * 2)
        bytes.forEachIndexed { index, byte ->
            hex[index * 2] = digits[(byte.toInt() and 0xff) ushr 4]
            hex[index * 2 + 1] = digits[byte.toInt() and 0x0f]
        }
        return String(hex)
    }

    companion object {
        fun base(
            stream: String, id: String, shard: String, incarnation: String, revision: Long,
            type: String?, data: String, codec: String?, fold: ByteArray?,
        ): String {
            val hash = ReplicaIntegrityHash("replicaman-base")
            for (field in listOf(stream, id, shard, incarnation, revision.toString(), type, data, codec)) hash.append(field)
            hash.append(fold)
            return hash.finish()
        }
    }
}

internal data class ReplicaIntegritySnapshot(val cursor: String, val generation: Long, val digest: String, val count: Long)

/** One bounded SQLite snapshot; local intents and optimistic rows are excluded. */
internal fun ReplicaStateStore.integritySnapshot(db: SQLiteConnection, shard: String): ReplicaIntegritySnapshot {
    val cursor = cursor(db, shard)
        ?: throw ReplicaError.Protocol("CheckpointRequired", "Synchronize before verifying the replica")
    val hash = ReplicaIntegrityHash("replicaman-view")
    var count = 0L
    db.prepare("""
        SELECT stream, row_id, incarnation, revision, type, data, codec, fold, integrity
        FROM base WHERE shard = ? ORDER BY stream COLLATE BINARY, row_id COLLATE BINARY
        """.trimIndent()).use { rows ->
        rows.bindText(1, shard)
        while (rows.step()) {
            val stream = rows.getText(0)
            val id = rows.getText(1)
            val incarnation = rows.getText(2)
            val revision = rows.getLong(3)
            val actual = ReplicaIntegrityHash.base(stream, id, shard, incarnation, revision,
                rows.textOrNull(4), rows.getText(5), rows.textOrNull(6), rows.blobOrNull(7))
            if (rows.textOrNull(8) != actual) {
                throw ReplicaError.Storage("Authoritative row integrity failed: $stream/$id; local work is retained")
            }
            for (field in listOf(stream, id, incarnation, revision.toString())) hash.append(field)
            count++
        }
    }
    return ReplicaIntegritySnapshot(cursor, readGeneration(db, shard), hash.finish(), count)
}

/**
 * Compare the authoritative base with the server's live membership at the
 * published cursor. This reads every base row: schedule periodically, not per
 * edit. A failure changes no data; `CursorBehind` asks the caller to pull first.
 */
public suspend fun ReplicaEngine.verifyIntegrity(shard: String = "user"): Unit = withContext(engineContext) {
    val store = beginWireOperation()
    try {
        val (dataset, proof) = store.read { store.meta(it).dataset to store.integritySnapshot(it, shard) }
        val response = ReplicaConnection(transport, schema).send(ReplicaEndpoint.VERIFY, dataset, mapOf(
            "shard" to ReplicaValue.Str(shard), "cursor" to ReplicaValue.Str(proof.cursor),
        ))
        val digest = response.requiredText("digest")
        val count = ReplicaProtocol.counter(response.requiredText("count"))
        if (response.requiredText("shard") != shard || response.requiredText("cursor") != proof.cursor || !ReplicaProtocol.isDigest(digest)) {
            ReplicaProtocol.invalid("Integrity response names a different shard or cursor")
        }
        val current = store.read { store.cursor(it, shard) == proof.cursor && store.readGeneration(it, shard) == proof.generation }
        if (!current) throw ReplicaError.Protocol("CheckpointChanged", "The published cursor changed during verification; retry")
        if (digest != proof.digest || count != proof.count) {
            throw ReplicaError.Protocol("ReplicaDiverged", "Authoritative membership differs from the server; local work is retained")
        }
    } finally {
        endWireOperation()
    }
}
