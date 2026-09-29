package io.replicaman

import androidx.sqlite.SQLiteConnection
import java.util.UUID

/** The store's identity, the dataset it synchronizes and the next local submission sequence. */
internal data class ReplicaMeta(val store: String, val dataset: String?, val nextSequence: Long)

/** `androidx.sqlite` prepares one statement at a time; comments may contain `;`. */
internal fun sqlStatements(script: String): List<String> =
    script.lines().joinToString("\n") { it.substringBefore("--") }
        .split(';').filter { it.isNotBlank() }

/** The layout this build reads and writes, as `PRAGMA user_version`. A store in any other format is refused, never migrated. */
private const val STORE_FORMAT = 3L

/**
 * Reads only, before anything writes: a file with tables but no format is an
 * earlier layout. Both it and a newer format are refused with their bytes untouched.
 */
internal fun requireStoreFormat(db: SQLiteConnection) {
    val format = db.queryLong("PRAGMA user_version") ?: 0L
    if (format > STORE_FORMAT) throw ReplicaError.Storage("Unsupported store format; upgrade required")
    if (format == STORE_FORMAT) return
    if (format != 0L || db.queryBool("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table')")) {
        throw ReplicaError.Storage("Unsupported earlier store format; open a fresh store")
    }
}

internal fun prepareSynchronization(db: SQLiteConnection) {
    for (statement in sqlStatements(ReplicaSyncSchema.sql)) db.exec(statement)
    db.exec("PRAGMA user_version = $STORE_FORMAT")
    db.exec("INSERT OR IGNORE INTO meta (id, store_id) VALUES (1, ?)", listOf(UUID.randomUUID().toString()))
}

internal fun ReplicaStateStore.meta(db: SQLiteConnection): ReplicaMeta =
    db.queryOne("SELECT store_id, dataset, next_sequence FROM meta WHERE id = 1") {
        ReplicaMeta(it.getText(0), it.textOrNull(1), it.getLong(2))
    } ?: throw ReplicaError.Storage("Missing durable store identity")

/** The first answer names the dataset; every later answer must repeat it. */
internal fun ReplicaStateStore.adoptDataset(db: SQLiteConnection, dataset: String) {
    when (meta(db).dataset) {
        null -> db.exec("UPDATE meta SET dataset = ? WHERE id = 1", listOf(dataset))
        dataset -> Unit
        else -> throw ReplicaError.Protocol("DatasetChanged", "The authoritative dataset changed")
    }
}

internal fun ReplicaStateStore.requireSchema(schema: ReplicaSchema) = write { db ->
    val saved = db.queryOne("SELECT namespace, schema_version FROM meta WHERE id = 1") {
        it.textOrNull(0) to if (it.isNull(1)) null else it.getLong(1)
    } ?: throw ReplicaError.Storage("Missing durable store identity")
    if (saved.first != null && (saved.first != schema.namespace || saved.second != schema.version.toLong())) {
        throw ReplicaError.Storage("Store belongs to another namespace or schema")
    }
    db.exec("UPDATE meta SET namespace = ?, schema_version = ? WHERE id = 1", listOf(schema.namespace, schema.version))
}

internal fun ReplicaStateStore.incarnation(db: SQLiteConnection, stream: String, id: String): String? =
    db.queryString("SELECT incarnation FROM entities WHERE stream = ? AND row_id = ?", listOf(stream, id))

internal fun ReplicaStateStore.setIncarnation(db: SQLiteConnection, stream: String, id: String, shard: String, incarnation: String) {
    val previous = incarnation(db, stream, id)
    if (previous != null && previous != incarnation) {
        db.exec("DELETE FROM entity_references WHERE stream = ? AND row_id = ?", listOf(stream, id))
    }
    db.exec("""
        INSERT INTO entities (stream, row_id, shard, incarnation) VALUES (?, ?, ?, ?)
        ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard,
            predecessor = CASE WHEN entities.incarnation = excluded.incarnation THEN entities.predecessor END,
            incarnation = excluded.incarnation
        """.trimIndent(), listOf(stream, id, shard, incarnation))
}

internal fun ReplicaStateStore.predecessor(db: SQLiteConnection, stream: String, id: String): String? =
    db.queryString("SELECT predecessor FROM entities WHERE stream = ? AND row_id = ?", listOf(stream, id))

/** A cancelled local birth must not become a predecessor the server never saw. */
internal fun ReplicaStateStore.cancelUnsentBirth(db: SQLiteConnection, stream: String, id: String) {
    val previous = predecessor(db, stream, id)
    db.exec("DELETE FROM entity_references WHERE stream = ? AND row_id = ?", listOf(stream, id))
    if (previous == null) {
        db.exec("DELETE FROM entities WHERE stream = ? AND row_id = ?", listOf(stream, id))
    } else {
        db.exec("UPDATE entities SET incarnation = ?, predecessor = NULL WHERE stream = ? AND row_id = ?",
            listOf(previous, stream, id))
    }
}

internal fun ReplicaStateStore.identify(
    db: SQLiteConnection, op: ReplicaOp, schema: ReplicaSchema,
    birth: Boolean = false, preimage: ByteArray? = null,
): ReplicaOp {
    val spec = schema.spec(op.stream)
    // Deletes have removed the visible row; the preimage retains references,
    // including when a held deletion is released after reopening.
    val baseline = snapshot(db, op.stream, op.rowId)?.data
        ?: (preimage?.let(ReplicaPreimage::decode) as? ReplicaPreimage.Row)?.data
    val data = (baseline ?: emptyMap()) + (op.data ?: emptyMap())
    val references = spec?.references.orEmpty().mapNotNull { reference ->
        val id = reference.target(op.rowId, data) ?: return@mapNotNull null
        val lifetime = incarnation(db, reference.stream, id)
        if (snapshot(db, reference.stream, id) == null || lifetime == null) {
            throw ReplicaError.Storage("Missing reference target: ${reference.stream}/$id")
        }
        val bound = if (birth) null else db.queryString("""
            SELECT target_incarnation FROM entity_references
            WHERE stream = ? AND row_id = ? AND name = ? AND target_stream = ? AND target_id = ?
            """.trimIndent(), listOf(op.stream, op.rowId, reference.name, reference.stream, id))
        if (bound != null && bound != lifetime) {
            throw ReplicaError.Storage("Referenced parent lifetime changed; recovery is required")
        }
        ReplicaReference(reference.name, reference.stream, id, lifetime)
    }
    val parent = references.firstOrNull { it.name == spec?.lifetimeFrom }
    val derived = parent?.let { ReplicaLifetime.derived(schema.namespace, op.stream, op.rowId, it) }
    val current = incarnation(db, op.stream, op.rowId)
    if (!birth && derived != null && derived != current) {
        throw ReplicaError.Storage("The parent lifetime changed; this child needs recovery")
    }
    val identity = if (birth) derived ?: UUID.randomUUID().toString() else current
    if (identity == null) throw ReplicaError.Storage("Missing incarnation for ${op.stream}/${op.rowId}")
    setIncarnation(db, op.stream, op.rowId, spec?.shard ?: "user", identity)
    if (birth) {
        db.exec("UPDATE entities SET predecessor = ? WHERE stream = ? AND row_id = ?", listOf(current, op.stream, op.rowId))
    }
    val replaces = if (op.verb == ReplicaOp.Verb.ROW_CREATE && derived == null) predecessor(db, op.stream, op.rowId) else null
    db.exec("DELETE FROM entity_references WHERE stream = ? AND row_id = ?", listOf(op.stream, op.rowId))
    for (reference in references) {
        db.exec("INSERT INTO entity_references VALUES (?, ?, ?, ?, ?, ?)",
            listOf(op.stream, op.rowId, reference.name, reference.stream, reference.id, reference.incarnation))
    }
    return op.copy(incarnation = identity, references = references, replaces = replaces)
}
