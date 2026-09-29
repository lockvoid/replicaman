package io.replicaman

import androidx.sqlite.SQLiteConnection

/**
 * One frozen submission: its operations exactly as every retry sends them, their
 * UUIDs in order, and the frozen intents they answer for, position by position.
 */
internal data class ReplicaSubmission(
    val sequence: Long,
    val operations: List<ReplicaValue>,
    val ids: List<String>,
    val entries: List<ReplicaStateStore.JournalRow>,
)

/** An owed intent on its way into a submission. */
internal class ReplicaFreezing(val id: String, val op: ReplicaOp)

/** The intents `selection` names, in its order, about to freeze. */
internal fun ReplicaStateStore.freezing(db: SQLiteConnection, selection: String, arguments: List<Any?>): List<ReplicaFreezing> =
    db.query("SELECT id, payload FROM intents WHERE $selection", arguments) {
        ReplicaFreezing(it.getText(0), ReplicaStateStore.JournalRow.decodeOperation(it.getText(1).toByteArray(Charsets.UTF_8)))
    }

/**
 * The oldest frozen submissions one push carries. When nothing is frozen, up to
 * `limit` owed intents of the lane freeze first, each its own submission; an
 * atomic action froze as one group when it was written (`freezeAtomicWrite`).
 */
internal fun ReplicaStateStore.freezeSubmissions(db: SQLiteConnection, lane: ReplicaLane?, limit: Int): List<ReplicaSubmission> {
    val frozen = frozenSubmissions(db)
    if (frozen.isNotEmpty()) return frozen
    var bytes = 0L
    val owed = freezing(db, "state = 'owed' AND (? IS NULL OR lane = ?) ORDER BY rowid LIMIT ?",
        listOf(lane?.rawValue, lane?.rawValue, limit))
    for (entry in owed) {
        val operations = submissionOperations(listOf(entry), group = null)
        val content = submissionContent(operations)
        if (content.size > ReplicaProtocol.ENTITY_BYTES) {
            throw ReplicaError.Storage("Mutation exceeds the entity limit; local bytes are retained")
        }
        if (bytes + content.size > ReplicaProtocol.ENTITY_BYTES) break
        insertSubmission(db, content, listOf(entry), operations)
        bytes += content.size
    }
    return frozenSubmissions(db)
}

/** The intents as the operations of one submission: each with a fresh UUID, sharing `group` when given. */
internal fun submissionOperations(entries: List<ReplicaFreezing>, group: String?): List<ReplicaOp> =
    entries.map { entry ->
        if (entry.op.incarnation == null) throw ReplicaError.Storage("Journal entry has no incarnation")
        entry.op.copy(id = ReplicaID.uuid7(), group = group)
    }

internal fun submissionContent(operations: List<ReplicaOp>): ByteArray =
    ReplicaJSON.encodeToBytes(ReplicaValue.Arr(operations.map { it.toValue() }))

/** Freeze `entries` under the next sequence, each intent carrying the UUID its operation goes out with. */
internal fun ReplicaStateStore.insertSubmission(
    db: SQLiteConnection, content: ByteArray, entries: List<ReplicaFreezing>, operations: List<ReplicaOp>,
) {
    val sequence = meta(db).nextSequence
    if (sequence == Long.MAX_VALUE) throw ReplicaError.Storage("Submission sequence exhausted; recovery is required")
    db.exec("INSERT INTO submissions (sequence, content) VALUES (?, ?)", listOf(sequence, content))
    for ((entry, operation) in entries.zip(operations)) {
        db.exec("UPDATE intents SET state = 'frozen', sequence = ?, operation = ? WHERE id = ?",
            listOf(sequence, operation.id, entry.id))
    }
    db.exec("UPDATE meta SET next_sequence = ? WHERE id = 1", listOf(sequence + 1))
}

/** Oldest first, at most one push's operations and bytes, never splitting a submission. */
internal fun ReplicaStateStore.frozenSubmissions(db: SQLiteConnection): List<ReplicaSubmission> {
    val submissions = mutableListOf<ReplicaSubmission>()
    var count = 0
    var bytes = 0L
    db.prepare("SELECT sequence, content FROM submissions ORDER BY sequence").use { rows ->
        while (rows.step()) {
            val sequence = rows.getLong(0)
            val content = rows.getBlob(1)
            val operations = ReplicaProtocol.decode(content).items ?: throw ReplicaError.Storage("Frozen submission is not an array")
            if (submissions.isNotEmpty() &&
                (count + operations.size > ReplicaProtocol.MAX_OPERATIONS || bytes + content.size > ReplicaProtocol.ENTITY_BYTES)) break
            val intents = db.query("SELECT operation, id, op, payload, preimage FROM intents WHERE sequence = ? AND state = 'frozen'", listOf(sequence)) {
                it.getText(0) to ReplicaStateStore.JournalRow(it.getText(1), it.getText(2),
                    it.getText(3).toByteArray(Charsets.UTF_8), it.textOrNull(4)?.toByteArray(Charsets.UTF_8), sent = true)
            }.toMap()
            val frozen = operations.map(ReplicaOp::fromValue)
            val entries = frozen.map { intents[it.id] ?: throw ReplicaError.Storage("Frozen submission and its intents disagree") }
            if (intents.size != frozen.size || frozen.zip(entries).any { (operation, entry) -> operation.copy(id = entry.id, group = null) != entry.op() }) {
                throw ReplicaError.Storage("Frozen submission and its intents disagree")
            }
            submissions += ReplicaSubmission(sequence, operations, frozen.map { it.id }, entries)
            count += operations.size
            bytes += content.size
        }
    }
    return submissions
}

/** Accepted: stays visible until a round that began after its acceptance publishes. */
internal fun ReplicaStateStore.accept(db: SQLiteConnection, id: String) {
    db.exec("UPDATE intents SET state = 'accepted', operation = NULL WHERE id = ?", listOf(id))
}

internal fun ReplicaStateStore.refuse(db: SQLiteConnection, id: String, reason: String) {
    db.exec("UPDATE intents SET state = 'refused', reason = ?, sequence = NULL, operation = NULL WHERE id = ?", listOf(reason, id))
}

/** A verdict that leaves nothing behind: the intent is gone with its answer. */
internal fun ReplicaStateStore.consume(db: SQLiteConnection, id: String) {
    db.exec("DELETE FROM intents WHERE id = ?", listOf(id))
}

internal fun ReplicaStateStore.finishSubmission(db: SQLiteConnection, sequence: Long) {
    db.exec("DELETE FROM submissions WHERE sequence = ?", listOf(sequence))
}

internal fun ReplicaStateStore.overlays(db: SQLiteConnection, stream: String, id: String): List<ReplicaOp> = db.query("""
    SELECT payload FROM intents WHERE row_id = ? AND stream = ? AND state = 'accepted' ORDER BY sequence, rowid
    """.trimIndent(), listOf(id, stream)) { ReplicaStateStore.JournalRow.decodeOperation(it.getText(0).toByteArray(Charsets.UTF_8)) }

/** The highest accepted local sequence: what a round starting now covers when it publishes. */
internal fun ReplicaStateStore.acceptedThrough(db: SQLiteConnection): Long =
    db.queryLong("SELECT MAX(sequence) FROM intents WHERE state = 'accepted'") ?: 0
