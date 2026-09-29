package io.replicaman

import androidx.sqlite.SQLiteConnection

/**
 * Save a row action and queue it as one server transaction. All gates must admit
 * every member. A hold, discard, unfrozen dependency or size limit rolls back
 * the entire local action. Committed groups are immutable across retries/restart.
 */
public fun <T> ReplicaEngine.writeAtomically(body: (ReplicaTransaction) -> T): T {
    if (ReplicaTransaction.open(this) != null || ReplicaEngine.currentDraftLocal.get() != null) {
        throw ReplicaError.AtomicWriteBlocked("Atomic writes cannot be nested or captured inside a draft")
    }
    return write { tx ->
        val captured = mutableListOf<String>()
        tx.atomicEntries = captured
        val value = body(tx)
        tx.store.freezeAtomicWrite(tx.db, captured)
        value
    }
}

internal fun ReplicaEngine.validateAtomicAddress(db: SQLiteConnection, stream: String, id: String, store: ReplicaStateStore) {
    val captured = ReplicaTransaction.open(this)?.atomicEntries ?: return
    val earlier = store.entriesAddressing(db, stream, id).any {
        it.parked == null && !it.sent && it.id !in captured
    }
    if (earlier || store.hold(db, stream, id) != null) {
        throw ReplicaError.AtomicWriteBlocked("Unsubmitted dependency: $stream/$id")
    }
}

internal fun ReplicaEngine.validateAtomicAdmission(db: SQLiteConnection, op: ReplicaOp, preimage: ByteArray?, store: ReplicaStateStore) {
    validateAtomicAddress(db, op.stream, op.rowId, store)
    for (reference in op.references) validateAtomicAddress(db, reference.stream, reference.id, store)
    when (val outcome = syncGates.judge(change(op, preimage))) {
        SyncGates.Outcome.Push -> Unit
        is SyncGates.Outcome.Hold -> throw ReplicaError.AtomicWriteBlocked("${outcome.gate}: ${outcome.reason}")
        SyncGates.Outcome.Discard -> throw ReplicaError.AtomicWriteBlocked("A gate would discard ${op.stream}/${op.rowId}")
    }
}

internal fun ReplicaStateStore.freezeAtomicWrite(db: SQLiteConnection, ids: List<String>) {
    if (ids.isEmpty()) return
    if (ids.size > ReplicaProtocol.MAX_OPERATIONS) throw ReplicaError.AtomicWriteBlocked("An atomic write supports at most 100 operations")
    val selection = "id IN (${questionMarks(ids.size)})"
    val bytes = db.queryLong("SELECT SUM(length(CAST(payload AS BLOB))) FROM intents WHERE $selection", ids) ?: 0
    if (bytes > ReplicaProtocol.ENTITY_BYTES) throw ReplicaError.AtomicWriteBlocked("An atomic write exceeds 32 MiB")
    val entries = freezing(db, "$selection ORDER BY rowid", ids)
    if (entries.isEmpty()) return
    val operations = submissionOperations(entries, group = if (entries.size > 1) ReplicaID.uuid7() else null)
    val content = submissionContent(operations)
    if (content.size > ReplicaProtocol.ENTITY_BYTES) throw ReplicaError.AtomicWriteBlocked("An atomic write exceeds 32 MiB")
    insertSubmission(db, content, entries, operations)
}
