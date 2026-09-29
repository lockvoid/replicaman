package io.replicaman

import androidx.sqlite.SQLiteConnection

/** Compute the final optimistic row before changing its observable snapshot. */
internal fun ReplicaEngine.rebaseRow(
    db: SQLiteConnection,
    initial: ReplicaStateStore.SnapshotRow?,
    stream: String,
    id: String,
    shard: String,
    store: ReplicaStateStore,
    entries: List<ReplicaStateStore.JournalRow>? = null,
): ReplicaStateStore.SnapshotRow? {
    var row = initial
    val incarnation = store.incarnation(db, stream, id)
    if (entries == null) {
        for (op in store.overlays(db, stream, id)) {
            if (op.incarnation == incarnation) row = project(op, row)
        }
    }

    val owed = entries ?: store.entriesAddressing(db, stream, id)
    for (entry in owed) {
        if (entry.parked != null) continue
        val op = entry.op()
        if (op.incarnation != incarnation || op.verb == ReplicaOp.Verb.DOC_DELTA) continue
        store.updatePreimage(db, entry.id, rowPreimage(op, row, shard).encoded())
        row = project(op, row)
    }
    return row
}

internal fun ReplicaEngine.rebaseOwedWrites(
    db: SQLiteConnection, stream: String, id: String, shard: String,
    store: ReplicaStateStore, entries: List<ReplicaStateStore.JournalRow>,
) {
    val initial = store.snapshot(db, stream, id)
    val row = rebaseRow(db, initial, stream, id, shard, store, entries)
    publishRow(row, db, stream, id, shard, store)
}

internal fun publishRow(
    row: ReplicaStateStore.SnapshotRow?, db: SQLiteConnection,
    stream: String, id: String, shard: String, store: ReplicaStateStore,
) {
    if (row == null) store.deleteSnapshot(db, stream, id)
    else store.upsertSnapshot(db, stream, id, shard, row.type, row.data)
}

private fun project(op: ReplicaOp, row: ReplicaStateStore.SnapshotRow?): ReplicaStateStore.SnapshotRow? =
    when (op.verb) {
        ReplicaOp.Verb.ROW_CREATE -> ReplicaStateStore.SnapshotRow(
            op.stream, op.rowId, op.type ?: row?.type, row?.data.orEmpty() + op.data.orEmpty(),
        )
        ReplicaOp.Verb.ROW_PATCH -> row?.copy(data = row.data + op.data.orEmpty())
        ReplicaOp.Verb.ROW_DELETE -> null
        // The durable authoring fold already contains document deltas.
        // Journal decoding refuses unknown verbs before reaching this reducer.
        else -> row
    }

private fun rowPreimage(
    op: ReplicaOp, row: ReplicaStateStore.SnapshotRow?, shard: String,
): ReplicaPreimage {
    if (row == null) return ReplicaPreimage.Absent
    if (op.verb == ReplicaOp.Verb.ROW_PATCH) {
        val fields = op.data.orEmpty().keys
        return ReplicaPreimage.Fields(row.data.filterKeys { it in fields }, (fields - row.data.keys).sorted())
    }
    return ReplicaPreimage.Row(shard, row.type, row.data)
}
