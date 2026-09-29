package io.replicaman

import androidx.sqlite.SQLiteConnection

internal class ReplicaPublication {
    val evicted = mutableSetOf<LiveDocuments.Key>()
    val absorbing = mutableListOf<Pair<LiveDocuments.Key, ByteArray>>()

    fun deliver(documents: LiveDocuments) {
        for (key in evicted) documents.evict(key)
        for ((key, fold) in absorbing) documents.absorb(key, listOf(fold))
    }
}

/**
 * Publishes a round inside the caller's transaction: its staged pages in order,
 * then `frames` of its last response, the rebased local authoring, the cursor,
 * and the removal of the accepted intents the round covers. Returns the
 * number of frames applied.
 */
internal fun ReplicaEngine.publishRound(
    db: SQLiteConnection, shard: String, round: ReplicaDownload, frames: List<ReplicaPulledFrame>, cursor: String,
    store: ReplicaStateStore, publication: ReplicaPublication,
): Int {
    preparePublication(db, shard, round.visible)
    var count = 0
    for (page in store.stagedPages(db, shard)) {
        for (frame in store.stagedFrames(db, shard, page)) {
            importBase(frame, db, shard, store)
            count++
        }
    }
    for (frame in frames) {
        importBase(frame, db, shard, store)
        count++
    }
    finishPublication(db, shard, round.reset, cursor, store, publication)
    checkpointFault?.invoke()
    return count
}

private fun preparePublication(db: SQLiteConnection, shard: String, visible: Long) {
    db.exec("CREATE TEMP TABLE IF NOT EXISTS publication_changed (stream TEXT, row_id TEXT, PRIMARY KEY(stream, row_id))")
    db.exec("CREATE TEMP TABLE IF NOT EXISTS publication_seen (stream TEXT, row_id TEXT, PRIMARY KEY(stream, row_id))")
    db.exec("DELETE FROM publication_changed")
    db.exec("DELETE FROM publication_seen")
    db.exec("""
        INSERT OR IGNORE INTO publication_changed
        SELECT o.stream, o.row_id FROM intents o JOIN entities e
        ON e.stream = o.stream AND e.row_id = o.row_id WHERE e.shard = ? AND o.state = 'accepted' AND o.sequence <= ?
        """.trimIndent(), listOf(shard, visible))
    db.exec("""
        DELETE FROM intents WHERE state = 'accepted' AND sequence <= ? AND EXISTS (
            SELECT 1 FROM entities e WHERE e.stream = intents.stream
            AND e.row_id = intents.row_id AND e.shard = ?)
        """.trimIndent(), listOf(visible, shard))
}

private fun ReplicaEngine.importBase(pulled: ReplicaPulledFrame, db: SQLiteConnection, shard: String, store: ReplicaStateStore) {
    val frame = pulled.frame
    val spec = schema.spec(frame.stream)
    if (spec == null || spec.shard != shard) throw ReplicaError.Protocol("UpgradeRequired", "Unknown pulled stream or shard")
    val previous = store.baseRow(db, frame.stream, frame.id)?.takeIf { it.incarnation == pulled.incarnation }
    val row = baseRow(frame, pulled.incarnation, previous, spec)
    if (row == null) {
        db.exec("DELETE FROM base WHERE stream = ? AND row_id = ?", listOf(frame.stream, frame.id))
    } else {
        store.saveBase(db, frame.stream, frame.id, shard, row)
        db.exec("INSERT OR IGNORE INTO publication_seen VALUES (?, ?)", listOf(frame.stream, frame.id))
    }
    db.exec("INSERT OR IGNORE INTO publication_changed VALUES (?, ?)", listOf(frame.stream, frame.id))
}

private fun ReplicaEngine.baseRow(frame: ReplicaFrame, incarnation: String, previous: ReplicaBaseRow?, spec: ReplicaStreamSpec): ReplicaBaseRow? =
    when (frame) {
        is ReplicaFrame.RowDelete -> null
        is ReplicaFrame.RowSet -> ReplicaBaseRow(incarnation, revision(frame.revision), frame.type, frame.data, previous?.codec, previous?.fold)
        is ReplicaFrame.DocSnapshot -> ReplicaBaseRow(incarnation, revision(frame.revision), null, frame.data, frame.codec,
            authoritativeFold(frame.codec, null, frame.snapshot, spec))
        is ReplicaFrame.DocDelta -> {
            if (previous == null || previous.codec != frame.codec ||
                (documentMode != ReplicaDocumentMode.PROJECTIONS_ONLY && previous.fold == null)) {
                ReplicaProtocol.invalid("Document delta has no authoritative baseline")
            }
            ReplicaBaseRow(incarnation, previous.revision, previous.type, previous.data, frame.codec,
                authoritativeFold(frame.codec, previous.fold, frame.payload, spec))
        }
    }

private fun revision(value: Long?): Long = value ?: throw ReplicaError.Storage("Missing frame revision")

private fun ReplicaEngine.authoritativeFold(name: String, baseline: ByteArray?, payload: ByteArray, spec: ReplicaStreamSpec): ByteArray? {
    if (documentMode == ReplicaDocumentMode.PROJECTIONS_ONLY) return null
    val codec = codecs[name] ?: throw ReplicaError.Codec("No codec registered for $name")
    return codec.merge(baseline, payload, spec.reflections).fold
}

private fun ReplicaEngine.finishPublication(
    db: SQLiteConnection, shard: String, reset: Boolean, cursor: String, store: ReplicaStateStore, publication: ReplicaPublication,
) {
    if (reset) {
        db.exec("""
            INSERT OR IGNORE INTO publication_changed SELECT stream, row_id FROM base
            WHERE shard = ? AND NOT EXISTS (SELECT 1 FROM publication_seen s
                WHERE s.stream = base.stream AND s.row_id = base.row_id)
            """.trimIndent(), listOf(shard))
        db.exec("""
            DELETE FROM base WHERE shard = ? AND NOT EXISTS (SELECT 1 FROM publication_seen s
                WHERE s.stream = base.stream AND s.row_id = base.row_id)
            """.trimIndent(), listOf(shard))
    }
    db.prepare("SELECT stream, row_id FROM publication_changed").use { rows ->
        while (rows.step()) materializeBase(db, rows.getText(0), rows.getText(1), shard, store, publication)
    }
    store.setCursor(db, cursor, shard)
    store.discardDownload(db, shard)
}

internal fun ReplicaEngine.materializeBase(
    db: SQLiteConnection, stream: String, id: String, shard: String, store: ReplicaStateStore, publication: ReplicaPublication,
) {
    val base = store.baseRow(db, stream, id)
    val current = store.incarnation(db, stream, id)
    if (current != null && current != base?.incarnation) {
        // A birth newer than this view remains optimistic until its own result.
        if (store.hasLocalBirth(db, stream, id, current)) return
        removeActiveEntity(db, stream, id, "Entity left this view or changed lifetime", store)
        publication.evicted += LiveDocuments.Key(stream, id)
    }
    if (base == null) return
    store.setIncarnation(db, stream, id, shard, base.incarnation)
    var row = ReplicaStateStore.SnapshotRow(stream, id, base.type, base.data)
    val held = if (store.hold(db, stream, id) != null) store.snapshot(db, stream, id) else null
    if (held != null) {
        val pushed = schema.spec(stream)?.pushed
        row = held.copy(data = held.data + base.data.filterKeys { pushed != null && it !in pushed })
        store.setHoldPreimage(db, stream, id, ReplicaPreimage.Row(shard, base.type, base.data).encoded())
    }

    if (base.fold != null && base.codec != null && documentMode != ReplicaDocumentMode.PROJECTIONS_ONLY) {
        val codec = codecs[base.codec] ?: throw ReplicaError.Codec("No codec registered for ${base.codec}")
        val doc = store.doc(db, stream, id)
        val merged = codec.merge(doc?.fold, base.fold, schema.spec(stream)?.reflections.orEmpty())
        val acked = codec.mergeVersions(doc?.acked, codec.payloadVersion(base.fold))
        store.upsertDoc(db, stream, id, shard, base.codec, merged.fold, acked, doc?.peer ?: peerMinter())
        row = row.copy(data = row.data + merged.reflected)
        publication.absorbing += LiveDocuments.Key(stream, id) to merged.fold
    }
    val projected = rebaseRow(db, row, stream, id, shard, store)
    publishRow(projected, db, stream, id, shard, store)
}

private fun removeActiveEntity(db: SQLiteConnection, stream: String, id: String, reason: String, store: ReplicaStateStore) {
    store.archiveEntity(db, stream, id, reason)
    store.deleteDoc(db, stream, id)
    store.deleteSnapshot(db, stream, id)
    // Refused writes remain visible until the caller dismisses their reason;
    // frozen ones wait for their own verdict.
    db.exec("DELETE FROM intents WHERE row_id = ? AND stream = ? AND state IN ('draft', 'owed', 'accepted')", listOf(id, stream))
    store.dropHold(db, stream, id)
    // Keep the last observed incarnation for deliberate recreation of this address.
    db.exec("DELETE FROM entity_references WHERE stream = ? AND row_id = ?", listOf(stream, id))
}
