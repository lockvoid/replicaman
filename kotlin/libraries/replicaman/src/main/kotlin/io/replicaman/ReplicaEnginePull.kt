package io.replicaman

import androidx.sqlite.SQLiteConnection

/**
 * One `/pull` request of the shard's current round, under the caller's wire
 * operation. An answer with `more` is staged; the answer that completes the
 * round publishes it whole. Returns the frames published and whether the shard
 * has more to pull. The first answer a store receives names its dataset.
 */
internal suspend fun ReplicaEngine.downloadPage(shard: String, store: ReplicaStateStore, transport: ReplicaTransport): Pair<Int, Boolean> {
    store.requireDocumentMode(documentMode)
    val (dataset, round, generation) = store.read { db ->
        Triple(store.meta(db).dataset, store.round(db, shard), store.readGeneration(db, shard))
    }

    // A reset, a recovery or another request that moved this round while the answer was out wins.
    fun current(db: SQLiteConnection): Boolean =
        store.readGeneration(db, shard) == generation && store.round(db, shard).let { it.cursor == round.cursor && it.reset == round.reset }

    val response = try {
        ReplicaConnection(transport, schema).send(ReplicaEndpoint.PULL, dataset, mapOf(
            "shard" to ReplicaValue.Str(shard),
            "cursor" to (round.cursor?.let(ReplicaValue::Str) ?: ReplicaValue.Null),
            "limit" to ReplicaValue.Integer(batchLimit.toLong()),
        ))
    } catch (error: ReplicaError.Protocol) {
        if (error.code != "CursorInvalid" || round.cursor == null) throw error
        store.write { db -> if (current(db)) store.clearCursor(db, shard) }
        return 0 to true
    }
    val page = ReplicaPullPage.decode(response)
    if (page.shard != shard || page.reset != (round.cursor == null)) ReplicaProtocol.invalid("Pull answered another shard or round")
    val served = response.requiredText("dataset")

    if (page.more) {
        store.write { db ->
            store.adoptDataset(db, served)
            if (current(db)) store.stage(db, shard, round, response.requiredArray("frames"), page.cursor)
        }
        return 0 to true
    }
    val publication = ReplicaPublication()
    val published = try {
        liveDocuments.publishing {
            store.write { db ->
                store.adoptDataset(db, served)
                if (current(db)) publishRound(db, shard, round, page.frames, page.cursor, store, publication) else null
            }?.also { publication.deliver(liveDocuments) }
        }
    } catch (error: ReplicaError.Protocol) {
        // A staged baseline the server can no longer answer coherently is forgotten, not resumed: the
        // shard bootstraps again. An incremental round keeps its checkpoint and throws; a first page throws.
        if (!round.reset || round.cursor == null) throw error
        store.write { db -> if (current(db)) store.clearCursor(db, shard) }
        return 0 to true
    }
    return if (published == null) 0 to true else published to false
}
