package io.replicaman

import androidx.sqlite.SQLiteConnection

/**
 * What one `/pull` request did: the frames it published, whether the shard has
 * more to pull, and — when the request found the round could not be published —
 * the failure the round was forgotten for.
 */
internal data class PullStep(val applied: Int, val more: Boolean, val forgotten: ReplicaError? = null) {
    companion object {
        val STAGED = PullStep(0, true)

        fun published(count: Int): PullStep = PullStep(count, false)

        fun restarted(reason: ReplicaError): PullStep = PullStep(0, true, reason)
    }
}

/**
 * One `/pull` request of the shard's current round, under the caller's wire
 * operation. An answer with `more` is staged; the answer that completes the
 * round publishes it whole. The first answer a store receives names its dataset.
 */
internal suspend fun ReplicaEngine.downloadPage(shard: String, store: ReplicaStateStore, transport: ReplicaTransport): PullStep {
    store.requireDocumentMode(documentMode)
    val (dataset, round, generation) = store.read { db ->
        Triple(store.meta(db).dataset, store.round(db, shard), store.readGeneration(db, shard))
    }

    // A reset, a recovery or another request that moved this round while the answer was out wins.
    fun current(db: SQLiteConnection): Boolean =
        store.readGeneration(db, shard) == generation && store.round(db, shard).let { it.cursor == round.cursor && it.reset == round.reset }

    fun forget(reason: ReplicaError): PullStep {
        store.write { db -> if (current(db)) store.clearCursor(db, shard) }
        return PullStep.restarted(reason)
    }

    val response = try {
        ReplicaConnection(transport, schema).send(ReplicaEndpoint.PULL, dataset, mapOf(
            "shard" to ReplicaValue.Str(shard),
            "cursor" to (round.cursor?.let(ReplicaValue::Str) ?: ReplicaValue.Null),
            "limit" to ReplicaValue.Integer(batchLimit.toLong()),
        ))
    } catch (error: ReplicaError.Protocol) {
        if (error.code != "CursorInvalid" || round.cursor == null) throw error
        return forget(error)
    }
    val page = ReplicaPullPage.decode(response)
    if (page.shard != shard || page.reset != (round.cursor == null)) ReplicaProtocol.invalid("Pull answered another shard or round")
    // A frame this build cannot hold is refused as it arrives, before anything is staged.
    for (pulled in page.frames) {
        val spec = schema.spec(pulled.frame.stream)
            ?: throw ReplicaError.Protocol("UpgradeRequired", "Pulled frame names an undeclared stream: ${pulled.frame.stream}")
        if (spec.shard != shard) ReplicaProtocol.invalid("Pulled frame belongs to another shard")
    }
    val served = response.requiredText("dataset")

    if (page.more) {
        store.write { db ->
            store.adoptDataset(db, served)
            if (current(db)) store.stage(db, shard, round, response.requiredArray("frames"), page.cursor)
        }
        return PullStep.STAGED
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
        return forget(error)
    } catch (error: ReplicaError.MissingCausalDeps) {
        // History the base cannot absorb means the base is no longer the server's: only a baseline
        // replaces it. A baseline that cannot be absorbed is the server's fault and throws.
        if (round.reset) throw error
        return forget(error)
    }
    return if (published == null) PullStep.STAGED else PullStep.published(published)
}
