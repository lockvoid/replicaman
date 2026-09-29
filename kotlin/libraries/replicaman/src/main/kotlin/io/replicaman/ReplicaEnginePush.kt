package io.replicaman

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

/**
 * Sends the oldest frozen submissions and applies the answer before the next
 * batch leaves. A failed request leaves its frozen bytes for a retry that sends
 * exactly the same operations. A store that has never synchronized learns its
 * dataset from one pull page first. Verdicts are reported by intent id.
 */
internal suspend fun ReplicaEngine.transmitSubmissions(store: ReplicaStateStore, lane: ReplicaLane?, transport: ReplicaTransport): List<ReplicaVerdict> {
    val connection = ReplicaConnection(transport, schema)
    val verdicts = mutableListOf<ReplicaVerdict>()
    repeat(ReplicaEngine.MAX_DRAIN_PASSES) {
        currentCoroutineContext().ensureActive()
        val (submissions, dataset) = store.write { db ->
            store.freezeSubmissions(db, lane, ReplicaEngine.MAX_OPS_PER_PUSH) to store.meta(db).dataset
        }
        if (submissions.isEmpty()) return verdicts
        val response = connection.send(ReplicaEndpoint.PUSH, dataset ?: learnDataset(store, transport), mapOf(
            "ops" to ReplicaValue.Arr(submissions.flatMap { it.operations }),
        ))
        verdicts += reconcileVerdicts(decodeVerdicts(response, submissions), submissions, store)
    }
    return verdicts
}

private suspend fun ReplicaEngine.learnDataset(store: ReplicaStateStore, transport: ReplicaTransport): String {
    downloadPage(schema.shards.first(), store, transport)
    return store.read { store.meta(it).dataset } ?: throw ReplicaError.Storage("The first pull did not record its dataset")
}

/**
 * The whole answer is validated before anything local changes: one verdict per
 * operation, in request order, and one outcome per submission. A missing,
 * duplicate or foreign verdict acknowledges nothing.
 */
private fun decodeVerdicts(response: ReplicaValue, submissions: List<ReplicaSubmission>): List<List<ReplicaVerdict>> {
    val answered = response.requiredArray("verdicts")
    if (answered.size != submissions.sumOf { it.ids.size }) ReplicaProtocol.invalid("Push did not answer every operation once")
    var offset = 0
    return submissions.map { submission ->
        val verdicts = submission.ids.mapIndexed { index, id ->
            val value = answered[offset + index]
            if (value.requiredText("id") != id) ReplicaProtocol.invalid("Push answered a different operation")
            val outcome = ReplicaVerdict.Outcome.fromRaw(value.requiredText("outcome")) ?: ReplicaProtocol.invalid("Unknown verdict outcome")
            ReplicaVerdict(id, outcome, value.optionalText("reason"))
        }
        offset += verdicts.size
        if (verdicts.any { it.outcome != verdicts.first().outcome }) ReplicaProtocol.invalid("One submission received different outcomes")
        verdicts
    }
}

private fun ReplicaEngine.reconcileVerdicts(
    answered: List<List<ReplicaVerdict>>, submissions: List<ReplicaSubmission>, store: ReplicaStateStore,
): List<ReplicaVerdict> {
    val reported = mutableListOf<ReplicaVerdict>()
    val rejected = mutableListOf<Pair<ReplicaOp, String>>()
    val publication = ReplicaPublication()
    liveDocuments.publishing {
        store.write { db ->
            val changed = mutableSetOf<LiveDocuments.Key>()
            val takenByRefusedBirths = mutableSetOf<String>()
            for ((submission, verdicts) in submissions.zip(answered)) {
                for ((entry, verdict) in submission.entries.zip(verdicts)) {
                    val op = entry.op()
                    reported += ReplicaVerdict(entry.id, verdict.outcome, verdict.reason)
                    if (store.incarnation(db, op.stream, op.rowId) != op.incarnation) {
                        // The verdict cannot mutate a replacement lifetime. Archived bytes remain recoverable.
                        store.consume(db, entry.id)
                        continue
                    }
                    val key = LiveDocuments.Key(op.stream, op.rowId)
                    changed += key
                    when (verdict.outcome) {
                        ReplicaVerdict.Outcome.ACCEPTED -> {
                            advanceAcked(db, op, store)
                            store.accept(db, entry.id)
                        }
                        ReplicaVerdict.Outcome.REJECTED -> {
                            val reason = verdict.reason ?: "Mutation refused"
                            if (entry.id in takenByRefusedBirths) {
                                store.consume(db, entry.id)
                            } else if (op.verb == ReplicaOp.Verb.DOC_DELTA) {
                                // Later deltas can include rejected history. Keep
                                // the whole branch as evidence and restore the base.
                                store.archiveEntity(db, op.stream, op.rowId, reason)
                                store.deleteDoc(db, op.stream, op.rowId)
                                store.discardEntries(db, op.stream, op.rowId)
                                store.refuse(db, entry.id, reason)
                                store.dropHold(db, op.stream, op.rowId)
                                publication.evicted += key
                            } else {
                                takenByRefusedBirths += rejectRow(db, entry, op, reason, store)
                            }
                            rejected += op to reason
                        }
                    }
                }
                store.finishSubmission(db, submission.sequence)
            }
            for (key in changed) materializeBase(db, key.stream, key.id, schema.spec(key.stream)?.shard ?: "user", store, publication)
        }
        publication.deliver(liveDocuments)
    }
    revertedCount += rejected.size
    for ((op, reason) in rejected) onRejected?.invoke(op, reason)
    return reported
}
