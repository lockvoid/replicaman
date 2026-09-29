import Foundation
import GRDB

extension ReplicaEngine {
    /// Frozen submissions, oldest first, one push at a time: the next batch
    /// leaves only after the previous answer is applied. A failed request
    /// leaves its frozen bytes for a retry that sends exactly the same ids.
    func transmitSubmissions(
        store: ReplicaStateStore, lane: ReplicaLane?, transport: any ReplicaTransport
    ) async throws -> [ReplicaVerdict] {
        let connection = ReplicaConnection(transport: transport, schema: schema)
        var verdicts: [ReplicaVerdict] = []

        for _ in 0..<Self.maxDrainPasses {
            try Task.checkCancellation()
            let (submissions, dataset) = try await store.pool.write { db in
                (try store.freezeSubmissions(db, lane: lane, limit: Self.maxOpsPerPush), try store.meta(db).dataset)
            }
            if submissions.isEmpty { break }
            let known: String
            if let dataset {
                known = dataset
            } else {
                known = try await learnDataset(store: store, connection: connection)
            }
            let answered = try await connection.push(submissions, dataset: known)
            try reconcile(answered, submissions: submissions, store: store)
            verdicts += answered
        }
        return verdicts
    }

    /// A store that has never synchronized learns its dataset from its first
    /// pull: the first page of a round, staged or published as any other.
    private func learnDataset(store: ReplicaStateStore, connection: ReplicaConnection) async throws -> String {
        _ = try await downloadPage(shard: schema.shards[0], store: store, connection: connection)
        guard let dataset = try await store.pool.read({ try store.meta($0).dataset }) else {
            throw ReplicaError.storage("The first pull did not record its dataset")
        }
        return dataset
    }

    private func reconcile(
        _ verdicts: [ReplicaVerdict], submissions: [ReplicaSubmission], store: ReplicaStateStore
    ) throws {
        var rejected: [(ReplicaOp, String)] = []
        var evicted = Set<LiveDocuments.Key>()
        var absorbing: [(LiveDocuments.Key, Data)] = []
        try liveDocuments.publishing {
            try store.pool.write { db in
                var changed = Set<LiveDocuments.Key>()
                var takenByRefusedBirth = Set<String>()
                var answered = verdicts[...]
                for submission in submissions {
                    for entry in submission.entries {
                        let verdict = answered.removeFirst()
                        let op = try entry.op()
                        let key = LiveDocuments.Key(stream: op.stream, id: op.rowId)
                        let sameLife = try store.incarnation(db, stream: op.stream, id: op.rowId) == op.incarnation
                        if !sameLife {
                            // A verdict for an archived lifetime must consume its
                            // intent without changing the replacement entity.
                            try store.consume(db, id: entry.id)
                            continue
                        }
                        changed.insert(key)
                        switch verdict.outcome {
                        case .accepted:
                            try advanceAcked(db, op: op, store: store)
                            try store.accept(db, id: entry.id)
                        case .rejected:
                            let reason = verdict.reason ?? "Mutation refused"
                            if takenByRefusedBirth.contains(entry.id) {
                                // Its birth was refused earlier in this answer; that refusal
                                // archived the branch and stays as the evidence.
                                try store.consume(db, id: entry.id)
                            } else if op.verb == ReplicaOp.Verb.docDelta {
                                // A later delta may contain the rejected history.
                                // Preserve that whole branch for recovery and
                                // restore only the authoritative fold for editing.
                                try store.archiveEntity(db, stream: op.stream, id: op.rowId, reason: reason)
                                try store.deleteDoc(db, stream: op.stream, rowId: op.rowId)
                                try store.discardEntries(db, stream: op.stream, rowId: op.rowId, except: entry.id)
                                try store.refuse(db, id: entry.id, reason: reason)
                                try store.dropHold(db, stream: op.stream, rowId: op.rowId)
                                evicted.insert(key)
                            } else {
                                _ = try rejectRow(db, entry: entry, op: op, reason: reason, store: store)
                                if op.verb == ReplicaOp.Verb.rowCreate {
                                    takenByRefusedBirth.formUnion(try store.frozenIntents(
                                        db, stream: op.stream, rowId: op.rowId, incarnation: op.incarnation, except: entry.id))
                                }
                            }
                            rejected.append((op, reason))
                        }
                    }
                    try store.finishSubmission(db, sequence: submission.sequence)
                }
                for key in changed {
                    try materializeBase(db, stream: key.stream, id: key.id,
                        shard: schema.spec(key.stream)?.shard ?? "user", store: store,
                        evicted: &evicted, absorbing: &absorbing)
                }
            }
            for key in evicted { liveDocuments.evict(key) }
            for (key, fold) in absorbing { liveDocuments.absorb(key, payloads: [fold]) }
        }
        revertedCount += rejected.count
        for (op, reason) in rejected { onRejected?(op, reason) }
    }
}
