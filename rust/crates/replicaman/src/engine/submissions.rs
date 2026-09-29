use super::*;
use crate::protocol::{self, Connection};
use crate::sync_store::Submission;

impl ReplicaEngine {
    /// Push the oldest frozen submissions, at most 100 operations at a time;
    /// the next batch goes only after the previous answer is applied.
    pub(super) async fn transmit_submissions(
        &self,
        store: &Arc<ReplicaStateStore>,
        lane: Option<ReplicaLane>,
        transport: &dyn ReplicaTransport,
    ) -> ReplicaResult<DrainReport> {
        let connection = Connection {
            transport,
            schema: &self.schema,
        };
        let mut verdicts = Vec::new();
        for _ in 0..MAX_DRAIN_PASSES {
            let (submissions, dataset) = store.pool().write(|ctx| {
                Ok((
                    store.freeze_submissions(&ctx.tx, lane, protocol::MAX_OPERATIONS)?,
                    store.meta(&ctx.tx)?.dataset,
                ))
            })?;
            if submissions.is_empty() {
                break;
            }
            let dataset = match dataset {
                Some(dataset) => dataset,
                None => self.learn_dataset(store, transport).await?,
            };
            let operations: Vec<ReplicaValue> = submissions
                .iter()
                .flat_map(|submission| submission.operations.iter().cloned())
                .collect();
            let answer = connection.push(&operations, &dataset).await?;
            self.reconcile_results(&answer, &submissions, store)?;
            verdicts.extend(answer);
        }
        Ok(DrainReport {
            verdicts,
            held: HashSet::new(),
        })
    }

    /// A push carries the dataset, and only a pull answer names it: a store
    /// that never synchronized pulls one page of a round before its first push.
    async fn learn_dataset(
        &self,
        store: &Arc<ReplicaStateStore>,
        transport: &dyn ReplicaTransport,
    ) -> ReplicaResult<String> {
        {
            let _flight = self.pull_serial.lock().await;
            self.download_page(&self.schema.shards()[0], store, transport)
                .await?;
        }
        store
            .pool()
            .read(|db| store.meta(db))?
            .dataset
            .ok_or_else(|| {
                ReplicaError::Storage("The first pull did not record its dataset".into())
            })
    }

    /// Validates the whole answer before changing local state: one verdict per
    /// operation in request order, one outcome per submission.
    fn reconcile_results(
        &self,
        verdicts: &[ReplicaVerdict],
        submissions: &[Submission],
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<()> {
        let ids: Vec<&String> = submissions
            .iter()
            .flat_map(|submission| &submission.ids)
            .collect();
        if verdicts.len() != ids.len()
            || verdicts
                .iter()
                .zip(&ids)
                .any(|(verdict, id)| verdict.id != **id)
        {
            return Err(protocol::invalid(
                "Push verdicts do not answer the submitted operations",
            ));
        }
        let mut answers = Vec::with_capacity(submissions.len());
        let mut offset = 0;
        for submission in submissions {
            let answer = &verdicts[offset..offset + submission.ids.len()];
            offset += submission.ids.len();
            if answer
                .iter()
                .any(|verdict| verdict.outcome != answer[0].outcome)
            {
                return Err(protocol::invalid("One submission received mixed verdicts"));
            }
            answers.push(answer);
        }
        let _serial = self.checkpoint_serial.lock();
        let mut rejected = Vec::new();
        let mut evicted = HashSet::new();
        store.pool().write(|ctx| {
            let mut changed = HashSet::new();
            let mut taken_by_refused_birth = HashSet::new();
            for (submission, answer) in submissions.iter().zip(&answers) {
                for (entry, verdict) in submission.entries.iter().zip(answer.iter()) {
                    let op = entry.op()?;
                    if store.incarnation(&ctx.tx, &op.stream, &op.row_id)? != op.incarnation {
                        // Consume results for archived lifetimes without changing
                        // a replacement row or its authoring document.
                        store.remove_entry(ctx, &entry.id)?;
                        continue;
                    }
                    changed.insert((op.stream.clone(), op.row_id.clone()));
                    match verdict.outcome {
                        VerdictOutcome::Accepted => {
                            self.advance_acked(ctx, &op, store)?;
                            store.accept(&ctx.tx, &entry.id)?;
                        }
                        VerdictOutcome::Rejected => {
                            let reason = verdict.reason.as_deref().unwrap_or("Mutation refused");
                            if taken_by_refused_birth.contains(&entry.id) {
                                // Its birth was refused earlier in this answer; that
                                // refusal archived the branch and stays the evidence.
                                store.remove_entry(ctx, &entry.id)?;
                            } else if op.verb == verb::DOC_DELTA {
                                // Later deltas can contain rejected history. Keep
                                // the full branch for recovery, then restore base.
                                store.archive_entity(&ctx.tx, &op.stream, &op.row_id, reason)?;
                                store.delete_doc(ctx, &op.stream, &op.row_id)?;
                                store.discard_entries(ctx, &op.stream, &op.row_id, None)?;
                                store.park(ctx, &entry.id, reason)?;
                                store.drop_gate_hold(ctx, &op.stream, &op.row_id)?;
                                evicted.insert((op.stream.clone(), op.row_id.clone()));
                            } else {
                                self.reject_row(ctx, entry, &op, reason, store)?;
                                if op.verb == verb::ROW_CREATE {
                                    taken_by_refused_birth.extend(store.frozen_intents(
                                        &ctx.tx,
                                        &op.stream,
                                        &op.row_id,
                                        op.incarnation.as_deref(),
                                        &entry.id,
                                    )?);
                                }
                            }
                            rejected.push((op, reason.to_owned()));
                        }
                    }
                }
                store.finish_submission(&ctx.tx, submission.sequence)?;
            }
            for (stream, id) in changed {
                self.materialize_base(
                    ctx,
                    &stream,
                    &id,
                    self.schema.shard_of(&stream),
                    store,
                    &mut evicted,
                )?;
            }
            Ok(())
        })?;
        self.evict_lifetimes(&evicted);
        let mut state = self.state.lock();
        state.reverted_count += rejected.len();
        let handler = state.on_rejected.clone();
        drop(state);
        drop(_serial);
        if let Some(handler) = handler {
            for (op, reason) in rejected {
                handler(&op, &reason);
            }
        }
        Ok(())
    }

    pub(super) fn evict_lifetimes(&self, addresses: &HashSet<(String, String)>) {
        for (stream, id) in addresses {
            let key = DocumentKey::new(stream, id);
            self.documents.lock().take(&key);
            if let Some(working) = self.working.lock().remove(&key) {
                working.writes.set_sealed(true);
            }
        }
    }
}
