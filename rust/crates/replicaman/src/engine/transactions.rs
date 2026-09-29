use super::*;
use crate::pool::WriteContext;
use crate::store::SnapshotRow;

/// Row reads and writes inside one SQLite transaction. The closure cannot await.
pub struct ReplicaTransaction<'engine, 'transaction, 'connection> {
    engine: &'engine ReplicaEngine,
    store: &'engine Arc<ReplicaStateStore>,
    context: &'transaction mut WriteContext<'connection>,
    lane: ReplicaLane,
}

impl ReplicaTransaction<'_, '_, '_> {
    pub fn find(&self, stream: &str, id: &str) -> ReplicaResult<Option<SnapshotRow>> {
        self.store.snapshot(&self.context.tx, stream, id)
    }

    pub fn create(
        &mut self,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
    ) -> ReplicaResult<()> {
        self.write_row(stream, id, row_type, data, RowWriteExpectation::Absent)
    }

    /// Only fields differing from this transaction's current row are authored.
    pub fn update(&mut self, stream: &str, id: &str, data: &ReplicaFields) -> ReplicaResult<()> {
        self.write_row(stream, id, None, data, RowWriteExpectation::Present)
    }

    pub fn delete(&mut self, stream: &str, id: &str) -> ReplicaResult<bool> {
        let spec = self.row_spec(stream)?;
        self.engine
            .apply_row_delete(self.context, self.store, &spec, stream, id, self.lane)
    }

    fn write_row(
        &mut self,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
        expectation: RowWriteExpectation,
    ) -> ReplicaResult<()> {
        let spec = self.row_spec(stream)?;
        self.engine.apply_row_write(
            self.context,
            self.store,
            &spec,
            stream,
            id,
            row_type,
            data,
            expectation,
            self.lane,
        )
    }

    fn row_spec(&self, stream: &str) -> ReplicaResult<ReplicaStreamSpec> {
        let spec = self.engine.writable_spec(stream)?;
        if spec.lane != StreamLane::Row {
            return Err(ReplicaError::LaneMismatch(stream.into()));
        }
        Ok(spec)
    }
}

impl ReplicaEngine {
    /// Local atomicity. Individual operations receive independent server results.
    pub async fn write<T>(
        &self,
        body: impl FnOnce(&mut ReplicaTransaction<'_, '_, '_>) -> ReplicaResult<T>,
    ) -> ReplicaResult<T> {
        self.transact_rows(false, body).await
    }

    /// Save a row action and queue it as one immutable server transaction.
    /// Every member must pass its gates. A hold, discard, unfrozen dependency or
    /// size limit returns an error and rolls back the entire local action.
    pub async fn write_atomically<T>(
        &self,
        body: impl FnOnce(&mut ReplicaTransaction<'_, '_, '_>) -> ReplicaResult<T>,
    ) -> ReplicaResult<T> {
        self.transact_rows(true, body).await
    }

    async fn transact_rows<T>(
        &self,
        atomic: bool,
        body: impl FnOnce(&mut ReplicaTransaction<'_, '_, '_>) -> ReplicaResult<T>,
    ) -> ReplicaResult<T> {
        let (store, _admitted) = self.admit_local_write().await?;
        let lane = current_lane();
        let value = store.pool().write(|context| {
            if atomic {
                context.atomic_entries = Some(Vec::new());
            }
            let value = body(&mut ReplicaTransaction {
                engine: self,
                store: &store,
                context,
                lane,
            })?;
            if let Some(entries) = &context.atomic_entries {
                store.freeze_atomic_write(&context.tx, entries)?;
            }
            Ok(value)
        })?;
        self.schedule_push().await;
        Ok(value)
    }

    pub(super) fn validate_atomic_address(
        &self,
        ctx: &WriteContext<'_>,
        store: &ReplicaStateStore,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<()> {
        let Some(captured) = &ctx.atomic_entries else {
            return Ok(());
        };
        let earlier = store
            .owed_ids(&ctx.tx, stream, id)?
            .iter()
            .any(|entry| !captured.contains(entry));
        if earlier || store.gate_hold(&ctx.tx, stream, id)?.is_some() {
            return Err(ReplicaError::AtomicWriteBlocked(format!(
                "Unsubmitted dependency: {stream}/{id}"
            )));
        }
        Ok(())
    }

    pub(super) fn validate_atomic_admission(
        &self,
        ctx: &WriteContext<'_>,
        store: &ReplicaStateStore,
        op: &ReplicaOp,
        preimage: Option<&[u8]>,
    ) -> ReplicaResult<()> {
        self.validate_atomic_address(ctx, store, &op.stream, &op.row_id)?;
        for reference in &op.references {
            self.validate_atomic_address(ctx, store, &reference.stream, &reference.id)?;
        }
        if let Some((gate, decision)) = self.gate_outcome(&Self::sync_change(op, preimage)?) {
            let reason = match decision {
                crate::SyncGateDecision::Hold(reason) => reason,
                crate::SyncGateDecision::Discard => "A gate would discard a group member".into(),
                crate::SyncGateDecision::Push => return Ok(()),
            };
            return Err(ReplicaError::AtomicWriteBlocked(format!(
                "{gate}: {reason}"
            )));
        }
        Ok(())
    }
}
