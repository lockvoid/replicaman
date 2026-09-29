use super::*;
use crate::protocol::{self, Connection};

impl ReplicaEngine {
    /// Compare the authoritative base with the server's live members at the
    /// published cursor. Reads every base row: schedule periodically, not per
    /// edit. Failure changes no data. `CursorBehind` means pull the shard,
    /// then verify again.
    pub async fn verify_integrity(&self, shard: &str) -> ReplicaResult<()> {
        let (store, _admitted) = self.begin_wire_operation().await?;
        self.verify_published(shard, &store).await
    }

    async fn verify_published(
        &self,
        shard: &str,
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<()> {
        let connection = Connection {
            transport: self.transport.as_ref(),
            schema: &self.schema,
        };
        let (proof, dataset) = store.pool().read(|db| {
            Ok((
                store.integrity_snapshot(db, shard)?,
                store.meta(db)?.dataset,
            ))
        })?;
        let dataset = dataset.ok_or_else(|| ReplicaError::Protocol {
            code: "CheckpointRequired".into(),
            message: "Synchronize before verifying the replica".into(),
        })?;
        let answer = connection.verify(shard, &proof.cursor, &dataset).await?;
        if answer.shard != shard
            || answer.cursor != proof.cursor
            || !protocol::is_digest(&answer.digest)
        {
            return Err(protocol::invalid(
                "Integrity answer names another shard or cursor",
            ));
        }
        let count = protocol::counter(&answer.count)?;
        let current = store.pool().read(|db| {
            Ok(store.cursor(db, shard)?.as_deref() == Some(&proof.cursor)
                && store.read_generation(db, shard)? == proof.generation)
        })?;
        if !current {
            return Err(ReplicaError::Protocol {
                code: "CheckpointChanged".into(),
                message: "The published cursor changed during verification; retry".into(),
            });
        }
        if answer.digest != proof.digest || count != proof.count {
            return Err(ReplicaError::Protocol {
                code: "ReplicaDiverged".into(),
                message: "Authoritative membership differs from the server; local work is retained"
                    .into(),
            });
        }
        Ok(())
    }
}
