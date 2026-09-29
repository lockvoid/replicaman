impl ReplicaEngine {
    pub async fn commit_session(&self) -> ReplicaResult<crate::ReplicaCommitSession> {
        let (_store, _admitted) = self.admit_local_write().await?;
        Ok(crate::ReplicaCommitSession {
            engine: self.commit_identity.clone(),
            binding: self.binding.generation(),
        })
    }

    /// A command reply requests fresh checkpoints for its affected shards.
    /// No command-specific importer can bypass membership or visibility rules.
    pub async fn apply_commit(&self, encoded: &str, session: &crate::ReplicaCommitSession) -> ReplicaResult<()> {
        let (store, _admitted) = self.begin_wire_operation().await?;
        if session.engine != self.commit_identity || session.binding != self.binding.generation() {
            return Err(ReplicaError::StaleCommit);
        }
        let dataset = store.pool().read(|db| store.meta(db))?.dataset;
        let shards = crate::commit::decode_commit(encoded, &self.schema, dataset.as_deref())?;
        store.pool().write(|ctx| {
            for shard in &shards { store.invalidate_download(&ctx.tx, shard)?; }
            Ok(())
        })?;
        self.pull_until_caught_up(Some(&shards)).await?;
        if self.is_sealed().await { return Err(ReplicaError::IdentityTransitionRequired); }
        Ok(())
    }
}
