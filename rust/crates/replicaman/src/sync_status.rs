use crate::{ReplicaOp, ReplicaResult, ReplicaStateStore};

/// One durable SQLite snapshot. Timestamps are Unix seconds; bytes include retained copies.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaSyncStatus {
    pub queued_operations: i64,
    pub held_entities: i64,
    pub submitted_groups: i64,
    pub rejected_operations: i64,
    pub accepted_operations: i64,
    pub recovery_branches: i64,
    pub oldest_intent_at: Option<f64>,
    pub oldest_download_at: Option<f64>,
    pub journal_bytes: i64,
    pub submitted_bytes: i64,
    pub accepted_bytes: i64,
    pub download_bytes: i64,
    pub recovery_bytes: i64,
    pub document_bytes: i64,
}

impl ReplicaSyncStatus {
    pub fn has_unsettled_work(&self) -> bool {
        self.queued_operations
            + self.held_entities
            + self.submitted_groups
            + self.rejected_operations
            + self.accepted_operations
            + self.recovery_branches
            > 0
    }
}

impl ReplicaStateStore {
    pub fn sync_status(&self) -> ReplicaResult<ReplicaSyncStatus> {
        self.pool().read(|db| {
            Ok(db.query_row(include_str!("status.sql"), [], |row| {
                Ok(ReplicaSyncStatus {
                    queued_operations: row.get(0)?,
                    held_entities: row.get(1)?,
                    submitted_groups: row.get(2)?,
                    rejected_operations: row.get(3)?,
                    accepted_operations: row.get(4)?,
                    recovery_branches: row.get(5)?,
                    oldest_intent_at: row.get(6)?,
                    oldest_download_at: row.get(7)?,
                    journal_bytes: row.get(8)?,
                    submitted_bytes: row.get(9)?,
                    accepted_bytes: row.get(10)?,
                    download_bytes: row.get(11)?,
                    recovery_bytes: row.get(12)?,
                    document_bytes: row.get(13)?,
                })
            })?)
        })
    }

    /// Inspect all delivery stages without materializing the backlog.
    /// The predicate must be read-only. Corrupt bytes throw; they cannot prove safe eviction.
    pub fn contains_unsettled_operation(
        &self,
        predicate: impl Fn(&ReplicaOp) -> ReplicaResult<bool>,
    ) -> ReplicaResult<bool> {
        self.pool().read(|db| {
            let mut query = db.prepare(include_str!("unsettled.sql"))?;
            let mut rows = query.query([])?;
            while let Some(row) = rows.next()? {
                let payload: Vec<u8> = row.get(0)?;
                if predicate(&ReplicaOp::from_json(&payload)?)? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
}
