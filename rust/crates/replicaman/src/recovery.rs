//! Durable branch evidence. SQL preserves raw bytes; export reads are bounded.
use crate::pool::WriteContext;
use crate::store::ReplicaStateStore;
use crate::{ReplicaError, ReplicaResult};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Clone, Debug)]
pub struct ReplicaRecoveryRecord {
    pub id: String,
    pub stream: String,
    pub row_id: String,
    pub incarnation: String,
    pub reason: String,
    pub created_at: f64,
}

#[derive(Clone, Debug)]
pub struct ReplicaRecoveryPart {
    pub kind: String,
    pub key: String,
    pub byte_count: i64,
}

impl ReplicaStateStore {
    pub(crate) fn archive_document(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        id: &str,
        reason: &str,
    ) -> ReplicaResult<()> {
        self.archive_branch(&ctx.tx, stream, id, reason, true)
    }

    pub(crate) fn archive_entity(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
        reason: &str,
    ) -> ReplicaResult<()> {
        self.archive_branch(db, stream, id, reason, false)
    }

    fn archive_branch(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
        reason: &str,
        force: bool,
    ) -> ReplicaResult<()> {
        let authored: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM intents WHERE stream = ?1 AND row_id = ?2 AND state <> 'refused')
                OR EXISTS(SELECT 1 FROM holds WHERE stream = ?1 AND row_id = ?2)",
            params![stream, id], |row| row.get(0))?;
        if !force && !authored {
            return Ok(());
        }
        let incarnation = self.incarnation(db, stream, id)?.ok_or_else(|| {
            ReplicaError::Storage("Cannot archive an entity without its incarnation".into())
        })?;
        let recovery_id = crate::id::ulid();
        db.execute(
            r#"INSERT INTO recoveries VALUES (?, ?, ?, ?, ?, CAST('{"format":1}' AS BLOB), unixepoch('subsec'))"#,
            params![recovery_id, stream, id, incarnation, reason])?;
        for statement in include_str!("recovery.sql")
            .split(';')
            .filter(|sql| !sql.trim().is_empty())
        {
            db.execute(statement, params![recovery_id, stream, id])?;
        }
        Ok(())
    }

    /// Rescan from the start to discover archives added during pagination.
    pub fn recovery_records(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> ReplicaResult<Vec<ReplicaRecoveryRecord>> {
        page_limit(limit)?;
        self.pool().read(|db| {
            let mut query = db.prepare(
                "SELECT id, stream, row_id, incarnation, reason, created_at FROM recoveries
                 WHERE (?1 IS NULL OR id > ?1) ORDER BY id LIMIT ?2",
            )?;
            Ok(query
                .query_map(params![after, limit], |row| {
                    Ok(ReplicaRecoveryRecord {
                        id: row.get(0)?,
                        stream: row.get(1)?,
                        row_id: row.get(2)?,
                        incarnation: row.get(3)?,
                        reason: row.get(4)?,
                        created_at: row.get(5)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?)
        })
    }

    pub fn recovery_parts(
        &self,
        id: &str,
        after: Option<&ReplicaRecoveryPart>,
        limit: usize,
    ) -> ReplicaResult<Vec<ReplicaRecoveryPart>> {
        page_limit(limit)?;
        self.pool().read(|db| {
            let mut query = db.prepare(
                "SELECT kind, part_key, length(content) FROM recovery_parts
                 WHERE recovery_id = ?1 AND (?2 IS NULL OR (kind, part_key) > (?2, ?3))
                 ORDER BY kind, part_key LIMIT ?4",
            )?;
            Ok(query
                .query_map(
                    params![
                        id,
                        after.map(|part| &part.kind),
                        after.map(|part| &part.key),
                        limit
                    ],
                    |row| {
                        Ok(ReplicaRecoveryPart {
                            kind: row.get(0)?,
                            key: row.get(1)?,
                            byte_count: row.get(2)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<_>>()?)
        })
    }

    pub fn recovery_chunk(
        &self,
        id: &str,
        part: &ReplicaRecoveryPart,
        offset: i64,
        limit: usize,
    ) -> ReplicaResult<Vec<u8>> {
        if offset < 0 || offset == i64::MAX || !(1..=262144).contains(&limit) {
            return Err(ReplicaError::Storage("Invalid recovery chunk range".into()));
        }
        self.pool().read(|db| {
            db.query_row(
                "SELECT substr(content, ?, ?) FROM recovery_parts
                WHERE recovery_id = ? AND kind = ? AND part_key = ?",
                params![offset + 1, limit, id, part.kind, part.key],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| ReplicaError::Storage("Recovery part does not exist".into()))
        })
    }

    /// Explicitly forget an exported or dismissed branch and all its parts.
    pub fn remove_recovery_record(&self, id: &str) -> ReplicaResult<()> {
        self.pool().write(|ctx| {
            ctx.tx
                .execute("DELETE FROM recovery_parts WHERE recovery_id = ?", [id])?;
            ctx.tx
                .execute("DELETE FROM recoveries WHERE id = ?", [id])?;
            Ok(())
        })
    }
}

fn page_limit(limit: usize) -> ReplicaResult<()> {
    if !(1..=1000).contains(&limit) {
        return Err(ReplicaError::Storage(
            "Recovery page limit must be 1..1000".into(),
        ));
    }
    Ok(())
}
