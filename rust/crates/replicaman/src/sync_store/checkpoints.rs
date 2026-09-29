use super::*;
use crate::wire::{RawFrame, ReplicaFrame, decode_frames};

/// A pull round in progress: the cursor to continue from (none for a
/// baseline's first request), whether publishing it replaces the shard's base,
/// and the highest accepted submission when it started.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Download {
    pub cursor: Option<String>,
    pub reset: bool,
    pub visible: i64,
}

impl ReplicaStateStore {
    pub(crate) fn read_generation(&self, db: &Connection, shard: &str) -> ReplicaResult<i64> {
        Ok(db
            .query_row(
                "SELECT generation FROM checkpoints WHERE shard = ?",
                [shard],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    /// A pull already on the wire must not stage its answer into a round that
    /// was abandoned while it waited.
    pub(crate) fn invalidate_download(&self, db: &Connection, shard: &str) -> ReplicaResult<()> {
        db.execute("INSERT INTO checkpoints (shard, generation) VALUES (?, 1) ON CONFLICT(shard) DO UPDATE SET generation = generation + 1", [shard])?;
        self.discard_download(db, shard)
    }

    pub(crate) fn download(&self, db: &Connection, shard: &str) -> ReplicaResult<Option<Download>> {
        Ok(db
            .query_row(
                "SELECT cursor, reset, visible FROM downloads WHERE shard = ?",
                [shard],
                |row| {
                    Ok(Download {
                        cursor: row.get(0)?,
                        reset: row.get(1)?,
                        visible: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    /// Resume the shard's round, or begin one from its published cursor.
    pub(crate) fn begin_download(&self, db: &Connection, shard: &str) -> ReplicaResult<Download> {
        if let Some(download) = self.download(db, shard)? {
            return Ok(download);
        }
        let cursor = self.cursor(db, shard)?;
        self.start_download(db, shard, cursor)
    }

    /// `CursorInvalid`: the staged pages go and the round starts over from no
    /// cursor. The published cursor and base stay until that baseline publishes.
    pub(crate) fn restart_download(&self, db: &Connection, shard: &str) -> ReplicaResult<()> {
        self.discard_download(db, shard)?;
        self.start_download(db, shard, None)?;
        Ok(())
    }

    /// Intents accepted after this moment carry higher sequences, so the round
    /// removes only accepted intents the server already shows.
    fn start_download(
        &self,
        db: &Connection,
        shard: &str,
        cursor: Option<String>,
    ) -> ReplicaResult<Download> {
        let visible: i64 = db.query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM intents WHERE state = 'accepted'",
            [],
            |row| row.get(0),
        )?;
        let download = Download {
            reset: cursor.is_none(),
            cursor,
            visible,
        };
        db.execute(
            "INSERT INTO downloads (shard, cursor, reset, visible, started_at) VALUES (?, ?, ?, ?, unixepoch('subsec'))",
            params![shard, download.cursor, download.reset, download.visible],
        )?;
        Ok(download)
    }

    /// Stage one answer's frames and continue the round from its cursor.
    pub(crate) fn stage_page(
        &self,
        db: &Connection,
        shard: &str,
        frames: &[ReplicaFrame],
        cursor: &str,
    ) -> ReplicaResult<()> {
        let content =
            protocol::encode(&frames.iter().map(ReplicaFrame::to_wire).collect::<Vec<_>>())?;
        db.execute(
            "INSERT INTO download_pages VALUES (?1, (SELECT COUNT(*) FROM download_pages WHERE shard = ?1), ?2)",
            params![shard, content],
        )?;
        db.execute(
            "UPDATE downloads SET cursor = ? WHERE shard = ?",
            params![cursor, shard],
        )?;
        Ok(())
    }

    pub(crate) fn staged_pages(&self, db: &Connection, shard: &str) -> ReplicaResult<i64> {
        Ok(db.query_row(
            "SELECT COUNT(*) FROM download_pages WHERE shard = ?",
            [shard],
            |row| row.get(0),
        )?)
    }

    /// One staged page, decoded; a round publishes its pages in order.
    pub(crate) fn staged_page(
        &self,
        db: &Connection,
        shard: &str,
        page: i64,
    ) -> ReplicaResult<Vec<ReplicaFrame>> {
        let content: Vec<u8> = db
            .query_row(
                "SELECT content FROM download_pages WHERE shard = ? AND page = ?",
                params![shard, page],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| ReplicaError::Storage("Staged page gap".into()))?;
        let raw: Vec<RawFrame> = serde_json::from_slice(&content)
            .map_err(|error| ReplicaError::Storage(format!("Staged page is corrupt: {error}")))?;
        decode_frames(raw)
            .map_err(|error| ReplicaError::Storage(format!("Staged page is corrupt: {error}")))
    }

    pub(crate) fn discard_download(&self, db: &Connection, shard: &str) -> ReplicaResult<()> {
        db.execute("DELETE FROM download_pages WHERE shard = ?", [shard])?;
        db.execute("DELETE FROM downloads WHERE shard = ?", [shard])?;
        Ok(())
    }
}
