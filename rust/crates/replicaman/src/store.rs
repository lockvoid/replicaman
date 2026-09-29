//! ReplicaMan's durable state — one sqlite file (rusqlite, WAL). This is the
//! app's PRIMARY database: `snapshots` is the client's raw truth for both
//! lanes. Losing the journal loses the user's unsent work; losing the cursor
//! forces a re-snapshot — which is why they live here together and why `reset`
//! wipes never touch the journal.
//!
//! Ported from `Sources/ReplicaMan/ReplicaStateStore.swift`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension};

use crate::error::{ReplicaError, ReplicaResult};
use crate::pool::{SqlitePool, WriteContext};
use crate::row_cache::{DecodedRowCache, MaterializedRow, RowMaterialization, RowRecord};
use crate::schema::ReplicaLane;
use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::{ReplicaJson, ReplicaOp};
mod lease;

pub use crate::row_cache::{MaterializedRow as StoreMaterializedRow, RowRecord as StoreRowRecord};

/// A snapshot row as raw truth holds it.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRow {
    pub stream: String,
    pub row_id: String,
    pub row_type: Option<String>,
    pub data: ReplicaFields,
}

/// The document fold plus its acked version vector and authoring peer.
#[derive(Clone, Debug, PartialEq)]
pub struct DocRow {
    pub stream: String,
    pub row_id: String,
    pub codec: String,
    pub fold: Vec<u8>,
    pub acked: Option<Vec<u8>>,
    pub peer: u64,
}

/// One local write the server is owed or refused (an `intents` row).
#[derive(Clone, Debug, PartialEq)]
pub struct JournalRow {
    pub id: String,
    pub verb: String,
    /// Encoded `ReplicaOp` JSON, byte-stable — what freezing puts on the wire.
    pub payload: Vec<u8>,
    /// Revert record (client-owned JSON) — what this op's client write
    /// displaced.
    pub preimage: Option<Vec<u8>>,
    /// The server's refusal, kept until the application dismisses it.
    pub parked: Option<String>,
}

impl JournalRow {
    pub fn op(&self) -> ReplicaResult<ReplicaOp> {
        ReplicaOp::from_json(&self.payload)
    }
}

/// A pending entry with the lane it sits on — the input to stickiness.
pub struct PendingEntry {
    pub id: String,
    pub lane: ReplicaLane,
    pub payload: Vec<u8>,
}

pub struct ReplicaStateStore {
    authoring_lease: parking_lot::Mutex<Option<std::fs::File>>,
    pool: Arc<SqlitePool>,
    /// Where this store was opened. The merge renames the file under a live
    /// pool, so the CURRENT path is the binding's to track — this is only the
    /// starting point.
    path: PathBuf,
    row_cache: Arc<DecodedRowCache>,
}

fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64())
        .unwrap_or(0.0)
}

pub(crate) const INTENT_COLUMNS: &str = "id, op, payload, preimage, reason";

/// Owed and frozen intents: what the server is still owed.
const PENDING: &str = "state IN ('owed', 'frozen')";

pub(crate) fn journal_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JournalRow> {
    let payload: String = row.get("payload")?;
    let preimage: Option<String> = row.get("preimage")?;
    Ok(JournalRow {
        id: row.get("id")?,
        verb: row.get("op")?,
        payload: payload.into_bytes(),
        preimage: preimage.map(String::into_bytes),
        parked: row.get("reason")?,
    })
}

impl ReplicaStateStore {
    /// Everything sqlite keeps for one store: the database and its WAL
    /// sidecars. A retired owner leaves none of the three behind.
    pub fn remove(path: &Path) -> ReplicaResult<()> {
        for suffix in ["", "-wal", "-shm"] {
            let mut target = path.as_os_str().to_owned();
            target.push(suffix);
            match std::fs::remove_file(PathBuf::from(target)) {
                Ok(()) => (),
                // SQLite can remove its sidecars at close. Absence already
                // satisfies this explicit deletion request.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => {
                    return Err(ReplicaError::Storage(format!(
                        "Remove retired store: {error}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Move a closed, checkpointed store without replacing another owner's
    /// world. Same-filesystem link creation is atomic and refuses collisions.
    pub fn move_store(source: &Path, destination: &Path) -> ReplicaResult<()> {
        if source == destination {
            return Ok(());
        }
        for base in [source, destination] {
            for suffix in ["-wal", "-shm"] {
                let mut path = base.as_os_str().to_owned();
                path.push(suffix);
                if PathBuf::from(path).exists() {
                    return Err(ReplicaError::Storage(
                        "store move requires closed, checkpointed files without sidecars".into(),
                    ));
                }
            }
        }
        std::fs::hard_link(source, destination).map_err(|error| {
            ReplicaError::Storage(format!(
                "store move refused; both owner worlds retained: {error}"
            ))
        })?;
        #[cfg(unix)]
        if let Some(parent) = destination.parent() {
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    ReplicaError::Storage(format!("sync moved store directory: {error}"))
                })?;
        }
        std::fs::remove_file(source)
            .map_err(|error| ReplicaError::Storage(format!("retire old store name: {error}")))?;
        #[cfg(unix)]
        if let Some(parent) = source.parent() {
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    ReplicaError::Storage(format!("sync source store directory: {error}"))
                })?;
        }
        Ok(())
    }

    pub fn open(path: &Path) -> ReplicaResult<Self> {
        let authoring_lease = lease::acquire(path)?;
        let pool = Arc::new(SqlitePool::open(path)?);
        let store = Self {
            authoring_lease: parking_lot::Mutex::new(Some(authoring_lease)),
            pool,
            path: path.to_path_buf(),
            row_cache: Arc::new(DecodedRowCache::default()),
        };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> ReplicaResult<()> {
        self.pool.write(|ctx| {
            crate::sync_store::prepare(&ctx.tx)?;
            ctx.tx
                .execute("UPDATE docs SET peer = ?", [crate::id::peer() as i64])?;
            Ok(())
        })
    }

    pub fn pool(&self) -> &Arc<SqlitePool> {
        &self.pool
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn close(&self) -> ReplicaResult<()> {
        self.pool.close()?;
        self.authoring_lease.lock().take();
        Ok(())
    }

    // MARK: - Cursor

    pub(crate) fn require_document_mode(
        &self,
        mode: crate::ReplicaDocumentMode,
    ) -> ReplicaResult<()> {
        let requested = if mode == crate::ReplicaDocumentMode::Replicated {
            "replicated"
        } else {
            "projections"
        };
        self.pool.write(|ctx| {
            ctx.tx.execute("UPDATE meta SET document_mode = ? WHERE id = 1 AND document_mode IS NULL", [requested])?;
            let actual: String = ctx.tx.query_row("SELECT document_mode FROM meta WHERE id = 1", [], |row| row.get(0))?;
            if actual != requested {
                return Err(ReplicaError::Storage("Document mode belongs to the store. Use a separate store for projection-only replicas.".into()));
            }
            Ok(())
        })
    }

    /// The server's opaque cursor of the shard's last published round.
    pub fn cursor(&self, db: &Connection, shard: &str) -> ReplicaResult<Option<String>> {
        Ok(db
            .query_row(
                "SELECT cursor FROM checkpoints WHERE shard = ?",
                [shard],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    pub fn set_cursor(
        &self,
        ctx: &mut WriteContext<'_>,
        value: &str,
        shard: &str,
    ) -> ReplicaResult<()> {
        ctx.tx.execute(
            "INSERT INTO checkpoints (shard, cursor) VALUES (?, ?) \
             ON CONFLICT(shard) DO UPDATE SET cursor = excluded.cursor",
            rusqlite::params![shard, value],
        )?;
        Ok(())
    }

    /// The next round of this shard is a baseline.
    pub fn clear_cursor(&self, ctx: &mut WriteContext<'_>, shard: &str) -> ReplicaResult<()> {
        self.invalidate_download(&ctx.tx, shard)?;
        ctx.tx.execute(
            "UPDATE checkpoints SET cursor = NULL WHERE shard = ?",
            [shard],
        )?;
        Ok(())
    }

    // MARK: - Snapshots (raw truth, both lanes)

    pub fn decode_data(json: Option<&str>) -> ReplicaResult<ReplicaFields> {
        ReplicaJson::decode_fields(json)
    }

    pub fn encode_data(data: &ReplicaFields) -> ReplicaResult<String> {
        ReplicaJson::encode_fields(data)
    }

    pub fn snapshot(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<Option<SnapshotRow>> {
        let found = db
            .query_row(
                "SELECT type, data FROM snapshots WHERE stream = ? AND row_id = ?",
                rusqlite::params![stream, row_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()?;
        found
            .map(|(row_type, data)| {
                Ok(SnapshotRow {
                    stream: stream.to_owned(),
                    row_id: row_id.to_owned(),
                    row_type,
                    data: Self::decode_data(data.as_deref())?,
                })
            })
            .transpose()
    }

    pub fn upsert_snapshot(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
        shard: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
    ) -> ReplicaResult<()> {
        let changed = ctx.tx.execute(
            "INSERT INTO snapshots (stream, row_id, shard, type, data) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard, type = excluded.type, data = excluded.data
             WHERE snapshots.shard IS NOT excluded.shard OR snapshots.type IS NOT excluded.type OR snapshots.data IS NOT excluded.data",
            rusqlite::params![stream, row_id, shard, row_type, Self::encode_data(data)?],
        )?;
        if changed > 0 {
            self.bump_change_sequence(ctx, stream)?;
        }
        Ok(())
    }

    pub fn delete_snapshot(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<()> {
        let changed = ctx.tx.execute(
            "DELETE FROM snapshots WHERE stream = ? AND row_id = ?",
            rusqlite::params![stream, row_id],
        )?;
        if changed > 0 {
            self.bump_change_sequence(ctx, stream)?;
        }
        Ok(())
    }

    /// The `reset: true` wipe: replace the shard's world. Snapshots and docs
    /// go; the journal SURVIVES — it still owes its ops.
    pub fn wipe_shard(&self, ctx: &mut WriteContext<'_>, shard: &str) -> ReplicaResult<()> {
        let streams: Vec<String> = ctx
            .tx
            .prepare(
                "SELECT stream FROM snapshots WHERE shard = ?1 \
                 UNION \
                 SELECT stream FROM docs WHERE shard = ?1",
            )?
            .query_map([shard], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        ctx.tx
            .execute("DELETE FROM snapshots WHERE shard = ? AND NOT EXISTS (SELECT 1 FROM intents WHERE intents.state IN ('owed', 'frozen') AND intents.stream = snapshots.stream AND intents.row_id = snapshots.row_id) AND NOT EXISTS (SELECT 1 FROM holds WHERE holds.stream = snapshots.stream AND holds.row_id = snapshots.row_id)", [shard])?;
        ctx.tx
            .execute("DELETE FROM docs WHERE shard = ? AND NOT EXISTS (SELECT 1 FROM intents WHERE intents.state IN ('owed', 'frozen') AND intents.stream = docs.stream AND intents.row_id = docs.row_id) AND NOT EXISTS (SELECT 1 FROM holds WHERE holds.stream = docs.stream AND holds.row_id = docs.row_id)", [shard])?;
        for stream in streams {
            self.bump_change_sequence(ctx, &stream)?;
        }
        Ok(())
    }

    // MARK: - Docs (fold + acked version vector + peer)

    pub fn doc(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<Option<DocRow>> {
        let found = db
            .query_row(
                "SELECT codec, fold, acked, peer FROM docs WHERE stream = ? AND row_id = ?",
                rusqlite::params![stream, row_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        Ok(found.map(|(codec, fold, acked, peer)| DocRow {
            stream: stream.to_owned(),
            row_id: row_id.to_owned(),
            codec,
            fold,
            acked,
            // A peer is a random u64 kept as an i64 bit pattern.
            peer: peer as u64,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_doc(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
        shard: &str,
        codec: &str,
        fold: &[u8],
        acked: Option<&[u8]>,
        peer: u64,
    ) -> ReplicaResult<()> {
        ctx.tx.execute(
            "INSERT OR REPLACE INTO docs (stream, row_id, shard, codec, fold, acked, peer) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![stream, row_id, shard, codec, fold, acked, peer as i64],
        )?;
        self.bump_change_sequence(ctx, stream)
    }

    pub fn delete_doc(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<()> {
        let changed = ctx.tx.execute(
            "DELETE FROM docs WHERE stream = ? AND row_id = ?",
            rusqlite::params![stream, row_id],
        )?;
        if changed > 0 {
            self.bump_change_sequence(ctx, stream)?;
        }
        Ok(())
    }

    /// Partial update for a live doc row — fold on merges, acked on
    /// accept/import — without disturbing shard or peer.
    pub fn update_doc(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
        fold: Option<&[u8]>,
        acked: Option<&[u8]>,
    ) -> ReplicaResult<()> {
        let mut changed = false;
        if let Some(fold) = fold {
            let rows = ctx.tx.execute(
                "UPDATE docs SET fold = ? WHERE stream = ? AND row_id = ?",
                rusqlite::params![fold, stream, row_id],
            )?;
            changed = changed || rows > 0;
        }
        if let Some(acked) = acked {
            let rows = ctx.tx.execute(
                "UPDATE docs SET acked = ? WHERE stream = ? AND row_id = ?",
                rusqlite::params![acked, stream, row_id],
            )?;
            changed = changed || rows > 0;
        }
        if changed {
            self.bump_change_sequence(ctx, stream)?;
        }
        Ok(())
    }

    // MARK: - Change sequence (the local watch counter, never on the wire)

    pub fn change_sequence(&self, db: &Connection, stream: &str) -> ReplicaResult<i64> {
        Ok(db
            .query_row(
                "SELECT change_seq FROM stream_meta WHERE stream = ?",
                [stream],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    fn bump_change_sequence(&self, ctx: &mut WriteContext<'_>, stream: &str) -> ReplicaResult<()> {
        ctx.tx.execute(
            "INSERT INTO stream_meta (stream, change_seq) VALUES (?, 1) \
             ON CONFLICT(stream) DO UPDATE SET change_seq = stream_meta.change_seq + 1",
            [stream],
        )?;
        let sequence = self.change_sequence(&ctx.tx, stream)?;
        self.row_cache.prepare_mutation(stream);
        let cache = self.row_cache.clone();
        let stream = stream.to_owned();
        ctx.hooks
            .after_next_transaction(move || cache.commit_mutation(&stream, sequence));
        Ok(())
    }

    // MARK: - Materialized reads

    pub fn materialized_rows<M, D>(
        &self,
        stream: &str,
        minimum_sequence: Option<i64>,
        decode: D,
    ) -> ReplicaResult<RowMaterialization<M>>
    where
        M: Send + Sync + Clone + 'static,
        D: Fn(&str, Option<&str>, &ReplicaFields) -> Option<M> + Copy,
    {
        loop {
            if let Some(materialization) = self
                .row_cache
                .materialization::<M>(stream, minimum_sequence)
            {
                return Ok(materialization);
            }

            if self.row_cache.raw_rows(stream, minimum_sequence).is_some() {
                let built = self
                    .row_cache
                    .with_materialization_lock::<M, _>(stream, || {
                        if let Some(existing) = self
                            .row_cache
                            .materialization::<M>(stream, minimum_sequence)
                        {
                            return Some(existing);
                        }
                        let raw = self.row_cache.raw_rows(stream, minimum_sequence)?;
                        // Per-row model reuse: a row whose raw text + type match
                        // the donor's decodes nothing — its model carries over.
                        // The check is self-validating (compared against the donor
                        // that HOLDS the model), so a donor superseded mid-flight
                        // merely reduces reuse, never poisons it.
                        let donor = self.row_cache.donor_snapshot::<M>(stream);
                        let rows: Vec<MaterializedRow<M>> = raw
                            .records
                            .into_iter()
                            .filter_map(|record| {
                                if let Some((records, models)) = &donor
                                    && let Some(prior) = records.get(&record.id)
                                    && prior.raw == record.raw
                                    && prior.row_type == record.row_type
                                    && let Some(model) = models.get(&record.id)
                                {
                                    return Some(MaterializedRow {
                                        record,
                                        model: model.clone(),
                                    });
                                }
                                decode(&record.id, record.row_type.as_deref(), &record.fields)
                                    .map(|model| MaterializedRow { record, model })
                            })
                            .collect();
                        let by_key = rows
                            .iter()
                            .map(|row| (row.record.id.clone(), row.model.clone()))
                            .collect();
                        self.row_cache.install_materialization(
                            RowMaterialization { rows, by_key },
                            stream,
                            raw.generation,
                            raw.sequence,
                        )
                    });
                if let Some(materialization) = built {
                    return Ok(materialization);
                }
                continue;
            }

            let generation = self.row_cache.generation(stream);
            // Field-tree reuse on the cold load itself: unchanged rows keep
            // their decoded `fields` from the donor — only changed raw text
            // pays `decode_data`.
            let donor_records = self.row_cache.donor_records(stream);
            let (sequence, records) = self.pool.read(|db| {
                let sequence = self.change_sequence(db, stream)?;
                let records: Vec<RowRecord> = db
                    .prepare(
                        "SELECT row_id, type, data FROM snapshots WHERE stream = ? ORDER BY row_id",
                    )?
                    .query_map([stream], |row| {
                        let id: String = row.get(0)?;
                        let row_type: Option<String> = row.get(1)?;
                        let raw: Option<String> = row.get(2)?;
                        Ok((id, row_type, raw))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?
                    .into_iter()
                    .map(|(id, row_type, raw)| {
                        if let Some(records) = &donor_records
                            && let Some(prior) = records.get(&id)
                            && prior.raw == raw
                            && prior.row_type == row_type
                        {
                            return Ok(prior.clone());
                        }
                        let fields = Self::decode_data(raw.as_deref())?;
                        Ok(RowRecord {
                            id,
                            row_type,
                            raw,
                            fields,
                        })
                    })
                    .collect::<ReplicaResult<_>>()?;
                Ok((sequence, records))
            })?;
            self.row_cache
                .install_raw_rows(stream, generation, sequence, records);
        }
    }

    // MARK: - Intents

    /// A new owed intent, or newer bytes for an owed one: the rowid (queue
    /// position) and created_at survive the supersession. Nothing but an owed
    /// intent of the same address is editable.
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue(
        &self,
        ctx: &mut WriteContext<'_>,
        id: &str,
        verb: &str,
        stream: &str,
        row_id: &str,
        payload: &[u8],
        preimage: Option<&[u8]>,
        lane: ReplicaLane,
    ) -> ReplicaResult<()> {
        let written = ctx.tx.execute(
            "INSERT INTO intents (id, stream, row_id, state, op, payload, preimage, lane, created_at) \
             VALUES (?, ?, ?, 'owed', ?, ?, ?, ?, ?) \
             ON CONFLICT(id) DO UPDATE \
             SET op = excluded.op, payload = excluded.payload, preimage = excluded.preimage, \
                 lane = excluded.lane \
             WHERE intents.state = 'owed' AND intents.stream = excluded.stream \
               AND intents.row_id = excluded.row_id",
            rusqlite::params![
                id,
                stream,
                row_id,
                verb,
                String::from_utf8_lossy(payload).into_owned(),
                preimage.map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
                lane.as_str(),
                now_seconds(),
            ],
        )?;
        if written == 0 {
            return Err(ReplicaError::Storage(format!(
                "Only an owed intent of the same address can be superseded: {id}"
            )));
        }
        Ok(())
    }

    /// Its bytes are on the wire: the server may have committed them even
    /// when the answer never arrives.
    pub(crate) fn is_frozen(&self, db: &Connection, id: &str) -> ReplicaResult<bool> {
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM intents WHERE id = ? AND state = 'frozen')",
            [id],
            |row| row.get(0),
        )?)
    }

    pub fn lane(&self, db: &Connection, entry_id: &str) -> ReplicaResult<ReplicaLane> {
        let raw: Option<String> = db
            .query_row("SELECT lane FROM intents WHERE id = ?", [entry_id], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(raw
            .as_deref()
            .and_then(ReplicaLane::parse)
            .unwrap_or(ReplicaLane::Bulk))
    }

    /// Entries still owed for a row, whatever lane they sit on — the input to
    /// stickiness. Carries the payload so a caller that promotes them can walk
    /// what THEY name without reading the intents a second time.
    pub fn pending_entries(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<Vec<PendingEntry>> {
        let rows = db
            .prepare(&format!(
                "SELECT id, lane, payload FROM intents \
                 WHERE {PENDING} AND row_id = ? AND stream = ? \
                 ORDER BY rowid"
            ))?
            .query_map(rusqlite::params![row_id, stream], |row| {
                Ok(PendingEntry {
                    id: row.get(0)?,
                    lane: ReplicaLane::parse(&row.get::<_, String>(1)?)
                        .unwrap_or(ReplicaLane::Bulk),
                    payload: row.get::<_, String>(2)?.into_bytes(),
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    /// Pending BULK entries addressing any of these rows — the promotion
    /// lookup. One index seek per named id, so an interactive write costs the
    /// same whether the bulk backlog holds ten ops or ten thousand.
    pub fn pending_bulk_entries(
        &self,
        db: &Connection,
        row_ids: &[String],
    ) -> ReplicaResult<Vec<JournalRow>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; row_ids.len()].join(", ");
        let sql = format!(
            "SELECT {INTENT_COLUMNS} FROM intents \
             WHERE {PENDING} AND lane = ? AND row_id IN ({placeholders}) \
             ORDER BY rowid"
        );
        let mut arguments: Vec<SqlValue> = Vec::with_capacity(row_ids.len() + 1);
        arguments.push(SqlValue::Text(ReplicaLane::Bulk.as_str().to_owned()));
        arguments.extend(row_ids.iter().map(|id| SqlValue::Text(id.clone())));
        let rows = db
            .prepare(&sql)?
            .query_map(rusqlite::params_from_iter(arguments), journal_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    fn entries(
        &self,
        db: &Connection,
        condition: &str,
        arguments: impl rusqlite::Params,
    ) -> ReplicaResult<Vec<JournalRow>> {
        let sql = format!("SELECT {INTENT_COLUMNS} FROM intents WHERE {condition} ORDER BY rowid");
        let rows = db
            .prepare(&sql)?
            .query_map(arguments, journal_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    /// Entries owed on one lane (or every lane when `lane` is `None`), in
    /// drain order.
    pub fn pending_lane(
        &self,
        db: &Connection,
        lane: Option<ReplicaLane>,
    ) -> ReplicaResult<Vec<JournalRow>> {
        match lane {
            None => self.pending(db),
            Some(lane) => self.entries(db, &format!("{PENDING} AND lane = ?"), [lane.as_str()]),
        }
    }

    pub fn pending(&self, db: &Connection) -> ReplicaResult<Vec<JournalRow>> {
        self.entries(db, PENDING, [])
    }

    pub fn parked(&self, db: &Connection) -> ReplicaResult<Vec<JournalRow>> {
        self.entries(db, "state = 'refused'", [])
    }

    /// Intents never attempted: the only ones a sync gate may still hold.
    pub(crate) fn owed(&self, db: &Connection) -> ReplicaResult<Vec<JournalRow>> {
        self.entries(db, "state = 'owed'", [])
    }

    pub fn entries_for_stream(
        &self,
        db: &Connection,
        stream: &str,
    ) -> ReplicaResult<Vec<JournalRow>> {
        self.entries(db, "state <> 'accepted' AND stream = ?", [stream])
    }

    pub fn lanes_owed(&self, db: &Connection) -> ReplicaResult<Vec<ReplicaLane>> {
        let raw: Vec<String> = db
            .prepare(&format!(
                "SELECT DISTINCT lane FROM intents WHERE {PENDING}"
            ))?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(raw
            .iter()
            .filter_map(|value| ReplicaLane::parse(value))
            .collect())
    }

    /// Just the ids of what this lane owes, in drain order — the scheduler's
    /// question when it has to subtract the entries a gate is holding
    /// (`owes_work` cannot answer that: gating is derived per drain and never
    /// written down).
    pub fn pending_entry_ids(
        &self,
        db: &Connection,
        lane: ReplicaLane,
    ) -> ReplicaResult<Vec<String>> {
        let ids: Vec<String> = db
            .prepare(&format!(
                "SELECT id FROM intents WHERE {PENDING} AND lane = ? ORDER BY rowid"
            ))?
            .query_map([lane.as_str()], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ids)
    }

    pub fn owes_work(&self, db: &Connection, lane: ReplicaLane) -> ReplicaResult<bool> {
        Ok(db.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM intents WHERE {PENDING} AND lane = ?)"),
            [lane.as_str()],
            |row| row.get::<_, i64>(0),
        )? != 0)
    }

    pub fn promote(&self, ctx: &mut WriteContext<'_>, entry_ids: &[String]) -> ReplicaResult<()> {
        if entry_ids.is_empty() {
            return Ok(());
        }
        let placeholders = vec!["?"; entry_ids.len()].join(", ");
        let sql = format!("UPDATE intents SET lane = ? WHERE id IN ({placeholders})");
        let mut arguments: Vec<SqlValue> = Vec::with_capacity(entry_ids.len() + 1);
        arguments.push(SqlValue::Text(ReplicaLane::Interactive.as_str().to_owned()));
        arguments.extend(entry_ids.iter().map(|id| SqlValue::Text(id.clone())));
        ctx.tx
            .execute(&sql, rusqlite::params_from_iter(arguments))?;
        Ok(())
    }

    /// Its own verdict consumes a frozen intent whose work no longer applies.
    pub fn remove_entry(&self, ctx: &mut WriteContext<'_>, id: &str) -> ReplicaResult<()> {
        let removed = ctx.tx.execute(
            "DELETE FROM intents WHERE id = ? AND state = 'frozen'",
            [id],
        )?;
        if removed != 1 {
            return Err(ReplicaError::Storage(format!(
                "A verdict found no frozen intent: {id}"
            )));
        }
        Ok(())
    }

    /// Kept with its reason until the application dismisses it.
    pub fn park(&self, ctx: &mut WriteContext<'_>, id: &str, reason: &str) -> ReplicaResult<()> {
        let parked = ctx.tx.execute(
            "UPDATE intents SET state = 'refused', reason = ?, sequence = NULL, operation = NULL \
             WHERE id = ? AND state IN ('owed', 'frozen')",
            rusqlite::params![reason, id],
        )?;
        if parked != 1 {
            return Err(ReplicaError::Storage(format!(
                "Only an owed or frozen intent can be refused: {id}"
            )));
        }
        Ok(())
    }

    /// Abandon an intent the server never heard or already refused — the
    /// discard path: a row thrown away must stop owing anything, or the next
    /// drain resurrects it. Frozen bytes stay until their verdict.
    pub fn discard(&self, ctx: &mut WriteContext<'_>, id: &str) -> ReplicaResult<()> {
        ctx.tx.execute(
            "DELETE FROM intents WHERE id = ? AND state IN ('owed', 'refused')",
            [id],
        )?;
        Ok(())
    }

    /// Every discardable intent addressed at one row — the document-lane
    /// delete cascade's intent half. `except` spares one entry — a rejected
    /// create's own refusal survives its revert cascade.
    pub fn discard_entries(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
        except: Option<&str>,
    ) -> ReplicaResult<()> {
        ctx.tx.execute(
            "DELETE FROM intents \
             WHERE stream = ? AND row_id = ? AND state IN ('owed', 'refused') \
               AND (?3 IS NULL OR id != ?3)",
            rusqlite::params![stream, row_id, except],
        )?;
        Ok(())
    }

    pub(crate) fn discard_lifetime(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
        incarnation: Option<&str>,
    ) -> ReplicaResult<()> {
        for entry in self.entries_addressing(&ctx.tx, stream, row_id)? {
            if entry.op()?.incarnation.as_deref() == incarnation {
                self.discard(ctx, &entry.id)?;
            }
        }
        Ok(())
    }

    /// The document's owed delta: the next edit merges into it.
    pub(crate) fn owed_delta(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<Option<String>> {
        Ok(db
            .query_row(
                "SELECT id FROM intents \
                 WHERE row_id = ? AND stream = ? AND op = ? AND state = 'owed'",
                rusqlite::params![row_id, stream, crate::wire::verb::DOC_DELTA],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub(crate) fn discard_owed_delta(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<()> {
        ctx.tx.execute(
            "DELETE FROM intents \
             WHERE row_id = ? AND stream = ? AND op = ? AND state = 'owed'",
            rusqlite::params![row_id, stream, crate::wire::verb::DOC_DELTA],
        )?;
        Ok(())
    }

    /// Owed intents of one address — what an atomic write may not overtake.
    pub(crate) fn owed_ids(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<Vec<String>> {
        let ids = db
            .prepare(
                "SELECT id FROM intents \
                 WHERE row_id = ? AND stream = ? AND state = 'owed' ORDER BY rowid",
            )?
            .query_map(rusqlite::params![row_id, stream], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ids)
    }

    /// A lifetime's writes still awaiting their verdict, other than `except`.
    pub(crate) fn frozen_intents(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
        incarnation: Option<&str>,
        except: &str,
    ) -> ReplicaResult<Vec<String>> {
        let rows: Vec<(String, String)> = db
            .prepare(
                "SELECT id, payload FROM intents \
                 WHERE row_id = ? AND stream = ? AND state = 'frozen' AND id <> ? ORDER BY rowid",
            )?
            .query_map(rusqlite::params![row_id, stream, except], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        let mut ids = Vec::new();
        for (id, payload) in rows {
            if ReplicaOp::from_json(payload.as_bytes())?.incarnation.as_deref() == incarnation {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    // MARK: - Cold-boot reads
    //
    // A store opened over an existing file answers these without an engine —
    // what a relaunched process (or a test standing in for one) sees.

    pub fn pending_ops(&self) -> ReplicaResult<Vec<JournalRow>> {
        self.pool.read(|db| self.pending(db))
    }

    pub fn parked_ops(&self) -> ReplicaResult<Vec<JournalRow>> {
        self.pool.read(|db| self.parked(db))
    }

    pub fn fold(&self, stream: &str, row_id: &str) -> ReplicaResult<Option<Vec<u8>>> {
        self.pool
            .read(|db| Ok(self.doc(db, stream, row_id)?.map(|doc| doc.fold)))
    }

    /// Every op this row still owes, refused or not, in drain order — what a
    /// pulled snapshot has to be rebased under, and what an "unborn" check
    /// must see (the server refused the birth; the row still never existed
    /// server-side). Accepted intents are overlays, not debts.
    pub fn entries_addressing(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
    ) -> ReplicaResult<Vec<JournalRow>> {
        let rows = db
            .prepare(&format!(
                "SELECT {INTENT_COLUMNS} FROM intents \
                 WHERE row_id = ? AND stream = ? AND state <> 'accepted' \
                 ORDER BY rowid"
            ))?
            .query_map(rusqlite::params![row_id, stream], journal_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    pub fn entries_addressing_verb(
        &self,
        db: &Connection,
        stream: &str,
        row_id: &str,
        verb: &str,
    ) -> ReplicaResult<Vec<JournalRow>> {
        let rows = db
            .prepare(&format!(
                "SELECT {INTENT_COLUMNS} FROM intents \
                 WHERE row_id = ? AND stream = ? AND op = ? AND state <> 'accepted' \
                 ORDER BY rowid"
            ))?
            .query_map(rusqlite::params![row_id, stream, verb], journal_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }
}

/// Simple equality predicates over generated columns, evaluated in SQL
/// semantics over the materialized raw data.
pub fn record_matches(record: &RowRecord, equals: &ReplicaFields) -> bool {
    equals.iter().all(|(key, expected)| {
        let Some(actual) = record.fields.get(key) else {
            return false;
        };
        match (actual, expected) {
            (ReplicaValue::String(actual), ReplicaValue::String(expected)) => {
                actual.as_bytes() == expected.as_bytes()
            }
            (ReplicaValue::Integer(actual), expected) => Some(*actual) == expected.as_int(),
            (actual, ReplicaValue::Integer(expected)) => actual.as_int() == Some(*expected),
            (ReplicaValue::Number(actual), ReplicaValue::Number(expected)) => actual == expected,
            (ReplicaValue::Bool(actual), ReplicaValue::Bool(expected)) => actual == expected,
            (ReplicaValue::Bool(actual), ReplicaValue::Number(expected)) => {
                (if *actual { 1.0 } else { 0.0 }) == *expected
            }
            (ReplicaValue::Number(actual), ReplicaValue::Bool(expected)) => {
                *actual == (if *expected { 1.0 } else { 0.0 })
            }
            _ => false,
        }
    })
}

/// Test-only helper mirroring the Swift suite's `allSnapshots()` peek.
impl ReplicaStateStore {
    pub fn all_snapshots(&self) -> ReplicaResult<Vec<SnapshotRow>> {
        self.pool.read(|db| {
            let mut statement = db.prepare(
                "SELECT stream, row_id, type, data FROM snapshots ORDER BY stream, row_id",
            )?;
            let mut query = statement.query([])?;
            let mut rows = Vec::new();
            while let Some(row) = query.next()? {
                rows.push(SnapshotRow {
                    stream: row.get(0)?,
                    row_id: row.get(1)?,
                    row_type: row.get(2)?,
                    data: Self::decode_data(row.get::<_, Option<String>>(3)?.as_deref())?,
                });
            }
            Ok(rows)
        })
    }

    pub fn peek_snapshot(&self, stream: &str, row_id: &str) -> ReplicaResult<Option<SnapshotRow>> {
        self.pool.read(|db| self.snapshot(db, stream, row_id))
    }

    pub fn peek_doc(&self, stream: &str, row_id: &str) -> ReplicaResult<Option<DocRow>> {
        self.pool.read(|db| self.doc(db, stream, row_id))
    }

    pub fn peek_pending(&self) -> ReplicaResult<Vec<JournalRow>> {
        self.pool.read(|db| self.pending(db))
    }

    pub fn peek_parked(&self) -> ReplicaResult<Vec<JournalRow>> {
        self.pool.read(|db| self.parked(db))
    }
}

include!("store/gates.rs");
