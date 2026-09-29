mod atomic_write;
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{ReplicaError, ReplicaResult};
use crate::protocol;
use crate::schema::{ReplicaLane, ReplicaSchema};
use crate::store::{INTENT_COLUMNS, JournalRow, ReplicaStateStore, journal_row};
use crate::value::ReplicaValue;
use crate::wire::{ReplicaJson, ReplicaOp};

mod base;
mod checkpoints;
pub(crate) use base::BaseRow;
pub(crate) use checkpoints::Download;

const FORMAT: i64 = 3;

/// The store's identity, the dataset it synchronizes (unknown until the first
/// pull answers) and the sequence the next frozen submission takes.
pub(crate) struct Meta {
    pub store: String,
    pub dataset: Option<String>,
    pub next: i64,
}

/// One frozen submission: the operations the server receives, their ids, and
/// the frozen intents they answer for, in the same order.
pub(crate) struct Submission {
    pub sequence: i64,
    pub operations: Vec<ReplicaValue>,
    pub ids: Vec<String>,
    pub entries: Vec<JournalRow>,
}

pub(crate) fn prepare(db: &Connection) -> ReplicaResult<()> {
    let format: i64 = db.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let earlier =
        || ReplicaError::Storage("Unsupported earlier store format; open a fresh store".into());
    match format {
        0 => {
            let occupied: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table')",
                [],
                |row| row.get(0),
            )?;
            if occupied {
                return Err(earlier());
            }
            db.execute_batch(include_str!("sync_schema.sql"))?;
            db.pragma_update(None, "user_version", FORMAT)?;
        }
        FORMAT => db.execute_batch(include_str!("sync_schema.sql"))?,
        format if format < FORMAT => return Err(earlier()),
        _ => {
            return Err(ReplicaError::Storage(
                "Unsupported store format; upgrade required".into(),
            ));
        }
    }
    db.execute(
        "INSERT OR IGNORE INTO meta (id, store_id) VALUES (1, ?)",
        [crate::id::ulid()],
    )?;
    Ok(())
}

impl ReplicaStateStore {
    pub(crate) fn meta(&self, db: &Connection) -> ReplicaResult<Meta> {
        Ok(db.query_row(
            "SELECT store_id, dataset, next_sequence FROM meta WHERE id = 1",
            [],
            |row| {
                Ok(Meta {
                    store: row.get(0)?,
                    dataset: row.get(1)?,
                    next: row.get(2)?,
                })
            },
        )?)
    }

    /// The first pull's answer names the dataset every later request carries;
    /// a known dataset is never replaced.
    pub(crate) fn adopt_dataset(&self, db: &Connection, dataset: &str) -> ReplicaResult<()> {
        db.execute(
            "UPDATE meta SET dataset = ? WHERE id = 1 AND dataset IS NULL",
            [dataset],
        )?;
        Ok(())
    }

    pub(crate) fn require_schema(&self, schema: &ReplicaSchema) -> ReplicaResult<()> {
        self.pool().write(|ctx| {
            let (namespace, version): (Option<String>, Option<i64>) = ctx.tx.query_row(
                "SELECT namespace, schema_version FROM meta WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if namespace.is_some()
                && (namespace.as_deref() != Some(&schema.namespace)
                    || version != Some(schema.version))
            {
                return Err(ReplicaError::Storage(
                    "Store belongs to another namespace or schema".into(),
                ));
            }
            ctx.tx.execute(
                "UPDATE meta SET namespace = ?, schema_version = ? WHERE id = 1",
                params![schema.namespace, schema.version],
            )?;
            Ok(())
        })
    }

    pub(crate) fn incarnation(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<Option<String>> {
        Ok(db
            .query_row(
                "SELECT incarnation FROM entities WHERE stream = ? AND row_id = ?",
                params![stream, id],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub(crate) fn set_incarnation(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
        shard: &str,
        incarnation: &str,
    ) -> ReplicaResult<()> {
        if self
            .incarnation(db, stream, id)?
            .as_deref()
            .is_some_and(|previous| previous != incarnation)
        {
            db.execute(
                "DELETE FROM entity_references WHERE stream = ? AND row_id = ?",
                params![stream, id],
            )?;
        }
        db.execute("INSERT INTO entities (stream, row_id, shard, incarnation) VALUES (?, ?, ?, ?)
            ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard,
                predecessor = CASE WHEN entities.incarnation = excluded.incarnation THEN entities.predecessor END,
                incarnation = excluded.incarnation",
            params![stream, id, shard, incarnation])?;
        Ok(())
    }

    pub(crate) fn predecessor(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<Option<String>> {
        Ok(db
            .query_row(
                "SELECT predecessor FROM entities WHERE stream = ? AND row_id = ?",
                params![stream, id],
                |row| row.get(0),
            )
            .optional()?
            .flatten())
    }

    // Undo only a birth known never to have reached the server.
    pub(crate) fn cancel_unsent_birth(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<()> {
        let previous = self.predecessor(db, stream, id)?;
        db.execute(
            "DELETE FROM entity_references WHERE stream = ? AND row_id = ?",
            params![stream, id],
        )?;
        if let Some(previous) = previous {
            db.execute("UPDATE entities SET incarnation = ?, predecessor = NULL WHERE stream = ? AND row_id = ?",
                params![previous, stream, id])?;
        } else {
            db.execute(
                "DELETE FROM entities WHERE stream = ? AND row_id = ?",
                params![stream, id],
            )?;
        }
        Ok(())
    }

    pub(crate) fn identify(
        &self,
        db: &Connection,
        operation: &ReplicaOp,
        schema: &crate::ReplicaSchema,
        birth: bool,
        preimage: Option<&[u8]>,
    ) -> ReplicaResult<ReplicaOp> {
        let spec = schema.spec(&operation.stream);
        let baseline = self.snapshot(db, &operation.stream, &operation.row_id)?;
        let mut data = match baseline {
            Some(row) => row.data,
            None => match preimage.map(crate::ReplicaPreimage::parse).transpose()? {
                // A deletion has already removed its visible row. Preserve
                // its references through the saved baseline and held release.
                Some(crate::ReplicaPreimage::Row { data, .. }) => data,
                _ => Default::default(),
            },
        };
        data.extend(operation.data.clone().unwrap_or_default());
        let mut references = Vec::new();
        for reference in spec
            .map(|spec| spec.references.as_slice())
            .unwrap_or_default()
        {
            let Some(id) = reference.target(&operation.row_id, &data)? else {
                continue;
            };
            let lifetime = self.incarnation(db, &reference.stream, &id)?;
            if self.snapshot(db, &reference.stream, &id)?.is_none() || lifetime.is_none() {
                return Err(ReplicaError::Storage(format!(
                    "Missing reference target: {}/{id}",
                    reference.stream
                )));
            }
            let bound: Option<String> = if birth {
                None
            } else {
                db.query_row("SELECT target_incarnation FROM entity_references
                    WHERE stream = ? AND row_id = ? AND name = ? AND target_stream = ? AND target_id = ?",
                    params![operation.stream, operation.row_id, reference.name, reference.stream, id], |row| row.get(0)).optional()?
            };
            if bound.is_some() && bound != lifetime {
                return Err(ReplicaError::Storage(
                    "Referenced parent lifetime changed; recovery is required".into(),
                ));
            }
            references.push(crate::ReplicaReference {
                name: reference.name.clone(),
                stream: reference.stream.clone(),
                id,
                incarnation: lifetime.expect("validated reference lifetime"),
            });
        }
        let parent = references.iter().find(|parent| {
            spec.and_then(|spec| spec.lifetime_from.as_deref()) == Some(parent.name.as_str())
        });
        let derived = parent.map(|parent| {
            crate::reference::derived_incarnation(
                &schema.namespace,
                &operation.stream,
                &operation.row_id,
                parent,
            )
        });
        let current = self.incarnation(db, &operation.stream, &operation.row_id)?;
        if !birth && derived.is_some() && derived != current {
            return Err(ReplicaError::Storage(
                "The parent lifetime changed; this child needs recovery".into(),
            ));
        }
        let is_derived = derived.is_some();
        let identity = if birth {
            Some(derived.unwrap_or_else(crate::id::ulid))
        } else {
            current.clone()
        }
        .ok_or_else(|| {
            ReplicaError::Storage(format!(
                "Missing incarnation for {}/{}",
                operation.stream, operation.row_id
            ))
        })?;
        self.set_incarnation(
            db,
            &operation.stream,
            &operation.row_id,
            schema.shard_of(&operation.stream),
            &identity,
        )?;
        if birth {
            db.execute(
                "UPDATE entities SET predecessor = ? WHERE stream = ? AND row_id = ?",
                params![current, operation.stream, operation.row_id],
            )?;
        }
        let mut op = operation.clone();
        op.replaces = if op.verb == crate::wire::verb::ROW_CREATE && !is_derived {
            self.predecessor(db, &op.stream, &op.row_id)?
        } else {
            None
        };
        op.incarnation = Some(identity);
        op.references = references;
        db.execute(
            "DELETE FROM entity_references WHERE stream = ? AND row_id = ?",
            params![op.stream, op.row_id],
        )?;
        for reference in &op.references {
            db.execute(
                "INSERT INTO entity_references VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    op.stream,
                    op.row_id,
                    reference.name,
                    reference.stream,
                    reference.id,
                    reference.incarnation
                ],
            )?;
        }
        Ok(op)
    }

    /// The oldest frozen submissions, or — when none remain — the lane's next
    /// owed intents frozen as single-operation submissions. Freezing mints
    /// each operation's server id, so a retry resends identical operations.
    pub(crate) fn freeze_submissions(
        &self,
        db: &Connection,
        lane: Option<ReplicaLane>,
        limit: usize,
    ) -> ReplicaResult<Vec<Submission>> {
        let frozen = self.frozen_submissions(db, limit)?;
        if !frozen.is_empty() {
            return Ok(frozen);
        }
        let lane = lane.map(ReplicaLane::as_str);
        let owed: Vec<String> = db
            .prepare(
                "SELECT id FROM intents WHERE state = 'owed' AND (?1 IS NULL OR lane = ?1) \
                 ORDER BY rowid LIMIT ?2",
            )?
            .query_map(params![lane, limit], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let mut sequence = self.meta(db)?.next;
        let mut bytes = 0;
        for id in owed {
            let payload: String =
                db.query_row("SELECT payload FROM intents WHERE id = ?", [&id], |row| {
                    row.get(0)
                })?;
            let mut op = ReplicaOp::from_json(payload.as_bytes())?;
            if op.incarnation.is_none() {
                return Err(ReplicaError::Storage("Intent has no incarnation".into()));
            }
            op.id = crate::id::uuid();
            let content = ReplicaJson::to_vec(&ReplicaValue::Array(vec![op.to_value()]))?;
            if content.len() > protocol::ENTITY_BYTES {
                return Err(ReplicaError::Storage(
                    "Mutation exceeds 32 MiB; local bytes are retained".into(),
                ));
            }
            if bytes + content.len() > protocol::ENTITY_BYTES {
                break;
            }
            if sequence == i64::MAX {
                return Err(ReplicaError::Storage(
                    "Submission sequence exhausted; recovery is required".into(),
                ));
            }
            db.execute(
                "INSERT INTO submissions (sequence, content) VALUES (?, ?)",
                params![sequence, content],
            )?;
            self.freeze(db, &id, sequence, &op.id)?;
            bytes += content.len();
            sequence += 1;
        }
        db.execute("UPDATE meta SET next_sequence = ? WHERE id = 1", [sequence])?;
        self.frozen_submissions(db, limit)
    }

    /// An owed intent's bytes now live in submission `sequence` as `operation`.
    pub(crate) fn freeze(
        &self,
        db: &Connection,
        id: &str,
        sequence: i64,
        operation: &str,
    ) -> ReplicaResult<()> {
        let frozen = db.execute(
            "UPDATE intents SET state = 'frozen', sequence = ?, operation = ? \
             WHERE id = ? AND state = 'owed'",
            params![sequence, operation, id],
        )?;
        if frozen != 1 {
            return Err(ReplicaError::Storage(format!(
                "Only an owed intent can be frozen: {id}"
            )));
        }
        Ok(())
    }

    /// The oldest frozen submissions holding at most `limit` operations
    /// together; a submission is never split.
    pub(crate) fn frozen_submissions(
        &self,
        db: &Connection,
        limit: usize,
    ) -> ReplicaResult<Vec<Submission>> {
        let mut statement =
            db.prepare("SELECT sequence, content FROM submissions ORDER BY sequence")?;
        let mut rows = statement.query([])?;
        let mut operations = 0;
        let mut bytes = 0;
        let mut submissions = Vec::new();
        while let Some(row) = rows.next()? {
            let sequence: i64 = row.get(0)?;
            let content: Vec<u8> = row.get(1)?;
            let values: Vec<ReplicaValue> = serde_json::from_slice(&content).map_err(|error| {
                ReplicaError::Storage(format!("Frozen submission unreadable: {error}"))
            })?;
            if !submissions.is_empty()
                && (operations + values.len() > limit
                    || bytes + content.len() > protocol::ENTITY_BYTES)
            {
                break;
            }
            operations += values.len();
            bytes += content.len();
            let disagree =
                || ReplicaError::Storage("Frozen submission and its intents disagree".into());
            let mut ids = Vec::with_capacity(values.len());
            let mut entries = Vec::with_capacity(values.len());
            for value in &values {
                let mut op = ReplicaOp::from_value(value)?;
                let entry = db
                    .query_row(
                        &format!(
                            "SELECT {INTENT_COLUMNS} FROM intents WHERE operation = ? AND sequence = ?"
                        ),
                        params![op.id, sequence],
                        journal_row,
                    )
                    .optional()?
                    .ok_or_else(disagree)?;
                ids.push(std::mem::replace(&mut op.id, entry.id.clone()));
                op.group = None;
                if op != entry.op()? {
                    return Err(disagree());
                }
                entries.push(entry);
            }
            submissions.push(Submission {
                sequence,
                operations: values,
                ids,
                entries,
            });
        }
        Ok(submissions)
    }

    /// The server accepted the intent: it stays visible over the base until a
    /// round that started after its submission publishes.
    pub(crate) fn accept(&self, db: &Connection, id: &str) -> ReplicaResult<()> {
        let accepted = db.execute(
            "UPDATE intents SET state = 'accepted', operation = NULL \
             WHERE id = ? AND state = 'frozen'",
            [id],
        )?;
        if accepted != 1 {
            return Err(ReplicaError::Storage(format!(
                "A verdict found no frozen intent: {id}"
            )));
        }
        Ok(())
    }

    /// Every operation of the submission was answered.
    pub(crate) fn finish_submission(&self, db: &Connection, sequence: i64) -> ReplicaResult<()> {
        let unanswered: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM intents WHERE state = 'frozen' AND sequence = ?)",
            [sequence],
            |row| row.get(0),
        )?;
        if unanswered {
            return Err(ReplicaError::Storage(format!(
                "Submission {sequence} still has an unanswered intent"
            )));
        }
        db.execute("DELETE FROM submissions WHERE sequence = ?", [sequence])?;
        Ok(())
    }

    /// Accepted intents of one address, in submission order.
    pub(crate) fn overlays(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<Vec<ReplicaOp>> {
        let mut statement = db.prepare(
            "SELECT payload FROM intents WHERE row_id = ? AND stream = ? AND state = 'accepted' \
             ORDER BY sequence, rowid",
        )?;
        statement
            .query_map(params![id, stream], |row| row.get::<_, String>(0))?
            .map(|payload| ReplicaOp::from_json(payload?.as_bytes()))
            .collect()
    }
}
