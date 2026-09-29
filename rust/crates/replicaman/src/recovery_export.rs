use crate::{ReplicaError, ReplicaResult, ReplicaStateStore};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

impl ReplicaStateStore {
    /// Stream one branch as JSON Lines from one SQLite snapshot. The sink must
    /// only write bytes, without reentering this store. Sink failures propagate
    /// and retain the archive. Publish only when this returns successfully;
    /// a complete export ends with a `complete` record.
    pub fn export_recovery(
        &self,
        id: &str,
        mut write: impl FnMut(&[u8]) -> ReplicaResult<()>,
    ) -> ReplicaResult<()> {
        self.pool().read(|db| export(db, id, &mut write))
    }
}

fn emit(value: Value, write: &mut impl FnMut(&[u8]) -> ReplicaResult<()>) -> ReplicaResult<()> {
    let mut bytes =
        serde_json::to_vec(&value).map_err(|error| ReplicaError::Codec(error.to_string()))?;
    bytes.push(b'\n');
    write(&bytes)
}

fn export(
    db: &Connection,
    id: &str,
    write: &mut impl FnMut(&[u8]) -> ReplicaResult<()>,
) -> ReplicaResult<()> {
    let record = db
        .query_row(
            "SELECT stream, row_id, incarnation, reason, created_at FROM recoveries WHERE id = ?",
            [id],
            |row| {
                Ok(json!({
                    "type": "record", "format": "replicaman-recovery", "version": 1, "id": id,
                    "stream": row.get::<_, String>(0)?, "row_id": row.get::<_, String>(1)?,
                    "incarnation": row.get::<_, String>(2)?, "reason": row.get::<_, String>(3)?,
                    "created_at": row.get::<_, f64>(4)?,
                }))
            },
        )
        .optional()?
        .ok_or_else(|| ReplicaError::Storage("Recovery record does not exist".into()))?;
    emit(record, write)?;

    let mut query = db.prepare(
        "SELECT kind, part_key, length(content) FROM recovery_parts
         WHERE recovery_id = ? ORDER BY kind, part_key",
    )?;
    let mut parts = query.query([id])?;
    let mut count = 0_i64;
    let mut bytes = 0_i64;
    while let Some(part) = parts.next()? {
        let size: i64 = part.get(2)?;
        export_part(
            db,
            id,
            &part.get::<_, String>(0)?,
            &part.get::<_, String>(1)?,
            size,
            write,
        )?;
        count += 1;
        bytes += size;
    }
    emit(
        json!({"type": "complete", "parts": count, "bytes": bytes.to_string()}),
        write,
    )
}

fn export_part(
    db: &Connection,
    id: &str,
    kind: &str,
    key: &str,
    size: i64,
    write: &mut impl FnMut(&[u8]) -> ReplicaResult<()>,
) -> ReplicaResult<()> {
    emit(
        json!({"type": "part", "kind": kind, "key": key, "bytes": size.to_string()}),
        write,
    )?;
    let mut offset = 0_i64;
    while offset < size {
        let chunk: Vec<u8> = db.query_row(
            "SELECT substr(content, ?, 262144) FROM recovery_parts
             WHERE recovery_id = ? AND kind = ? AND part_key = ?",
            params![offset + 1, id, kind, key],
            |row| row.get(0),
        )?;
        if chunk.is_empty() {
            return Err(ReplicaError::Storage("Recovery part is incomplete".into()));
        }
        emit(
            json!({
                "type": "chunk", "offset": offset.to_string(),
                "sha256": crate::protocol::digest(&chunk), "content": STANDARD.encode(&chunk),
            }),
            write,
        )?;
        offset += chunk.len() as i64;
    }
    Ok(())
}
