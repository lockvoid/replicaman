use super::*;

struct AtomicEntry {
    id: String,
    payload: Vec<u8>,
}

impl ReplicaStateStore {
    pub(crate) fn freeze_atomic_write(&self, db: &Connection, ids: &[String]) -> ReplicaResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        if ids.len() > protocol::MAX_OPERATIONS {
            return Err(ReplicaError::AtomicWriteBlocked(
                "An atomic write supports at most 100 operations".into(),
            ));
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let selection = format!("id IN ({placeholders})");
        let bytes: i64 = db.query_row(&format!("SELECT COALESCE(SUM(length(CAST(payload AS BLOB))), 0) FROM intents WHERE {selection}"),
            rusqlite::params_from_iter(ids), |row| row.get(0))?;
        if bytes > protocol::ENTITY_BYTES as i64 {
            return Err(ReplicaError::AtomicWriteBlocked(
                "An atomic write exceeds 32 MiB".into(),
            ));
        }
        let mut statement = db.prepare(&format!(
            "SELECT id, payload FROM intents WHERE {selection} ORDER BY rowid"
        ))?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(ids), |row| {
                Ok(AtomicEntry {
                    id: row.get(0)?,
                    payload: row.get::<_, String>(1)?.into_bytes(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if rows.is_empty() {
            return Ok(());
        }
        let group = (rows.len() > 1).then(crate::id::uuid);
        let operations = rows
            .iter()
            .map(|entry| {
                let mut op = ReplicaOp::from_json(&entry.payload)?;
                op.id = crate::id::uuid();
                op.group = group.clone();
                Ok(op)
            })
            .collect::<ReplicaResult<Vec<_>>>()?;
        let content = ReplicaJson::to_vec(&ReplicaValue::Array(
            operations.iter().map(ReplicaOp::to_value).collect(),
        ))?;
        if content.len() > protocol::ENTITY_BYTES {
            return Err(ReplicaError::AtomicWriteBlocked(
                "An atomic write exceeds 32 MiB".into(),
            ));
        }
        let sequence = self.meta(db)?.next;
        if sequence == i64::MAX {
            return Err(ReplicaError::Storage(
                "Submission sequence exhausted".into(),
            ));
        }
        db.execute(
            "INSERT INTO submissions (sequence, content) VALUES (?, ?)",
            params![sequence, content],
        )?;
        for (entry, op) in rows.iter().zip(operations) {
            self.freeze(db, &entry.id, sequence, &op.id)?;
        }
        db.execute(
            "UPDATE meta SET next_sequence = ? WHERE id = 1",
            [sequence + 1],
        )?;
        Ok(())
    }
}
