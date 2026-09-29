use super::*;
use crate::value::ReplicaFields;

pub(crate) struct BaseRow {
    pub incarnation: String,
    pub revision: i64,
    pub row_type: Option<String>,
    pub data: ReplicaFields,
    pub codec: Option<String>,
    pub fold: Option<Vec<u8>>,
}

impl ReplicaStateStore {
    pub(crate) fn base_row(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<Option<BaseRow>> {
        let mut statement = db.prepare("SELECT incarnation, revision, type, data, codec, fold FROM base WHERE stream = ? AND row_id = ?")?;
        let mut rows = statement.query(params![stream, id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let data: String = row.get(3)?;
        Ok(Some(BaseRow {
            incarnation: row.get(0)?,
            revision: row.get(1)?,
            row_type: row.get(2)?,
            data: protocol::decode(data.as_bytes())?,
            codec: row.get(4)?,
            fold: row.get(5)?,
        }))
    }

    pub(crate) fn save_base(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
        shard: &str,
        row: &BaseRow,
    ) -> ReplicaResult<()> {
        let data = ReplicaJson::encode_fields(&row.data)?;
        let revision = row.revision.to_string();
        let integrity = crate::integrity::base_integrity([
            Some(stream.as_bytes()),
            Some(id.as_bytes()),
            Some(shard.as_bytes()),
            Some(row.incarnation.as_bytes()),
            Some(revision.as_bytes()),
            row.row_type.as_deref().map(str::as_bytes),
            Some(data.as_bytes()),
            row.codec.as_deref().map(str::as_bytes),
            row.fold.as_deref(),
        ]);
        db.execute(
            "INSERT INTO base (stream, row_id, shard, incarnation, revision, type, data, codec, fold, integrity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard,
            incarnation = excluded.incarnation, revision = excluded.revision, type = excluded.type,
            data = excluded.data, codec = excluded.codec, fold = excluded.fold, integrity = excluded.integrity",
            params![
                stream,
                id,
                shard,
                row.incarnation,
                row.revision,
                row.row_type,
                data,
                row.codec,
                row.fold,
                integrity
            ],
        )?;
        Ok(())
    }

    pub(crate) fn has_local_birth(
        &self,
        db: &Connection,
        stream: &str,
        id: &str,
        incarnation: &str,
    ) -> ReplicaResult<bool> {
        let payloads: Vec<String> = db
            .prepare(
                "SELECT payload FROM intents WHERE row_id = ? AND stream = ? AND state <> 'refused'",
            )?
            .query_map(params![id, stream], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let operations = payloads
            .iter()
            .map(|payload| ReplicaOp::from_json(payload.as_bytes()))
            .collect::<ReplicaResult<Vec<_>>>()?;
        Ok(operations.iter().any(|op| {
            op.verb == crate::verb::ROW_CREATE && op.incarnation.as_deref() == Some(incarnation)
        }))
    }
}
