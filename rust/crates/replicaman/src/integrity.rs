use crate::error::{ReplicaError, ReplicaResult};
use crate::store::ReplicaStateStore;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

pub(crate) struct IntegrityHash(Sha256);

impl IntegrityHash {
    pub fn new(domain: &str) -> Self {
        let mut hash = Sha256::new();
        hash.update(domain.as_bytes());
        hash.update([0]);
        Self(hash)
    }

    pub fn append(&mut self, value: Option<&[u8]>) {
        self.0.update(
            value
                .map_or(u64::MAX, |bytes| bytes.len() as u64)
                .to_be_bytes(),
        );
        if let Some(bytes) = value {
            self.0.update(bytes);
        }
    }

    pub fn finish(self) -> String {
        format!("{:x}", self.0.finalize())
    }
}

pub(crate) fn base_integrity(fields: [Option<&[u8]>; 9]) -> String {
    let mut hash = IntegrityHash::new("replicaman-base");
    for field in fields {
        hash.append(field);
    }
    hash.finish()
}

pub(crate) struct IntegritySnapshot {
    pub cursor: String,
    pub generation: i64,
    pub digest: String,
    pub count: i64,
}

impl ReplicaStateStore {
    /// Stream one SQLite snapshot, excluding optimistic rows and local intents.
    pub(crate) fn integrity_snapshot(
        &self,
        db: &Connection,
        shard: &str,
    ) -> ReplicaResult<IntegritySnapshot> {
        let cursor = self
            .cursor(db, shard)?
            .ok_or_else(|| ReplicaError::Protocol {
                code: "CheckpointRequired".into(),
                message: "Synchronize before verifying the replica".into(),
            })?;
        let mut hash = IntegrityHash::new("replicaman-view");
        let mut count = 0;
        let mut statement = db.prepare(
            "SELECT stream, row_id, incarnation, revision, type, data, codec, fold, integrity
             FROM base WHERE shard = ? ORDER BY stream COLLATE BINARY, row_id COLLATE BINARY",
        )?;
        let mut rows = statement.query([shard])?;
        while let Some(row) = rows.next()? {
            let stream: String = row.get(0)?;
            let id: String = row.get(1)?;
            let incarnation: String = row.get(2)?;
            let revision = row.get::<_, i64>(3)?.to_string();
            let row_type: Option<String> = row.get(4)?;
            let data: String = row.get(5)?;
            let codec: Option<String> = row.get(6)?;
            let fold: Option<Vec<u8>> = row.get(7)?;
            let expected: Option<String> = row.get(8)?;
            let actual = base_integrity([
                Some(stream.as_bytes()),
                Some(id.as_bytes()),
                Some(shard.as_bytes()),
                Some(incarnation.as_bytes()),
                Some(revision.as_bytes()),
                row_type.as_deref().map(str::as_bytes),
                Some(data.as_bytes()),
                codec.as_deref().map(str::as_bytes),
                fold.as_deref(),
            ]);
            if expected.as_deref() != Some(actual.as_str()) {
                return Err(ReplicaError::Storage(format!(
                    "Authoritative row integrity failed: {stream}/{id}; local work is retained"
                )));
            }
            for field in [&stream, &id, &incarnation, &revision] {
                hash.append(Some(field.as_bytes()));
            }
            count += 1;
        }
        Ok(IntegritySnapshot {
            cursor,
            generation: self.read_generation(db, shard)?,
            digest: hash.finish(),
            count,
        })
    }
}
