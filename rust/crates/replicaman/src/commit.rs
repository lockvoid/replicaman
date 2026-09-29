use crate::protocol::{self, Header};
use crate::{ReplicaError, ReplicaResult, ReplicaSchema};
use serde::Deserialize;

/// Capture before sending a domain command. A reply may only trigger refresh
/// while this engine still owns the same authenticated store.
#[derive(Clone, Debug)]
pub struct ReplicaCommitSession {
    pub(crate) engine: String,
    pub(crate) binding: u64,
}

#[derive(Deserialize)]
struct Hint {
    #[serde(flatten)]
    header: Header,
    shards: Vec<String>,
}

pub(crate) fn decode_commit(
    encoded: &str,
    schema: &ReplicaSchema,
    dataset: Option<&str>,
) -> ReplicaResult<Vec<String>> {
    let bytes = protocol::binary(encoded, 64 * 1024)?;
    let hint: Hint = protocol::decode(&bytes)?;
    hint.header.validate(schema, dataset)?;
    let unique: std::collections::HashSet<_> = hint.shards.iter().collect();
    if unique.len() != hint.shards.len()
        || hint
            .shards
            .iter()
            .any(|shard| !schema.shards().contains(shard))
    {
        return Err(ReplicaError::InvalidCommit);
    }
    Ok(hint.shards)
}
