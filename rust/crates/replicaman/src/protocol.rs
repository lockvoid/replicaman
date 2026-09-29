use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::error::{ReplicaError, ReplicaResult};
use crate::schema::ReplicaSchema;
use crate::transport::{ReplicaEndpoint, ReplicaTransport};
use crate::value::ReplicaValue;
use crate::wire::{ReplicaPullResponse, ReplicaVerdict, decode_pull};

pub(crate) const VERSION: i64 = 2;
pub(crate) const MAX_OPERATIONS: usize = 100;
pub(crate) const ENTITY_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const RESPONSE_BYTES: usize = 2 * ENTITY_BYTES;

pub(crate) fn invalid(message: impl Into<String>) -> ReplicaError {
    ReplicaError::Protocol {
        code: "InvalidResponse".into(),
        message: message.into(),
    }
}

pub(crate) fn encode<T: serde::Serialize>(value: &T) -> ReplicaResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| invalid(format!("Encode JSON: {error}")))
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> ReplicaResult<T> {
    serde_json::from_slice(bytes).map_err(|error| invalid(format!("Decode JSON: {error}")))
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn counter(value: &str) -> ReplicaResult<i64> {
    if value.is_empty()
        || value.len() > 19
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid("Expected decimal int64 counter"));
    }
    value
        .parse::<i64>()
        .map_err(|error| invalid(format!("Counter exceeds int64: {error}")))
}

pub(crate) fn binary(value: &str, limit: usize) -> ReplicaResult<Vec<u8>> {
    if value.len() > limit.div_ceil(3) * 4 {
        return Err(invalid("Oversized binary field"));
    }
    let bytes = BASE64
        .decode(value)
        .map_err(|error| invalid(format!("Invalid base64: {error}")))?;
    if bytes.len() > limit || BASE64.encode(&bytes) != value {
        return Err(invalid("Noncanonical or oversized base64"));
    }
    Ok(bytes)
}

#[derive(Deserialize)]
pub(crate) struct Header {
    pub protocol: i64,
    pub namespace: String,
    pub schema: i64,
    pub dataset: String,
}

impl Header {
    /// A store that never synchronized accepts the server's dataset; one that
    /// did refuses any other.
    pub fn validate(&self, schema: &ReplicaSchema, dataset: Option<&str>) -> ReplicaResult<()> {
        if self.protocol != VERSION
            || self.namespace != schema.namespace
            || self.schema != schema.version
        {
            return Err(ReplicaError::Protocol {
                code: "UpgradeRequired".into(),
                message: "Incompatible protocol, namespace or schema".into(),
            });
        }
        if self.dataset.is_empty() || dataset.is_some_and(|saved| saved != self.dataset) {
            return Err(ReplicaError::Protocol {
                code: "DatasetChanged".into(),
                message: "The authoritative dataset changed".into(),
            });
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Pushed {
    verdicts: Vec<ReplicaVerdict>,
}

#[derive(Deserialize)]
pub(crate) struct Verified {
    pub shard: String,
    pub cursor: String,
    pub count: String,
    pub digest: String,
}

pub(crate) struct Connection<'a> {
    pub transport: &'a dyn ReplicaTransport,
    pub schema: &'a ReplicaSchema,
}

impl Connection<'_> {
    async fn exchange(
        &self,
        endpoint: ReplicaEndpoint,
        dataset: Option<&str>,
        fields: Value,
    ) -> ReplicaResult<(Header, Vec<u8>)> {
        let mut request = Map::new();
        request.insert("protocol".into(), json!(VERSION));
        request.insert("namespace".into(), json!(self.schema.namespace));
        request.insert("schema".into(), json!(self.schema.version));
        request.insert("dataset".into(), json!(dataset));
        let Value::Object(fields) = fields else {
            return Err(invalid("Request fields are not an object"));
        };
        request.extend(fields);
        let bytes = self.transport.exchange(endpoint, encode(&request)?).await?;
        let header: Header = decode(&bytes)?;
        header.validate(self.schema, dataset)?;
        Ok((header, bytes))
    }

    /// One page of a pull round. `cursor` is `None` for a baseline.
    pub async fn pull(
        &self,
        shard: &str,
        cursor: Option<&str>,
        limit: usize,
        dataset: Option<&str>,
    ) -> ReplicaResult<(Header, ReplicaPullResponse)> {
        let fields = json!({"shard": shard, "cursor": cursor, "limit": limit});
        let (header, bytes) = self
            .exchange(ReplicaEndpoint::Pull, dataset, fields)
            .await?;
        Ok((header, decode_pull(&bytes)?))
    }

    pub async fn push(
        &self,
        operations: &[ReplicaValue],
        dataset: &str,
    ) -> ReplicaResult<Vec<ReplicaVerdict>> {
        let fields = json!({"ops": operations});
        let (_, bytes) = self
            .exchange(ReplicaEndpoint::Push, Some(dataset), fields)
            .await?;
        Ok(decode::<Pushed>(&bytes)?.verdicts)
    }

    pub async fn verify(
        &self,
        shard: &str,
        cursor: &str,
        dataset: &str,
    ) -> ReplicaResult<Verified> {
        let fields = json!({"shard": shard, "cursor": cursor});
        let (_, bytes) = self
            .exchange(ReplicaEndpoint::Verify, Some(dataset), fields)
            .await?;
        decode(&bytes)
    }
}
