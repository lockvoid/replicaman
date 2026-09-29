//! What a client write displaced, captured with its journal entry
//! (client-local, never on the wire) so a rejected verdict can undo the write
//! atomically in the verdict transaction.
//!
//! Ported from the `ReplicaPreimage` enum at the head of `ReplicaEngine.swift`.
//! The encoding mirrors Swift's synthesized enum `Codable` shape — one
//! single-key object naming the case — and rides the same byte-stable encoder
//! as everything else, so a replayed identity rebind is byte-identical.

use crate::error::{ReplicaError, ReplicaResult};
use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::ReplicaJson;

#[derive(Clone, Debug, PartialEq)]
pub enum ReplicaPreimage {
    /// The row did not exist — a rejected create removes it.
    Absent,
    /// A patch touched exactly these fields: `values` restore, `missing` were
    /// introduced by the patch and are removed. Interim server fields stay
    /// untouched.
    Fields {
        values: ReplicaFields,
        missing: Vec<String>,
    },
    /// A delete displaced this whole row — restore it byte-identical.
    Row {
        shard: String,
        row_type: Option<String>,
        data: ReplicaFields,
    },
}

impl ReplicaPreimage {
    fn to_value(&self) -> ReplicaValue {
        let mut outer = ReplicaFields::new();
        match self {
            Self::Absent => {
                outer.insert("absent".into(), ReplicaValue::Object(ReplicaFields::new()));
            }
            Self::Fields { values, missing } => {
                let mut body = ReplicaFields::new();
                body.insert(
                    "missing".into(),
                    ReplicaValue::Array(missing.iter().map(ReplicaValue::string).collect()),
                );
                body.insert("values".into(), ReplicaValue::Object(values.clone()));
                outer.insert("fields".into(), ReplicaValue::Object(body));
            }
            Self::Row {
                shard,
                row_type,
                data,
            } => {
                let mut body = ReplicaFields::new();
                body.insert("data".into(), ReplicaValue::Object(data.clone()));
                body.insert("shard".into(), ReplicaValue::string(shard.clone()));
                if let Some(row_type) = row_type {
                    body.insert("type".into(), ReplicaValue::string(row_type.clone()));
                }
                outer.insert("row".into(), ReplicaValue::Object(body));
            }
        }
        ReplicaValue::Object(outer)
    }

    pub fn encoded(&self) -> ReplicaResult<Vec<u8>> {
        ReplicaJson::to_vec(&self.to_value())
    }

    /// Strict: the identity rebind's preflight fails closed on an unreadable
    /// preimage rather than rewriting half the world.
    pub fn parse(bytes: &[u8]) -> ReplicaResult<Self> {
        let value: ReplicaValue = serde_json::from_slice(bytes)
            .map_err(|error| ReplicaError::Storage(format!("preimage unreadable: {error}")))?;
        Self::from_value(&value)
    }

    pub(crate) fn undoing(mut self, images: &[Self]) -> Self {
        for image in images.iter().rev() {
            match image {
                Self::Absent | Self::Row { .. } => self = image.clone(),
                Self::Fields { values, missing } => {
                    if let Self::Row { data, .. } = &mut self {
                        for key in missing {
                            data.remove(key);
                        }
                        data.extend(values.clone());
                    }
                }
            }
        }
        self
    }

    fn from_value(value: &ReplicaValue) -> ReplicaResult<Self> {
        let bad = || ReplicaError::Storage("preimage is not a known case".to_owned());
        let ReplicaValue::Object(outer) = value else {
            return Err(bad());
        };
        if outer.len() != 1 {
            return Err(bad());
        }
        if let Some(ReplicaValue::Object(_)) = outer.get("absent") {
            return Ok(Self::Absent);
        }
        if let Some(ReplicaValue::Object(body)) = outer.get("fields") {
            let values = body
                .get("values")
                .and_then(ReplicaValue::fields)
                .ok_or_else(bad)?
                .clone();
            let missing = body
                .get("missing")
                .and_then(ReplicaValue::items)
                .ok_or_else(bad)?
                .iter()
                .map(|item| item.as_string().map(str::to_owned).ok_or_else(bad))
                .collect::<ReplicaResult<Vec<_>>>()?;
            return Ok(Self::Fields { values, missing });
        }
        if let Some(ReplicaValue::Object(body)) = outer.get("row") {
            let shard = body
                .get("shard")
                .and_then(ReplicaValue::as_string)
                .ok_or_else(bad)?
                .to_owned();
            let row_type = match body.get("type") {
                None | Some(ReplicaValue::Null) => None,
                Some(ReplicaValue::String(value)) => Some(value.clone()),
                _ => return Err(bad()),
            };
            let data = body
                .get("data")
                .and_then(ReplicaValue::fields)
                .ok_or_else(bad)?
                .clone();
            return Ok(Self::Row {
                shard,
                row_type,
                data,
            });
        }
        Err(bad())
    }
}
