//! The wire grammar, frozen by the server engine (`ruby/lib/replica_man`):
//! `noun.verb`, two nouns (`row`, `doc`), everything addressed by
//! `(stream, id)`. Frames flow down on pull; ops flow up on push and come
//! back as verdicts keyed by the operation ids the client minted.
//!
//! Ported from `Sources/ReplicaMan/ReplicaWire.swift`.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{ReplicaError, ReplicaResult};
use crate::protocol::{self, invalid};
use crate::value::{ReplicaFields, ReplicaValue};

// MARK: - Frames (down)

/// One pulled frame. Every frame names the entity lifetime it describes.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplicaFrame {
    /// Unconditional replacement of the local copy — deliberately NOT
    /// "upsert": no insert-or-update decision exists.
    RowSet {
        stream: String,
        id: String,
        incarnation: String,
        revision: i64,
        row_type: Option<String>,
        data: ReplicaFields,
    },
    /// The lifetime ended. On a document stream the ENGINE cascades (fold,
    /// owed journal ops) — driven by the manifest's lane.
    RowDelete {
        stream: String,
        id: String,
        incarnation: String,
        revision: i64,
    },
    /// History the client lacks, always followed in the same response by the
    /// document's `row.set`.
    DocDelta {
        stream: String,
        id: String,
        incarnation: String,
        seq: i64,
        codec: String,
        payload: Vec<u8>,
    },
    /// A full document baseline; `data` carries the server projection so the
    /// importer stays dumb.
    DocSnapshot {
        stream: String,
        id: String,
        incarnation: String,
        revision: i64,
        codec: String,
        snapshot: Vec<u8>,
        data: ReplicaFields,
    },
}

impl ReplicaFrame {
    pub fn stream(&self) -> &str {
        match self {
            Self::RowSet { stream, .. }
            | Self::RowDelete { stream, .. }
            | Self::DocDelta { stream, .. }
            | Self::DocSnapshot { stream, .. } => stream,
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::RowSet { id, .. }
            | Self::RowDelete { id, .. }
            | Self::DocDelta { id, .. }
            | Self::DocSnapshot { id, .. } => id,
        }
    }

    pub fn incarnation(&self) -> &str {
        match self {
            Self::RowSet { incarnation, .. }
            | Self::RowDelete { incarnation, .. }
            | Self::DocDelta { incarnation, .. }
            | Self::DocSnapshot { incarnation, .. } => incarnation,
        }
    }

    /// The frame as the server writes it.
    pub fn to_wire(&self) -> serde_json::Value {
        match self {
            Self::RowSet {
                stream,
                id,
                incarnation,
                revision,
                row_type,
                data,
            } => json!({
                "frame": "row.set", "stream": stream, "id": id, "incarnation": incarnation,
                "revision": revision.to_string(), "type": row_type, "data": data,
            }),
            Self::RowDelete {
                stream,
                id,
                incarnation,
                revision,
            } => json!({
                "frame": "row.delete", "stream": stream, "id": id, "incarnation": incarnation,
                "revision": revision.to_string(),
            }),
            Self::DocDelta {
                stream,
                id,
                incarnation,
                seq,
                codec,
                payload,
            } => json!({
                "frame": "doc.delta", "stream": stream, "id": id, "incarnation": incarnation,
                "seq": seq, "codec": codec, "payload": BASE64.encode(payload),
            }),
            Self::DocSnapshot {
                stream,
                id,
                incarnation,
                revision,
                codec,
                snapshot,
                data,
            } => json!({
                "frame": "doc.snapshot", "stream": stream, "id": id, "incarnation": incarnation,
                "revision": revision.to_string(), "codec": codec,
                "snapshot": BASE64.encode(snapshot), "data": data,
            }),
        }
    }
}

/// One pull response. `reset` answers a round that started without a cursor:
/// publishing it replaces the shard's base. `more` means the server's heads
/// were ahead of `cursor`: stage the frames and continue from it.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaPullResponse {
    pub shard: String,
    pub reset: bool,
    pub frames: Vec<ReplicaFrame>,
    pub cursor: String,
    pub more: bool,
}

// MARK: - Ops (up)

/// The four verbs the client can journal.
pub mod verb {
    pub const ROW_CREATE: &str = "row.create";
    pub const ROW_PATCH: &str = "row.patch";
    pub const ROW_DELETE: &str = "row.delete";
    pub const DOC_DELTA: &str = "doc.delta";
}

/// One client op. In the journal `id` is the entry id; a frozen submission
/// carries the UUID minted for the server, and `group` joins the operations of
/// one atomic action. The same encoded JSON is what the store persists and
/// what the wire carries (`row_id` snake_case, binary fields base64) —
/// serialized once, byte-stable.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ReplicaOp {
    pub id: String,
    pub verb: String,
    pub stream: String,
    pub row_id: String,
    pub incarnation: Option<String>,
    pub replaces: Option<String>,
    pub references: Vec<crate::ReplicaReference>,
    pub row_type: Option<String>,
    pub data: Option<ReplicaFields>,
    pub codec: Option<String>,
    pub seed: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub group: Option<String>,
}

impl ReplicaOp {
    pub fn new(
        id: impl Into<String>,
        verb: impl Into<String>,
        stream: impl Into<String>,
        row_id: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            verb: verb.into(),
            stream: stream.into(),
            row_id: row_id.into(),
            ..Self::default()
        }
    }

    pub fn with_type(mut self, row_type: Option<String>) -> Self {
        self.row_type = row_type;
        self
    }

    pub fn with_data(mut self, data: ReplicaFields) -> Self {
        self.data = Some(data);
        self
    }

    pub fn with_codec(mut self, codec: impl Into<String>) -> Self {
        self.codec = Some(codec.into());
        self
    }

    pub fn with_seed(mut self, seed: Vec<u8>) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = Some(payload);
        self
    }

    /// The op envelope as a value tree: `row_id` is the one snake_case key,
    /// `op` carries the verb, binary rides base64, and absent members are
    /// omitted (Swift's `encodeIfPresent`).
    pub fn to_value(&self) -> ReplicaValue {
        let mut object = ReplicaFields::new();
        object.insert("id".into(), ReplicaValue::string(self.id.clone()));
        object.insert("op".into(), ReplicaValue::string(self.verb.clone()));
        object.insert("stream".into(), ReplicaValue::string(self.stream.clone()));
        object.insert("row_id".into(), ReplicaValue::string(self.row_id.clone()));
        if let Some(incarnation) = &self.incarnation {
            object.insert(
                "incarnation".into(),
                ReplicaValue::string(incarnation.clone()),
            );
        }
        if let Some(replaces) = &self.replaces {
            object.insert("replaces".into(), ReplicaValue::string(replaces.clone()));
        }
        if !self.references.is_empty() {
            object.insert(
                "references".into(),
                ReplicaValue::Array(
                    self.references
                        .iter()
                        .map(|reference| reference.to_value())
                        .collect(),
                ),
            );
        }
        if let Some(row_type) = &self.row_type {
            object.insert("type".into(), ReplicaValue::string(row_type.clone()));
        }
        if let Some(data) = &self.data {
            object.insert("data".into(), ReplicaValue::Object(data.clone()));
        }
        if let Some(codec) = &self.codec {
            object.insert("codec".into(), ReplicaValue::string(codec.clone()));
        }
        if let Some(seed) = &self.seed {
            object.insert("seed".into(), ReplicaValue::string(BASE64.encode(seed)));
        }
        if let Some(payload) = &self.payload {
            object.insert(
                "payload".into(),
                ReplicaValue::string(BASE64.encode(payload)),
            );
        }
        if let Some(group) = &self.group {
            object.insert("group".into(), ReplicaValue::string(group.clone()));
        }
        ReplicaValue::Object(object)
    }

    /// Byte-stable encoding: sorted keys, whole doubles as integers, no slash
    /// escaping. The journal compares SENT payload bytes against the entry's
    /// current bytes to apply verdicts, so this must be deterministic.
    pub fn to_json(&self) -> ReplicaResult<Vec<u8>> {
        ReplicaJson::to_vec(&self.to_value())
    }

    pub fn from_json(bytes: &[u8]) -> ReplicaResult<Self> {
        let value: ReplicaValue = serde_json::from_slice(bytes)
            .map_err(|error| ReplicaError::Storage(format!("op payload unreadable: {error}")))?;
        Self::from_value(&value)
    }

    pub fn from_value(value: &ReplicaValue) -> ReplicaResult<Self> {
        let missing = |key: &str| ReplicaError::Storage(format!("op payload missing `{key}`"));
        let text = |key: &str| -> ReplicaResult<String> {
            value
                .get(key)
                .and_then(ReplicaValue::as_string)
                .map(str::to_owned)
                .ok_or_else(|| missing(key))
        };
        let optional_text = |key: &str| -> ReplicaResult<Option<String>> {
            value
                .get(key)
                .map(|v| {
                    v.as_string().map(str::to_owned).ok_or_else(|| {
                        ReplicaError::Storage(format!("op `{key}` must be a string"))
                    })
                })
                .transpose()
        };
        let binary = |key: &str| -> ReplicaResult<Option<Vec<u8>>> {
            optional_text(key)?
                .map(|text| {
                    BASE64
                        .decode(text)
                        .map_err(|_| ReplicaError::Storage(format!("invalid op `{key}` base64")))
                })
                .transpose()
        };
        let op = Self {
            id: text("id")?,
            verb: text("op")?,
            stream: text("stream")?,
            row_id: text("row_id")?,
            incarnation: optional_text("incarnation")?,
            replaces: optional_text("replaces")?,
            references: value
                .get("references")
                .map(|value| {
                    let ReplicaValue::Array(items) = value else {
                        return Err(ReplicaError::Storage("references must be an array".into()));
                    };
                    if items.len() > 64 {
                        return Err(ReplicaError::Storage("too many entity references".into()));
                    }
                    items
                        .iter()
                        .map(crate::ReplicaReference::from_value)
                        .collect::<ReplicaResult<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default(),
            row_type: optional_text("type")?,
            data: value
                .get("data")
                .map(|v| {
                    v.fields()
                        .cloned()
                        .ok_or_else(|| ReplicaError::Storage("op data must be an object".into()))
                })
                .transpose()?,
            codec: optional_text("codec")?,
            seed: binary("seed")?,
            payload: binary("payload")?,
            group: optional_text("group")?,
        };
        if op.id.is_empty()
            || op.stream.is_empty()
            || op.row_id.is_empty()
            || !matches!(
                op.verb.as_str(),
                verb::ROW_CREATE | verb::ROW_PATCH | verb::ROW_DELETE | verb::DOC_DELTA
            )
        {
            return Err(ReplicaError::Storage(
                "invalid operation identity or verb".into(),
            ));
        }
        let references: std::collections::HashSet<_> = op
            .references
            .iter()
            .map(|reference| &reference.name)
            .collect();
        if references.len() != op.references.len() {
            return Err(ReplicaError::Storage("duplicate entity references".into()));
        }
        if op.incarnation.as_deref() == Some("")
            || op.replaces.as_deref() == Some("")
            || (op.replaces.is_some() && op.verb != verb::ROW_CREATE)
        {
            return Err(ReplicaError::Storage("invalid entity lifetime".into()));
        }
        if op.verb == verb::ROW_PATCH && op.data.is_none() {
            return Err(ReplicaError::Storage("patch requires fields".into()));
        }
        if op.verb == verb::DOC_DELTA
            && (op.payload.is_none() || op.codec.as_deref().is_none_or(str::is_empty))
        {
            return Err(ReplicaError::Storage(
                "document delta requires payload and codec".into(),
            ));
        }
        Ok(op)
    }
}

// MARK: - Verdicts

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VerdictOutcome {
    Accepted,
    Rejected,
}

/// The server's word on one op. `Rejected` is a VERDICT — the entry parks,
/// never auto-retries; transport failure never produces one (it throws, and
/// the journal retries). Never conflate — v1's core discipline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaVerdict {
    pub id: String,
    pub outcome: VerdictOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ReplicaVerdict {
    pub fn accepted(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            outcome: VerdictOutcome::Accepted,
            reason: None,
        }
    }

    pub fn rejected(id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            outcome: VerdictOutcome::Rejected,
            reason: Some(reason.into()),
        }
    }
}

// MARK: - Shared serialization

/// Byte-stable encoding helpers. Object keys are sorted (`ReplicaFields` is a
/// `BTreeMap`), whole doubles emit as integers, and slashes are not escaped —
/// the three rules the journal's payload matching depends on.
pub struct ReplicaJson;

impl ReplicaJson {
    pub fn to_vec<T: serde::Serialize>(value: &T) -> ReplicaResult<Vec<u8>> {
        serde_json::to_vec(value)
            .map_err(|error| ReplicaError::Codec(format!("JSON encoding failed: {error}")))
    }

    pub fn to_string<T: serde::Serialize>(value: &T) -> ReplicaResult<String> {
        serde_json::to_string(value)
            .map_err(|error| ReplicaError::Codec(format!("JSON encoding failed: {error}")))
    }

    /// Absence or corruption of a stored row is never an empty baseline.
    pub fn decode_fields(json: Option<&str>) -> ReplicaResult<ReplicaFields> {
        let text = json.ok_or_else(|| ReplicaError::Storage("Missing snapshot data".into()))?;
        serde_json::from_str(text)
            .map_err(|error| ReplicaError::Storage(format!("Invalid snapshot JSON: {error}")))
    }

    pub fn encode_fields(fields: &ReplicaFields) -> ReplicaResult<String> {
        Self::to_string(fields)
    }
}

// MARK: - Pull decode

/// Wire fields are validated before any page is staged.
#[derive(Deserialize)]
pub(crate) struct RawFrame {
    frame: Option<String>,
    stream: Option<String>,
    id: Option<String>,
    incarnation: Option<String>,
    revision: Option<String>,
    #[serde(rename = "type")]
    row_type: Option<String>,
    data: Option<ReplicaFields>,
    seq: Option<i64>,
    codec: Option<String>,
    payload: Option<String>,
    snapshot: Option<String>,
}

#[derive(Deserialize)]
struct RawPull {
    shard: String,
    reset: bool,
    frames: Vec<RawFrame>,
    cursor: String,
    more: bool,
}

/// Refuse the whole page if any frame is malformed. Unknown row fields and
/// types are retained, but an unknown operation cannot be safely skipped.
pub fn decode_pull(bytes: &[u8]) -> ReplicaResult<ReplicaPullResponse> {
    let raw: RawPull = serde_json::from_slice(bytes)
        .map_err(|error| invalid(format!("Pull answer unreadable: {error}")))?;
    if raw.shard.is_empty() || raw.cursor.is_empty() {
        return Err(invalid("Pull answer has no shard or cursor"));
    }
    Ok(ReplicaPullResponse {
        shard: raw.shard,
        reset: raw.reset,
        frames: decode_frames(raw.frames)?,
        cursor: raw.cursor,
        more: raw.more,
    })
}

pub(crate) fn decode_frames(frames: Vec<RawFrame>) -> ReplicaResult<Vec<ReplicaFrame>> {
    let invalid = |field: &str| invalid(format!("Invalid replica frame: {field}"));
    let text = |value: Option<String>, field: &str| {
        value
            .filter(|text| !text.is_empty())
            .ok_or_else(|| invalid(field))
    };
    frames
        .into_iter()
        .map(|frame| {
            let stream = text(frame.stream, "stream")?;
            let id = text(frame.id, "id")?;
            let incarnation = text(frame.incarnation, "incarnation")?;
            let revision = || {
                frame
                    .revision
                    .as_deref()
                    .map(protocol::counter)
                    .transpose()
                    .map_err(|_| invalid("revision"))?
                    .filter(|revision| *revision > 0)
                    .ok_or_else(|| invalid("revision"))
            };
            let binary = |value: Option<String>, field: &str| {
                protocol::binary(
                    &value.ok_or_else(|| invalid(field))?,
                    protocol::ENTITY_BYTES,
                )
                .map_err(|_| invalid(field))
            };
            match frame.frame.as_deref() {
                Some("row.set") => Ok(ReplicaFrame::RowSet {
                    revision: revision()?,
                    stream,
                    id,
                    incarnation,
                    row_type: frame.row_type,
                    data: frame.data.ok_or_else(|| invalid("data"))?,
                }),
                Some("row.delete") => Ok(ReplicaFrame::RowDelete {
                    revision: revision()?,
                    stream,
                    id,
                    incarnation,
                }),
                Some("doc.delta") => Ok(ReplicaFrame::DocDelta {
                    stream,
                    id,
                    incarnation,
                    seq: frame
                        .seq
                        .filter(|seq| *seq > 0)
                        .ok_or_else(|| invalid("seq"))?,
                    codec: text(frame.codec, "codec")?,
                    payload: binary(frame.payload, "payload")?,
                }),
                Some("doc.snapshot") => Ok(ReplicaFrame::DocSnapshot {
                    revision: revision()?,
                    stream,
                    id,
                    incarnation,
                    codec: text(frame.codec, "codec")?,
                    snapshot: binary(frame.snapshot, "snapshot")?,
                    data: frame.data.ok_or_else(|| invalid("data"))?,
                }),
                _ => Err(invalid("unknown kind")),
            }
        })
        .collect()
}
