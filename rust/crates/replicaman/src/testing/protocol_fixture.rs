//! Protocol 2 around scripted content, as `ruby/lib/replica_man` speaks it.
//! Tests script what the server has to deliver and how it judges fresh
//! operations; the fixture keeps what the server keeps — claimed operation ids
//! with their digests and verdicts, the lifetimes accepted births created, the
//! cursors it issued and the members it served — so retries, groups, paging,
//! cursors and verification behave as they do against PostgreSQL, which the
//! mixed-client histories cover separately.
use super::{ScriptedFrame, ScriptedPull, StubTransport};
use crate::integrity::IntegrityHash;
use crate::protocol;
use crate::transport::ReplicaEndpoint;
use crate::wire::{ReplicaJson, VerdictOutcome};
use crate::{ReplicaError, ReplicaFrame, ReplicaOp, ReplicaResult, ReplicaValue, ReplicaVerdict};
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

/// The server's page budget beside `limit`.
const PAGE_BYTES: usize = 256 * 1024;

struct Claim {
    digest: String,
    verdict: ReplicaVerdict,
}

#[derive(Default)]
struct Shard {
    issued: HashSet<String>,
    head: Option<String>,
    members: BTreeMap<(String, String), (String, i64)>,
}

struct State {
    dataset: String,
    claims: HashMap<String, Claim>,
    incarnations: HashMap<(String, String), String>,
    shards: HashMap<String, Shard>,
    revision: i64,
    continuations: u64,
}

pub(super) struct ProtocolFixture(Mutex<State>);

impl Default for ProtocolFixture {
    fn default() -> Self {
        Self(Mutex::new(State {
            dataset: "fixture-dataset".into(),
            claims: HashMap::new(),
            incarnations: HashMap::new(),
            shards: HashMap::new(),
            revision: 0,
            continuations: 0,
        }))
    }
}

/// The server's `{error, message}` answer as the HTTP transport reports it.
fn refused(status: u16, code: &str) -> ReplicaError {
    ReplicaError::Protocol {
        code: code.into(),
        message: format!("HTTP {status}: {code}"),
    }
}

fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

impl ProtocolFixture {
    pub fn rotate_dataset(&self, dataset: &str) {
        self.0.lock().dataset = dataset.into();
    }

    pub fn forget_cursors(&self) {
        for shard in self.0.lock().shards.values_mut() {
            shard.issued.clear();
            shard.head = None;
        }
    }

    pub async fn exchange(
        &self,
        transport: &StubTransport,
        endpoint: ReplicaEndpoint,
        body: &[u8],
    ) -> ReplicaResult<Vec<u8>> {
        let request: Value = protocol::decode(body)?;
        if request["protocol"] != json!(protocol::VERSION) {
            return Err(refused(409, "UpgradeRequired"));
        }
        let dataset = self.0.lock().dataset.clone();
        let initial = endpoint == ReplicaEndpoint::Pull && request["dataset"].is_null();
        if !initial && request["dataset"] != json!(dataset) {
            return Err(refused(409, "DatasetChanged"));
        }
        let fields = match endpoint {
            ReplicaEndpoint::Pull => self.pull(transport, &request).await?,
            ReplicaEndpoint::Push => self.push(transport, &request).await?,
            ReplicaEndpoint::Verify => self.verify(transport, &request)?,
        };
        let mut response = json!({
            "protocol": protocol::VERSION, "namespace": request["namespace"],
            "schema": request["schema"], "dataset": dataset
        });
        response
            .as_object_mut()
            .expect("header")
            .extend(fields.as_object().expect("response").clone());
        protocol::encode(&response)
    }

    /// The next scripted answer for the shard, paged by `limit` entities and
    /// the byte budget; a page the script overfills continues from a cursor
    /// the fixture mints.
    async fn pull(&self, transport: &StubTransport, request: &Value) -> ReplicaResult<Value> {
        let shard = string(request, "shard")?;
        let cursor = match &request["cursor"] {
            Value::Null => None,
            Value::String(cursor) => Some(cursor.as_str()),
            _ => return Err(refused(400, "cursor must be a string")),
        };
        let limit = match request.get("limit") {
            None => 500,
            Some(limit) => limit
                .as_u64()
                .filter(|limit| (1..=1000).contains(limit))
                .ok_or_else(|| refused(400, "limit must be an integer from 1 to 1000"))?,
        };
        transport.admit_pull(shard, cursor).await?;
        if let Some(cursor) = cursor
            && !self
                .0
                .lock()
                .shards
                .get(shard)
                .is_some_and(|known| known.issued.contains(cursor))
        {
            return Err(refused(409, "CursorInvalid"));
        }
        let (frames, next, more) = match transport.scripted_pull(shard) {
            Some(answer) => {
                let (page, rest) = first_page(answer.frames, limit as usize);
                if rest.is_empty() {
                    (page, answer.cursor, answer.more)
                } else {
                    let continuation = {
                        let mut state = self.0.lock();
                        state.continuations += 1;
                        format!("{}~{}", answer.cursor, state.continuations)
                    };
                    transport
                        .requeue_pull(shard, ScriptedPull::new(rest, answer.cursor, answer.more));
                    (page, continuation, true)
                }
            }
            None => (Vec::new(), cursor.unwrap_or("0:").to_owned(), false),
        };
        let mut state = self.0.lock();
        let State {
            incarnations,
            shards,
            revision,
            ..
        } = &mut *state;
        let served = shards.entry(shard.to_owned()).or_default();
        if cursor.is_none() {
            served.members.clear();
        }
        let frames: Vec<Value> = frames
            .into_iter()
            .map(|frame| {
                let address = (frame.stream().to_owned(), frame.id().to_owned());
                let incarnation = incarnations
                    .entry(address.clone())
                    .or_insert_with(crate::id::uuid)
                    .clone();
                *revision += 1;
                let frame = stamped(frame, incarnation.clone(), *revision);
                match &frame {
                    ReplicaFrame::RowSet { revision, .. }
                    | ReplicaFrame::DocSnapshot { revision, .. } => {
                        served.members.insert(address, (incarnation, *revision));
                    }
                    ReplicaFrame::RowDelete { .. } => {
                        served.members.remove(&address);
                        incarnations.remove(&address);
                    }
                    ReplicaFrame::DocDelta { .. } => {}
                }
                frame.to_wire()
            })
            .collect();
        served.issued.insert(next.clone());
        served.head = Some(next.clone());
        Ok(json!({
            "shard": shard, "reset": cursor.is_none(), "frames": frames, "cursor": next, "more": more
        }))
    }

    async fn push(&self, transport: &StubTransport, request: &Value) -> ReplicaResult<Value> {
        let raw = request["ops"]
            .as_array()
            .ok_or_else(|| refused(400, "operations must be an array"))?;
        if raw.len() > protocol::MAX_OPERATIONS {
            return Err(refused(400, "at most 100 operations are allowed"));
        }
        let mut operations = Vec::with_capacity(raw.len());
        let mut digests = Vec::with_capacity(raw.len());
        for value in raw {
            let value: ReplicaValue = serde_json::from_value(value.clone())
                .map_err(|_| refused(400, "operation must be an object"))?;
            let op = ReplicaOp::from_value(&value)
                .map_err(|_| refused(400, "operation is malformed"))?;
            if op.incarnation.is_none() {
                return Err(refused(400, "missing request field: incarnation"));
            }
            if !is_uuid(&op.id) {
                return Err(refused(400, "operation id must be a UUID"));
            }
            if op.group.as_deref().is_some_and(|group| !is_uuid(group)) {
                return Err(refused(400, "operation group must be a UUID"));
            }
            digests.push(protocol::digest(&ReplicaJson::to_vec(&value)?));
            operations.push(op);
        }
        let ids: Vec<String> = operations.iter().map(|op| op.id.clone()).collect();
        if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
            return Err(refused(
                400,
                "operation IDs must be unique within a submission",
            ));
        }
        let groups = groups(&operations)?;
        transport.admit_push(ids)?;

        let mut fresh = Vec::new();
        {
            let state = self.0.lock();
            for group in &groups {
                let claimed = group
                    .clone()
                    .filter(|index| state.claims.contains_key(&operations[*index].id))
                    .count();
                if claimed == 0 {
                    fresh.extend(group.clone());
                } else if claimed != group.len() {
                    return Err(refused(
                        400,
                        "an operation group changed after it was applied",
                    ));
                } else if group
                    .clone()
                    .any(|index| state.claims[&operations[index].id].digest != digests[index])
                {
                    return Err(refused(409, "MutationChanged"));
                }
            }
        }

        let scripted = if fresh.is_empty() {
            Vec::new()
        } else {
            transport
                .scripted_push(
                    fresh
                        .iter()
                        .map(|index| operations[*index].clone())
                        .collect(),
                )
                .await
        };
        let mut state = self.0.lock();
        for group in &groups {
            if !fresh.contains(&group.start) {
                continue;
            }
            let refusal = group
                .clone()
                .map(|index| {
                    scripted
                        .iter()
                        .find(|verdict| verdict.id == operations[index].id)
                        .unwrap_or_else(|| {
                            panic!(
                                "the push script answered no verdict for {}",
                                operations[index].id
                            )
                        })
                })
                .find(|verdict| verdict.outcome == VerdictOutcome::Rejected)
                .map(|verdict| {
                    verdict
                        .reason
                        .clone()
                        .expect("a scripted refusal names its reason")
                });
            for index in group.clone() {
                let op = &operations[index];
                let verdict = match &refusal {
                    Some(reason) => ReplicaVerdict::rejected(&op.id, reason.clone()),
                    None => {
                        state.incarnations.insert(
                            (op.stream.clone(), op.row_id.clone()),
                            op.incarnation.clone().expect("validated incarnation"),
                        );
                        ReplicaVerdict::accepted(&op.id)
                    }
                };
                state.claims.insert(
                    op.id.clone(),
                    Claim {
                        digest: digests[index].clone(),
                        verdict,
                    },
                );
            }
        }
        let verdicts = operations
            .iter()
            .map(|op| state.claims[&op.id].verdict.clone())
            .collect();
        drop(state);
        Ok(json!({"verdicts": transport.answer_push(verdicts)?}))
    }

    /// Answers only for the head: a cursor behind it, or any change the
    /// script still holds for the shard, is `CursorBehind`.
    fn verify(&self, transport: &StubTransport, request: &Value) -> ReplicaResult<Value> {
        let shard = string(request, "shard")?;
        let cursor = string(request, "cursor")?;
        let state = self.0.lock();
        let Some(served) = state
            .shards
            .get(shard)
            .filter(|served| served.issued.contains(cursor))
        else {
            return Err(refused(409, "CursorInvalid"));
        };
        if served.head.as_deref() != Some(cursor) || transport.holds_pull(shard) {
            return Err(refused(409, "CursorBehind"));
        }
        let mut hash = IntegrityHash::new("replicaman-view");
        for ((stream, id), (incarnation, revision)) in &served.members {
            for field in [stream, id, incarnation, &revision.to_string()] {
                hash.append(Some(field.as_bytes()));
            }
        }
        Ok(json!({
            "shard": shard, "cursor": cursor,
            "count": served.members.len().to_string(), "digest": hash.finish()
        }))
    }
}

/// At most `limit` entities and about the byte budget, but always one entity:
/// a document's `doc.delta` frames and its following `row.set` are one entity.
fn first_page(
    frames: Vec<ScriptedFrame>,
    limit: usize,
) -> (Vec<ScriptedFrame>, Vec<ScriptedFrame>) {
    let mut entities: Vec<Vec<ScriptedFrame>> = Vec::new();
    for frame in frames {
        match entities.last_mut() {
            Some(entity) if continues(entity, &frame) => entity.push(frame),
            _ => entities.push(vec![frame]),
        }
    }
    let mut entities = entities.into_iter();
    let mut page = Vec::new();
    let mut count = 0;
    let mut bytes = 0;
    for entity in entities.by_ref() {
        count += 1;
        bytes += entity.iter().map(wire_bytes).sum::<usize>();
        page.extend(entity);
        if count >= limit || bytes >= PAGE_BYTES {
            break;
        }
    }
    (page, entities.flatten().collect())
}

fn continues(entity: &[ScriptedFrame], frame: &ScriptedFrame) -> bool {
    let last = &entity[entity.len() - 1];
    matches!(last, ScriptedFrame::DocDelta { .. })
        && matches!(
            frame,
            ScriptedFrame::DocDelta { .. } | ScriptedFrame::RowSet { .. }
        )
        && (last.stream(), last.id()) == (frame.stream(), frame.id())
}

fn wire_bytes(frame: &ScriptedFrame) -> usize {
    let frame = stamped(frame.clone(), String::new(), 0).to_wire();
    serde_json::to_vec(&frame).expect("a frame encodes").len()
}

/// Runs of one group, or a single ungrouped operation; a group must be
/// contiguous.
fn groups(operations: &[ReplicaOp]) -> ReplicaResult<Vec<std::ops::Range<usize>>> {
    let mut groups: Vec<std::ops::Range<usize>> = Vec::new();
    let mut seen = HashSet::new();
    for (index, op) in operations.iter().enumerate() {
        match (&op.group, groups.last_mut()) {
            (Some(group), Some(last)) if operations[last.start].group.as_ref() == Some(group) => {
                last.end = index + 1;
            }
            (group, _) => {
                if let Some(group) = group
                    && !seen.insert(group.clone())
                {
                    return Err(refused(400, "an operation group must be contiguous"));
                }
                groups.push(index..index + 1);
            }
        }
    }
    Ok(groups)
}

fn stamped(frame: ScriptedFrame, incarnation: String, revision: i64) -> ReplicaFrame {
    match frame {
        ScriptedFrame::RowSet {
            stream,
            id,
            row_type,
            data,
        } => ReplicaFrame::RowSet {
            stream,
            id,
            incarnation,
            revision,
            row_type,
            data,
        },
        ScriptedFrame::RowDelete { stream, id } => ReplicaFrame::RowDelete {
            stream,
            id,
            incarnation,
            revision,
        },
        ScriptedFrame::DocDelta {
            stream,
            id,
            seq,
            codec,
            payload,
        } => ReplicaFrame::DocDelta {
            stream,
            id,
            incarnation,
            seq,
            codec,
            payload,
        },
        ScriptedFrame::DocSnapshot {
            stream,
            id,
            codec,
            snapshot,
            data,
        } => ReplicaFrame::DocSnapshot {
            stream,
            id,
            incarnation,
            revision,
            codec,
            snapshot,
            data,
        },
    }
}

fn string<'a>(value: &'a Value, field: &str) -> ReplicaResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| refused(400, &format!("missing request field: {field}")))
}
