//! The engine's map of the replica. Ported from
//! `Sources/ReplicaMan/ReplicaSchema.swift`.

use crate::models::ReplicaCreateStamp;
use std::collections::{HashMap, HashSet};

/// Which drain a write leaves on. Claimed by an ACTION, never by a stream:
/// the same stream carries a foreground write and a background one, so stream
/// granularity cannot express "someone is waiting on this one".
///
/// Lanes drain concurrently, so an interactive write OVERTAKES a bulk backlog.
/// Order holds within a lane only — which is safe because the engine keeps two
/// invariants automatically: a row's later ops join its pending lane, and an
/// interactive op promotes any pending row it names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum ReplicaLane {
    /// A human is waiting on this write — a tap, a send, a foreground edit.
    Interactive,
    /// Background work: imports, cook fan-out, catalog fill. The default.
    #[default]
    Bulk,
}

impl ReplicaLane {
    pub const ALL: [ReplicaLane; 2] = [ReplicaLane::Interactive, ReplicaLane::Bulk];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Bulk => "bulk",
        }
    }

    /// The durable column value, back to a lane. An unrecognised spelling is
    /// `None`; every caller falls back to `Bulk`, exactly as upstream.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "interactive" => Some(Self::Interactive),
            "bulk" => Some(Self::Bulk),
            _ => None,
        }
    }
}

/// A stream's direction: row ops, or a CRDT document fold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamLane {
    Row,
    Document,
}

/// A row field owned by its document at a path through nested maps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaReflection {
    pub field: String,
    pub path: Vec<String>,
}

impl ReplicaReflection {
    pub fn new(
        field: impl Into<String>,
        path: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            field: field.into(),
            path: path.into_iter().map(Into::into).collect(),
        }
    }
}

/// One declared stream, as the manifest describes it. The manifest knows the
/// lane and the direction — that is what drives the engine's `row.delete`
/// cascade and lets codegen omit write verbs entirely on readonly streams
/// (the client *can't* express the mistake).
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaStreamSpec {
    pub name: String,
    pub lane: StreamLane,
    pub readonly: bool,
    pub shard: String,
    /// Document-lane codec wire name (`loro@1`); `None` on row streams.
    pub codec: Option<String>,
    pub reflections: Vec<ReplicaReflection>,
    pub stamp: Option<ReplicaCreateStamp>,
    /// Carried by every patch, even when their value did not change.
    pub preconditions: Vec<String>,
    /// Fields a held row is allowed to send. None means all fields.
    pub pushed: Option<HashSet<String>>,
    pub references: Vec<crate::ReplicaReferenceSpec>,
    pub lifetime_from: Option<String>,
}

impl ReplicaStreamSpec {
    pub fn row(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            lane: StreamLane::Row,
            readonly: false,
            shard: "user".into(),
            codec: None,
            reflections: Vec::new(),
            stamp: None,
            preconditions: Vec::new(),
            pushed: None,
            references: Vec::new(),
            lifetime_from: None,
        }
    }

    pub fn document(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            lane: StreamLane::Document,
            readonly: false,
            shard: "user".into(),
            codec: None,
            reflections: Vec::new(),
            stamp: None,
            preconditions: Vec::new(),
            pushed: None,
            references: Vec::new(),
            lifetime_from: None,
        }
    }

    pub fn reflections(mut self, reflections: Vec<ReplicaReflection>) -> Self {
        self.reflections = reflections;
        self
    }

    pub fn stamp(mut self, stamp: ReplicaCreateStamp) -> Self {
        self.stamp = Some(stamp);
        self
    }

    pub fn preconditions(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.preconditions = fields.into_iter().map(Into::into).collect();
        self
    }

    pub fn pushed(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.pushed = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    pub fn shard(mut self, shard: impl Into<String>) -> Self {
        self.shard = shard.into();
        self
    }

    pub fn codec(mut self, codec: impl Into<String>) -> Self {
        self.codec = Some(codec.into());
        self
    }
}

/// Stream specs plus the shard list, in declaration order. Generated code
/// ships one of these; tests build them by hand.
#[derive(Clone, Debug)]
pub struct ReplicaSchema {
    pub namespace: String,
    pub version: i64,
    specs: Vec<ReplicaStreamSpec>,
    /// Shards in first-appearance order — pulled independently, one cursor
    /// each.
    shards: Vec<String>,
    by_name: HashMap<String, ReplicaStreamSpec>,
}

impl ReplicaSchema {
    pub fn new(streams: Vec<ReplicaStreamSpec>) -> Self {
        let by_name = streams
            .iter()
            .map(|spec| (spec.name.clone(), spec.clone()))
            .collect();
        let mut shards: Vec<String> = Vec::new();
        for spec in &streams {
            if !shards.iter().any(|shard| shard == &spec.shard) {
                shards.push(spec.shard.clone());
            }
        }
        if shards.is_empty() {
            shards.push("user".into());
        }
        Self {
            namespace: "replicaman".into(),
            version: 1,
            specs: streams,
            shards,
            by_name,
        }
    }

    pub fn with_identity(mut self, namespace: impl Into<String>, version: i64) -> Self {
        self.namespace = namespace.into();
        self.version = version;
        self
    }

    pub fn specs(&self) -> &[ReplicaStreamSpec] {
        &self.specs
    }

    pub fn shards(&self) -> &[String] {
        &self.shards
    }

    pub fn spec(&self, name: &str) -> Option<&ReplicaStreamSpec> {
        self.by_name.get(name)
    }

    /// The lane an incoming frame's stream belongs to. Unknown streams are
    /// row-lane by definition (nothing to cascade) — the importer stays total.
    pub fn lane_of(&self, stream: &str) -> StreamLane {
        self.by_name
            .get(stream)
            .map_or(StreamLane::Row, |spec| spec.lane)
    }

    /// The shard a stream lands on; unknown streams answer `user`, which is
    /// the engine's fallback everywhere it asks.
    pub fn shard_of(&self, stream: &str) -> &str {
        self.by_name
            .get(stream)
            .map_or("user", |spec| spec.shard.as_str())
    }

    pub fn streams(&self, shard: &str) -> Vec<String> {
        self.specs
            .iter()
            .filter(|spec| spec.shard == shard)
            .map(|spec| spec.name.clone())
            .collect()
    }
}
