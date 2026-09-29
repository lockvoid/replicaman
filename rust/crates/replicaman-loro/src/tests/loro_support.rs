//! Loro authoring helpers standing in for the app's document layer — the
//! `LoroFixture` half of `Tests/ReplicaManLoroTests/LoroTestSupport.swift`.
//! A client doc is born FROM the store's fold under the store's peer, and
//! edits export as update payloads.
#![allow(dead_code)]

use loro::{ExportMode, LoroDoc, LoroValue};

pub const SERVER_PEER: u64 = 1;

pub fn doc(peer: u64, fold: Option<&[u8]>) -> LoroDoc {
    let doc = LoroDoc::new();
    doc.set_record_timestamp(false);
    doc.set_peer_id(peer).expect("set peer");
    if let Some(fold) = fold.filter(|bytes| !bytes.is_empty()) {
        doc.import(fold).expect("import fold");
    }
    doc
}

pub fn set_meta(doc: &LoroDoc, key: &str, value: &str) {
    doc.get_map("meta").insert(key, value).expect("insert meta");
    doc.commit();
}

/// Edit-and-export: the payload a real client would journal for this
/// mutation (updates since the doc's version before the edit).
pub fn edit_payload(doc: &LoroDoc, key: &str, value: &str) -> Vec<u8> {
    let before = doc.oplog_vv();
    set_meta(doc, key, value);
    doc.export(ExportMode::updates(&before))
        .expect("export updates")
}

pub fn meta(doc: &LoroDoc, key: &str) -> Option<String> {
    let LoroValue::Map(root) = doc.get_deep_value() else {
        return None;
    };
    let LoroValue::Map(meta) = root.get("meta")? else {
        return None;
    };
    match meta.get(key)? {
        LoroValue::String(value) => Some(value.to_string()),
        _ => None,
    }
}

pub fn meta_in_fold(fold: &[u8], key: &str) -> Option<String> {
    meta(&doc(999_999, Some(fold)), key)
}

pub fn snapshot(doc: &LoroDoc) -> Vec<u8> {
    doc.export(ExportMode::snapshot()).expect("export snapshot")
}

// MARK: - Engine fixtures (the `LoroFixture` engine half)

use std::sync::Arc;

use crate::LoroReplicaCodec;
use replicaman::ReplicaCodec;
use replicaman::ReplicaStateStore;
use replicaman::testing::{
    OWNER, StoreFixture, StubTransport, TokioSpawner, engine_directory, sequential_minter,
};
use replicaman::{ReplicaEngine, ReplicaEngineOptions};
use replicaman::{ReplicaSchema, ReplicaStreamSpec};

/// `LoroFixture.schema()` — boards on the document lane under the REAL codec,
/// notes alongside on the row lane.
pub fn schema() -> ReplicaSchema {
    ReplicaSchema::new(vec![
        ReplicaStreamSpec::document("boards").codec(LoroReplicaCodec::CODEC_NAME),
        ReplicaStreamSpec::row("notes"),
    ])
}

/// The fixture store, RAII-cleaned like the core suite's.
pub fn store(name: &str) -> StoreFixture {
    replicaman::testing::store(name)
}

/// `LoroFixture.engine(store:transport:)` — owner 42, deterministic minter
/// from 100, and writes that go out only on an explicit `drain()`.
pub fn engine(store: Arc<ReplicaStateStore>, transport: Arc<StubTransport>) -> Arc<ReplicaEngine> {
    let mut options = ReplicaEngineOptions::new(
        engine_directory(),
        transport,
        schema(),
        Arc::new(TokioSpawner),
    );
    options.codecs = vec![Arc::new(LoroReplicaCodec::new()) as Arc<dyn ReplicaCodec>];
    options.peer_minter = sequential_minter(100);
    options.automatically_push_writes = false;
    options.spawner = Arc::new(TokioSpawner);
    ReplicaEngine::with_store(store, OWNER, options)
}
