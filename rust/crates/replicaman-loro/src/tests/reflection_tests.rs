use super::loro_support as fixture;
use crate::LoroReplicaCodec;
use replicaman::testing::{
    OWNER, ScriptedPull, StubTransport, TokioSpawner, doc_snapshot, engine_directory, fields,
    row_set, text,
};
use replicaman::*;
use std::sync::Arc;

fn engine(store: Arc<ReplicaStateStore>, transport: Arc<StubTransport>) -> Arc<ReplicaEngine> {
    let schema = ReplicaSchema::new(vec![
        ReplicaStreamSpec::document("boards")
            .codec(LoroReplicaCodec::CODEC_NAME)
            .reflections(vec![ReplicaReflection::new("name", ["meta", "name"])])
            .stamp(ReplicaCreateStamp::standard()),
    ]);
    let mut options = ReplicaEngineOptions::new(
        engine_directory(),
        transport,
        schema,
        Arc::new(TokioSpawner),
    );
    options.codecs = vec![Arc::new(LoroReplicaCodec)];
    options.automatically_push_writes = false;
    ReplicaEngine::with_store(store, OWNER, options)
}

#[tokio::test]
async fn birth_edit_and_undo_publish_the_document_projection_with_the_fold() {
    let store = fixture::store("reflections");
    let engine = engine(store.clone(), StubTransport::new());
    let doc = fixture::doc(7, None);
    fixture::set_meta(&doc, "name", "Seed");
    engine
        .create_doc(
            "boards",
            "b",
            &fixture::snapshot(&doc),
            7,
            &fields(&[("name", text("Caller"))]),
            None,
        )
        .await
        .unwrap();
    let row = store.peek_snapshot("boards", "b").unwrap().unwrap();
    assert_eq!(row.data.get("name"), Some(&text("Seed")));
    assert_eq!(
        row.data.get("userId"),
        Some(&ReplicaValue::signed_integer(OWNER))
    );
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| {
            doc.document()
                .get_map("meta")
                .insert("name", "Edited")
                .map_err(|e| ReplicaError::Codec(e.to_string()))
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .peek_snapshot("boards", "b")
            .unwrap()
            .unwrap()
            .data
            .get("name"),
        Some(&text("Edited"))
    );
    engine
        .undo_document::<LoroReplicaCodec>("boards", "b")
        .await
        .unwrap();
    assert_eq!(
        store
            .peek_snapshot("boards", "b")
            .unwrap()
            .unwrap()
            .data
            .get("name"),
        Some(&text("Seed"))
    );
}

#[tokio::test]
async fn a_stale_server_projection_cannot_erase_a_local_document_edit() {
    let store = fixture::store("reflection-remote");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let doc = fixture::doc(7, None);
    fixture::set_meta(&doc, "name", "Seed");
    let seed = fixture::snapshot(&doc);
    engine
        .create_doc("boards", "b", &seed, 7, &ReplicaFields::new(), None)
        .await
        .unwrap();
    engine.drain().await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b",
                LoroReplicaCodec::CODEC_NAME,
                &seed,
                ReplicaFields::new(),
            )],
            "baseline",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    transport.fail_pushes(true);
    let delta = fixture::edit_payload(&doc, "name", "Offline");
    engine
        .record_doc_delta("boards", "b", &delta)
        .await
        .unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![row_set(
                "boards",
                "b",
                None,
                fields(&[("name", text("Seed")), ("color", text("remote"))]),
            )],
            "2:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    let row = store.peek_snapshot("boards", "b").unwrap().unwrap();
    assert_eq!(row.data.get("name"), Some(&text("Offline")));
    assert_eq!(row.data.get("color"), Some(&text("remote")));
}

#[test]
fn corrupted_version_vectors_are_errors_and_reflected_integers_stay_exact() {
    let codec = LoroReplicaCodec;
    let doc = fixture::doc(7, None);
    doc.get_map("meta").insert("owner", i64::MAX).unwrap();
    let seed = fixture::snapshot(&doc);
    let reflected = codec
        .reflect(&seed, &[ReplicaReflection::new("owner", ["meta", "owner"])])
        .unwrap();
    assert_eq!(
        reflected.get("owner"),
        Some(&ReplicaValue::Integer(i64::MAX))
    );
    assert!(codec.diff(&seed, Some(b"broken version")).is_err());
    assert!(
        codec
            .merge_versions(Some(b"broken version"), &codec.version(&seed).unwrap())
            .is_err()
    );
}
