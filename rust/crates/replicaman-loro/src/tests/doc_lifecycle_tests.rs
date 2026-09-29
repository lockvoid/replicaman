//! Transliterated from `Tests/ReplicaManLoroTests/DocLifecycleTests.swift`
//! (5 cases).
//!
//! The document lifecycle over the REAL codec: create(seed) ⇒
//! journal op; delta supersede folds two local edits into ONE pending op;
//! accepted verdicts advance acked; rejections discard and re-bootstrap;
//! a served snapshot MERGES rather than replaces, so unpushed local ops live.

use crate::LoroReplicaCodec;
use crate::tests::loro_support as fixture;
use replicaman::ReplicaCodec;
use replicaman::ReplicaFields;
use replicaman::testing::{
    ScriptedPull, StubTransport, doc_delta, doc_snapshot, fields, reject_all, text,
};
use replicaman::wire::verb;

// MARK: - DocLifecycleTests (5)

/// `testCreateSeedJournalsAndAcceptanceAdvancesAcked` — the create carries the
/// seed itself (not a delta), records the authoring peer, and an accepted
/// verdict advances `acked` to the seed's version so the doc owes nothing.
#[tokio::test]
async fn create_seed_journals_and_acceptance_advances_acked() {
    let store = fixture::store("doc-create-seed");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());

    let author = fixture::doc(7, None);
    fixture::set_meta(&author, "name", "Plans");
    let seed = fixture::snapshot(&author);

    engine
        .create_doc(
            "boards",
            "b1",
            &seed,
            7,
            &fields(&[("name", text("Plans"))]),
            None,
        )
        .await
        .expect("create the document");

    let pending = store.peek_pending().expect("peek pending");
    assert_eq!(pending.len(), 1);
    let op = pending[0].op().expect("decode the journalled op");
    assert_eq!(op.verb, verb::ROW_CREATE);
    assert_eq!(op.codec.as_deref(), Some(LoroReplicaCodec::CODEC_NAME));
    assert_eq!(
        op.seed.as_deref(),
        Some(seed.as_slice()),
        "the create carries the seed, not a delta"
    );
    assert_eq!(
        store
            .peek_snapshot("boards", "b1")
            .expect("peek snapshot")
            .expect("the snapshot row exists")
            .data
            .get("name"),
        Some(&text("Plans"))
    );
    assert_eq!(
        store
            .peek_doc("boards", "b1")
            .expect("peek doc")
            .expect("the doc row exists")
            .peer,
        7,
        "the authoring peer is recorded"
    );

    engine.drain().await.expect("drain");
    assert_eq!(store.peek_pending().expect("peek pending").len(), 0);

    // Acked caught up with the seed: the document owes nothing further.
    let doc = store
        .peek_doc("boards", "b1")
        .expect("peek doc")
        .expect("the doc row exists");
    let codec = LoroReplicaCodec::new();
    assert!(
        codec.is_empty_diff(
            &codec
                .diff(&doc.fold, doc.acked.as_deref())
                .expect("diff the fold since acked")
        ),
        "an accepted create advances acked to the seed's version"
    );
}

/// `testTwoLocalEditsSupersedeIntoOnePendingOp` — per-document supersede: two
/// local edits collapse into ONE pending `doc.delta` under a stable id, and
/// that single payload still carries both edits.
#[tokio::test]
async fn two_local_edits_supersede_into_one_pending_op() {
    let store = fixture::store("doc-supersede");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());

    let author = fixture::doc(7, None);
    fixture::set_meta(&author, "name", "Plans");
    let seed = fixture::snapshot(&author);
    engine
        .create_doc("boards", "b1", &seed, 7, &ReplicaFields::new(), None)
        .await
        .expect("create the document");
    engine.drain().await.expect("drain the create");

    engine
        .record_doc_delta(
            "boards",
            "b1",
            &fixture::edit_payload(&author, "color", "red"),
        )
        .await
        .expect("record the first edit");
    let first = store.peek_pending().expect("peek pending");
    assert_eq!(first.len(), 1);
    engine
        .record_doc_delta(
            "boards",
            "b1",
            &fixture::edit_payload(&author, "mood", "calm"),
        )
        .await
        .expect("record the second edit");

    let pending = store.peek_pending().expect("peek pending");
    assert_eq!(
        pending.len(),
        1,
        "per-document supersede: ONE pending merged doc.delta"
    );
    assert_eq!(pending[0].id, first[0].id, "the supersede id is stable");
    let op = pending[0].op().expect("decode the superseded op");
    assert_eq!(op.verb, verb::DOC_DELTA);

    // The one payload carries BOTH edits: apply it to the server's copy.
    let server = fixture::doc(fixture::SERVER_PEER, Some(&seed));
    server
        .import(op.payload.as_deref().expect("the delta carries a payload"))
        .expect("import the superseded payload");
    assert_eq!(fixture::meta(&server, "color").as_deref(), Some("red"));
    assert_eq!(fixture::meta(&server, "mood").as_deref(), Some("calm"));

    // The fold absorbed both too.
    let fold = store
        .peek_doc("boards", "b1")
        .expect("peek doc")
        .expect("the doc row exists")
        .fold;
    assert_eq!(
        fixture::meta_in_fold(&fold, "color").as_deref(),
        Some("red")
    );
    assert_eq!(
        fixture::meta_in_fold(&fold, "mood").as_deref(),
        Some("calm")
    );
}

/// `testRejectedDeltaDiscardsAndRebootstrapsToServerTruth` — a rejected
/// `doc.delta` is DISCARDED rather than parked (replaying refused
/// history would push it forever) and the shard re-bootstraps to server truth.
#[tokio::test]
async fn rejected_delta_discards_and_rebootstraps_to_server_truth() {
    let store = fixture::store("doc-rejected-delta");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());

    let author = fixture::doc(7, None);
    fixture::set_meta(&author, "name", "Plans");
    let seed = fixture::snapshot(&author);
    engine
        .create_doc("boards", "b1", &seed, 7, &ReplicaFields::new(), None)
        .await
        .expect("create the document");
    engine.drain().await.expect("drain the create");

    engine
        .record_doc_delta(
            "boards",
            "b1",
            &fixture::edit_payload(&author, "color", "red"),
        )
        .await
        .expect("record the edit");
    reject_all(&transport, "beyond quota");
    engine.drain().await.expect("drain into a rejection");

    assert_eq!(
        store.peek_pending().expect("peek pending").len(),
        0,
        "the refused delta is gone"
    );
    assert_eq!(
        store.peek_parked().expect("peek parked").len(),
        1,
        "the refusal stays inspectable without automatic retry"
    );
    assert!(
        store.peek_doc("boards", "b1").expect("peek doc").is_none(),
        "the refused history leaves the authoring fold until the server's copy arrives"
    );

    // The next round lands the server's copy of the doc fresh.
    let server_truth = fixture::doc(fixture::SERVER_PEER, Some(&seed));
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                LoroReplicaCodec::CODEC_NAME,
                &fixture::snapshot(&server_truth),
                fields(&[("name", text("Plans"))]),
            )],
            "9:",
            false,
        ),
    );
    engine.pull_once("user").await.expect("pull the truth");

    let doc = store
        .peek_doc("boards", "b1")
        .expect("peek doc")
        .expect("the doc row exists");
    assert_eq!(
        fixture::meta_in_fold(&doc.fold, "name").as_deref(),
        Some("Plans")
    );
    assert_eq!(
        fixture::meta_in_fold(&doc.fold, "color"),
        None,
        "the refused edit is genuinely undone"
    );
    assert_eq!(doc.peer, 100, "the reborn fold minted a fresh peer");
    engine.try_close().await.unwrap();
    let reopened = replicaman::ReplicaStateStore::open(store.path()).unwrap();
    let records = reopened.recovery_records(None, 100).unwrap();
    assert_eq!(records.len(), 1);
    let parts = reopened.recovery_parts(&records[0].id, None, 100).unwrap();
    let fold = parts
        .iter()
        .find(|part| part.kind == "document.fold")
        .unwrap();
    let bytes = reopened
        .recovery_chunk(&records[0].id, fold, 0, 262144)
        .unwrap();
    assert_eq!(
        fixture::meta_in_fold(&bytes, "color").as_deref(),
        Some("red")
    );
    assert_eq!(records[0].reason, "beyond quota");
    let mut states: Vec<String> = parts
        .iter()
        .filter(|part| part.kind == "intent.metadata")
        .map(|part| {
            let metadata: serde_json::Value = serde_json::from_slice(
                &reopened
                    .recovery_chunk(&records[0].id, part, 0, 262144)
                    .unwrap(),
            )
            .unwrap();
            metadata["state"].as_str().unwrap().to_owned()
        })
        .collect();
    states.sort();
    assert_eq!(
        states,
        ["accepted", "frozen"],
        "the refused delta, and the accepted create no round has shown yet"
    );
    assert_eq!(parts.iter().filter(|part| part.kind == "intent").count(), 2);
    reopened.remove_recovery_record(&records[0].id).unwrap();
    assert!(reopened.recovery_records(None, 100).unwrap().is_empty());
}

/// `testServerSnapshotMergePreservesUnpushedLocalOps` — a served snapshot is
/// MERGED, never blindly substituted: a local op that has not yet been pushed
/// survives the server's newer snapshot, and is still owed until its verdict.
#[tokio::test]
async fn server_snapshot_merge_preserves_unpushed_local_ops() {
    let store = fixture::store("doc-snapshot-merge");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());

    // Server-born document arrives on bootstrap.
    let server = fixture::doc(fixture::SERVER_PEER, None);
    fixture::set_meta(&server, "name", "Server");
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                LoroReplicaCodec::CODEC_NAME,
                &fixture::snapshot(&server),
                fields(&[("name", text("Server"))]),
            )],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.expect("bootstrap the shard");

    // A local edit authored from the fold, under the store's minted peer.
    let peer = engine
        .doc_peer("boards", "b1")
        .expect("read the doc peer")
        .expect("the doc has a peer");
    assert_eq!(peer, 100, "the fold's peer was minted by the engine");
    let fold = engine
        .doc_fold("boards", "b1")
        .expect("read the doc fold")
        .expect("the doc has a fold");
    let app = fixture::doc(peer, Some(&fold));
    engine
        .record_doc_delta("boards", "b1", &fixture::edit_payload(&app, "color", "red"))
        .await
        .expect("record the local edit");

    // Push side dead: the local op is still UNPUSHED when the server's
    // snapshot arrives — the exact shape the merge rule protects.
    transport.fail_pushes(true);

    // The server evolves WITHOUT our edit; its fresh snapshot arrives.
    fixture::set_meta(&server, "name", "Server2");
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                LoroReplicaCodec::CODEC_NAME,
                &fixture::snapshot(&server),
                fields(&[("name", text("Server2"))]),
            )],
            "9:",
            false,
        ),
    );
    engine
        .pull_once("user")
        .await
        .expect("pull the server's newer snapshot");

    // MERGE, never a blind replace: both sides survive.
    let merged = store
        .peek_doc("boards", "b1")
        .expect("peek doc")
        .expect("the doc row exists");
    assert_eq!(
        fixture::meta_in_fold(&merged.fold, "name").as_deref(),
        Some("Server2")
    );
    assert_eq!(
        fixture::meta_in_fold(&merged.fold, "color").as_deref(),
        Some("red"),
        "the unpushed local op survived the snapshot"
    );
    assert_eq!(merged.peer, 100, "no rotation while the fold lives");

    // Still owed: the local op is not acked until its own verdict.
    assert_eq!(store.peek_pending().expect("peek pending").len(), 1);
    let codec = LoroReplicaCodec::new();
    assert!(
        !codec.is_empty_diff(
            &codec
                .diff(&merged.fold, merged.acked.as_deref())
                .expect("diff the merged fold since acked")
        ),
        "the unpushed local op is still owed"
    );

    transport.fail_pushes(false);
    engine.drain().await.expect("drain the local op");
    let drained = store
        .peek_doc("boards", "b1")
        .expect("peek doc")
        .expect("the doc row exists");
    assert!(
        codec.is_empty_diff(
            &codec
                .diff(&drained.fold, drained.acked.as_deref())
                .expect("diff the drained fold since acked")
        ),
        "the accepted delta advances acked over the local op"
    );
}

/// `testServerDeltaFrameMergesAndAdvancesAcked` — a delta the server served is
/// by definition already acked: it merges into the fold and the client owes
/// nothing for it.
#[tokio::test]
async fn server_delta_frame_merges_and_advances_acked() {
    let store = fixture::store("doc-served-delta");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());

    let server = fixture::doc(fixture::SERVER_PEER, None);
    fixture::set_meta(&server, "name", "Server");
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                LoroReplicaCodec::CODEC_NAME,
                &fixture::snapshot(&server),
                ReplicaFields::new(),
            )],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.expect("bootstrap the shard");

    let delta = fixture::edit_payload(&server, "name", "Server2");
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_delta(
                "boards",
                "b1",
                1,
                LoroReplicaCodec::CODEC_NAME,
                &delta,
            )],
            "6:",
            false,
        ),
    );
    engine
        .pull_once("user")
        .await
        .expect("pull the served delta");

    let doc = store
        .peek_doc("boards", "b1")
        .expect("peek doc")
        .expect("the doc row exists");
    assert_eq!(
        fixture::meta_in_fold(&doc.fold, "name").as_deref(),
        Some("Server2")
    );
    let codec = LoroReplicaCodec::new();
    assert!(
        codec.is_empty_diff(
            &codec
                .diff(&doc.fold, doc.acked.as_deref())
                .expect("diff the fold since acked")
        ),
        "a served delta is by definition acked — the client owes nothing for it"
    );
}

#[tokio::test]
async fn invalid_document_frame_rolls_back_the_whole_checkpoint() {
    for kind in [
        "delta",
        "existing snapshot",
        "new snapshot",
        "reset snapshot",
    ] {
        let store = fixture::store("invalid-checkpoint");
        let transport = StubTransport::new();
        let engine = fixture::engine(store.clone(), transport.clone());
        let author = fixture::doc(7, None);
        fixture::set_meta(&author, "name", "Before");
        let seed = fixture::snapshot(&author);
        transport.queue_pull(
            "user",
            ScriptedPull::new(
                vec![doc_snapshot(
                    "boards",
                    "b1",
                    LoroReplicaCodec::CODEC_NAME,
                    &seed,
                    ReplicaFields::new(),
                )],
                "5:",
                false,
            ),
        );
        engine.pull_once("user").await.unwrap();
        let before = store.peek_doc("boards", "b1").unwrap().unwrap().fold;
        let invalid = if kind == "delta" {
            doc_delta("boards", "b1", 1, LoroReplicaCodec::CODEC_NAME, b"broken")
        } else {
            doc_snapshot(
                "boards",
                if kind == "existing snapshot" {
                    "b1"
                } else {
                    "b2"
                },
                LoroReplicaCodec::CODEC_NAME,
                b"broken",
                ReplicaFields::new(),
            )
        };
        if kind == "reset snapshot" {
            transport.forget_cursors();
            assert_eq!(engine.pull_once("user").await.unwrap(), 0, "{kind}");
        }
        transport.queue_pull(
            "user",
            ScriptedPull::new(
                vec![replicaman::testing::note("partial", "new", None), invalid],
                "9:",
                false,
            ),
        );
        assert!(engine.pull_once("user").await.is_err(), "{kind}");
        assert_eq!(
            engine.current_cursor("user").await.unwrap().as_deref(),
            Some("5:"),
            "{kind}"
        );
        assert!(
            store.peek_snapshot("notes", "partial").unwrap().is_none(),
            "{kind}"
        );
        assert!(
            store.peek_snapshot("boards", "b2").unwrap().is_none(),
            "{kind}"
        );
        assert_eq!(
            store.peek_doc("boards", "b1").unwrap().unwrap().fold,
            before,
            "{kind}"
        );
    }
}
