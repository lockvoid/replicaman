//! The pull/checkpoint group, transliterated file by file.
//!
//! - `BootstrapResetTests` (1)
//! - `CheckpointAtomicityTests` (1)
//! - `TailApplyTests` (2)
//! - `PullRebaseTests` (3)
//! - `DecodeToleranceTests` (2)
//! - `DeleteCascadeTests` (3)
//! - `PeerRotationTests` (2)
//! - `SettlingPullTests` (3)
//! - `DoorbellShardTests` (2)
//! - `LoroFreeCoreTests` (1)

use std::sync::Arc;

use crate::error::ReplicaError;
use crate::transport::BoxFuture;
use crate::models::RowStream;
use crate::tests::support::*;
use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::{ReplicaVerdict, verb};

// MARK: - BootstrapResetTests (1)

/// Bootstrap with `reset` replaces the world; the journal survives
/// (it still owes its ops).
#[tokio::test]
async fn reset_replaces_the_shard_world_and_journal_survives() {
    let store = store("bootstrap-reset");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    // World v1: two notes on the user shard, one asset on global.
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "one", None), note("n2", "two", None)],
            "10:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    transport.queue_pull(
        "global",
        ScriptedPull::new(
            vec![row_set(
                "assets",
                "a1",
                None,
                fields(&[("kind", text("font"))]),
            )],
            "10:",
            false,
        ),
    );
    engine.pull_once("global").await.unwrap();

    // Unsent local work: a third note, journaled — and the wire's push side
    // goes dead, so the drain barrier can't discharge it first (the offline
    // shape this matrix item exists for).
    engine
        .save_row("notes", "n3", None, &fields(&[("title", text("local"))]))
        .await
        .unwrap();
    assert_eq!(store.peek_pending().unwrap().len(), 1);
    transport.fail_pushes(true);

    engine.reset_cursors().await.unwrap();

    // Forced resnapshot (GC horizon / rebuild): the user shard's world is
    // REPLACED — not merged.
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n4", "fresh", None)], "20:", false),
    );
    engine.pull_once("user").await.unwrap();

    let user_rows: Vec<String> = store
        .all_snapshots()
        .unwrap()
        .into_iter()
        .filter(|row| row.stream == "notes")
        .map(|row| row.row_id)
        .collect();
    assert_eq!(
        user_rows,
        ["n3", "n4"],
        "reset replaces confirmed server state and preserves the unsent n3 row"
    );

    assert_eq!(
        store
            .peek_snapshot("assets", "a1")
            .unwrap()
            .unwrap()
            .data
            .get("kind"),
        Some(&text("font")),
        "another shard's rows are untouched by this shard's reset"
    );

    let pending = store.peek_pending().unwrap();
    assert_eq!(pending.len(), 1, "the journal SURVIVES a reset");
    assert_eq!(pending[0].op().unwrap().row_id, "n3");

    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("20:"),
        "the reset's cursor is the new checkpoint"
    );
}

// MARK: - CheckpointAtomicityTests (1)

/// One pull batch applies ATOMICALLY with its cursor advance. A
/// crash mid-batch leaves the previous checkpoint intact: store unchanged,
/// cursor unmoved, and the same batch re-serves cleanly afterwards.
#[tokio::test]
async fn fault_before_commit_leaves_previous_checkpoint_intact() {
    let store = store("checkpoint-atomicity");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();

    engine
        .set_checkpoint_fault(Some(Arc::new(|| {
            Err(ReplicaError::Storage("injected fault".into()))
        })))
        .await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "poisoned", None), note("n2", "half", None)],
            "9:",
            false,
        ),
    );
    assert!(
        engine.pull_once("user").await.is_err(),
        "the injected fault must surface"
    );

    assert_eq!(
        store
            .peek_snapshot("notes", "n1")
            .unwrap()
            .unwrap()
            .data
            .get("title"),
        Some(&text("one")),
        "a faulted batch must not leave partial writes"
    );
    assert!(store.peek_snapshot("notes", "n2").unwrap().is_none());
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("5:"),
        "the cursor must not advance past an unapplied batch"
    );

    // The next pull re-serves from the intact checkpoint and lands.
    engine.set_checkpoint_fault(None).await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "poisoned", None), note("n2", "half", None)],
            "9:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        store
            .peek_snapshot("notes", "n1")
            .unwrap()
            .unwrap()
            .data
            .get("title"),
        Some(&text("poisoned"))
    );
    assert_eq!(
        store
            .peek_snapshot("notes", "n2")
            .unwrap()
            .unwrap()
            .data
            .get("title"),
        Some(&text("half"))
    );
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("9:")
    );
}

// MARK: - TailApplyTests (2)

/// Tail apply: `row.set` REPLACES (no merge, no insert-or-update
/// branching), `row.delete` removes, and ordering within a batch is preserved.
#[tokio::test]
async fn row_set_replaces_row_delete_removes_order_preserved() {
    let store = store("tail-apply");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                note("n1", "first", Some("a")),
                // Replacement drops fields the new copy doesn't carry — that is
                // what "unconditional replace" means.
                row_set("notes", "n1", None, fields(&[("title", text("second"))])),
                note("n2", "doomed", None),
                row_delete("notes", "n2"),
                note("n3", "last", None),
            ],
            "7:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let n1 = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(n1.data.get("title"), Some(&text("second")));
    assert!(
        !n1.data.contains_key("rank"),
        "row.set replaced the whole copy; the stale field is gone"
    );
    assert!(
        store.peek_snapshot("notes", "n2").unwrap().is_none(),
        "set-then-delete within one batch lands deleted"
    );
    assert_eq!(
        store
            .peek_snapshot("notes", "n3")
            .unwrap()
            .unwrap()
            .data
            .get("title"),
        Some(&text("last"))
    );
}

#[tokio::test]
async fn pull_until_caught_up_follows_more_and_threads_the_cursor() {
    let store = store("tail-apply-more");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "5:x", true),
    );
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n2", "two", None)], "9:", false),
    );

    let applied = engine.pull_until_caught_up(None).await.unwrap();
    assert_eq!(applied, 2);

    let user_pulls: Vec<Option<String>> = transport
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WireEvent::Pull { shard, cursor } if shard == "user" => Some(cursor),
            _ => None,
        })
        .collect();
    assert_eq!(
        user_pulls,
        vec![None, Some("5:x".to_owned())],
        "the second page pulls FROM the first page's cursor"
    );
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("9:")
    );
    assert!(store.peek_snapshot("notes", "n2").unwrap().is_some());
}

// MARK: - PullRebaseTests (3)

/// The shape: a row `running` pushed → pull in flight →
/// `succeeded` written locally → pull answer says `running`.
#[tokio::test]
async fn stale_frame_does_not_regress_a_row_with_a_pending_write() {
    let store = store("pull-rebase-race");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("running"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();

    // The server answers with what it had when the request arrived — BEFORE the
    // local write below.
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "running", None)], "1:", false),
    );
    let hook_failure: Arc<parking_lot::Mutex<Option<String>>> =
        Arc::new(parking_lot::Mutex::new(None));
    {
        let engine = engine.clone();
        let hook_failure = hook_failure.clone();
        transport.on_pull(move |_shard| {
            let engine = engine.clone();
            let hook_failure = hook_failure.clone();
            Box::pin(async move {
                if let Err(error) = engine
                    .save_row(
                        "notes",
                        "n1",
                        None,
                        &fields(&[("title", text("succeeded"))]),
                    )
                    .await
                {
                    *hook_failure.lock() = Some(error.to_string());
                }
            })
        });
    }

    engine.pull_once("user").await.unwrap();
    assert!(
        hook_failure.lock().is_none(),
        "the mid-pull write failed; the race this test stages never happened"
    );

    let row = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(
        row.data.get("title").and_then(ReplicaValue::as_string),
        Some("succeeded"),
        "the pull's older server state overwrote a write the server has not seen yet"
    );
    assert_eq!(
        store.peek_pending().unwrap().len(),
        1,
        "the newer write is still owed — nothing may drop it"
    );

    // The baseline, asserted where it discriminates: once nothing is owed, the
    // rebase must stop protecting the row and the server's frame wins.
    transport.on_pull(|_shard| Box::pin(async {}));
    engine.drain().await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "theirs", None)], "2:", false),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        store
            .peek_snapshot("notes", "n1")
            .unwrap()
            .unwrap()
            .data
            .get("title")
            .and_then(ReplicaValue::as_string),
        Some("theirs")
    );
}

/// The same overwrite without a race: the lane is cold, the pull does not
/// drain, the pending write is in the journal when the frame lands.
#[tokio::test]
async fn stale_frame_does_not_regress_a_pending_write_on_a_cold_lane() {
    let store = store("pull-rebase-cold");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[("title", text("running")), ("rank", text("a"))]),
        )
        .await
        .unwrap();
    engine.drain().await.unwrap();

    transport.fail_pushes(true);
    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[("title", text("succeeded")), ("rank", text("a"))]),
        )
        .await
        .unwrap();
    assert!(
        engine.drain().await.is_err(),
        "the dead push side must surface — its throw is what cools the lane"
    );
    transport.fail_pushes(false);

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "running", Some("server-touched"))],
            "1:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let row = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(
        row.data.get("title").and_then(ReplicaValue::as_string),
        Some("succeeded"),
        "my unacked patch must ride on top of the server's row"
    );
    assert_eq!(
        row.data.get("rank").and_then(ReplicaValue::as_string),
        Some("server-touched"),
        "fields I did not touch take the server's value"
    );
    assert_eq!(store.peek_pending().unwrap().len(), 1);
}

/// An owed CREATE is not replayed over the frame: ids are client-minted, so the
/// server having the row means the create landed and the frame is the fuller
/// truth (server-side fields, a peer's edit since).
#[tokio::test]
async fn unprocessed_birth_is_not_replaced_by_a_different_incarnation() {
    let store = store("pull-rebase-create");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.fail_pushes(true);
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("Before"))]))
        .await
        .unwrap();
    assert!(
        engine.drain().await.is_err(),
        "the dead push side must surface"
    );
    transport.fail_pushes(false);

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "Renamed elsewhere", Some("server"))],
            "1:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let row = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(
        row.data.get("title").and_then(ReplicaValue::as_string),
        Some("Before")
    );
    assert_eq!(row.data.get("rank").and_then(ReplicaValue::as_string), None);
    assert_eq!(store.peek_pending().unwrap().len(), 1);
}

// MARK: - DecodeToleranceTests (2)

/// The importer is TOTAL, typing is best-effort:
/// unknown stream / unknown STI `type` / unknown fields are stored-and-skipped,
/// never a thrown import.
#[tokio::test]
async fn known_stream_retains_unknown_type_and_fields() {
    let store = store("decode-tolerance");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                // A known stream with an unknown STI type and an unknown field.
                row_set(
                    "notes",
                    "n1",
                    Some("HoloNote"),
                    fields(&[
                        ("title", text("future")),
                        ("hologram", ReplicaValue::Bool(true)),
                    ]),
                ),
                note("n2", "plain", None),
            ],
            "4:",
            false,
        ),
    );

    // The import itself must not throw.
    engine.pull_once("user").await.unwrap();

    let holo = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(
        holo.data.get("hologram"),
        Some(&ReplicaValue::Bool(true)),
        "unknown fields are stored verbatim"
    );

    // The typed layer skips what it cannot decode — and only that.
    let stream = RowStream::<TestNote>::new(engine.clone());
    assert!(
        stream.find("n1").unwrap().is_none(),
        "unknown STI type decodes to nothing, not a crash"
    );
    assert_eq!(
        stream
            .all()
            .unwrap()
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>(),
        ["n2"],
        "typed reads skip the undecodable row"
    );
}

#[tokio::test]
async fn unknown_stream_refuses_the_entire_checkpoint() {
    let store = store("unknown-checkpoint-stream");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                note("n1", "valid", None),
                row_set("widgets", "w1", None, ReplicaFields::new()),
            ],
            "4:",
            false,
        ),
    );
    assert!(matches!(
        engine.pull_once("user").await,
        Err(ReplicaError::Protocol { .. })
    ));
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    assert_eq!(engine.current_cursor("user").await.unwrap(), None);
}

#[test]
fn http_pull_refuses_the_whole_page_when_a_frame_is_invalid() {
    let page = |vanish: &str| {
        format!(
            r#"{{"shard": "user", "reset": false, "frames": [
            {{"frame": "row.set", "stream": "notes", "id": "n1", "incarnation": "l1", "revision": "1", "data": {{"title": "ok"}}}},
            {{"frame": "{vanish}", "stream": "notes", "id": "n2", "incarnation": "l2", "revision": "2"}},
            {{"frame": "doc.delta", "stream": "boards", "id": "b1", "incarnation": "l3", "seq": 3, "codec": "loro@1", "payload": "AAEC"}}
        ], "cursor": "7:k", "more": true}}"#
        )
    };

    assert_eq!(
        crate::wire::decode_pull(page("row.delete").as_bytes())
            .unwrap()
            .frames
            .len(),
        3
    );
    assert!(crate::wire::decode_pull(page("row.vanish").as_bytes()).is_err());
}

// MARK: - DeleteCascadeTests (3)

/// The `row.delete` cascade is manifest-driven: a document stream's
/// delete evicts snapshot + fold + every owed journal entry (parked included);
/// a row stream's delete drops the snapshot row alone.
#[tokio::test]
async fn document_stream_delete_cascades_fold_and_journal() {
    let store = store("delete-cascade-doc");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                "stub@1",
                b"SEED",
                ReplicaFields::new(),
            )],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("stays"))]))
        .await
        .unwrap();
    // Push side dead and cooling: the note is frozen, and the doc's entry is
    // still owed when the delete frame arrives — the cascade, not the drain,
    // must clear it.
    transport.fail_pushes(true);
    assert!(engine.drain().await.is_err());
    engine
        .record_doc_delta("boards", "b1", b"+edit")
        .await
        .unwrap();
    assert_eq!(store.peek_pending().unwrap().len(), 2);

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![row_delete("boards", "b1")], "9:", false),
    );
    engine.pull_once("user").await.unwrap();

    assert!(
        store.peek_snapshot("boards", "b1").unwrap().is_none(),
        "the projection row is gone"
    );
    assert!(
        store.peek_doc("boards", "b1").unwrap().is_none(),
        "the fold is gone"
    );
    let pending: Vec<String> = store
        .peek_pending()
        .unwrap()
        .iter()
        .map(|entry| entry.op().unwrap().row_id)
        .collect();
    assert_eq!(
        pending,
        ["n1"],
        "every op the dead doc owed is discharged; the unrelated note entry survives"
    );
}

async fn park_the_board_birth_and_accept_the_note(
    engine: &crate::ReplicaEngine,
    transport: &StubTransport,
) {
    engine
        .create_doc("boards", "b1", b"SEED", 7, &ReplicaFields::new(), None)
        .await
        .unwrap();
    engine
        .record_doc_delta("boards", "b1", b"+edit")
        .await
        .unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("stays"))]))
        .await
        .unwrap();
    transport.script_push(|ops| {
        ops.iter()
            .map(|op| {
                if op.verb == verb::ROW_CREATE && op.stream == "boards" {
                    ReplicaVerdict::rejected(&op.id, "quota")
                } else {
                    ReplicaVerdict::accepted(&op.id)
                }
            })
            .collect()
    });
    engine.drain().await.unwrap();
}

async fn owe_a_frozen_and_an_owed_note_behind_a_cold_wire(
    engine: &crate::ReplicaEngine,
    transport: &StubTransport,
) {
    transport.fail_pushes(true);
    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("frozen"))]))
        .await
        .unwrap();
    assert!(engine.drain().await.is_err());
    engine
        .save_row("notes", "n3", None, &fields(&[("title", text("owed"))]))
        .await
        .unwrap();
}

async fn pull_the_board_deletion(engine: &crate::ReplicaEngine, transport: &StubTransport) {
    let deletion = ScriptedPull::new(vec![row_delete("boards", "b1")], "9:", false);
    transport.queue_pull("user", deletion);
    engine.pull_once("user").await.unwrap();
}

fn pending_row_ids(store: &crate::store::ReplicaStateStore) -> Vec<String> {
    store
        .peek_pending()
        .unwrap()
        .iter()
        .map(|entry| entry.op().unwrap().row_id)
        .collect()
}

#[tokio::test]
async fn document_cascade_spares_other_rows_entries() {
    let store = store("delete-cascade-parked");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    park_the_board_birth_and_accept_the_note(&engine, &transport).await;
    owe_a_frozen_and_an_owed_note_behind_a_cold_wire(&engine, &transport).await;

    pull_the_board_deletion(&engine, &transport).await;

    let parked = store.peek_parked().unwrap().len();
    assert_eq!(parked, 1, "a removal cannot hide the refusal");
    assert_eq!(pending_row_ids(&store), ["n2", "n3"]);
}

#[tokio::test]
async fn row_removal_archives_pending_patch() {
    let store = store("delete-cascade-row");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "server copy", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();
    // A pending patch for the same row. Push side dead so the drain barrier
    // can't discharge it first: its frozen bytes wait for their verdict.
    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[("title", text("local edit"))]),
        )
        .await
        .unwrap();
    let owed = store.peek_pending().unwrap()[0].payload.clone();
    transport.fail_pushes(true);

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![row_delete("notes", "n1")], "9:", false),
    );
    engine.pull_once("user").await.unwrap();

    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    assert_eq!(store.sync_status().unwrap().submitted_groups, 1);
    transport.fail_pushes(false);
    engine.drain().await.unwrap();
    assert!(store.peek_pending().unwrap().is_empty());
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    let record = store.recovery_records(None, 100).unwrap().remove(0);
    let part = store
        .recovery_parts(&record.id, None, 100)
        .unwrap()
        .into_iter()
        .find(|part| part.kind == "intent")
        .unwrap();
    assert_eq!(
        store.recovery_chunk(&record.id, &part, 0, 262144).unwrap(),
        owed
    );
}

// MARK: - PeerRotationTests (2)

/// Peer rotation: a wiped fold's document is reborn with a NEW loro
/// peer. Loro dedups by (peer, counter); a reborn doc reusing its peer would
/// have its edits silently discarded — the v1 lesson, kept.
#[tokio::test]
async fn reset_wiped_doc_is_recreated_with_a_fresh_peer() {
    let store = store("peer-rotation");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    let snapshot = b"SNAP-1";
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                "stub@1",
                snapshot,
                fields(&[("name", text("Plans"))]),
            )],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let born = store.peek_doc("boards", "b1").unwrap().unwrap();
    assert_eq!(born.peer, 100, "first birth mints the first peer");

    engine.resync_document("boards", "b1").await.unwrap();

    // Forced resnapshot: the same document arrives again after its fold was
    // wiped with the shard.
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                "stub@1",
                snapshot,
                fields(&[("name", text("Plans"))]),
            )],
            "9:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let reborn = store.peek_doc("boards", "b1").unwrap().unwrap();
    assert_eq!(reborn.peer, 101, "a reborn doc NEVER reuses its old peer");
    assert_ne!(reborn.peer, born.peer);
}

#[tokio::test]
async fn surviving_doc_keeps_its_peer_across_ordinary_pulls() {
    let store = store("peer-stability");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                "stub@1",
                b"SNAP",
                ReplicaFields::new(),
            )],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    // An ordinary tail (no reset) touching the same doc must not rotate.
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_delta("boards", "b1", 1, "stub@1", b"+d1")],
            "6:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let doc = store.peek_doc("boards", "b1").unwrap().unwrap();
    assert_eq!(doc.peer, 100, "peers are stable while the fold lives");
}

// MARK: - DoorbellShardTests (2)

/// The doorbell rings for the USER shard only, so its pull must ask for that
/// shard alone — walking every shard per ring wastes a catalog round-trip per
/// doorbell.
#[tokio::test]
async fn pulling_named_shards_touches_only_those() {
    let store = store("doorbell-named");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .pull_until_caught_up(Some(&["user".to_owned()]))
        .await
        .unwrap();

    assert_eq!(transport.pulled_shards(), ["user"]);
}

#[tokio::test]
async fn pulling_every_shard_still_walks_all() {
    let store = store("doorbell-all");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine.pull_until_caught_up(None).await.unwrap();

    assert_eq!(transport.pulled_shards(), ["user", "global"]);
}

// MARK: - LoroFreeCoreTests (1)

/// The loro-free core. The MODULE GRAPH is the enforcement: the
/// `loro` feature is default-off, no `use loro::` exists on this side of the
/// boundary, and `cargo check --no-default-features` builds the core in
/// isolation to prove it. This case adds the functional half: a rows-only
/// consumer (no codec registered at all) still replicates rows and even keeps
/// document PROJECTIONS readable.
#[tokio::test]
async fn rows_only_engine_with_no_codec_replicates() {
    let store = store("loro-free-core");
    let transport = StubTransport::new();
    // No codecs at all — the rows-only consumer.
    let mut opts = options(engine_directory(), transport.clone());
    opts.codecs = Vec::new();
    opts.document_mode = crate::ReplicaDocumentMode::ProjectionsOnly;
    let engine = engine_with(store.clone(), OWNER, opts);

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                note("n1", "rows work", None),
                // Document frames arrive anyway; without a codec the fold is
                // beyond reach, but the projection data still lands in raw
                // truth (stored-and-skipped, never a throw).
                doc_snapshot(
                    "boards",
                    "b1",
                    "loro@1",
                    b"OPAQUE",
                    fields(&[("name", text("Plans"))]),
                ),
                doc_delta("boards", "b1", 1, "loro@1", b"OPS"),
            ],
            "6:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    assert_eq!(
        store
            .peek_snapshot("notes", "n1")
            .unwrap()
            .unwrap()
            .data
            .get("title"),
        Some(&text("rows work"))
    );
    assert_eq!(
        store
            .peek_snapshot("boards", "b1")
            .unwrap()
            .unwrap()
            .data
            .get("name"),
        Some(&text("Plans")),
        "the document's projection row is still useful without the codec"
    );
    assert!(
        store.peek_doc("boards", "b1").unwrap().is_none(),
        "no codec, no fold — skipped, not thrown"
    );

    // And the row write door works end to end.
    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();
    assert_eq!(store.peek_pending().unwrap().len(), 0);
}

// MARK: - PullRoundTests (3)

fn user_cursors(transport: &StubTransport) -> Vec<Option<String>> {
    transport
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WireEvent::Pull { shard, cursor } if shard == "user" => Some(cursor),
            _ => None,
        })
        .collect()
}

/// History the base cannot absorb means the base is no longer the server's:
/// the round is forgotten and the shard baselines again, once.
#[tokio::test]
async fn history_the_base_cannot_absorb_forgets_the_round_and_the_shard_baselines_again() {
    let store = store("pull-round-causal-gap");
    let transport = StubTransport::new();
    let mut options = options(engine_directory(), transport.clone());
    options.schema = causal_schema();
    options.codecs = vec![Arc::new(CausalCodec) as Arc<dyn crate::ReplicaCodec>];
    let engine = engine_with((*store).clone(), OWNER, options);
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_snapshot(
                "boards",
                "b1",
                "causal@1",
                &CausalCodec::payload(1..=3),
                fields(&[("name", text("Board"))]),
            )],
            "c1",
            false,
        ),
    );
    engine
        .pull_until_caught_up(Some(&["user".to_owned()]))
        .await
        .unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                doc_delta("boards", "b1", 1, "causal@1", &CausalCodec::payload(5..=5)),
                row_set("boards", "b1", None, fields(&[("name", text("Renamed"))])),
                note("n2", "two", None),
            ],
            "c2",
            false,
        ),
    );
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                doc_snapshot(
                    "boards",
                    "b1",
                    "causal@1",
                    &CausalCodec::payload(1..=5),
                    fields(&[("name", text("Renamed"))]),
                ),
                note("n2", "two", None),
            ],
            "b1",
            false,
        ),
    );

    let published = engine
        .pull_until_caught_up(Some(&["user".to_owned()]))
        .await
        .unwrap();

    assert_eq!(published, 2, "the baseline publishes");
    assert_eq!(
        CausalCodec::tokens(&store.peek_doc("boards", "b1").unwrap().unwrap().fold),
        (1..=5).collect()
    );
    assert_eq!(
        store.peek_snapshot("notes", "n2").unwrap().unwrap().data["title"],
        text("two")
    );
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("b1")
    );
    assert_eq!(
        user_cursors(&transport),
        vec![None, Some("c1".to_owned()), None],
        "the round is forgotten and the shard baselines"
    );
    assert!(
        store.recovery_records(None, 100).unwrap().is_empty(),
        "nothing was authored, nothing is archived"
    );
}

/// A baseline the server cannot answer coherently is forgotten once; the
/// second one's failure is the caller's to see.
#[tokio::test]
async fn a_second_round_the_shard_cannot_publish_reaches_the_caller() {
    let store = store("pull-round-forgotten-twice");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    for index in 0..3 {
        transport.queue_pull(
            "user",
            ScriptedPull::new(vec![note("n1", "one", None)], format!("p1-{index}"), true),
        );
        transport.queue_pull(
            "user",
            ScriptedPull::new(
                vec![doc_delta("boards", "b9", 1, "stub@1", b"x")],
                format!("p2-{index}"),
                false,
            ),
        );
    }

    let refused = engine.pull_until_caught_up(Some(&["user".to_owned()])).await;

    assert!(
        matches!(refused, Err(ReplicaError::Protocol { ref code, .. }) if code == "InvalidResponse"),
        "a baseline that cannot be published twice must fail: {refused:?}"
    );
    assert_eq!(
        transport.pull_count(),
        4,
        "one baseline forgotten, the second one's failure thrown"
    );
    assert!(
        store.peek_snapshot("notes", "n1").unwrap().is_none(),
        "no partial round published"
    );
}

/// A frame of a stream the schema does not declare is refused at receipt as
/// an upgrade, before anything is staged: the checkpoint stands, no round is
/// forgotten.
#[tokio::test]
async fn an_undeclared_stream_is_refused_at_receipt_as_an_upgrade() {
    let store = store("pull-round-undeclared-stream");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "c1", false),
    );
    engine.pull_once("user").await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![row_set("ghosts", "g1", None, fields(&[("title", text("boo"))]))],
            "c2",
            false,
        ),
    );

    let refused = engine.pull_until_caught_up(Some(&["user".to_owned()])).await;

    assert!(
        matches!(refused, Err(ReplicaError::Protocol { ref code, .. }) if code == "UpgradeRequired"),
        "{refused:?}"
    );
    assert_eq!(transport.pull_count(), 2);
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("c1")
    );
    assert_eq!(
        store.peek_snapshot("notes", "n1").unwrap().unwrap().data["title"],
        text("one")
    );
}

/// An accepted delete the server shows to a round that started before the
/// delete was accepted is a settled lifetime, not a lost branch: nothing is
/// archived.
#[tokio::test]
async fn an_accepted_delete_seen_by_a_round_started_before_it_leaves_no_recovery_record() {
    let store = store("delete-cascade-accepted-delete");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "c1", false),
    );
    engine.pull_once("user").await.unwrap();

    let entered = Gate::new();
    let held = Gate::new();
    transport.on_pull({
        let entered = entered.clone();
        let held = held.clone();
        move |_shard| {
            let entered = entered.clone();
            let held = held.clone();
            Box::pin(async move {
                entered.release();
                held.wait().await;
            }) as BoxFuture<'static, ()>
        }
    });
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![row_delete("notes", "n1")], "c2", false),
    );
    let pull = tokio::spawn({
        let engine = engine.clone();
        async move { engine.pull_once("user").await }
    });
    entered.wait().await;
    engine.delete_row("notes", "n1").await.unwrap();
    engine.drain().await.unwrap();
    held.release();
    pull.await.unwrap().unwrap();

    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    assert!(store.recovery_records(None, 100).unwrap().is_empty());
    assert!(!store.sync_status().unwrap().has_unsettled_work());
    assert!(store.peek_pending().unwrap().is_empty());
}
