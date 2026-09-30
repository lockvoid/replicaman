//! The local write door, transliterated file by file.
//!
//! - `RevertOnRejectionTests` (9)
//! - `WriteSchedulingTests` (7)
//! - `SaveDeleteTests` (6)

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::error::ReplicaError;
use crate::models::{ReplicaCreateStamp, ReplicaRowModel, ReplicaWritableRowModel, RowStream};
use crate::schema::{ReplicaLane, ReplicaSchema, ReplicaStreamSpec};
use crate::tests::support::*;
use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::{ReplicaOp, ReplicaVerdict, verb};

#[tokio::test]
async fn corrupt_rows_and_nonfinite_values_cannot_be_acknowledged_as_saved() {
    let store = store("corrupt-write");
    let engine = engine(store.clone(), StubTransport::new());
    store
        .pool()
        .write(|ctx| {
            ctx.tx.execute(
                "INSERT INTO snapshots (stream, row_id, shard, data) VALUES ('notes', 'n1', 'user', '{broken')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(
        engine
            .save_row(
                "notes",
                "n1",
                None,
                &fields(&[("title", text("replacement"))])
            )
            .await
            .is_err()
    );
    assert!(store.peek_pending().unwrap().is_empty());
    assert!(store.peek_snapshot("notes", "n1").is_err());
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(
            engine
                .save_row(
                    "notes",
                    "n2",
                    None,
                    &fields(&[("value", ReplicaValue::Number(bad))])
                )
                .await
                .is_err()
        );
        assert!(store.peek_snapshot("notes", "n2").unwrap().is_none());
        assert!(store.peek_pending().unwrap().is_empty());
    }
}

// MARK: - RevertOnRejectionTests (9)
//
// The rejected-op revert path. A rejection is a VERDICT: the entry parks
// — but the client write must not survive it, because the server will never
// send a correcting frame (pull ships only changed rows). Journal entries carry
// a client-local PRE-IMAGE; the verdict transaction reverts atomically: create
// ⇒ delete the row, patch ⇒ restore the pre-image fields, delete ⇒ restore the
// row. Document rejections are rare by design (the server repairs rather than
// rejects) — they discard the op and force a re-bootstrap instead.

/// Upstream's private `rejectAll(_:)` helper.
fn reject_all_stub(transport: &StubTransport) {
    reject_all(transport, "refused (stub)");
}

#[tokio::test]
async fn rejected_create_deletes_the_client_written_row() {
    let store = store("revert-rejected-create");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_some());

    reject_all_stub(&transport);
    engine.drain().await.unwrap();

    assert!(
        store.peek_snapshot("notes", "n1").unwrap().is_none(),
        "a refused birth leaves no ghost row"
    );
    assert_eq!(
        store.peek_parked().unwrap().len(),
        1,
        "the entry parks as evidence"
    );
}

#[tokio::test]
async fn rejected_row_create_drops_every_dependent_address_entry() {
    let store = store("revert-rejected-create-cascade");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("birth"))]))
        .await
        .unwrap();
    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[("title", text("later patch"))]),
        )
        .await
        .unwrap();

    reject_all_stub(&transport);
    engine.drain().await.unwrap();

    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    assert!(store.peek_pending().unwrap().is_empty());
    let parked = store.peek_parked().unwrap();
    assert_eq!(
        parked.len(),
        1,
        "only the refused birth remains as evidence"
    );
    let evidence = parked.first().unwrap().op().unwrap();
    assert_eq!(evidence.verb, verb::ROW_CREATE);
}

#[tokio::test]
async fn rejected_patch_restores_exactly_the_patched_fields() {
    let store = store("revert-rejected-patch");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    // Server truth arrives first.
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "server", Some("a"))], "5:", false),
    );
    engine.pull_once("user").await.unwrap();

    // The local patch touches title AND adds a brand-new field.
    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[
                ("title", text("local")),
                ("rank", text("a")),
                ("mood", text("bold")),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .peek_snapshot("notes", "n1")
            .unwrap()
            .unwrap()
            .data
            .get("mood"),
        Some(&text("bold"))
    );

    reject_all_stub(&transport);
    engine.drain().await.unwrap();

    let reverted = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(
        reverted.data.get("title"),
        Some(&text("server")),
        "the patched field returns to its pre-image"
    );
    assert_eq!(
        reverted.data.get("rank"),
        Some(&text("a")),
        "untouched fields stay"
    );
    assert!(
        // Upstream's `XCTAssertNil(data["mood"])`: the KEY is gone. A stored
        // `.null` would be `Some(&ReplicaValue::Null)` here and must not pass.
        !reverted.data.contains_key("mood"),
        "a field the patch INTRODUCED is removed, not nulled"
    );
}

#[tokio::test]
async fn rejected_patch_leaves_interim_server_fields_alone() {
    let store = store("revert-rejected-patch-interim");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "server", Some("a"))], "5:", false),
    );
    engine.pull_once("user").await.unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("local"))]))
        .await
        .unwrap();

    // While the patch is in flight, the server replaces the row with a fresher
    // copy carrying a field the patch never touched.
    transport.fail_pushes(true);
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![row_set(
                "notes",
                "n1",
                None,
                fields(&[
                    ("title", text("remote")),
                    ("rank", text("z")),
                    ("starred", ReplicaValue::Bool(true)),
                ]),
            )],
            "9:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    transport.fail_pushes(false);

    reject_all_stub(&transport);
    engine.drain().await.unwrap();

    let row = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(
        row.data.get("title"),
        Some(&text("remote")),
        "rejection restores the newest authoritative field value"
    );
    assert_eq!(
        row.data.get("rank"),
        Some(&text("z")),
        "interim server fields survive the revert"
    );
    assert_eq!(row.data.get("starred"), Some(&ReplicaValue::Bool(true)));
}

#[tokio::test]
async fn rejected_delete_restores_the_row_byte_identical() {
    let store = store("revert-rejected-delete");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![row_set(
                "notes",
                "n1",
                Some("Note"),
                fields(&[("title", text("keep me"))]),
            )],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    let before = store.peek_snapshot("notes", "n1").unwrap().unwrap();

    engine.delete_row("notes", "n1").await.unwrap();
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());

    reject_all_stub(&transport);
    engine.drain().await.unwrap();

    let restored = store
        .peek_snapshot("notes", "n1")
        .unwrap()
        .expect("a refused delete resurrects the row");
    assert_eq!(
        restored, before,
        "byte-identical prior state — type and data alike"
    );
}

#[tokio::test]
async fn rejected_doc_create_removes_the_whole_client_written_doc() {
    let store = store("revert-rejected-doc-create");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .create_doc(
            "boards",
            "b1",
            b"SEED",
            7,
            &fields(&[("name", text("Plans"))]),
            None,
        )
        .await
        .unwrap();
    engine
        .record_doc_delta("boards", "b1", b"+edit")
        .await
        .unwrap();

    reject_all_stub(&transport);
    engine.drain().await.unwrap();

    assert!(
        store.peek_snapshot("boards", "b1").unwrap().is_none(),
        "a refused doc birth leaves no projection row"
    );
    assert!(
        store.peek_doc("boards", "b1").unwrap().is_none(),
        "…and no fold"
    );
    assert_eq!(
        store.peek_pending().unwrap().len(),
        0,
        "the doc's superseded delta dies with it"
    );
}

#[tokio::test]
async fn rejected_doc_delta_restores_base_and_keeps_recovery() {
    let store = store("revert-rejected-doc-delta");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    // A server-born doc plus an unrelated pending note — the note's op must
    // survive the reset (bootstrap replaces the WORLD, the journal still owes
    // its ops).
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
    engine
        .record_doc_delta("boards", "b1", b"+edit")
        .await
        .unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("honest"))]))
        .await
        .unwrap();

    transport.script_push(|ops| {
        ops.iter()
            .map(|op| {
                if op.verb == verb::DOC_DELTA {
                    ReplicaVerdict::rejected(&op.id, "delta refused")
                } else {
                    ReplicaVerdict::accepted(&op.id)
                }
            })
            .collect()
    });
    assert_eq!(
        engine.doc_fold("boards", "b1").unwrap(),
        Some(b"SNAP+edit".to_vec())
    );
    engine.drain().await.unwrap();

    assert_eq!(
        engine.doc_fold("boards", "b1").unwrap(),
        Some(b"SNAP".to_vec())
    );
    assert_eq!(
        store.peek_parked().unwrap().len(),
        1,
        "the refusal remains inspectable without automatic retry"
    );
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("5:")
    );
    assert_eq!(store.recovery_records(None, 100).unwrap().len(), 1);
    assert_eq!(
        store.peek_pending().unwrap().len(),
        0,
        "the note create was accepted in the same drain; nothing else was thrown away"
    );
}

/// The classification the whole offline story rests on: a severed wire is
/// RETRYABLE, never a verdict. A transport failure that parked would both
/// strand the op forever (parked entries are never retried) and trip the
/// revert — undoing work the user can see, because the network blinked.
#[tokio::test]
async fn transport_failure_neither_parks_nor_reverts() {
    let store = store("revert-transport-failure");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    engine.delete_row("notes", "n2").await.unwrap();
    transport.fail_pushes(true);

    assert!(
        engine.drain().await.is_err(),
        "a dead wire surfaces from the drain"
    );

    assert_eq!(
        store.peek_parked().unwrap().len(),
        0,
        "no connection is not a refusal"
    );
    assert_eq!(
        store.peek_pending().unwrap().len(),
        1,
        "the real write stays owed; deleting an absent row is a no-op"
    );
    assert!(
        store.peek_snapshot("notes", "n1").unwrap().is_some(),
        "the client row must survive: only a VERDICT reverts"
    );
    assert_eq!(engine.reverted_count().await, 0);
}

#[tokio::test]
async fn rejection_hook_fires_with_the_verdict_reason() {
    let store = store("revert-rejection-hook");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    let heard: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let heard = heard.clone();
        engine
            .set_rejection_handler(Some(Arc::new(move |op: &ReplicaOp, reason: &str| {
                heard
                    .lock()
                    .push(format!("{}:{}:{reason}", op.verb, op.row_id));
            })))
            .await;
    }

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    reject_all_stub(&transport);
    engine.drain().await.unwrap();

    // The seam fires inside the drain, after the verdict transaction commits —
    // upstream polls because its handler hops a Task; ours does not, so the
    // stronger direct read is available.
    assert_eq!(
        heard.lock().clone(),
        ["row.create:n1:refused (stub)"],
        "the rejection seam never fired"
    );
    assert_eq!(
        engine.reverted_count().await,
        1,
        "the debug surface can read how many writes were undone"
    );
}

// MARK: - WriteSchedulingTests (7)
//
// Generated CRUD verbs stop at the database boundary. Delivery is engine
// behavior: an app caller must never need to ring a second transport bell after
// `save`, `create`, `delete`, or a document edit.

#[tokio::test]
async fn save_schedules_its_own_push() {
    let store = store("write-scheduling-save");
    let transport = StubTransport::new();
    let engine = self_delivering_engine(store.clone(), transport.clone());

    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[("title", text("ordinary CRUD"))]),
        )
        .await
        .unwrap();

    until(
        "save() returned but the engine never pushed its write",
        || {
            let transport = transport.clone();
            async move { transport.push_count() == 1 }
        },
    )
    .await;
    assert!(store.peek_pending().unwrap().is_empty());
}

#[tokio::test]
async fn create_stamps_the_bound_owner_and_clock_inside_the_engine() {
    let store = store("write-scheduling-stamp");
    let transport = StubTransport::new();
    let instant = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut opts = options(engine_directory(), transport.clone());
    opts.clock = Arc::new(move || -> SystemTime { instant });
    opts.automatically_push_writes = false;
    let engine = engine_with(store.clone(), OWNER, opts);

    engine
        .create_doc(
            "boards",
            "b1",
            b"SEED",
            7,
            &fields(&[("name", text("Stamped"))]),
            Some(&ReplicaCreateStamp::standard()),
        )
        .await
        .unwrap();

    let row = store.peek_snapshot("boards", "b1").unwrap().unwrap();
    assert_eq!(row.data.get("name"), Some(&text("Stamped")));
    assert_eq!(
        row.data.get("userId"),
        Some(&ReplicaValue::Number(42.0)),
        "the bound owner is the only owner a create can carry"
    );
    assert_eq!(
        row.data.get("createdAt"),
        Some(&text("2023-11-14T22:13:20Z"))
    );
    assert_eq!(
        row.data.get("updatedAt"),
        Some(&text("2023-11-14T22:13:20Z"))
    );
}

#[tokio::test]
async fn document_create_edit_and_delete_each_schedule_delivery() {
    let store = store("write-scheduling-doc");
    let transport = StubTransport::new();
    let engine = self_delivering_engine(store.clone(), transport.clone());

    engine
        .create_doc("boards", "b1", b"S", 7, &ReplicaFields::new(), None)
        .await
        .unwrap();
    until("the doc create never reached the wire", || {
        let transport = transport.clone();
        async move { transport.push_count() == 1 }
    })
    .await;

    engine.record_doc_delta("boards", "b1", b"D").await.unwrap();
    until("the doc edit never reached the wire", || {
        let transport = transport.clone();
        async move { transport.push_count() == 2 }
    })
    .await;

    engine.delete_row("boards", "b1").await.unwrap();
    until("the doc delete never reached the wire", || {
        let transport = transport.clone();
        async move { transport.push_count() == 3 }
    })
    .await;

    let verbs: Vec<String> = transport
        .pushed_ops()
        .into_iter()
        .map(|op| op.verb)
        .collect();
    assert_eq!(verbs, [verb::ROW_CREATE, verb::DOC_DELTA, verb::ROW_DELETE]);
}

#[tokio::test]
async fn delete_during_an_in_flight_create_still_sends_the_delete() {
    let store = store("write-scheduling-inflight-delete");
    let transport = StubTransport::new();
    transport.delay_pushes(Duration::from_millis(250));
    let engine = self_delivering_engine(store.clone(), transport.clone());

    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[("title", text("short lived"))]),
        )
        .await
        .unwrap();
    until("the create never reached the wire", || {
        let transport = transport.clone();
        async move { transport.push_count() == 1 }
    })
    .await;

    engine.delete_row("notes", "n1").await.unwrap();
    until(
        "the in-flight create reached the server without its delete",
        || {
            let transport = transport.clone();
            async move { transport.push_count() == 2 }
        },
    )
    .await;

    let verbs: Vec<String> = transport
        .pushed_ops()
        .into_iter()
        .map(|op| op.verb)
        .collect();
    assert_eq!(verbs, [verb::ROW_CREATE, verb::ROW_DELETE]);
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
}

#[tokio::test]
async fn repeated_document_create_preserves_the_first_birth_atomically() {
    let store = store("write-scheduling-repeat-create");
    let engine = engine(store.clone(), StubTransport::new());

    let first = engine
        .create_doc(
            "boards",
            "b1",
            b"FIRST",
            7,
            &fields(&[("name", text("First"))]),
            None,
        )
        .await
        .unwrap();
    let replay = engine
        .create_doc(
            "boards",
            "b1",
            b"SECOND",
            8,
            &fields(&[("name", text("Second"))]),
            None,
        )
        .await
        .unwrap();

    assert!(first);
    assert!(!replay);
    assert_eq!(
        engine.doc_fold("boards", "b1").unwrap(),
        Some(b"FIRST".to_vec())
    );
    assert_eq!(
        store
            .peek_snapshot("boards", "b1")
            .unwrap()
            .unwrap()
            .data
            .get("name"),
        Some(&text("First"))
    );
    assert_eq!(store.peek_pending().unwrap().len(), 1);
}

#[tokio::test]
async fn automatic_delivery_honors_the_cold_window() {
    let store = store("write-scheduling-cold");
    let transport = StubTransport::new();
    transport.fail_pushes(true);
    let mut opts = options(engine_directory(), transport.clone());
    opts.automatically_push_writes = true;
    opts.cold_window = Duration::from_secs(60);
    let engine = engine_with(store.clone(), OWNER, opts);

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("first"))]))
        .await
        .unwrap();
    until("the first write never reached the wire", || {
        let transport = transport.clone();
        async move { transport.push_count() == 1 }
    })
    .await;

    // The STAMP is the mechanism, so assert the stamp rather than waiting out a
    // window: a cold lane refuses the second write's scheduled push at
    // `push_scheduled_writes`' loop condition.
    assert!(
        engine.is_cold_for_testing(ReplicaLane::Bulk).await,
        "the failed push must cool the lane it failed on"
    );

    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("second"))]))
        .await
        .unwrap();
    // `save_row` SPAWNS its scheduled push, so the count has to be read after
    // that task has had its turn — otherwise "still 1" is true of a warm lane
    // too and the assertion proves nothing. Upstream's actor hop makes the same
    // gap; a current-thread yield closes it deterministically.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        transport.push_count(),
        1,
        "a write onto a known-cold lane must not re-attempt the wire"
    );
    assert_eq!(store.peek_pending().unwrap().len(), 2);
}

#[tokio::test]
async fn rejected_in_flight_row_birth_drops_its_queued_delete() {
    let store = store("write-scheduling-rejected-birth");
    let transport = StubTransport::new();
    transport.delay_pushes(Duration::from_millis(250));
    reject_all(&transport, "refused");
    let engine = self_delivering_engine(store.clone(), transport.clone());

    engine
        .save_row(
            "notes",
            "n1",
            None,
            &fields(&[("title", text("refused birth"))]),
        )
        .await
        .unwrap();
    until("the birth never reached the wire", || {
        let transport = transport.clone();
        async move { transport.push_count() == 1 }
    })
    .await;
    engine.delete_row("notes", "n1").await.unwrap();

    // The park IS the settle point: the verdict transaction that parks the
    // birth is the same one that discards its dependent delete, so once the
    // park is visible there is nothing left in flight to wait for.
    until("the rejected birth never settled", || {
        let store = store.clone();
        async move { store.peek_parked().unwrap().len() == 1 }
    })
    .await;

    assert_eq!(
        transport.push_count(),
        1,
        "the dependent delete must not reach the wire"
    );
    assert!(store.peek_pending().unwrap().is_empty());
    let evidence = store.peek_parked().unwrap().first().unwrap().op().unwrap();
    assert_eq!(evidence.verb, verb::ROW_CREATE);
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
}

// MARK: - SaveDeleteTests (6)

#[tokio::test]
async fn explicit_row_birth_and_edit_refuse_identity_mistakes_atomically() {
    let store = store("explicit-row-expectations");
    let engine = engine(store.clone(), StubTransport::new());
    let notes = RowStream::<TestNote>::new(engine.clone());
    let first = TestNote::new("n1", Some("first"), None);
    assert!(matches!(
        notes.update(&first).await,
        Err(ReplicaError::UnknownRow { .. })
    ));
    assert!(store.peek_pending().unwrap().is_empty());
    assert!(notes.find("n1").unwrap().is_none());
    notes.create(&first).await.unwrap();
    let before = store
        .pool()
        .read(|db| store.change_sequence(db, "notes"))
        .unwrap();
    assert!(matches!(
        notes
            .create(&TestNote::new("n1", Some("collision"), None))
            .await,
        Err(ReplicaError::RowExists { .. })
    ));
    assert_eq!(
        notes.find("n1").unwrap().unwrap().title.as_deref(),
        Some("first")
    );
    assert_eq!(store.peek_pending().unwrap().len(), 1);
    assert_eq!(
        store
            .pool()
            .read(|db| store.change_sequence(db, "notes"))
            .unwrap(),
        before
    );
    notes.update(&first).await.unwrap();
    assert_eq!(
        store
            .pool()
            .read(|db| store.change_sequence(db, "notes"))
            .unwrap(),
        before
    );
    notes.delete("n1").await.unwrap();
    assert!(matches!(
        notes.update(&first).await,
        Err(ReplicaError::UnknownRow { .. })
    ));
    assert!(
        notes.find("n1").unwrap().is_none(),
        "update cannot resurrect a deleted row"
    );
}

#[tokio::test]
async fn concurrent_explicit_creates_admit_only_one_birth() {
    let store = store("concurrent-row-birth");
    let engine = engine(store.clone(), StubTransport::new());
    let left = fields(&[("title", text("left"))]);
    let right = fields(&[("title", text("right"))]);
    let (a, b) = tokio::join!(
        engine.create_row("notes", "n1", None, &left),
        engine.create_row("notes", "n1", None, &right)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(store.peek_pending().unwrap().len(), 1);
}
//
// The local write door under the generated `save()`/`delete()` verbs: diff
// against last-known ⇒ `row.create` (absent) / `row.patch` (changed fields
// ONLY) / nothing (no change), with the client snapshot write in the same
// transaction. Deletes discharge what the row still owed.

#[tokio::test]
async fn save_diffs_into_create_then_patch_then_nothing() {
    let store = store("save-delete-diff");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let notes = RowStream::<TestNote>::new(engine.clone());

    notes
        .create(&TestNote::new("n1", Some("draft"), Some("a")))
        .await
        .unwrap();
    let mut pending = store.peek_pending().unwrap();
    assert_eq!(pending.len(), 1);
    let mut op = pending.first().unwrap().op().unwrap();
    assert_eq!(op.verb, verb::ROW_CREATE);
    assert_eq!(
        op.data,
        Some(fields(&[("title", text("draft")), ("rank", text("a"))])),
        "creation is the only full-row write"
    );
    assert_eq!(
        notes.find("n1").unwrap().unwrap().title.as_deref(),
        Some("draft"),
        "the client write is visible immediately"
    );

    notes
        .update(&TestNote::new("n1", Some("final"), Some("a")))
        .await
        .unwrap();
    pending = store.peek_pending().unwrap();
    assert_eq!(pending.len(), 2);
    op = pending.last().unwrap().op().unwrap();
    assert_eq!(op.verb, verb::ROW_PATCH);
    assert_eq!(
        op.data,
        Some(fields(&[("title", text("final"))])),
        "updates are always patches — changed fields ONLY"
    );

    notes
        .update(&TestNote::new("n1", Some("final"), Some("a")))
        .await
        .unwrap();
    assert_eq!(
        store.peek_pending().unwrap().len(),
        2,
        "an unchanged save owes the server nothing"
    );
}

#[tokio::test]
async fn generated_optional_field_can_be_cleared_with_null_patch() {
    let store = store("save-delete-null-patch");
    let transport = StubTransport::new();
    let mut opts = options(engine_directory(), transport.clone());
    opts.schema = ReplicaSchema::new(vec![ReplicaStreamSpec::row("items")]);
    let engine = engine_with(store.clone(), OWNER, opts);
    let items = RowStream::<Item>::new(engine.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![row_set(
                "items",
                "i1",
                Some("PhotoItem"),
                fields(&[
                    ("boardId", text("b1")),
                    ("label", text("idea")),
                    ("rank", text("a")),
                ]),
            )],
            "1:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    items
        .update(&Item::Photo(PhotoItem::new("i1", "b1", "a")))
        .await
        .unwrap();

    let pending = store.peek_pending().unwrap();
    assert_eq!(pending.len(), 1);
    let op = pending.first().unwrap().op().unwrap();
    assert_eq!(op.verb, verb::ROW_PATCH);
    assert_eq!(
        op.data,
        Some(fields(&[("label", ReplicaValue::Null)])),
        "nil is an authored clear, not an omitted diff"
    );

    let Some(Item::Photo(written)) = items.find("i1").unwrap() else {
        panic!("the generated STI projection must survive its client write");
    };
    assert!(
        written.label.is_none(),
        "the client snapshot must clear the old value immediately"
    );
    assert_eq!(
        store
            .peek_snapshot("items", "i1")
            .unwrap()
            .unwrap()
            .data
            .get("label"),
        Some(&ReplicaValue::Null)
    );
}

#[tokio::test]
async fn save_to_readonly_stream_is_refused() {
    let store = store("save-delete-readonly");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    match engine
        .save_row("jobs", "j1", None, &fields(&[("state", text("hacked"))]))
        .await
    {
        Ok(()) => panic!("readonly streams take no client writes"),
        Err(ReplicaError::ReadonlyStream(name)) => assert_eq!(name, "jobs"),
        Err(other) => panic!("wrong refusal: {other}"),
    }
}

#[tokio::test]
async fn delete_of_never_pushed_create_owes_the_server_nothing() {
    let store = store("save-delete-unborn");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("oops"))]))
        .await
        .unwrap();
    engine.delete_row("notes", "n1").await.unwrap();

    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    assert_eq!(
        store.peek_pending().unwrap().len(),
        0,
        "the server never heard n1 — nothing to push, nothing to resurrect"
    );
}

#[tokio::test]
async fn delete_of_synced_row_journals_row_delete() {
    let store = store("save-delete-synced");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "server copy", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();

    engine.delete_row("notes", "n1").await.unwrap();
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    let pending = store.peek_pending().unwrap();
    assert_eq!(pending.len(), 1);
    let op = pending.first().unwrap().op().unwrap();
    assert_eq!(op.verb, verb::ROW_DELETE);
    assert_eq!(op.row_id, "n1");

    // The discriminating half: a row that never existed is ordinary CRUD
    // silence. Asserted HERE, next to the positive, because on its own "pending
    // is empty" is also satisfied by a delete that does nothing at all.
    let deleted_nothing = engine.delete_row("notes", "never-existed").await.unwrap();
    assert!(!deleted_nothing);
    assert_eq!(
        store.peek_pending().unwrap().len(),
        1,
        "an absent row adds no work"
    );
}

#[tokio::test]
async fn local_doc_delete_cascades_its_own_journal() {
    let store = store("save-delete-doc-cascade");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .create_doc("boards", "b1", b"SEED", 7, &ReplicaFields::new(), None)
        .await
        .unwrap();
    engine
        .record_doc_delta("boards", "b1", b"+edit")
        .await
        .unwrap();
    engine.delete_row("boards", "b1").await.unwrap();

    assert!(store.peek_snapshot("boards", "b1").unwrap().is_none());
    assert!(store.peek_doc("boards", "b1").unwrap().is_none());
    assert_eq!(
        store.peek_pending().unwrap().len(),
        0,
        "an unborn doc dies silently — create and deltas discarded, no delete op"
    );
}

// MARK: - Generated model fixture (`Tests/ReplicaManTests/Generated/Item.swift`)
//
// The STI shape codegen emits, hand-carried here because `crates/replica-models`
// is R3's. Only the push-set is encoded (`rankBadge` is pull-only), and an
// authored `nil` encodes as `.null` — the null-patch case's whole point.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ItemLabel {
    Idea,
    Task,
}

impl ItemLabel {
    fn from_raw(raw: &str) -> Option<Self> {
        match raw {
            "idea" => Some(Self::Idea),
            "task" => Some(Self::Task),
            _ => None,
        }
    }

    fn as_raw(self) -> &'static str {
        match self {
            Self::Idea => "idea",
            Self::Task => "task",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ItemCaption {
    Wide,
    Square,
}

impl ItemCaption {
    fn from_raw(raw: &str) -> Option<Self> {
        match raw {
            "wide" => Some(Self::Wide),
            "square" => Some(Self::Square),
            _ => None,
        }
    }

    fn as_raw(self) -> &'static str {
        match self {
            Self::Wide => "wide",
            Self::Square => "square",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct PhotoItem {
    id: String,
    board_id: String,
    label: Option<ItemLabel>,
    rank: String,
    rank_badge: Option<String>,
    staged_preview: Option<String>,
    tags: Option<Vec<String>>,
    caption: Option<ItemCaption>,
    width: Option<i64>,
}

impl PhotoItem {
    fn new(id: &str, board_id: &str, rank: &str) -> Self {
        Self {
            id: id.to_owned(),
            board_id: board_id.to_owned(),
            label: None,
            rank: rank.to_owned(),
            rank_badge: None,
            staged_preview: None,
            tags: None,
            caption: None,
            width: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct TextItem {
    id: String,
    board_id: String,
    label: Option<ItemLabel>,
    rank: String,
    rank_badge: Option<String>,
    staged_preview: Option<String>,
    tags: Option<Vec<String>>,
    body: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum Item {
    Photo(PhotoItem),
    Text(TextItem),
}

fn optional_string(data: &ReplicaFields, key: &str) -> Option<String> {
    data.get(key)
        .and_then(ReplicaValue::as_string)
        .map(str::to_owned)
}

fn optional_tags(data: &ReplicaFields) -> Option<Vec<String>> {
    Some(
        data.get("tags")?
            .items()?
            .iter()
            .filter_map(ReplicaValue::as_string)
            .map(str::to_owned)
            .collect(),
    )
}

/// `value.map { .string($0) } ?? .null` — an omitted optional is an authored
/// clear on the wire.
fn or_null(value: Option<ReplicaValue>) -> ReplicaValue {
    value.unwrap_or(ReplicaValue::Null)
}

impl ReplicaRowModel for Item {
    fn stream_name() -> &'static str {
        "items"
    }

    fn decode(id: &str, row_type: Option<&str>, data: &ReplicaFields) -> Option<Self> {
        let board_id = data.get("boardId").and_then(ReplicaValue::as_string)?;
        let rank = data.get("rank").and_then(ReplicaValue::as_string)?;
        let label = data
            .get("label")
            .and_then(ReplicaValue::as_string)
            .and_then(ItemLabel::from_raw);
        match row_type {
            Some("PhotoItem") => Some(Self::Photo(PhotoItem {
                id: id.to_owned(),
                board_id: board_id.to_owned(),
                label,
                rank: rank.to_owned(),
                rank_badge: optional_string(data, "rankBadge"),
                staged_preview: optional_string(data, "stagedPreview"),
                tags: optional_tags(data),
                caption: data
                    .get("caption")
                    .and_then(ReplicaValue::as_string)
                    .and_then(ItemCaption::from_raw),
                width: data.get("width").and_then(ReplicaValue::as_int),
            })),
            Some("TextItem") => Some(Self::Text(TextItem {
                id: id.to_owned(),
                board_id: board_id.to_owned(),
                label,
                rank: rank.to_owned(),
                rank_badge: optional_string(data, "rankBadge"),
                staged_preview: optional_string(data, "stagedPreview"),
                tags: optional_tags(data),
                body: optional_string(data, "body"),
            })),
            _ => None,
        }
    }

    fn id(&self) -> &str {
        match self {
            Self::Photo(model) => &model.id,
            Self::Text(model) => &model.id,
        }
    }

    fn type_name(&self) -> Option<&str> {
        match self {
            Self::Photo(_) => Some("PhotoItem"),
            Self::Text(_) => Some("TextItem"),
        }
    }

    fn encode(&self) -> ReplicaFields {
        let mut encoded = ReplicaFields::new();
        match self {
            Self::Photo(model) => {
                encoded.insert("boardId".into(), ReplicaValue::string(&model.board_id));
                encoded.insert(
                    "label".into(),
                    or_null(
                        model
                            .label
                            .map(|value| ReplicaValue::string(value.as_raw())),
                    ),
                );
                encoded.insert("rank".into(), ReplicaValue::string(&model.rank));
                encoded.insert(
                    "stagedPreview".into(),
                    or_null(model.staged_preview.as_deref().map(ReplicaValue::string)),
                );
                encoded.insert(
                    "tags".into(),
                    or_null(model.tags.as_ref().map(|tags| {
                        ReplicaValue::Array(tags.iter().map(ReplicaValue::string).collect())
                    })),
                );
                encoded.insert(
                    "caption".into(),
                    or_null(
                        model
                            .caption
                            .map(|value| ReplicaValue::string(value.as_raw())),
                    ),
                );
                encoded.insert(
                    "width".into(),
                    or_null(model.width.map(|value| ReplicaValue::Number(value as f64))),
                );
            }
            Self::Text(model) => {
                encoded.insert("boardId".into(), ReplicaValue::string(&model.board_id));
                encoded.insert(
                    "label".into(),
                    or_null(
                        model
                            .label
                            .map(|value| ReplicaValue::string(value.as_raw())),
                    ),
                );
                encoded.insert("rank".into(), ReplicaValue::string(&model.rank));
                encoded.insert(
                    "stagedPreview".into(),
                    or_null(model.staged_preview.as_deref().map(ReplicaValue::string)),
                );
                encoded.insert(
                    "tags".into(),
                    or_null(model.tags.as_ref().map(|tags| {
                        ReplicaValue::Array(tags.iter().map(ReplicaValue::string).collect())
                    })),
                );
                encoded.insert(
                    "body".into(),
                    or_null(model.body.as_deref().map(ReplicaValue::string)),
                );
            }
        }
        encoded
    }
}

impl ReplicaWritableRowModel for Item {}

#[tokio::test]
async fn delete_after_an_ambiguous_create_reply_keeps_a_durable_delete() {
    let store = store("ambiguous-birth-delete");
    let wire = StubTransport::new();
    let first = engine(store.clone(), wire.clone());
    first
        .save_row("notes", "n", None, &fields(&[("title", text("temporary"))]))
        .await
        .unwrap();
    wire.fail_pushes(true);
    assert!(first.drain().await.is_err());
    drop(first);
    let reopened = engine(store.clone(), StubTransport::new());
    assert!(reopened.delete_row("notes", "n").await.unwrap());
    let verbs: Vec<_> = store
        .peek_pending()
        .unwrap()
        .iter()
        .map(|entry| entry.verb.clone())
        .collect();
    assert_eq!(verbs, [verb::ROW_CREATE, verb::ROW_DELETE]);
}

#[tokio::test]
async fn corrupt_journal_prevents_a_false_blob_garbage_collection_proof() {
    let store = store("corrupt-gc-proof");
    let engine = engine(store.clone(), StubTransport::new());
    engine
        .save_row("notes", "n", None, &fields(&[("blob", text("local:keep"))]))
        .await
        .unwrap();
    store
        .pool()
        .write(|ctx| {
            ctx.tx
                .execute("UPDATE intents SET payload = '{broken'", [])?;
            Ok(())
        })
        .unwrap();
    assert!(
        engine
            .pending_field_strings("notes", &["blob".into()])
            .await
            .is_err()
    );
    assert!(engine.pending_row_ids("notes").await.is_err());
    assert_eq!(store.peek_pending().unwrap().len(), 1);
}

#[tokio::test]
async fn a_birth_keeps_its_snapshot_locally_and_journals_only_authored_fields() {
    let store = store("birth-snapshot");
    let engine = engine(store.clone(), StubTransport::new());
    let authored = fields(&[("title", text("draft"))]);
    let snapshot = fields(&[
        ("title", text("draft")),
        ("createdAt", text("2026-09-30T12:00:00Z")),
    ]);

    engine
        .create_model_row("notes", "n1", None, &authored, &snapshot)
        .await
        .unwrap();

    let row = store.peek_snapshot("notes", "n1").unwrap().expect("the birth is stored");
    assert_eq!(
        row.data.get("createdAt"),
        Some(&text("2026-09-30T12:00:00Z")),
        "the local birth keeps the server-owned value the model supplied"
    );
    let pending = store.peek_pending().unwrap();
    let op = pending.first().expect("the birth is owed").op().unwrap();
    let journaled: Vec<String> = op.data.unwrap_or_default().keys().cloned().collect();
    assert_eq!(journaled, vec!["title".to_owned()], "the journal carries only authored fields");
}
