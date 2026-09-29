use replicaman::ReplicaFields;
use replicaman::testing::doc_snapshot;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::{FutureExt, StreamExt};

use crate::tests::loro_support as fixture;
use crate::{LoroDocument, LoroReplicaCodec};
use replicaman::testing::{ScriptedPull, StubTransport, doc_delta, fields, text};
use replicaman::{Bound, ReplicaDocState, ReplicaEngine, ReplicaError, ReplicaResult};

#[derive(Debug, PartialEq)]
struct BoardState {
    name: Option<String>,
    color: Option<String>,
    version: Vec<u8>,
    can_undo: bool,
    can_redo: bool,
}

impl ReplicaDocState for BoardState {
    type Codec = LoroReplicaCodec;

    fn from_document(
        document: &LoroDocument,
        version: Vec<u8>,
        can_undo: bool,
        can_redo: bool,
    ) -> ReplicaResult<Self> {
        Ok(Self {
            name: fixture::meta(document.document(), "name"),
            color: fixture::meta(document.document(), "color"),
            version,
            can_undo,
            can_redo,
        })
    }
}

async fn create(engine: &ReplicaEngine, id: &str, name: &str) -> Vec<u8> {
    let author = fixture::doc(7, None);
    fixture::set_meta(&author, "name", name);
    let seed = fixture::snapshot(&author);
    assert!(
        engine
            .create_doc(
                "boards",
                id,
                &seed,
                7,
                &fields(&[("name", text(name))]),
                None
            )
            .await
            .unwrap()
    );
    seed
}

fn read(engine: &ReplicaEngine, id: &str) -> Arc<BoardState> {
    engine
        .document_state::<BoardState>("boards", id)
        .unwrap()
        .unwrap()
}

async fn next_state(
    watch: &mut Pin<Box<impl futures::Stream<Item = ReplicaResult<Option<Arc<BoardState>>>>>>,
) -> Option<Arc<BoardState>> {
    tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .expect("document watch did not deliver")
        .expect("document watch ended")
        .expect("document watch failed")
}

fn set(document: &mut LoroDocument, key: &str, value: &str) -> ReplicaResult<()> {
    // Do not commit per field: the engine commits the whole user action.
    document
        .document()
        .get_map("meta")
        .insert(key, value)
        .map_err(|error| ReplicaError::Codec(error.to_string()))
}

#[tokio::test]
async fn working_actions_and_undo_are_visible_before_their_ordered_disk_receipts() {
    let store = fixture::store("working-undo");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    working.edit(|doc| set(doc, "name", "One")).unwrap();
    assert!(Arc::ptr_eq(
        &read(&engine, "b"),
        &engine
            .document_held_state::<BoardState>("boards", "b")
            .unwrap()
            .unwrap()
    ));
    working.edit(|doc| set(doc, "name", "Two")).unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Two"));
    assert_eq!(
        fixture::meta_in_fold(
            &store.peek_doc("boards", "b").unwrap().unwrap().fold,
            "name"
        )
        .as_deref(),
        Some("Seed")
    );
    working.undo().unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("One"));
    working.redo().unwrap();
    let status = working.status();
    assert_eq!(status.pending, 4);
    working.wait_saved(status.accepted).await.unwrap();
    assert_eq!(working.status().pending, 0);
    assert_eq!(
        fixture::meta_in_fold(
            &store.peek_doc("boards", "b").unwrap().unwrap().fold,
            "name"
        )
        .as_deref(),
        Some("Two")
    );
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "color", "Blue"))
        .await
        .unwrap();
    working.undo().unwrap();
    assert_eq!(
        read(&engine, "b").color,
        None,
        "legacy edits use the same undo manager"
    );
    working.flush().await.unwrap();
}

#[tokio::test]
async fn working_saves_keep_failed_heads_and_later_edits_and_refuse_close_until_retry() {
    let store = fixture::store("working-failed-save");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    replicaman::testing::write(&store, |ctx| {
        ctx.tx.execute_batch("CREATE TRIGGER fail_working_save BEFORE UPDATE OF fold ON docs BEGIN SELECT RAISE(ABORT, 'disk unavailable'); END;")?;
        Ok(())
    }).unwrap();
    working.edit(|doc| set(doc, "name", "Unsaved")).unwrap();
    assert!(working.flush().await.is_err());
    working.edit(|doc| set(doc, "color", "Kept")).unwrap();
    assert!(engine.try_seal().await.is_err());
    let owner = engine.owner();
    assert!(engine.try_close().await.is_err());
    assert!(engine.try_retire().await.is_err());
    assert_eq!(
        engine.owner(),
        owner,
        "failed durability cannot release or retire this world"
    );
    assert_eq!(working.status().pending, 2);
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Unsaved"));
    assert_eq!(read(&engine, "b").color.as_deref(), Some("Kept"));
    replicaman::testing::write(&store, |ctx| {
        ctx.tx.execute_batch("DROP TRIGGER fail_working_save;")?;
        Ok(())
    })
    .unwrap();
    working.flush().await.unwrap();
    assert_eq!(working.status().error, None);
    assert_eq!(working.status().pending, 0);
    assert_eq!(
        fixture::meta_in_fold(
            &store.peek_doc("boards", "b").unwrap().unwrap().fold,
            "color"
        )
        .as_deref(),
        Some("Kept")
    );
}

#[tokio::test]
async fn escaped_mutable_handle_cannot_be_persisted_by_a_later_edit_or_reconcile() {
    let store = fixture::store("escaped-edit-handle");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Saved").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    let mut escaped = None;
    working
        .edit(|doc| {
            escaped = Some(doc.document().clone());
            set(doc, "color", "Legitimate")
        })
        .unwrap();
    working.flush().await.unwrap();
    let saved = store.peek_doc("boards", "b").unwrap().unwrap();
    fixture::set_meta(&escaped.unwrap(), "name", "Unjournaled");

    assert!(working.edit(|doc| set(doc, "color", "Next")).is_err());
    assert!(
        working
            .status()
            .error
            .unwrap()
            .contains("outside its edit scope")
    );
    assert!(replicaman::testing::reconcile(&working, Some(&saved), i64::MAX).is_err());
    assert_eq!(
        store.peek_doc("boards", "b").unwrap().unwrap().fold,
        saved.fold
    );
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Saved"));
    assert_eq!(
        fixture::meta_in_fold(&working.recovery_snapshot().unwrap(), "name").as_deref(),
        Some("Unjournaled")
    );
}

#[tokio::test]
async fn reset_preserves_unsaved_work_and_rebuild_requires_a_fresh_peer() {
    let store = fixture::store("working-reset");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    let seed = create(&engine, "b", "Saved").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    replicaman::testing::write(&store, |ctx| {
        ctx.tx.execute_batch("CREATE TRIGGER fail_working_save BEFORE UPDATE OF fold ON docs BEGIN SELECT RAISE(ABORT, 'disk unavailable'); END")?;
        Ok(())
    }).unwrap();
    working.edit(|doc| set(doc, "name", "Unsaved")).unwrap();
    assert!(working.flush().await.is_err());
    assert!(engine.resync_document("boards", "b").await.is_err());
    assert!(
        engine
            .rebuild_document("boards", "b", &seed, 90)
            .await
            .is_err()
    );
    assert!(store.recovery_records(None, 100).unwrap().is_empty());
    assert_eq!(working.status().pending, 1);
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Unsaved"));

    replicaman::testing::write(&store, |ctx| {
        ctx.tx.execute_batch("DROP TRIGGER fail_working_save")?;
        Ok(())
    })
    .unwrap();
    working.flush().await.unwrap();
    let saved = store.peek_doc("boards", "b").unwrap().unwrap();
    assert!(
        engine
            .rebuild_document("boards", "b", &saved.fold, saved.peer)
            .await
            .is_err()
    );
    assert!(store.recovery_records(None, 100).unwrap().is_empty());
    working
        .edit(|doc| set(doc, "color", "Still writable"))
        .unwrap();
    working.flush().await.unwrap();

    engine
        .rebuild_document("boards", "b", &seed, 90)
        .await
        .unwrap();
    assert!(working.edit(|doc| set(doc, "name", "Stale")).is_err());
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Saved"));
    let archive = store.recovery_records(None, 100).unwrap();
    assert_eq!(archive.len(), 1);
    let parts = store.recovery_parts(&archive[0].id, None, 100).unwrap();
    let fold = parts
        .iter()
        .find(|part| part.kind == "document.fold")
        .unwrap();
    let bytes = store
        .recovery_chunk(&archive[0].id, fold, 0, 262_144)
        .unwrap();
    assert_eq!(
        fixture::meta_in_fold(&bytes, "name").as_deref(),
        Some("Unsaved")
    );

    let replacement = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    engine.resync_document("boards", "b").await.unwrap();
    assert!(replacement.edit(|doc| set(doc, "name", "Stale")).is_err());
    assert!(store.peek_doc("boards", "b").unwrap().is_none());
    assert_eq!(store.recovery_records(None, 100).unwrap().len(), 2);
}

#[tokio::test]
async fn an_old_working_lease_cannot_save_into_a_recreated_identical_document() {
    let store = fixture::store("working-incarnation");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    let seed = create(&engine, "b", "Seed").await;
    let old = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    let generation = engine.binding().snapshot().1;
    let incarnation = replicaman::testing::incarnation(&store, "boards", "b")
        .unwrap()
        .unwrap();
    let row = store.peek_doc("boards", "b").unwrap().unwrap();
    let confirmed = old.state::<BoardState>().unwrap().version.clone();

    engine.delete_row("boards", "b").await.unwrap();
    assert!(old.edit(|doc| set(doc, "name", "Stale")).is_err());
    engine
        .create_doc("boards", "b", &seed, row.peer, &ReplicaFields::new(), None)
        .await
        .unwrap();
    let replacement = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &replacement));

    // Identical history passes a causal coverage check. The entity lease must
    // still reject the old queue before importing any bytes into its replacement.
    let author = fixture::doc(91, Some(&seed));
    fixture::set_meta(&author, "name", "Stale queue");
    let payload = Arc::<[u8]>::from(fixture::snapshot(&author));
    let result = replicaman::testing::record_working_delta(
        &engine,
        "boards",
        "b",
        &[payload],
        generation,
        &incarnation,
        row.peer,
        &confirmed,
        replicaman::ReplicaLane::Bulk,
    )
    .await;
    assert!(matches!(result, Err(ReplicaError::Codec(_))));
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Seed"));
    assert_eq!(
        fixture::meta_in_fold(
            &store.peek_doc("boards", "b").unwrap().unwrap().fold,
            "name"
        )
        .as_deref(),
        Some("Seed")
    );
}

#[tokio::test]
async fn working_reader_and_structural_edits_do_not_wait_for_a_blocked_sqlite_writer() {
    let store = fixture::store("working-blocked-writer");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    let (entered, ready) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let writer_store = store.clone();
    let writer = std::thread::spawn(move || {
        replicaman::testing::write(&writer_store, |_| {
            entered.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(3)).unwrap();
            Ok(())
        })
        .unwrap()
    });
    ready.recv_timeout(Duration::from_secs(3)).unwrap();
    let start = std::time::Instant::now();
    working
        .edit(|doc| {
            doc.write_entry_field(
                "tracks",
                "new-track",
                "name",
                &replicaman::DocumentValue::String("Track".into()),
            )?;
            set(doc, "name", "Local")
        })
        .unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Local"));
    working.undo().unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Seed"));
    let elapsed = start.elapsed();
    release.send(()).unwrap();
    writer.join().unwrap();
    assert!(
        elapsed < Duration::from_millis(250),
        "memory edit/read/undo took {elapsed:?}"
    );
    working.flush().await.unwrap();
}

#[tokio::test]
async fn working_remote_merge_preserves_pending_local_actions_and_collaborative_undo() {
    let store = fixture::store("working-remote");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    let seed = create(&engine, "b", "Seed").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    working.edit(|doc| set(doc, "name", "Local")).unwrap();
    let remote = fixture::doc(998, Some(&seed));
    let payload = fixture::edit_payload(&remote, "color", "Remote");
    engine
        .record_doc_delta("boards", "b", &payload)
        .await
        .unwrap();
    engine.refresh_working_document("boards", "b").unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Local"));
    assert_eq!(read(&engine, "b").color.as_deref(), Some("Remote"));
    working.undo().unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Seed"));
    assert_eq!(read(&engine, "b").color.as_deref(), Some("Remote"));
    working.flush().await.unwrap();
}

#[tokio::test]
async fn working_partial_refusal_never_publishes_or_journals_half_an_action() {
    let store = fixture::store("working-refused-edit");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    working.edit(|doc| set(doc, "name", "Kept")).unwrap();
    assert!(
        working
            .edit(|doc| {
                set(doc, "name", "Partial")?;
                Err(ReplicaError::Codec("refused".into()))
            })
            .is_err()
    );
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Kept"));
    assert_eq!(working.status().pending, 1);
    working.flush().await.unwrap();
    assert_eq!(
        fixture::meta_in_fold(
            &store.peek_doc("boards", "b").unwrap().unwrap().fold,
            "name"
        )
        .as_deref(),
        Some("Kept")
    );
}

#[tokio::test]
async fn working_replacement_adopts_clean_state_but_never_replays_unsaved_ops_over_a_reset() {
    for pending in [false, true] {
        let store = fixture::store(if pending {
            "working-reset-pending"
        } else {
            "working-reset-clean"
        });
        let engine = fixture::engine(store.clone(), StubTransport::new());
        create(&engine, "b", "Seed").await;
        let working = engine
            .open_working_document::<BoardState>("boards", "b")
            .unwrap();
        working.edit(|doc| set(doc, "name", "Local")).unwrap();
        if !pending {
            working.flush().await.unwrap();
        }
        let authority = fixture::doc(98, None);
        fixture::set_meta(&authority, "name", "Replacement");
        let replacement = fixture::snapshot(&authority);
        replicaman::testing::write(&store, |ctx| {
            store.update_doc(ctx, "boards", "b", Some(&replacement), None)
        })
        .unwrap();
        let result = engine.refresh_working_document("boards", "b");
        if pending {
            assert!(result.is_err());
            assert_eq!(read(&engine, "b").name.as_deref(), Some("Local"));
            assert!(working.flush().await.is_err());
            assert_eq!(working.status().pending, 1);
            assert_eq!(
                fixture::meta_in_fold(&working.recovery_snapshot().unwrap(), "name").as_deref(),
                Some("Local")
            );
        } else {
            result.unwrap();
            assert_eq!(read(&engine, "b").name.as_deref(), Some("Replacement"));
            assert!(
                !read(&engine, "b").can_undo,
                "old undo cannot resurrect a replaced baseline"
            );
            working.edit(|doc| set(doc, "color", "Recovered")).unwrap();
            working.flush().await.unwrap();
        }
        assert_eq!(
            fixture::meta_in_fold(
                &store.peek_doc("boards", "b").unwrap().unwrap().fold,
                "name"
            )
            .as_deref(),
            Some("Replacement")
        );
    }
}

#[tokio::test]
async fn working_seal_rejects_new_authors_and_reopen_readmits_the_same_live_document() {
    let store = fixture::store("working-seal-open");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    create(&engine, "c", "Cold").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    working
        .edit(|doc| set(doc, "name", "Saved on seal"))
        .unwrap();
    engine.try_seal().await.unwrap();
    assert_eq!(working.status().pending, 0);
    assert!(working.edit(|doc| set(doc, "name", "Refused")).is_err());
    assert!(
        engine
            .open_working_document::<BoardState>("boards", "c")
            .is_err()
    );
    engine.open(engine.owner().unwrap()).await.unwrap();
    assert!(Arc::ptr_eq(
        &working,
        &engine
            .open_working_document::<BoardState>("boards", "b")
            .unwrap()
    ));
    working.edit(|doc| set(doc, "color", "Again")).unwrap();
    working.flush().await.unwrap();
}

#[tokio::test]
async fn working_panic_preserves_accepted_state_and_never_saves_partial_mutation() {
    let store = fixture::store("working-panic");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    let working = engine
        .open_working_document::<BoardState>("boards", "b")
        .unwrap();
    working.edit(|doc| set(doc, "name", "Accepted")).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        working.edit(|doc| {
            set(doc, "color", "Partial")?;
            panic!("injected editor panic");
        })
    }));
    assert!(result.is_err());
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Accepted"));
    assert_eq!(read(&engine, "b").color, None);
    working.edit(|doc| set(doc, "color", "Later")).unwrap();
    working.flush().await.unwrap();
    assert_eq!(
        fixture::meta_in_fold(
            &store.peek_doc("boards", "b").unwrap().unwrap().fold,
            "color"
        )
        .as_deref(),
        Some("Later")
    );
}

#[tokio::test]
async fn first_read_is_sync_warm_state_is_shared_and_peek_does_not_open() {
    let store = fixture::store("live-warm");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "b")
            .unwrap()
            .is_none()
    );
    let first = read(&engine, "b");
    assert_eq!(first.name.as_deref(), Some("Seed"));
    assert!(!first.can_undo, "seed import must not be a local undo step");
    assert!(Arc::ptr_eq(&first, &read(&engine, "b")));
    assert!(Arc::ptr_eq(
        &first,
        &engine
            .document_held_state::<BoardState>("boards", "b")
            .unwrap()
            .unwrap()
    ));
    engine.drain().await.unwrap();
    assert!(
        Arc::ptr_eq(&first, &read(&engine, "b")),
        "ack-only writes must not rematerialize state"
    );
}

#[tokio::test]
async fn local_action_is_durable_and_noop_does_not_journal_or_change_sequence() {
    let store = fixture::store("live-noop");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    engine.drain().await.unwrap();
    let initial = read(&engine, "b");
    let sequence = store
        .pool()
        .read(|db| store.change_sequence(db, "boards"))
        .unwrap();
    assert!(
        !engine
            .update_document::<LoroReplicaCodec>("boards", "b", |_| Ok(()))
            .await
            .unwrap()
    );
    assert!(store.peek_pending().unwrap().is_empty());
    assert_eq!(
        sequence,
        store
            .pool()
            .read(|db| store.change_sequence(db, "boards"))
            .unwrap()
    );
    assert!(Arc::ptr_eq(&initial, &read(&engine, "b")));
    assert!(
        engine
            .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "name", "Edited"))
            .await
            .unwrap()
    );
    let state = read(&engine, "b");
    assert_eq!(state.name.as_deref(), Some("Edited"));
    assert!(state.can_undo);
    let row = store.peek_doc("boards", "b").unwrap().unwrap();
    assert_eq!(
        fixture::meta_in_fold(&row.fold, "name").as_deref(),
        Some("Edited")
    );
    assert_eq!(store.peek_pending().unwrap().len(), 1);
}

#[tokio::test]
async fn one_action_one_undo_step_and_remote_changes_survive_undo() {
    let store = fixture::store("live-undo");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());
    let seed = create(&engine, "b", "Seed").await;
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

    engine.drain().await.unwrap();
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| {
            set(doc, "name", "First")?;
            set(doc, "color", "Local")
        })
        .await
        .unwrap();
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "name", "Second"))
        .await
        .unwrap();
    let remote = fixture::doc(999, Some(&seed));
    let payload = fixture::edit_payload(&remote, "remote", "Kept");
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_delta(
                "boards",
                "b",
                1,
                LoroReplicaCodec::CODEC_NAME,
                &payload,
            )],
            "1",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    assert!(
        engine
            .undo_document::<LoroReplicaCodec>("boards", "b")
            .await
            .unwrap()
    );
    let first = read(&engine, "b");
    assert_eq!(first.name.as_deref(), Some("First"));
    assert_eq!(first.color.as_deref(), Some("Local"));
    assert!(first.can_redo);
    assert!(
        engine
            .undo_document::<LoroReplicaCodec>("boards", "b")
            .await
            .unwrap()
    );
    let original = read(&engine, "b");
    assert_eq!(original.name.as_deref(), Some("Seed"));
    assert_eq!(
        original.color, None,
        "both fields in the first action must undo together"
    );
    assert!(!original.can_undo);
    assert!(
        !engine
            .undo_document::<LoroReplicaCodec>("boards", "b")
            .await
            .unwrap()
    );
    assert!(
        engine
            .redo_document::<LoroReplicaCodec>("boards", "b")
            .await
            .unwrap()
    );
    assert_eq!(read(&engine, "b").name.as_deref(), Some("First"));
    let row = store.peek_doc("boards", "b").unwrap().unwrap();
    assert_eq!(
        fixture::meta_in_fold(&row.fold, "remote").as_deref(),
        Some("Kept")
    );
}

#[tokio::test]
async fn closure_error_and_panic_cannot_publish_an_uncommitted_edit() {
    let store = fixture::store("live-failed-body");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    let seed = create(&engine, "b", "Seed").await;
    engine.drain().await.unwrap();
    let failure = engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| {
            set(doc, "name", "Never committed")?;
            Err(ReplicaError::Codec("body failed".into()))
        })
        .await;
    assert!(failure.is_err());
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Seed"));
    let panicked = std::panic::AssertUnwindSafe(engine.update_document::<LoroReplicaCodec>(
        "boards",
        "b",
        |doc| {
            set(doc, "name", "Panicked")?;
            panic!("edit panic")
        },
    ))
    .catch_unwind()
    .await;
    assert!(panicked.is_err());
    assert_eq!(store.peek_doc("boards", "b").unwrap().unwrap().fold, seed);
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Seed"));
    assert!(store.peek_pending().unwrap().is_empty());
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "name", "Recovered"))
        .await
        .unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Recovered"));
}

#[tokio::test]
async fn journal_failure_rolls_back_fold_and_evicts_dirty_live_copy() {
    let store = fixture::store("live-failed-commit");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    let seed = create(&engine, "b", "Seed").await;
    engine.drain().await.unwrap();
    read(&engine, "b");
    replicaman::testing::write(&store, |ctx| {
        ctx.tx.execute_batch("CREATE TRIGGER fail_document_journal BEFORE INSERT ON intents BEGIN SELECT RAISE(ABORT, 'journal unavailable'); END;")?;
        Ok(())
    }).unwrap();
    assert!(
        engine
            .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "name", "Not saved"))
            .await
            .is_err()
    );
    assert_eq!(store.peek_doc("boards", "b").unwrap().unwrap().fold, seed);
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Seed"));
    assert!(store.peek_pending().unwrap().is_empty());
}

#[tokio::test]
async fn refused_registry_entry_rolls_back_the_whole_edit_and_cannot_leak_into_the_next_save() {
    let store = fixture::store("refused-registry");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    let author = fixture::doc(7, None);
    fixture::set_meta(&author, "name", "Before");
    author
        .get_map("items")
        .insert("broken", "peer value")
        .unwrap();
    let seed = fixture::snapshot(&author);
    engine
        .create_doc(
            "boards",
            "b",
            &seed,
            100,
            &fields(&[("name", text("Before"))]),
            None,
        )
        .await
        .unwrap();
    engine.drain().await.unwrap();
    let before = read(&engine, "b");
    let fold = store.peek_doc("boards", "b").unwrap().unwrap().fold;

    let result = engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| {
            set(doc, "name", "Unsaved")?;
            doc.write_registry(
                "items",
                &[replicaman::DocumentEntry::new(
                    "broken",
                    std::collections::BTreeMap::from([(
                        "title".into(),
                        replicaman::DocumentValue::String("Lost".into()),
                    )]),
                )],
                None,
            )
        })
        .await;
    assert!(
        result.is_err(),
        "a refused entry must not report a successful save"
    );
    assert_eq!(read(&engine, "b").name, before.name);
    assert_eq!(read(&engine, "b").version, before.version);
    assert_eq!(store.peek_doc("boards", "b").unwrap().unwrap().fold, fold);
    assert!(store.peek_pending().unwrap().is_empty());

    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "color", "blue"))
        .await
        .unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Before"));
    let saved = fixture::doc(
        200,
        Some(&store.peek_doc("boards", "b").unwrap().unwrap().fold),
    );
    assert_eq!(
        saved
            .get_map("items")
            .get("broken")
            .unwrap()
            .get_deep_value(),
        loro::LoroValue::String("peer value".into())
    );
}

#[tokio::test]
async fn failed_checkpoint_does_not_leak_remote_changes_into_live_state() {
    let store = fixture::store("live-failed-pull");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());
    let seed = create(&engine, "b", "Seed").await;
    let initial = read(&engine, "b");
    let remote = fixture::doc(999, Some(&seed));
    let payload = fixture::edit_payload(&remote, "color", "Remote");
    transport.queue_pull(
        "default",
        ScriptedPull::new(
            vec![doc_delta(
                "boards",
                "b",
                1,
                LoroReplicaCodec::CODEC_NAME,
                &payload,
            )],
            "1",
            false,
        ),
    );
    engine
        .set_checkpoint_fault(Some(Arc::new(|| {
            Err(ReplicaError::Storage("checkpoint failed".into()))
        })))
        .await;
    assert!(engine.pull_once("default").await.is_err());
    assert!(Arc::ptr_eq(&initial, &read(&engine, "b")));
    assert_eq!(store.peek_doc("boards", "b").unwrap().unwrap().fold, seed);
}

#[tokio::test]
async fn corrupt_fold_is_preserved_and_durable_replacement_does_not_keep_removed_history() {
    let store = fixture::store("live-corrupt");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    let seed = create(&engine, "b", "Seed").await;
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "name", "Newer"))
        .await
        .unwrap();
    assert_eq!(read(&engine, "b").name.as_deref(), Some("Newer"));
    replicaman::testing::write(&store, |ctx| {
        store.update_doc(ctx, "boards", "b", Some(&seed), None)
    })
    .unwrap();
    assert_eq!(
        read(&engine, "b").name.as_deref(),
        Some("Seed"),
        "a reset must not retain removed local history"
    );
    replicaman::testing::write(&store, |ctx| {
        store.update_doc(ctx, "boards", "b", Some(b"broken fold"), None)
    })
    .unwrap();
    assert!(engine.document_state::<BoardState>("boards", "b").is_err());
    let stored = store.peek_doc("boards", "b").unwrap().unwrap();
    assert_eq!(stored.fold, b"broken fold");
    assert_eq!(stored.peer, 7);
}

#[tokio::test]
async fn watch_delivers_local_and_remote_changes_and_close_without_duplicates() {
    let store = fixture::store("live-watch");
    let transport = StubTransport::new();
    let engine = fixture::engine(store.clone(), transport.clone());
    let seed = create(&engine, "b", "Seed").await;
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

    let mut watch = Box::pin(engine.watch_document::<BoardState>("boards", "b", true));
    assert_eq!(
        next_state(&mut watch).await.unwrap().name.as_deref(),
        Some("Seed")
    );
    assert!(matches!(futures::poll!(watch.next()), Poll::Pending));
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "name", "Local"))
        .await
        .unwrap();
    assert_eq!(
        next_state(&mut watch).await.unwrap().name.as_deref(),
        Some("Local")
    );
    let remote = fixture::doc(999, Some(&seed));
    let payload = fixture::edit_payload(&remote, "color", "Remote");
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_delta(
                "boards",
                "b",
                1,
                LoroReplicaCodec::CODEC_NAME,
                &payload,
            )],
            "1",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        next_state(&mut watch).await.unwrap().color.as_deref(),
        Some("Remote")
    );
    assert!(matches!(futures::poll!(watch.next()), Poll::Pending));
    engine.close().await.unwrap();
    assert!(next_state(&mut watch).await.is_none());
    assert!(matches!(futures::poll!(watch.next()), Poll::Pending));
}

#[tokio::test]
async fn lru_counts_unpinned_documents_and_pins_are_reference_counted() {
    let store = fixture::store("live-pins");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "pinned", "Pinned").await;
    let first = engine.pin_document("boards", "pinned");
    let second = engine.pin_document("boards", "pinned");
    read(&engine, "pinned");
    drop(first);
    for n in 0..33 {
        let id = format!("b{n}");
        create(&engine, &id, &id).await;
        read(&engine, &id);
    }
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "pinned")
            .unwrap()
            .is_some()
    );
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "b0")
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "b1")
            .unwrap()
            .is_some(),
        "32 unpinned entries plus pins are allowed"
    );
    drop(second);
    for n in 1..33 {
        read(&engine, &format!("b{n}"));
    }
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "pinned")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn unknown_document_edit_is_an_explicit_error_not_a_create() {
    let store = fixture::store("live-unknown");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    assert_eq!(
        engine
            .update_document::<LoroReplicaCodec>("boards", "missing", |_| panic!("must not run"))
            .await,
        Err(ReplicaError::UnknownDocument {
            stream: "boards".into(),
            id: "missing".into()
        })
    );
    assert!(
        engine
            .document_state::<BoardState>("boards", "missing")
            .unwrap()
            .is_none()
    );
    assert!(store.peek_pending().unwrap().is_empty());
    assert!(
        engine
            .update_document::<LoroReplicaCodec>("notes", "missing", |_| Ok(()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn old_owner_pin_cannot_release_a_new_owner_document_with_the_same_id() {
    let first = fixture::store("live-owner-one");
    let second = fixture::store("live-owner-two");
    let engine = fixture::engine(first.clone(), StubTransport::new());
    create(&engine, "shared", "First owner's project").await;
    let old_pin = engine.pin_document("boards", "shared");
    read(&engine, "shared");
    engine.binding().replace(Bound {
        owner: 99,
        store: second.clone(),
        path: second.path().to_owned(),
    });
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "shared")
            .unwrap()
            .is_none()
    );
    create(&engine, "shared", "Second owner's project").await;
    let _new_pin = engine.pin_document("boards", "shared");
    assert_eq!(
        read(&engine, "shared").name.as_deref(),
        Some("Second owner's project")
    );
    drop(old_pin);
    for n in 0..33 {
        let id = format!("second-{n}");
        create(&engine, &id, &id).await;
        read(&engine, &id);
    }
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "shared")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fixture::meta_in_fold(
            &first.peek_doc("boards", "shared").unwrap().unwrap().fold,
            "name"
        )
        .as_deref(),
        Some("First owner's project")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_edit_closures_see_the_previous_committed_edit() {
    let store = fixture::store("live-concurrent-edits");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "").await;
    let mut tasks = Vec::new();
    for letter in ["a", "b"] {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..20 {
                engine
                    .update_document::<LoroReplicaCodec>("boards", "b", |doc| {
                        let mut name = fixture::meta(doc.document(), "name").unwrap();
                        name.push_str(letter);
                        set(doc, "name", &name)
                    })
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
    let state = read(&engine, "b");
    let name = state.name.as_ref().unwrap();
    assert_eq!(name.len(), 40);
    assert_eq!(name.chars().filter(|letter| *letter == 'a').count(), 20);
    assert_eq!(name.chars().filter(|letter| *letter == 'b').count(), 20);
}

#[tokio::test]
async fn change_only_watch_ignores_baseline_and_unrelated_documents() {
    let store = fixture::store("live-change-only");
    let engine = fixture::engine(store.clone(), StubTransport::new());
    create(&engine, "b", "Seed").await;
    let mut watch = Box::pin(engine.watch_document::<BoardState>("boards", "b", false));
    assert!(matches!(futures::poll!(watch.next()), Poll::Pending));
    create(&engine, "unrelated", "Other").await;
    assert!(matches!(futures::poll!(watch.next()), Poll::Pending));
    engine
        .update_document::<LoroReplicaCodec>("boards", "b", |doc| set(doc, "name", "Change"))
        .await
        .unwrap();
    assert_eq!(
        next_state(&mut watch).await.unwrap().name.as_deref(),
        Some("Change")
    );
    engine.delete_row("boards", "b").await.unwrap();
    assert!(next_state(&mut watch).await.is_none());
    assert!(
        engine
            .document_held_state::<BoardState>("boards", "b")
            .unwrap()
            .is_none()
    );
}
