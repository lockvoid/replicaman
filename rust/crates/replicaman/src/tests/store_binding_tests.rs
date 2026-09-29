//! The store / journal / binding plane, transliterated file by file.
//!
//! Upstream exercises almost all of it THROUGH the engine — the store is not a
//! product on its own — so these cases live beside the engine's, exactly as
//! they do in `ReplicaManTests`.
//!
//! - `JournalAddressTests` (2 of 3 — `testAnOlderStoreIsUpgradedAndBackfilled`
//!   needs no engine and lands in `store_migration_tests.rs`)
//! - `ChangeSequenceTests` (3 of 4 — same, for
//!   `testOpeningALegacyStoreAddsMetadataWithoutTouchingTheJournal`)
//! - `RowReadCacheTests` (6)
//! - `RowReuseTests` (5)
//! - `OwnerBindingTests` (10)

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use parking_lot::Mutex;
use rusqlite::types::Value as SqlValue;

use crate::error::{ReplicaError, ReplicaResult};
use crate::models::{ReplicaCreateStamp, ReplicaRowModel, ReplicaWritableRowModel, RowStream};
use crate::schema::ReplicaLane;
use crate::store::ReplicaStateStore;
use crate::tests::support::*;
use crate::value::{ReplicaFields, ReplicaValue};

#[test]
fn consumer_reads_are_read_only_and_hold_one_snapshot() {
    let store = store("read-boundary");
    assert!(
        store
            .pool()
            .read(|db| {
                db.execute("DELETE FROM snapshots", [])?;
                Ok(())
            })
            .is_err()
    );
    store
        .pool()
        .read(|db| {
            let before: i64 =
                db.query_row("SELECT count(*) FROM snapshots", [], |row| row.get(0))?;
            store.pool().write(|ctx| {
                store.upsert_snapshot(
                    ctx,
                    "notes",
                    "between",
                    "user",
                    None,
                    &fields(&[("title", text("new"))]),
                )
            })?;
            let after: i64 =
                db.query_row("SELECT count(*) FROM snapshots", [], |row| row.get(0))?;
            assert_eq!(
                before, after,
                "one read closure must observe one SQLite snapshot"
            );
            Ok(())
        })
        .unwrap();
    assert!(store.peek_snapshot("notes", "between").unwrap().is_some());
}

// MARK: - Local fixtures

/// `ChangeSequenceTests.sequence(_:stream:)` — the local watch counter read
/// raw, so the assertion does not go through the same accessor the engine
/// writes with.
fn sequence(store: &ReplicaStateStore, stream: &str) -> i64 {
    store
        .pool()
        .read(|db| {
            Ok(db
                .query_row(
                    "SELECT change_seq FROM stream_meta WHERE stream = ?",
                    [stream],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0))
        })
        .expect("read the stream's change sequence")
}

/// `RowReadCacheTests.sqlIDs(_:stream:equals:)` — the SQL oracle the warm,
/// in-memory predicate path is graded against. Same shape as upstream: one
/// `json_extract(...) = ?` per key in sorted order, with `.null`/`.array`/
/// `.object` binding SQL NULL (so they can never match).
fn sql_ids(store: &ReplicaStateStore, stream: &str, equals: &ReplicaFields) -> Vec<String> {
    let mut sql = "SELECT row_id FROM snapshots WHERE stream = ?".to_owned();
    let mut arguments: Vec<SqlValue> = vec![SqlValue::Text(stream.to_owned())];
    // `ReplicaFields` is a BTreeMap, so iteration IS upstream's `sorted(by:)`.
    for (key, value) in equals {
        sql.push_str(" AND json_extract(data, ?) = ?");
        arguments.push(SqlValue::Text(format!("$.{key}")));
        arguments.push(match value {
            ReplicaValue::String(string) => SqlValue::Text(string.clone()),
            ReplicaValue::Integer(integer) => SqlValue::Integer(*integer),
            ReplicaValue::Number(number) if *number == number.round() => {
                SqlValue::Integer(*number as i64)
            }
            ReplicaValue::Number(number) => SqlValue::Real(*number),
            ReplicaValue::Bool(flag) => SqlValue::Integer(i64::from(*flag)),
            ReplicaValue::Null | ReplicaValue::Array(_) | ReplicaValue::Object(_) => SqlValue::Null,
        });
    }
    sql.push_str(" ORDER BY row_id");
    store
        .pool()
        .read(|db| {
            let rows = db
                .prepare(&sql)?
                .query_map(rusqlite::params_from_iter(arguments), |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .expect("the SQL oracle query")
}

/// Every value an observation delivered, in order — what proves a re-arm
/// happened and that no post-retirement picture carries the old world.
/// Upstream's `Picture` / `TitlePictures` actors, which differ only in what
/// they project out of the models.
struct Log<T> {
    values: Mutex<Vec<T>>,
}

impl<T: Clone> Log<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            values: Mutex::new(Vec::new()),
        })
    }

    fn record(&self, value: T) {
        self.values.lock().push(value);
    }

    fn len(&self) -> usize {
        self.values.lock().len()
    }

    fn last(&self) -> Option<T> {
        self.values.lock().last().cloned()
    }

    fn values(&self) -> Vec<T> {
        self.values.lock().clone()
    }
}

/// `OwnerBindingTests.assertNoOwner` — a closed engine admits nothing, and it
/// refuses with `noOwner` specifically (a storage error would mean the write
/// reached a store).
#[track_caller]
fn assert_no_owner<T: std::fmt::Debug>(outcome: ReplicaResult<T>) {
    match outcome {
        Ok(value) => panic!("an ownerless engine admitted a write: {value:?}"),
        Err(ReplicaError::NoOwner) => {}
        Err(error) => panic!("unexpected closed-engine error: {error}"),
    }
}

/// Upstream resets a type-level decode counter in `setUp`, which XCTest can do
/// because a test class runs its cases serially. `cargo test` runs the cases in
/// this file in PARALLEL threads of one process, so one shared counter would
/// race. Each case therefore gets its OWN model type: its own static counter
/// and its own `TypeId` — and `TypeId` is the row cache's materialization key,
/// so this is exactly the isolation XCTest hands out for free.
///
/// `RowReadCacheTests.CountingNote`: counts decodes, optionally sleeps inside
/// one (the concurrent-cold-read probe), and refuses any STI type.
macro_rules! counting_note {
    ($name:ident) => {
        counting_note!($name, 0);
    };
    ($name:ident, $decode_delay_millis:expr) => {
        #[derive(Clone, Debug, PartialEq)]
        struct $name {
            id: String,
            title: Option<String>,
        }

        #[allow(dead_code)]
        impl $name {
            fn counter() -> &'static AtomicUsize {
                static COUNT: AtomicUsize = AtomicUsize::new(0);
                &COUNT
            }

            fn decodes() -> usize {
                Self::counter().load(Ordering::SeqCst)
            }

            fn titles(rows: &[Self]) -> Vec<Option<String>> {
                rows.iter().map(|row| row.title.clone()).collect()
            }

            fn ids(rows: &[Self]) -> Vec<String> {
                rows.iter().map(|row| row.id.clone()).collect()
            }
        }

        impl ReplicaRowModel for $name {
            fn stream_name() -> &'static str {
                "notes"
            }

            fn decode(id: &str, row_type: Option<&str>, data: &ReplicaFields) -> Option<Self> {
                Self::counter().fetch_add(1, Ordering::SeqCst);
                let delay = Duration::from_millis($decode_delay_millis);
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                if row_type.is_some() {
                    return None;
                }
                Some(Self {
                    id: id.to_owned(),
                    title: data
                        .get("title")
                        .and_then(ReplicaValue::as_string)
                        .map(str::to_owned),
                })
            }

            fn id(&self) -> &str {
                &self.id
            }

            fn type_name(&self) -> Option<&str> {
                None
            }

            fn encode(&self) -> ReplicaFields {
                let mut data = ReplicaFields::new();
                if let Some(title) = &self.title {
                    data.insert("title".into(), ReplicaValue::string(title));
                }
                data
            }
        }

        impl ReplicaWritableRowModel for $name {}
    };
}

/// `RowReuseTests.ReusableNote`: counts decodes and switches on the STI `type`,
/// which is what makes a donor reuse across a type flip observable.
macro_rules! reusable_note {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq)]
        struct $name {
            id: String,
            title: Option<String>,
            special: bool,
        }

        #[allow(dead_code)]
        impl $name {
            fn counter() -> &'static AtomicUsize {
                static COUNT: AtomicUsize = AtomicUsize::new(0);
                &COUNT
            }

            fn decodes() -> usize {
                Self::counter().load(Ordering::SeqCst)
            }

            fn titles(rows: &[Self]) -> Vec<Option<String>> {
                rows.iter().map(|row| row.title.clone()).collect()
            }

            fn ids(rows: &[Self]) -> Vec<String> {
                rows.iter().map(|row| row.id.clone()).collect()
            }
        }

        impl ReplicaRowModel for $name {
            fn stream_name() -> &'static str {
                "notes"
            }

            fn decode(id: &str, row_type: Option<&str>, data: &ReplicaFields) -> Option<Self> {
                Self::counter().fetch_add(1, Ordering::SeqCst);
                Some(Self {
                    id: id.to_owned(),
                    title: data
                        .get("title")
                        .and_then(ReplicaValue::as_string)
                        .map(str::to_owned),
                    special: row_type == Some("Special"),
                })
            }

            fn id(&self) -> &str {
                &self.id
            }

            fn type_name(&self) -> Option<&str> {
                if self.special { Some("Special") } else { None }
            }

            fn encode(&self) -> ReplicaFields {
                let mut data = ReplicaFields::new();
                if let Some(title) = &self.title {
                    data.insert("title".into(), ReplicaValue::string(title));
                }
                data
            }
        }

        impl ReplicaWritableRowModel for $name {}
    };
}

// MARK: - JournalAddressTests (2 of 3)
//
// The journal's address (stream, row_id) is a COLUMN, and the "was this row
// ever born on the server?" question reads it WITH THE VERB. Both pins here
// were holes: blanking the verb filter in `entries_addressing` passed the whole
// suite, and every caller of it takes a destructive branch on the answer — a
// pending PATCH counted as a birth silently swallows a delete the server needed
// to hear, and silently refuses a create.

/// A row the server already knows, with a pending patch owed for it. The patch
/// is not a birth: deleting must still journal `row.delete`.
#[tokio::test]
async fn a_pending_patch_is_not_a_birth_so_the_delete_still_ships() {
    let store = store("journal-address-delete");
    let transport = StubTransport::new();
    accept_all(&transport);
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("born"))]))
        .await
        .unwrap();
    engine.drain_lane(ReplicaLane::Bulk).await.unwrap();
    assert!(
        store.pending_ops().unwrap().is_empty(),
        "the create settled — the server has heard of n1"
    );

    // A patch, still owed. The row exists server-side.
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("edited"))]))
        .await
        .unwrap();
    let deleted = engine.delete_row("notes", "n1").await.unwrap();

    assert!(
        deleted,
        "a known row's delete is real work, not local silence"
    );
    let verbs: Vec<String> = store
        .pending_ops()
        .unwrap()
        .iter()
        .map(|entry| entry.op().unwrap().verb)
        .collect();
    assert_eq!(
        verbs,
        ["row.patch", "row.delete"],
        "the row is born server-side: its owed patch keeps its place, and the delete is \
         appended behind it — nothing is discarded as never-born"
    );
}

fn owe_a_patch(store: &ReplicaStateStore, stream: &str, row_id: &str) {
    let op = crate::wire::ReplicaOp::new("p1", crate::wire::verb::ROW_PATCH, stream, row_id);
    let payload = op.to_json().unwrap();
    store
        .pool()
        .write(|ctx| {
            store.enqueue(
                ctx,
                &op.id,
                &op.verb,
                stream,
                row_id,
                &payload,
                None,
                ReplicaLane::Bulk,
            )
        })
        .unwrap();
}

/// Same confusion at the other caller: a pending patch must not make the
/// document create think the row is already born.
#[tokio::test]
async fn a_pending_patch_does_not_block_a_document_create() {
    let store = store("journal-address-create");
    let transport = StubTransport::new();
    accept_all(&transport);
    let engine = engine(store.clone(), transport.clone());

    owe_a_patch(&store, "boards", "shared-id");

    let data = ReplicaFields::new();
    let inserted = engine
        .create_doc("boards", "shared-id", b"SEED", 7, &data, None)
        .await
        .unwrap();
    assert!(inserted, "no board has ever been born under this id");
}

// MARK: - ChangeSequenceTests (3 of 4)

#[tokio::test]
async fn checkpoint_reset_and_rollback_move_only_committed_stream_sequences() {
    let store = store("change-seq-checkpoint");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();
    let after_checkpoint = sequence(&store, "notes");
    let untouched_asset_sequence = sequence(&store, "assets");
    engine.reset_cursors().await.unwrap();

    transport.queue_pull("user", ScriptedPull::new(Vec::new(), "6:", false));
    engine.pull_once("user").await.unwrap();
    let after_reset = sequence(&store, "notes");
    assert!(after_reset > after_checkpoint);
    assert_eq!(sequence(&store, "assets"), untouched_asset_sequence);

    engine
        .set_checkpoint_fault(Some(Arc::new(|| {
            Err(ReplicaError::Storage("checkpoint fault".into()))
        })))
        .await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n2", "rolled back", None)], "7:", false),
    );
    assert_eq!(
        engine.pull_once("user").await,
        Err(ReplicaError::Storage("checkpoint fault".into())),
        "the faulted checkpoint unexpectedly committed"
    );
    assert_eq!(sequence(&store, "notes"), after_reset);

    engine.set_checkpoint_fault(None).await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n2", "restored", None)], "7:", false),
    );
    engine.pull_once("user").await.unwrap();
    assert!(sequence(&store, "notes") > after_reset);
}

#[tokio::test]
async fn local_delete_and_rejected_create_revert_advance_the_sequence() {
    let store = store("change-seq-local");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("one"))]))
        .await
        .unwrap();
    let after_create = sequence(&store, "notes");
    engine.delete_row("notes", "n1").await.unwrap();
    let after_delete = sequence(&store, "notes");
    assert!(after_delete > after_create);

    reject_all(&transport, "refused");
    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("two"))]))
        .await
        .unwrap();
    let before_revert = sequence(&store, "notes");
    engine.drain().await.unwrap();

    assert!(store.peek_snapshot("notes", "n2").unwrap().is_none());
    assert!(sequence(&store, "notes") > before_revert);
}

#[tokio::test]
async fn document_upsert_update_and_delete_advance_the_sequence() {
    let store = store("change-seq-document");
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
    let before = sequence(&store, "boards");
    engine.pull_once("user").await.unwrap();
    let after_upsert = sequence(&store, "boards");
    assert!(after_upsert > before);

    engine
        .record_doc_delta("boards", "b1", b"+delta")
        .await
        .unwrap();
    let after_update = sequence(&store, "boards");
    assert!(after_update > after_upsert);

    engine.resync_document("boards", "b1").await.unwrap();
    assert!(sequence(&store, "boards") > after_update);
}

// MARK: - RowReadCacheTests (6)

counting_note!(WarmNote);

#[tokio::test]
async fn warm_where_and_find_materialize_once_per_committed_sequence() {
    let store = store("row-cache-warm");
    let engine = engine(store.clone(), StubTransport::new());
    let notes = RowStream::<WarmNote>::new(engine.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("one"))]))
        .await
        .unwrap();

    assert_eq!(
        WarmNote::titles(&notes.all().unwrap()),
        [Some("one".to_owned())]
    );
    assert_eq!(WarmNote::decodes(), 1);
    assert_eq!(
        WarmNote::titles(&notes.all().unwrap()),
        [Some("one".to_owned())]
    );
    assert_eq!(
        notes.find("n1").unwrap().unwrap().title.as_deref(),
        Some("one")
    );
    assert_eq!(WarmNote::decodes(), 1);

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("two"))]))
        .await
        .unwrap();

    let rows = notes.all().unwrap();
    assert_eq!(WarmNote::titles(&rows), [Some("two".to_owned())]);
    assert_eq!(notes.find("n1").unwrap().as_ref(), rows.first());
    assert_eq!(WarmNote::decodes(), 2);
}

counting_note!(PredicateNote);

#[tokio::test]
async fn warm_predicate_filtering_matches_sql_without_rematerializing() {
    let store = store("row-cache-predicates");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let notes = RowStream::<PredicateNote>::new(engine.clone());
    let rows: Vec<(&str, ReplicaFields)> = vec![
        (
            "n1",
            fields(&[
                ("kind", text("clip")),
                ("score", ReplicaValue::Number(1.0)),
                ("active", ReplicaValue::Bool(true)),
            ]),
        ),
        (
            "n2",
            fields(&[
                ("kind", text("clip")),
                ("score", ReplicaValue::Number(1.5)),
                ("active", ReplicaValue::Bool(false)),
            ]),
        ),
        (
            "n3",
            fields(&[
                ("kind", text("still")),
                ("score", ReplicaValue::Number(0.0)),
                ("empty", ReplicaValue::Null),
            ]),
        ),
        (
            "n4",
            fields(&[
                (
                    "items",
                    ReplicaValue::Array(vec![ReplicaValue::Number(1.0)]),
                ),
                (
                    "meta",
                    ReplicaValue::Object(fields(&[("x", ReplicaValue::Number(1.0))])),
                ),
            ]),
        ),
        ("n5", fields(&[("kind", text("cafe\u{301}"))])),
    ];

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            rows.iter()
                .map(|(id, data)| row_set("notes", id, None, data.clone()))
                .collect(),
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    assert_eq!(
        PredicateNote::ids(&notes.all().unwrap()),
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );
    let initial_decode_count = PredicateNote::decodes();

    let predicates: Vec<ReplicaFields> = vec![
        ReplicaFields::new(),
        fields(&[("kind", text("clip"))]),
        fields(&[("score", ReplicaValue::Number(1.0))]),
        fields(&[("score", ReplicaValue::Number(1.5))]),
        fields(&[("active", ReplicaValue::Bool(true))]),
        fields(&[("active", ReplicaValue::Number(1.0))]),
        fields(&[("score", ReplicaValue::Bool(false))]),
        fields(&[("empty", ReplicaValue::Null)]),
        fields(&[(
            "items",
            ReplicaValue::Array(vec![ReplicaValue::Number(1.0)]),
        )]),
        fields(&[(
            "meta",
            ReplicaValue::Object(fields(&[("x", ReplicaValue::Number(1.0))])),
        )]),
        fields(&[("missing", text("no"))]),
        fields(&[
            ("kind", text("clip")),
            ("active", ReplicaValue::Bool(false)),
        ]),
        fields(&[("kind", text("caf\u{e9}"))]),
    ];

    for predicate in &predicates {
        assert_eq!(
            PredicateNote::ids(&notes.where_equals(predicate).unwrap()),
            sql_ids(&store, "notes", predicate),
            "predicate: {predicate:?}"
        );
    }
    assert_eq!(PredicateNote::decodes(), initial_decode_count);
}

counting_note!(ConcurrentNote, 50);

#[tokio::test]
async fn concurrent_cold_reads_share_one_materialization() {
    let store = store("row-cache-concurrent");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("one"))]))
        .await
        .unwrap();

    let outcomes: Arc<Mutex<Vec<Vec<Option<String>>>>> = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let engine = engine.clone();
            let outcomes = outcomes.clone();
            scope.spawn(move || {
                let notes = RowStream::<ConcurrentNote>::new(engine);
                let titles = ConcurrentNote::titles(&notes.all().expect("a cold read"));
                outcomes.lock().push(titles);
            });
        }
    });

    let outcomes = std::mem::take(&mut *outcomes.lock());
    assert_eq!(outcomes.len(), 2);
    for outcome in outcomes {
        assert_eq!(outcome, [Some("one".to_owned())]);
    }
    assert_eq!(ConcurrentNote::decodes(), 1);
}

counting_note!(RolledBackNote);

#[tokio::test]
async fn rolled_back_checkpoint_keeps_the_warm_materialization() {
    let store = store("row-cache-rollback");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let notes = RowStream::<RolledBackNote>::new(engine.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        RolledBackNote::titles(&notes.all().unwrap()),
        [Some("one".to_owned())]
    );
    assert_eq!(RolledBackNote::decodes(), 1);

    engine
        .set_checkpoint_fault(Some(Arc::new(|| {
            Err(ReplicaError::Storage("checkpoint fault".into()))
        })))
        .await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "two", None)], "6:", false),
    );
    assert_eq!(
        engine.pull_once("user").await,
        Err(ReplicaError::Storage("checkpoint fault".into()))
    );

    assert_eq!(
        RolledBackNote::titles(&notes.all().unwrap()),
        [Some("one".to_owned())]
    );
    assert_eq!(RolledBackNote::decodes(), 1);

    engine.set_checkpoint_fault(None).await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "two", None)], "6:", false),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        RolledBackNote::titles(&notes.all().unwrap()),
        [Some("two".to_owned())]
    );
    assert_eq!(RolledBackNote::decodes(), 2);
}

counting_note!(PrimedNote);

#[tokio::test]
async fn typed_watch_primes_synchronous_where_and_find() {
    let store = store("row-cache-watch-primes");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let notes = RowStream::<PrimedNote>::new(engine.clone());
    let delivered = Tally::new();

    let listener = {
        let delivered = delivered.clone();
        let mut watch = Box::pin(notes.watch());
        tokio::spawn(async move {
            while watch.next().await.is_some() {
                delivered.bump();
            }
        })
    };

    until("the typed watch baseline was not delivered", || async {
        delivered.count() == 1
    })
    .await;

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();
    until("the typed watch did not deliver", || async {
        delivered.count() == 2
    })
    .await;

    assert_eq!(PrimedNote::decodes(), 1);
    assert_eq!(
        PrimedNote::titles(&notes.all().unwrap()),
        [Some("one".to_owned())]
    );
    assert_eq!(
        notes.find("n1").unwrap().unwrap().title.as_deref(),
        Some("one")
    );
    assert_eq!(PrimedNote::decodes(), 1);
    listener.abort();
}

counting_note!(PairingNote);

/// The window upstream opens with GRDB's `afterNextTransaction` + a semaphore:
/// the transaction has COMMITTED (so a reader sees the new `change_seq`) while
/// the post-commit callbacks — the decoded-row cache's `commit_mutation` among
/// them — are still held. A typed watch that runs in that window must never
/// pair the new sequence with the old cache entry.
///
/// One adaptation, and only one: GRDB's `ValueObservation` fires from its own
/// connection, so it is NOT ordered behind `afterNextTransaction`; our pool
/// rings its doorbell only after the callbacks return. The test therefore rings
/// the doorbell itself (`pool().watch().notify()`) to reproduce the upstream
/// arrival ordering. Everything the case asserts is unchanged.
#[tokio::test]
async fn typed_watch_cannot_pair_a_new_sequence_with_the_old_cache_entry() {
    let store = store("row-cache-watch-pairing");
    let engine = engine(store.clone(), StubTransport::new());
    let notes = RowStream::<PairingNote>::new(engine.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("one"))]))
        .await
        .unwrap();
    assert_eq!(
        PairingNote::titles(&notes.all().unwrap()),
        [Some("one".to_owned())]
    );
    assert_eq!(PairingNote::decodes(), 1);
    let committed_sequence = sequence(&store, "notes");

    let pictures: Arc<Log<Vec<Option<String>>>> = Log::new();
    let listener = {
        let pictures = pictures.clone();
        let mut watch = Box::pin(notes.watch());
        tokio::spawn(async move {
            while let Some(models) = watch.next().await {
                pictures.record(PairingNote::titles(&models));
            }
        })
    };
    until("the typed-watch baseline was not delivered", || async {
        pictures.len() == 1
    })
    .await;

    // Hold the post-commit callbacks: the hook is queued BEFORE the write, so
    // it runs ahead of the cache's own `commit_mutation`. Bounded, exactly like
    // upstream's `wait(timeout: .now() + 2)` — a wedged runner fails the case
    // instead of hanging it.
    let release = Gate::new();
    let writer = {
        let store = Arc::clone(&store);
        let release = release.clone();
        std::thread::spawn(move || {
            store
                .pool()
                .write(|ctx| {
                    ctx.hooks.after_next_transaction(move || {
                        release.block_until_open(Duration::from_secs(2));
                    });
                    store.upsert_snapshot(
                        ctx,
                        "notes",
                        "n1",
                        "user",
                        None,
                        &fields(&[("title", text("two"))]),
                    )
                })
                .expect("the raw snapshot write");
        })
    };

    until("the raw write never committed", || async {
        sequence(&store, "notes") > committed_sequence
    })
    .await;
    // The window is now open, and this proves it rather than assuming it: the
    // commit is visible to a reader while the cache still SERVES the old entry
    // (once `commit_mutation` runs, that entry is retired to the donor slot and
    // this read would cold-load "two"). Everything below therefore grades the
    // stale window, not a race the runner happened to lose.
    assert_eq!(
        PairingNote::titles(&notes.all().unwrap()),
        [Some("one".to_owned())],
        "the post-commit callbacks are not being held — the window under test never opened"
    );
    until(
        "the post-commit typed-watch value was not delivered",
        || async {
            // Re-rung each poll: the watcher may not have re-armed its listener
            // at the instant of the first ring.
            store.pool().watch().notify();
            pictures.len() == 2
        },
    )
    .await;
    release.release();
    writer.join().expect("the raw writer thread");

    let delivered_after_commit = pictures.values()[1].clone();
    assert_eq!(delivered_after_commit, [Some("two".to_owned())]);
    assert_eq!(
        PairingNote::titles(&notes.all().unwrap()),
        [Some("two".to_owned())]
    );
    assert_eq!(
        notes.find("n1").unwrap().unwrap().title.as_deref(),
        Some("two")
    );
    assert_eq!(PairingNote::decodes(), 2);
    listener.abort();
}

// MARK: - RowReuseTests (5)
//
// A one-row patch must cost one row's decode. During a processing storm the
// stream cache invalidates per commit; without per-row reuse every cold reload
// re-decodes EVERY row's JSON, model, and eagerly-materialized payload. The
// superseded cache entry is a
// reuse DONOR: rows whose raw snapshot text is unchanged carry their decoded
// record and model across materializations; only actually-changed rows decode.

reusable_note!(PatchNote);

#[tokio::test]
async fn one_row_patch_decodes_exactly_one_model() {
    let store = store("row-reuse-patch");
    let engine = engine(store.clone(), StubTransport::new());
    let notes = RowStream::<PatchNote>::new(engine.clone());
    for id in ["n1", "n2", "n3"] {
        engine
            .save_row(
                "notes",
                id,
                None,
                &fields(&[("title", text(&format!("seed-{id}")))]),
            )
            .await
            .unwrap();
    }

    assert_eq!(notes.all().unwrap().len(), 3);
    let seeded = PatchNote::decodes();
    assert_eq!(seeded, 3);

    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("patched"))]))
        .await
        .unwrap();

    assert_eq!(
        PatchNote::titles(&notes.all().unwrap()),
        [
            Some("seed-n1".to_owned()),
            Some("patched".to_owned()),
            Some("seed-n3".to_owned())
        ]
    );
    assert_eq!(
        PatchNote::decodes(),
        seeded + 1,
        "a one-row patch re-decoded the whole stream — unchanged rows must reuse their donor decode"
    );
}

reusable_note!(StormNote);

#[tokio::test]
async fn a_progress_shaped_storm_costs_one_decode_per_commit() {
    let store = store("row-reuse-storm");
    let engine = engine(store.clone(), StubTransport::new());
    let notes = RowStream::<StormNote>::new(engine.clone());
    for id in ["n1", "n2", "n3", "n4"] {
        engine
            .save_row(
                "notes",
                id,
                None,
                &fields(&[("progress", ReplicaValue::Number(0.0))]),
            )
            .await
            .unwrap();
    }
    assert_eq!(notes.all().unwrap().len(), 4);
    let seeded = StormNote::decodes();

    // The storm: one row marches, every tick commits, a reader follows each
    // commit — the shape of a long job reporting progress.
    for tick in 1..=5 {
        engine
            .save_row(
                "notes",
                "n1",
                None,
                &fields(&[("progress", ReplicaValue::Number(f64::from(tick) / 10.0))]),
            )
            .await
            .unwrap();
        assert_eq!(notes.all().unwrap().len(), 4);
    }

    assert_eq!(
        StormNote::decodes(),
        seeded + 5,
        "five one-row ticks must cost five decodes, not five whole-stream reloads"
    );
}

reusable_note!(SurvivorNote);

#[tokio::test]
async fn delete_reuses_the_survivors() {
    let store = store("row-reuse-delete");
    let engine = engine(store.clone(), StubTransport::new());
    let notes = RowStream::<SurvivorNote>::new(engine.clone());
    for id in ["n1", "n2", "n3"] {
        engine
            .save_row("notes", id, None, &fields(&[("title", text(id))]))
            .await
            .unwrap();
    }
    assert_eq!(notes.all().unwrap().len(), 3);
    let seeded = SurvivorNote::decodes();

    engine.delete_row("notes", "n2").await.unwrap();

    assert_eq!(SurvivorNote::ids(&notes.all().unwrap()), ["n1", "n3"]);
    assert_eq!(
        SurvivorNote::decodes(),
        seeded,
        "a delete must not re-decode the surviving rows"
    );
}

reusable_note!(TypeFlipNote);

#[tokio::test]
async fn a_type_flip_decodes_fresh() {
    let store = store("row-reuse-type-flip");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let notes = RowStream::<TypeFlipNote>::new(engine.clone());
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("one"))]))
        .await
        .unwrap();
    assert_eq!(
        notes.all().unwrap().first().unwrap().title.as_deref(),
        Some("one")
    );
    let seeded = TypeFlipNote::decodes();

    // Same data text, different STI type, arriving the way type flips really do
    // — a pulled row.set. The donor must NOT be reused: the model's decode
    // switches on `type`.
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![row_set(
                "notes",
                "n1",
                Some("Special"),
                fields(&[("title", text("one"))]),
            )],
            "2:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    assert!(notes.all().unwrap().first().unwrap().special);
    assert_eq!(TypeFlipNote::decodes(), seeded + 1);
}

reusable_note!(ReusedRecordNote);

/// Reused records keep their decoded fields — the in-memory predicate path
/// filters over them.
#[tokio::test]
async fn predicates_filter_over_reused_records() {
    let store = store("row-reuse-predicates");
    let engine = engine(store.clone(), StubTransport::new());
    let notes = RowStream::<ReusedRecordNote>::new(engine.clone());
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("keep"))]))
        .await
        .unwrap();
    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("other"))]))
        .await
        .unwrap();
    notes.all().unwrap();

    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("changed"))]))
        .await
        .unwrap();

    // The reused donor must carry the UNCHANGED row's decoded fields…
    assert_eq!(
        ReusedRecordNote::ids(
            &notes
                .where_equals(&fields(&[("title", text("keep"))]))
                .unwrap()
        ),
        ["n1"]
    );
    // …and the CHANGED row must be filtered by its NEW value. Without this half
    // the test passes even when reuse hands back a stale record: a stale
    // "other" matches neither predicate, so nothing would notice.
    assert_eq!(
        ReusedRecordNote::ids(
            &notes
                .where_equals(&fields(&[("title", text("changed"))]))
                .unwrap()
        ),
        ["n2"]
    );
    assert!(
        notes
            .where_equals(&fields(&[("title", text("other"))]))
            .unwrap()
            .is_empty(),
        "a stale reused record answered a predicate with a superseded value"
    );
}

// MARK: - OwnerBindingTests (10)
//
// Identity is first-class: one store FILE per owner, and no owner means no
// store at all. A write with nobody to own it has nowhere to land — that is
// what kills the "a mint wipes the writes made before it" class structurally
// instead of by gate.

// MARK: Closed semantics

#[tokio::test]
async fn closed_engine_refuses_every_write_with_no_owner() {
    let directory = temp_directory("owner-binding-closed-writes");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());

    assert_no_owner(
        engine
            .save_row("notes", "n1", None, &fields(&[("title", text("x"))]))
            .await,
    );
    assert_no_owner(engine.delete_row("notes", "n1").await);
    assert_no_owner(
        engine
            .create_doc("boards", "b1", b"seed", 1, &ReplicaFields::new(), None)
            .await,
    );
    assert_no_owner(engine.record_doc_delta("boards", "b1", b"d").await);
    assert_no_owner(engine.reset_cursors().await);
    assert_no_owner(engine.discard_ops(&["x".to_owned()]).await);
}

#[tokio::test]
async fn closed_engine_answers_every_read_empty() {
    let directory = temp_directory("owner-binding-closed-reads");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());

    assert!(engine.owner().is_none());
    assert!(engine.store().is_none());
    // Upstream's `engine.database` is `binding.store?.pool` — the same fact,
    // read the way the doc plane reads it.
    assert!(
        engine
            .store()
            .map(|store| Arc::clone(store.pool()))
            .is_none()
    );
    assert!(engine.doc_fold("boards", "b1").unwrap().is_none());
    assert!(engine.doc_peer("boards", "b1").unwrap().is_none());
    let pending = engine.pending_ops().await.unwrap();
    let parked = engine.parked_ops().await.unwrap();
    let cursor = engine.current_cursor("user").await.unwrap();
    assert_eq!(pending.len(), 0);
    assert_eq!(parked.len(), 0);
    assert!(cursor.is_none());

    let notes = RowStream::<TestNote>::new(engine.clone());
    assert_eq!(notes.all().unwrap(), Vec::new());
    assert!(notes.find("n1").unwrap().is_none());
}

#[tokio::test]
async fn closed_engine_never_touches_the_wire() {
    let directory = temp_directory("owner-binding-closed-wire");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());

    let verdicts = engine.drain().await.unwrap();
    let applied = engine.pull_until_caught_up(None).await.unwrap();
    engine.drain_if_warm().await.unwrap();
    assert_eq!(verdicts.len(), 0);
    assert_eq!(applied, 0);

    assert_eq!(
        transport.pull_count(),
        0,
        "a closed engine has no owner to authorize a pull"
    );
    assert_eq!(
        transport.push_count(),
        0,
        "a closed engine holds no journal to push"
    );
}

/// The stale-handle hazard: a generated verb surface captured while an owner
/// was open must refuse once that owner is gone, not write into a store the
/// process no longer owns.
#[tokio::test]
async fn a_handle_captured_while_open_refuses_after_close() {
    let directory = temp_directory("owner-binding-stale-handle");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    engine.open(1).await.unwrap();
    let notes = RowStream::<TestNote>::new(engine.clone());
    notes
        .create(&TestNote::new("n1", Some("mine"), None))
        .await
        .unwrap();

    engine.close().await.unwrap();

    assert_no_owner(
        notes
            .create(&TestNote::new("n2", Some("orphan"), None))
            .await,
    );
    assert_eq!(notes.all().unwrap(), Vec::new());
}

// MARK: One file per owner

#[tokio::test]
async fn open_creates_the_owners_file_and_reopen_serves_the_same_world() {
    let directory = temp_directory("owner-binding-one-file");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());

    engine.open(1).await.unwrap();
    assert_eq!(engine.owner(), Some(1));
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("kept"))]))
        .await
        .unwrap();
    assert!(directory.path().join("replica-1.sqlite").exists());

    engine.close().await.unwrap();
    assert!(engine.owner().is_none());

    engine.open(1).await.unwrap();
    let notes = RowStream::<TestNote>::new(engine.clone());
    assert_eq!(
        notes.find("n1").unwrap().unwrap().title.as_deref(),
        Some("kept"),
        "reopening the same owner reopens the same file"
    );

    // The OPENED owner is the only owner a create can carry — the stamp follows
    // the binding, not a value the caller passed in.
    engine
        .create_doc(
            "boards",
            "b1",
            b"seed",
            1,
            &fields(&[("title", text("board"))]),
            Some(&ReplicaCreateStamp::standard()),
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .store()
            .expect("the bound store")
            .peek_snapshot("boards", "b1")
            .unwrap()
            .unwrap()
            .data
            .get("userId"),
        Some(&ReplicaValue::Number(1.0))
    );
}

#[tokio::test]
async fn retire_deletes_the_owners_three_files_and_the_next_owner_starts_empty() {
    let directory = temp_directory("owner-binding-retire");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());

    engine.open(1).await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "10:", false),
    );
    engine.pull_once("user").await.unwrap();
    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("unpushed"))]))
        .await
        .unwrap();
    let owed = engine.pending_ops().await.unwrap();
    assert_eq!(owed.len(), 1);
    assert!(
        directory.path().join("replica-1.sqlite").exists(),
        "the outgoing owner's file has to exist for its removal to mean anything"
    );

    engine.retire().await.unwrap();

    assert!(
        !directory.path().join("replica-1.sqlite").exists(),
        "retire leaves no trace of the outgoing owner"
    );

    engine.open(2).await.unwrap();
    let notes = RowStream::<TestNote>::new(engine.clone());
    let cursor_after_switch = engine.current_cursor("user").await.unwrap();
    let owed_after_switch = engine.pending_ops().await.unwrap();
    assert_eq!(
        notes.all().unwrap(),
        Vec::new(),
        "the next owner never sees the retired owner's rows"
    );
    assert!(
        cursor_after_switch.is_none(),
        "a blank cursor makes the next pull re-snapshot"
    );
    assert_eq!(
        owed_after_switch.len(),
        0,
        "an unpushed op must never ride the next identity's bearer"
    );
}

/// The coherence check: an EMPTY store
/// holding a warm cursor can never heal — the cursor claims coverage the store
/// does not hold, so every tail pull serves nothing.
#[tokio::test]
async fn reopen_keeps_the_cursor_of_an_empty_checkpoint() {
    let directory = temp_directory("owner-binding-heal");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());

    engine.open(1).await.unwrap();
    transport.queue_pull("user", ScriptedPull::new(vec![], "10:", false));
    engine.pull_once("user").await.unwrap();
    let warm_cursor = engine.current_cursor("user").await.unwrap();
    assert_eq!(warm_cursor.as_deref(), Some("10:"));

    engine.close().await.unwrap();
    engine.open(1).await.unwrap();

    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("10:")
    );
}

#[tokio::test]
async fn open_keeps_a_warm_cursor_when_the_store_still_holds_its_world() {
    let directory = temp_directory("owner-binding-warm-cursor");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());

    engine.open(1).await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "10:", false),
    );
    engine.pull_once("user").await.unwrap();

    engine.close().await.unwrap();
    engine.open(1).await.unwrap();

    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("10:"),
        "a coherent store keeps its read position"
    );
}

// MARK: L1 — watchers ride the owner, never a dead pool

#[tokio::test]
async fn a_watcher_armed_without_an_owner_serves_the_owner_that_arrives() {
    let directory = temp_directory("owner-binding-watch-arrival");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    let seen: Arc<Log<Vec<String>>> = Log::new();

    let watcher = {
        let seen = seen.clone();
        let mut watch = Box::pin(RowStream::<TestNote>::new(engine.clone()).watch());
        tokio::spawn(async move {
            while let Some(rows) = watch.next().await {
                let mut ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
                ids.sort();
                seen.record(ids);
            }
        })
    };

    until(
        "a closed engine must still deliver its empty picture",
        || async { seen.values() == vec![Vec::<String>::new()] },
    )
    .await;

    engine.open(1).await.unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("first"))]))
        .await
        .unwrap();

    until(
        "the watcher never re-armed on the owner that arrived",
        || async { seen.last().as_deref() == Some(["n1".to_owned()].as_slice()) },
    )
    .await;
    watcher.abort();
}

#[tokio::test]
async fn a_watcher_stops_serving_a_retired_owner_and_picks_up_the_next_one() {
    let directory = temp_directory("owner-binding-watch-retire");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    engine.open(1).await.unwrap();
    engine
        .save_row(
            "notes",
            "outgoing",
            None,
            &fields(&[("title", text("theirs"))]),
        )
        .await
        .unwrap();

    let seen: Arc<Log<Vec<String>>> = Log::new();
    let watcher = {
        let seen = seen.clone();
        let mut watch = Box::pin(RowStream::<TestNote>::new(engine.clone()).watch());
        tokio::spawn(async move {
            while let Some(rows) = watch.next().await {
                let mut ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
                ids.sort();
                seen.record(ids);
            }
        })
    };
    until("the outgoing owner's picture was never served", || async {
        seen.last().as_deref() == Some(["outgoing".to_owned()].as_slice())
    })
    .await;

    engine.retire().await.unwrap();
    engine.open(2).await.unwrap();
    engine
        .save_row(
            "notes",
            "incoming",
            None,
            &fields(&[("title", text("mine"))]),
        )
        .await
        .unwrap();

    until(
        "the watcher kept serving the retired owner's rows",
        || async { seen.last().as_deref() == Some(["incoming".to_owned()].as_slice()) },
    )
    .await;
    let pictures = seen.values();
    let outgoing = vec!["outgoing".to_owned()];
    let after = pictures
        .iter()
        .position(|picture| *picture == outgoing)
        .map_or(0, |index| index + 1);
    assert!(
        !pictures[after..].contains(&outgoing),
        "no picture after the retirement may carry the outgoing owner's row"
    );
    watcher.abort();
}

#[tokio::test]
async fn cancelling_a_pull_releases_the_identity_barrier() {
    let store = store("cancel-pull");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let arrived = Arc::new(tokio::sync::Notify::new());
    let signal = arrived.clone();
    transport.on_pull(move |_| {
        let signal = signal.clone();
        Box::pin(async move {
            signal.notify_one();
            std::future::pending::<()>().await
        })
    });
    let running = engine.clone();
    let task = tokio::spawn(async move { running.pull_once("user").await });
    arrived.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), engine.try_seal())
        .await
        .expect("a cancelled pull leaked its wire admission")
        .unwrap();
}

#[tokio::test]
async fn cancelling_a_push_releases_the_flight_and_keeps_the_journal_retryable() {
    let store = store("cancel-push");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("saved"))]))
        .await
        .unwrap();
    let arrived = Arc::new(tokio::sync::Notify::new());
    let signal = arrived.clone();
    transport.on_push(move |_| {
        let signal = signal.clone();
        Box::pin(async move {
            signal.notify_one();
            std::future::pending::<()>().await
        })
    });
    let running = engine.clone();
    let task = tokio::spawn(async move { running.drain().await });
    arrived.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(store.peek_pending().unwrap().len(), 1);
    tokio::time::timeout(Duration::from_secs(1), engine.try_seal())
        .await
        .expect("a cancelled push leaked its wire admission")
        .unwrap();
    engine.unseal().await;
    transport.on_push(|_| Box::pin(async {}));
    tokio::time::timeout(Duration::from_secs(1), engine.drain())
        .await
        .expect("a cancelled drain left the lane claimed")
        .unwrap();
    assert!(store.peek_pending().unwrap().is_empty());
}

#[test]
fn saved_writes_request_durable_wal_commits() {
    let store = store("durable-wal");
    store
        .pool()
        .write(|ctx| {
            assert_eq!(
                ctx.tx
                    .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))?,
                2
            );
            assert_eq!(
                ctx.tx
                    .query_row("PRAGMA fullfsync", [], |row| row.get::<_, i64>(0))?,
                1
            );
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn projection_store_cannot_become_editable_with_an_incomplete_history() {
    let directory = temp_directory("projection-mode");
    let mut config = options(directory.path().to_path_buf(), StubTransport::new());
    config.codecs.clear();
    config.document_mode = crate::ReplicaDocumentMode::ProjectionsOnly;
    let projections = crate::ReplicaEngine::new(config);
    projections.open_for_cold_boot(42).unwrap();
    projections.try_close().await.unwrap();
    let editable = crate::ReplicaEngine::new(options(
        directory.path().to_path_buf(),
        StubTransport::new(),
    ));
    assert_eq!(
        editable.open_for_cold_boot(42),
        Err(ReplicaError::Storage(
            "Document mode belongs to the store. Use a separate store for projection-only replicas."
                .into()
        ))
    );
}

#[test]
fn removing_a_store_deletes_the_database_and_both_sidecars() {
    let directory = temp_directory("remove-store-sidecars");
    let database = directory.join("retired.sqlite");
    let files =
        ["", "-wal", "-shm"].map(|suffix| directory.join(format!("retired.sqlite{suffix}")));
    for file in &files {
        std::fs::write(file, b"retired").unwrap();
    }
    ReplicaStateStore::remove(&database).unwrap();
    for file in &files {
        assert!(!file.exists(), "{} survived", file.display());
    }
}

#[test]
fn moving_a_store_cannot_destroy_a_different_target_world() {
    let directory = temp_directory("move-store-no-clobber");
    let source = directory.join("source.sqlite");
    let target = directory.join("target.sqlite");
    std::fs::write(&source, b"source offline data").unwrap();
    std::fs::write(&target, b"target offline data").unwrap();
    assert!(ReplicaStateStore::move_store(&source, &target).is_err());
    assert_eq!(std::fs::read(&source).unwrap(), b"source offline data");
    assert_eq!(std::fs::read(&target).unwrap(), b"target offline data");
}

#[test]
fn closing_a_store_waits_for_active_snapshot_readers() {
    let store = store("close-reader-fence");
    let (entered, wait_entered) = std::sync::mpsc::channel();
    let (release, wait_release) = std::sync::mpsc::channel();
    let reader = std::thread::spawn({
        let store = store.clone();
        move || {
            store
                .pool()
                .read(|db| {
                    let _: i64 =
                        db.query_row("SELECT COUNT(*) FROM snapshots", [], |row| row.get(0))?;
                    entered.send(()).unwrap();
                    wait_release.recv().unwrap();
                    Ok(())
                })
                .unwrap();
        }
    });
    wait_entered.recv().unwrap();
    let (closed, wait_closed) = std::sync::mpsc::channel();
    let closer = std::thread::spawn({
        let store = store.clone();
        move || {
            store.close().unwrap();
            closed.send(()).unwrap();
        }
    });
    let prematurely_closed = wait_closed.recv_timeout(Duration::from_millis(100)).is_ok();
    release.send(()).unwrap();
    reader.join().unwrap();
    closer.join().unwrap();
    assert!(
        !prematurely_closed,
        "a file can be renamed while a live SQLite reader still owns its WAL"
    );
}
