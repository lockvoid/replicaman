//! The drain group, transliterated file by file.
//!
//! - `LaneDrainTests` (9)
//! - `ColdBootDrainTests` (5)
//! - `RawJournalRetryTests` (3)
//! - `ChunkedDrainTests` (2)
//! - `DrainBeforePullTests` (2)
//! - `ConcurrentDrainTests` (1)
//! - `DrainGovernorTests` (1)
//!
//! These are the suite's timing-sensitive cases, and every wait here is a
//! deterministic seam rather than a sleep: `StubTransport::on_push` is awaited
//! INSIDE the flight, so "what had already happened when this chunk went on
//! the wire" is observed rather than guessed. Where upstream uses its
//! `Gate(seconds: 5)` the Rust `Gate` is wrapped in a 5 s `tokio::time::timeout`
//! with the same reason attached, so a wedged engine fails the case instead of
//! hanging the runner.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use crate::engine::ReplicaEngine;
use crate::models::RowStream;
use crate::schema::ReplicaLane;
use crate::store::ReplicaStateStore;
use crate::tests::support::*;
use crate::transport::BoxFuture;
use crate::value::ReplicaFields;

// MARK: - Private fixtures (the Rust half of what these suites keep local)

/// `LaneDrainTests.lanes(of:)` — row id ⇒ the durable lane column of the
/// entries it still owes.
fn lanes_of(store: &ReplicaStateStore) -> HashMap<String, String> {
    store
        .pool()
        .read(|db| {
            let mut by_row = HashMap::new();
            for entry in store.pending(db)? {
                let op = entry.op()?;
                by_row.insert(op.row_id, store.lane(db, &entry.id)?.as_str().to_owned());
            }
            Ok(by_row)
        })
        .expect("read the journal's lanes")
}

/// `RawJournalRetryTests.RawEntry` — retry and backoff are graded from the RAW
/// intents, never through `pending_ops()`, which is the accessor the engine
/// wrote with. A decode through `op()` would hide a payload the engine had
/// rewritten. Accepted intents are overlays, not the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RawEntry {
    id: String,
    lane: String,
    reason: Option<String>,
    payload: String,
}

fn raw_journal(store: &ReplicaStateStore) -> Vec<RawEntry> {
    store
        .pool()
        .read(|db| {
            let rows = db
                .prepare(
                    "SELECT id, lane, reason, payload FROM intents \
                     WHERE state <> 'accepted' ORDER BY rowid",
                )?
                .query_map([], |row| {
                    Ok(RawEntry {
                        id: row.get(0)?,
                        lane: row.get(1)?,
                        reason: row.get(2)?,
                        payload: row.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .expect("read the raw journal")
}

/// `ColdBootDrainTests.rawPendingIds` — read RAW, never `pending_ops()`: the
/// point of the cold-boot durability case is that the TABLE survived a
/// process, not that an accessor still answers.
fn raw_pending_ids(store: &ReplicaStateStore) -> Vec<String> {
    store
        .pool()
        .read(|db| {
            let rows = db
                .prepare("SELECT id FROM intents WHERE state IN ('owed', 'frozen') ORDER BY rowid")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .expect("read the raw journal ids")
}

/// The row ids of the still-owed entries, read out of the stored PAYLOAD
/// rather than the address column — `DrainGovernorTests.owed()` and the tail
/// check in `RawJournalRetryTests`.
fn owed_payload_row_ids(store: &ReplicaStateStore) -> Vec<String> {
    store
        .pool()
        .read(|db| {
            let rows = db
                .prepare(
                    "SELECT json_extract(payload, '$.row_id') FROM intents \
                     WHERE state IN ('owed', 'frozen') ORDER BY rowid",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .expect("read the owed row ids")
}

/// Upstream's `Gate` carries its own 5 s deadline; the fixture's does not, and
/// a drain that never reaches the wire must fail this suite rather than wedge
/// the runner.
async fn wait_for(gate: &Gate, reason: &str) {
    gate.wait_within(Duration::from_secs(5), reason).await;
}

/// `ChunkedDrainTests.PendingLog` — the push-time observation log.
#[derive(Default)]
struct PendingLog {
    values: Mutex<Vec<usize>>,
}

impl PendingLog {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn append(&self, value: usize) {
        self.values.lock().push(value);
    }

    fn counts(&self) -> Vec<usize> {
        self.values.lock().clone()
    }
}

/// `Fixture.unopenedEngine(in:transport:automaticallyPushWrites:syncReducers:)`
/// — the arguments `support::unopened_engine` pins to their defaults.
fn unopened_engine_with(
    directory: &Path,
    transport: Arc<StubTransport>,
    automatically_push_writes: bool,
    sync_gates: Vec<Arc<dyn crate::SyncGate>>,
) -> Arc<ReplicaEngine> {
    let mut opts = options(directory.to_path_buf(), transport);
    opts.automatically_push_writes = automatically_push_writes;
    opts.sync_gates = sync_gates;
    ReplicaEngine::new(opts)
}

// MARK: - LaneDrainTests (9)
//
// One FIFO is wrong when it carries two kinds of write. A chat message the
// user had just typed sat behind ~19 bulk import rows and reached the server
// ~9 s after the tap — nothing was slow, it was waiting its turn.
//
// A LANE is claimed by an ACTION, not by a stream. Overtaking is only safe
// because two engine invariants make dependency ordering automatic rather than
// remembered: row stickiness, and causal promotion.

// MARK: The scope

/// A nested helper inherits the claimed lane without knowing lanes exist —
/// that is the reason the scope is a scope.
async fn helper_that_knows_nothing_about_lanes(engine: &ReplicaEngine) {
    engine
        .save_row("notes", "nested", None, &fields(&[("title", text("x"))]))
        .await
        .unwrap();
}

#[tokio::test]
async fn writes_inside_the_scope_claim_the_lane_and_outside_stay_bulk() {
    let store = store("lane-scope-claim");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row("notes", "hot", None, &fields(&[("title", text("typed"))]))
                .await
                .unwrap();
            helper_that_knows_nothing_about_lanes(&engine).await;
        })
        .await;
    engine
        .save_row(
            "notes",
            "cold",
            None,
            &fields(&[("title", text("imported"))]),
        )
        .await
        .unwrap();

    let claimed = lanes_of(&store);
    assert_eq!(claimed.get("hot").map(String::as_str), Some("interactive"));
    assert_eq!(
        claimed.get("nested").map(String::as_str),
        Some("interactive"),
        "a nested helper inherits the claimed lane"
    );
    assert_eq!(
        claimed.get("cold").map(String::as_str),
        Some("bulk"),
        "the default is background — a stream says nothing on its own"
    );
}

/// Lanes are scoped, and background work started from inside a user action
/// would otherwise ride the fast lane and flood the very thing it exists to
/// keep clear. Claiming bulk RESETS the scope.
#[tokio::test]
async fn bulk_nested_inside_an_interactive_action_resets_the_lane() {
    let store = store("lane-scope-reset");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row(
                    "notes",
                    "message",
                    None,
                    &fields(&[("title", text("typed"))]),
                )
                .await
                .unwrap();
            engine
                .lane(ReplicaLane::Bulk, async {
                    engine
                        .save_row(
                            "notes",
                            "cook",
                            None,
                            &fields(&[("title", text("progress"))]),
                        )
                        .await
                        .unwrap();
                })
                .await;
        })
        .await;

    let claimed = lanes_of(&store);
    assert_eq!(
        claimed.get("message").map(String::as_str),
        Some("interactive")
    );
    assert_eq!(
        claimed.get("cook").map(String::as_str),
        Some("bulk"),
        "fan-out started inside an action must not inherit its urgency"
    );
}

#[tokio::test]
async fn the_lane_survives_a_store_reopen() {
    let scratch = temp_path("lane-durability", ".sqlite");
    let store = Arc::new(ReplicaStateStore::open(scratch.path()).expect("open the fixture store"));
    let engine = engine(store.clone(), StubTransport::new());
    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row("notes", "n1", None, &fields(&[("title", text("t"))]))
                .await
                .unwrap();
        })
        .await;

    // A second store over the same file — upstream's "the app was killed and
    // relaunched", expressed the only way a test can.
    engine.try_close().await.unwrap();
    let reopened = ReplicaStateStore::open(scratch.path()).expect("reopen the store");
    let claimed = reopened
        .pool()
        .read(|db| {
            let entry = reopened.pending(db)?.remove(0);
            Ok(reopened.lane(db, &entry.id)?.as_str().to_owned())
        })
        .unwrap();

    assert_eq!(
        claimed, "interactive",
        "a queued message is still urgent after the app is killed"
    );
}

// MARK: The overtake (the whole point)

/// The feature itself: the interactive push must be ON THE WIRE while the bulk
/// one is still held. An "it eventually shipped alone" assertion is NOT this —
/// that version stays green with the lanes fully serialised. So the bulk hook
/// records that it is still inside its push, and the interactive push reads
/// that flag.
#[tokio::test]
async fn an_interactive_write_waits_for_the_frozen_bulk_prefix() {
    let store = store("lane-overtake");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    for index in 0..19 {
        engine
            .save_row(
                "notes",
                &format!("bulk{index}"),
                None,
                &fields(&[("title", text(&format!("i{index}")))]),
            )
            .await
            .unwrap();
    }
    let bulk_released = Gate::new();
    let bulk_arrived = Gate::new();
    let bulk_in_flight = Arc::new(AtomicBool::new(false));
    let overtook = Arc::new(AtomicBool::new(false));
    transport.on_push({
        let bulk_released = bulk_released.clone();
        let bulk_arrived = bulk_arrived.clone();
        let bulk_in_flight = bulk_in_flight.clone();
        let overtook = overtook.clone();
        move |ops| {
            let bulk_released = bulk_released.clone();
            let bulk_arrived = bulk_arrived.clone();
            let bulk_in_flight = bulk_in_flight.clone();
            let overtook = overtook.clone();
            Box::pin(async move {
                if ops.iter().any(|op| op.row_id.starts_with("bulk")) {
                    bulk_in_flight.store(true, Ordering::SeqCst);
                    bulk_arrived.release();
                    bulk_released.wait().await;
                    bulk_in_flight.store(false, Ordering::SeqCst);
                } else if ops.iter().any(|op| op.row_id == "message") {
                    overtook.store(bulk_in_flight.load(Ordering::SeqCst), Ordering::SeqCst);
                }
            }) as BoxFuture<'static, ()>
        }
    });
    let bulk_drain = tokio::spawn({
        let engine = engine.clone();
        async move { engine.drain_lane(ReplicaLane::Bulk).await }
    });
    // The bulk push signals its OWN arrival from inside `push` — the
    // interactive write below is provably racing a flight that is on the wire,
    // not one that a sleep hoped had started.
    wait_for(&bulk_arrived, "the bulk push never reached the wire").await;

    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row(
                    "notes",
                    "message",
                    None,
                    &fields(&[("title", text("typed"))]),
                )
                .await
                .unwrap();
        })
        .await;
    let interactive = tokio::spawn({
        let engine = engine.clone();
        async move { engine.drain_lane(ReplicaLane::Interactive).await }
    });
    bulk_released.release();
    bulk_drain.await.unwrap().unwrap();
    interactive.await.unwrap().unwrap();
    assert!(!overtook.load(Ordering::SeqCst));
    assert_eq!(transport.pushed_row_ids().last().unwrap(), "message");
    assert!(store.peek_pending().unwrap().is_empty());
}

/// Promotion can move an entry the OTHER lane is holding on the wire. If the
/// drain then re-selects it, the same create ships twice and comes back a
/// collision that reverts the row.
#[tokio::test]
async fn a_promoted_entry_already_on_the_wire_is_not_sent_twice() {
    let store = store("lane-promoted-in-flight");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row(
            "notes",
            "element",
            None,
            &fields(&[("title", text("clip"))]),
        )
        .await
        .unwrap();
    let release = Gate::new();
    let arrived = Gate::new();
    transport.on_push({
        let release = release.clone();
        let arrived = arrived.clone();
        move |ops| {
            let release = release.clone();
            let arrived = arrived.clone();
            Box::pin(async move {
                if !ops.iter().any(|op| op.row_id == "element") {
                    return;
                }
                arrived.release();
                release.wait().await;
            }) as BoxFuture<'static, ()>
        }
    });
    let bulk_drain = tokio::spawn({
        let engine = engine.clone();
        async move { engine.drain_lane(ReplicaLane::Bulk).await }
    });
    wait_for(&arrived, "the bulk push never reached the wire").await;

    // The user attaches that very element — promotion pulls it up while its
    // push is still open.
    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row(
                    "notes",
                    "attachment",
                    None,
                    &fields(&[("recordId", text("element"))]),
                )
                .await
                .unwrap();
        })
        .await;
    let interactive = tokio::spawn({
        let engine = engine.clone();
        async move { engine.drain_lane(ReplicaLane::Interactive).await }
    });
    release.release();
    bulk_drain.await.unwrap().unwrap();
    interactive.await.unwrap().unwrap();

    let sent = transport.pushed_row_ids();
    assert_eq!(
        sent.iter().filter(|row_id| *row_id == "element").count(),
        1,
        "an in-flight entry must not be re-sent by the lane that promoted it"
    );
}

/// Sign-out flushes the journal through a pinned transport and then wipes. If
/// that flush only drains one lane, the user's just-typed message is destroyed
/// by the wipe.
#[tokio::test]
async fn the_sign_out_flush_drains_both_lanes() {
    let store = store("lane-sign-out-flush");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "import", None, &fields(&[("title", text("bulk"))]))
        .await
        .unwrap();
    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row(
                    "notes",
                    "message",
                    None,
                    &fields(&[("title", text("typed"))]),
                )
                .await
                .unwrap();
        })
        .await;

    engine.seal().await.unwrap();
    let pinned = StubTransport::new();
    engine.seal_and_drain(pinned.clone()).await.unwrap();

    let sent: HashSet<String> = pinned.pushed_row_ids().into_iter().collect();
    let expected: HashSet<String> = ["import".to_owned(), "message".to_owned()].into();
    assert_eq!(
        sent, expected,
        "everything owed goes out before the wipe, whatever lane it claimed"
    );
}

#[tokio::test]
async fn order_is_preserved_within_a_lane() {
    let store = store("lane-order");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .lane(ReplicaLane::Interactive, async {
            for index in 0..5 {
                engine
                    .save_row(
                        "notes",
                        &format!("n{index}"),
                        None,
                        &fields(&[("title", text(&format!("t{index}")))]),
                    )
                    .await
                    .unwrap();
            }
        })
        .await;
    engine.drain_lane(ReplicaLane::Interactive).await.unwrap();

    assert_eq!(transport.pushed_row_ids(), ["n0", "n1", "n2", "n3", "n4"]);
}

// MARK: The invariants that keep overtaking safe

/// DEFECT THIS PREVENTS: a bulk patch passing the interactive create of the
/// same row — the server refuses an update to a row it has never seen.
#[tokio::test]
async fn a_later_bulk_write_joins_the_lane_its_row_is_already_queued_on() {
    let store = store("lane-stickiness");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row("notes", "n1", None, &fields(&[("title", text("created"))]))
                .await
                .unwrap();
        })
        .await;
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("edited"))]))
        .await
        .unwrap();

    assert_eq!(
        lanes_of(&store).get("n1").map(String::as_str),
        Some("interactive"),
        "an update must never outrun the create of its own row"
    );
}

/// DEFECT THIS PREVENTS: attaching a clip whose element create is still queued
/// in bulk. The attachment names the element id; if it overtakes, the door
/// raises "unknown element", the entry parks, and the local row is reverted.
#[tokio::test]
async fn an_interactive_write_naming_a_pending_bulk_row_promotes_that_row() {
    let store = store("lane-promotion");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    // The import authored this element and has not pushed yet.
    engine
        .save_row(
            "notes",
            "element-7",
            None,
            &fields(&[("title", text("clip"))]),
        )
        .await
        .unwrap();
    // The user attaches it to a message they just typed.
    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row(
                    "notes",
                    "attachment-1",
                    None,
                    &fields(&[("recordId", text("element-7"))]),
                )
                .await
                .unwrap();
        })
        .await;

    assert_eq!(
        lanes_of(&store).get("element-7").map(String::as_str),
        Some("interactive"),
        "what an interactive write depends on is interactive too"
    );

    engine.drain_lane(ReplicaLane::Interactive).await.unwrap();
    assert_eq!(
        transport.pushed_row_ids(),
        ["element-7", "attachment-1"],
        "and it still lands first"
    );
}

// MARK: - ColdBootDrainTests (5)
//
// The cold boot: a process starts over a store file that already exists and
// already owes work. `open_for_cold_boot` is not `open(owner)` with a
// different name — it binds only from nothing, runs its OWN `heal_cursors`,
// and, the finding this suite exists to pin, never calls `unseal()`, so it
// never schedules a push for the journal it just re-opened.

/// Everything a killed process leaves on disk for the next one.
async fn write_owed_world(
    directory: &Path,
    owner: i64,
    rows: &[(&str, ReplicaFields)],
) -> Vec<String> {
    let engine = unopened_engine(directory.to_path_buf(), StubTransport::new());
    engine.open(owner).await.unwrap();
    for (id, data) in rows {
        engine.save_row("notes", id, None, data).await.unwrap();
    }
    let owed = engine
        .pending_ops()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    // The process ends. The file stays; `close` never retires it.
    engine.close().await.unwrap();
    owed
}

/// The store's own header states the stake: losing the journal loses the
/// user's unsent work. Read RAW — `SELECT id FROM journal`, never
/// `pending_ops()`.
#[tokio::test]
async fn the_owed_journal_survives_the_process_and_is_still_owed_after_a_cold_boot() {
    let directory = temp_directory("cold-boot-journal");
    let owed_before = write_owed_world(
        directory.path(),
        1,
        &[
            ("n1", fields(&[("title", text("typed before the kill"))])),
            ("n2", fields(&[("title", text("and this one too"))])),
        ],
    )
    .await;
    assert_eq!(owed_before.len(), 2);

    // A NEW engine — a new process, in every way the test can express.
    let reborn = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    reborn.open_for_cold_boot(1).unwrap();

    let store = reborn.store().expect("the cold door bound nothing");
    assert_eq!(
        raw_pending_ids(&store),
        owed_before,
        "the relaunched process no longer owes the work the user did before the kill"
    );
    assert_eq!(
        RowStream::<TestNote>::new(reborn.clone())
            .find("n1")
            .unwrap()
            .and_then(|note| note.title),
        Some("typed before the kill".to_owned()),
        "…and the returning user's world is readable with no actor hop, on the first frame"
    );
}

/// The cold door runs its OWN coherence heal — a separate call site from the
/// warm `bind_store` one. An empty store holding a warm cursor can never heal:
/// the cursor claims coverage the store does not hold, so every tail pull
/// serves nothing and the user's whole world stays stranded server-side.
#[tokio::test]
async fn cold_boot_keeps_the_cursor_of_an_empty_checkpoint() {
    let directory = temp_directory("cold-boot-heal");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());
    engine.open(1).await.unwrap();
    transport.queue_pull("user", ScriptedPull::new(vec![], "10:", false));
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("10:")
    );

    engine.close().await.unwrap();

    let reborn = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    reborn.open_for_cold_boot(1).unwrap();

    assert_eq!(
        reborn.current_cursor("user").await.unwrap(),
        Some("10:".to_owned()),
        "an empty authorized view is a valid checkpoint"
    );
}

/// THE FINDING. `open(owner)` ends in `unseal()`, which schedules a push for
/// every lane that owes work. `open_for_cold_boot` does not. So a relaunch
/// over an owed journal is SILENT until something wakes it — in production the
/// host's `nudge()`, which the cold path does not reach.
///
/// Both halves are graded in ONE test against the SAME budget, so the negative
/// is proven by contrast with a positive that did fire, never by elapsed time.
#[tokio::test]
async fn the_cold_door_never_self_schedules_while_the_warm_door_always_does() {
    let cold_directory = temp_directory("cold-boot-silent");
    write_owed_world(
        cold_directory.path(),
        1,
        &[("n1", fields(&[("title", text("owed across the kill"))]))],
    )
    .await;
    let warm_directory = temp_directory("cold-boot-warm");
    write_owed_world(
        warm_directory.path(),
        2,
        &[("n1", fields(&[("title", text("owed across the kill"))]))],
    )
    .await;

    let cold_transport = StubTransport::new();
    let cold = unopened_engine_with(
        cold_directory.path(),
        cold_transport.clone(),
        true,
        Vec::new(),
    );
    let warm_transport = StubTransport::new();
    let warm = unopened_engine_with(
        warm_directory.path(),
        warm_transport.clone(),
        true,
        Vec::new(),
    );

    cold.open_for_cold_boot(1).unwrap();
    warm.open(2).await.unwrap();

    // The warm door's own delivery is the budget. When it has fired, a
    // scheduled push has had at least as long to reach the stub on the cold
    // side — `schedule_push` is one spawn away.
    until(
        "the warm door never scheduled the journal it re-opened",
        || async { warm_transport.push_count() == 1 },
    )
    .await;
    assert_eq!(
        cold_transport.push_count(),
        0,
        "the cold door scheduled a push; if that becomes true, the host's nudge is dead weight"
    );

    // And the contract that makes the silence safe: ONE explicit drain — what
    // the host's doorbell ultimately calls — clears everything owed.
    cold.drain().await.unwrap();
    assert_eq!(cold_transport.push_count(), 1);
    let store = cold.store().expect("the cold door bound nothing");
    assert!(
        raw_pending_ids(&store).is_empty(),
        "the first wake must clear the whole owed journal"
    );
}

/// AT COLD BOOT — a write that happened BEFORE this process existed is
/// still gated by the engine's very first judge. The gated write is authored
/// by a DIFFERENT engine that had no reducers at all, so the entry in the
/// journal is genuinely unjudged when the reborn engine picks it up.
#[tokio::test]
async fn the_first_drain_after_a_cold_boot_is_already_judged() {
    let directory = temp_directory("cold-boot-judged");
    // Authored by an engine with NO reducers: nothing judged this entry.
    write_owed_world(
        directory.path(),
        1,
        &[("n1", fields(&[("title", text("a")), ("blob", text("k1"))]))],
    )
    .await;

    let transport = StubTransport::new();
    let released = ReleaseLedger::new(&[]);
    let reborn = unopened_engine_with(
        directory.path(),
        transport.clone(),
        false,
        vec![blob_gate(released)],
    );
    reborn.open_for_cold_boot(1).unwrap();

    reborn.drain().await.unwrap();

    assert!(
        transport.pushed_ops().is_empty(),
        "the whole birth waits for its upload gate"
    );
    assert_eq!(reborn.held_rows().unwrap().len(), 1);
    assert_eq!(
        reborn
            .store()
            .unwrap()
            .peek_snapshot("notes", "n1")
            .unwrap()
            .unwrap()
            .data["blob"],
        text("k1")
    );
}

/// The cold door binds only from NOTHING: changing owners is a transition and
/// must go through `open(owner)`, where in-flight work is quiesced. A cold
/// door that could re-bind would swap the store under a live pull.
#[tokio::test]
async fn a_second_cold_boot_cannot_steal_an_already_bound_process() {
    let directory = temp_directory("cold-boot-steal");
    write_owed_world(
        directory.path(),
        1,
        &[("n1", fields(&[("title", text("first owner"))]))],
    )
    .await;
    write_owed_world(
        directory.path(),
        2,
        &[("n2", fields(&[("title", text("second owner"))]))],
    )
    .await;

    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    engine.open_for_cold_boot(1).unwrap();
    engine.open_for_cold_boot(2).unwrap();

    assert_eq!(
        engine.owner(),
        Some(1),
        "the cold door re-bound a process that already had an owner"
    );
    assert!(
        RowStream::<TestNote>::new(engine.clone())
            .find("n2")
            .unwrap()
            .is_none(),
        "…and served the other identity's rows"
    );
}

// MARK: - RawJournalRetryTests (3)
//
// Retry and backoff graded from the RAW journal and from the engine's own cold
// stamp — never through `pending_ops()`, which is the accessor the engine
// wrote with. `cold_window: 0` expresses "no cooling" exactly, so nothing here
// sleeps.

/// The cold stamp is PER LANE by design: a fat bulk batch is what dies on a
/// slow link, and the one-row message that would have succeeded must not wait
/// out ITS backoff.
#[tokio::test]
async fn failed_prefix_cools_both_priorities_until_explicit_retry() {
    let store = store("raw-retry-lane-cold");
    let transport = StubTransport::new();
    let mut opts = options(engine_directory(), transport.clone());
    opts.cold_window = Duration::from_secs(60);
    let engine = engine_with(store.clone(), OWNER, opts);

    engine
        .save_row("notes", "import", None, &fields(&[("title", text("bulk"))]))
        .await
        .unwrap();
    engine
        .lane(ReplicaLane::Interactive, async {
            engine
                .save_row(
                    "notes",
                    "message",
                    None,
                    &fields(&[("title", text("typed"))]),
                )
                .await
                .unwrap();
        })
        .await;
    transport.fail_pushes(true);

    assert!(
        engine.drain_lane(ReplicaLane::Bulk).await.is_err(),
        "a dead wire must surface from an explicit drain"
    );

    assert!(
        engine.is_cold_for_testing(ReplicaLane::Bulk).await,
        "the lane that proved the wire dead is the lane that cools"
    );
    assert!(
        engine.is_cold_for_testing(ReplicaLane::Interactive).await,
        "the interactive lane inherited a backoff it never earned"
    );

    // The wire recovers for everyone, but bulk is still inside its window:
    // `drain_if_warm` must ship the message and leave the import owed.
    transport.fail_pushes(false);
    engine.drain_if_warm().await.unwrap();

    assert!(transport.pushed_row_ids().is_empty());
    assert_eq!(raw_journal(&store).len(), 2);
    engine.drain().await.unwrap();
    assert_eq!(transport.pushed_row_ids(), ["import", "message"]);
    assert!(raw_journal(&store).is_empty());
}

/// A transport failure is RETRYABLE, never a verdict: the entries keep their
/// ids, their lanes and their exact BYTES, so the retry is the same request.
/// `cold_window: 0` is what makes this sleep-free — the stamp is set to "now",
/// so the very next cold check is already false.
#[tokio::test]
async fn a_failed_drain_keeps_every_byte_and_the_successful_retry_is_the_same_request() {
    let store = store("raw-retry-same-bytes");
    let transport = StubTransport::new();
    let mut opts = options(engine_directory(), transport.clone());
    opts.cold_window = Duration::ZERO;
    let engine = engine_with(store.clone(), OWNER, opts);

    for index in 0..3 {
        engine
            .save_row(
                "notes",
                &format!("n{index}"),
                None,
                &fields(&[("title", text(&format!("t{index}")))]),
            )
            .await
            .unwrap();
    }
    let before = raw_journal(&store);
    assert_eq!(before.len(), 3);

    transport.fail_pushes(true);
    assert!(
        engine.drain_lane(ReplicaLane::Bulk).await.is_err(),
        "the dead wire never surfaced"
    );

    assert_eq!(
        raw_journal(&store),
        before,
        "a severed wire rewrote the journal — ids, lanes or bytes moved when nothing was judged"
    );
    assert!(
        before.iter().all(|entry| entry.reason.is_none()),
        "no connection is not a refusal"
    );
    let failed = transport.push_requests();
    assert_eq!(failed.len(), 1);

    // Zero cold window: the retry is admitted immediately, no sleep.
    assert!(!engine.is_cold_for_testing(ReplicaLane::Bulk).await);
    transport.fail_pushes(false);
    engine.drain_if_warm().await.unwrap();

    assert_eq!(
        transport.push_requests(),
        [failed[0].clone(), failed[0].clone()],
        "the retry must re-present exactly the operations that failed"
    );
    assert_eq!(
        transport
            .pushed_ops()
            .into_iter()
            .map(|op| op.row_id)
            .collect::<Vec<_>>(),
        ["n0", "n1", "n2"]
    );
    assert!(raw_journal(&store).is_empty());
    assert!(
        !engine.is_cold_for_testing(ReplicaLane::Bulk).await,
        "a successful drain leaves no stamp behind"
    );
}

/// The sign-out flush is the one drain whose failure destroys work: what it
/// leaves behind, the retirement wipes. Incremental acks bound the loss to one
/// chunk. Graded raw, because this is the count that decides whether a user's
/// typed message survives.
#[tokio::test]
async fn a_mid_flush_chunk_death_strands_only_the_unacked_chunks() {
    let store = store("raw-retry-flush-chunks");
    let transport = StubTransport::new();
    transport.fail_pushes_after(1);
    let engine = engine(store.clone(), transport.clone());

    for index in 0..250 {
        engine
            .save_row(
                "notes",
                &format!("n{index:03}"),
                None,
                &fields(&[("title", text(&format!("t{index}")))]),
            )
            .await
            .unwrap();
    }

    engine.seal().await.unwrap();
    assert!(
        engine.seal_and_drain(transport.clone()).await.is_err(),
        "the second chunk's transport death never surfaced"
    );

    let owed = raw_journal(&store);
    assert_eq!(
        owed.len(),
        150,
        "chunk 1's 100 ops acked incrementally and left the journal; only the unsent 150 may remain"
    );
    assert!(
        owed.iter().all(|entry| entry.reason.is_none()),
        "a flush failure parks nothing — the wipe is what threatens it"
    );
    assert!(!owed[0].id.is_empty());
    // The survivors are the TAIL: the first 100 are gone, in journal order.
    assert_eq!(
        owed_payload_row_ids(&store).first().map(String::as_str),
        Some("n100"),
        "the acked prefix must be the prefix, not an arbitrary 100"
    );
}

// MARK: - ChunkedDrainTests (2)
//
// The drain ships BOUNDED chunks with INCREMENTAL acks: at most 100 operations
// per push, and every acked chunk leaves the journal for good, so a mid-drain
// transport death costs one chunk, never the whole backlog.

async fn fill(engine: &ReplicaEngine, count: usize) {
    for index in 0..count {
        engine
            .save_row(
                "notes",
                &format!("n{index:03}"),
                None,
                &fields(&[("title", text(&format!("t{index}")))]),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn drain_ships_bounded_chunks_with_incremental_acks() {
    let store = store("chunked-drain-acks");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    // The load-bearing assertion is the MID-DRAIN one: pending counts observed
    // at each chunk's push time. Chunk 2 must find chunk 1's 100 entries
    // already GONE from the journal (applied before the next chunk ships),
    // chunk 3 must find 200 gone. An end-state check alone stays green under
    // an apply-everything-at-the-end drain.
    let observed = PendingLog::new();
    let hook = HookOutcome::new();
    transport.on_push({
        let store: Arc<ReplicaStateStore> = (*store).clone();
        let observed = observed.clone();
        let hook = hook.clone();
        move |_ops| {
            let store = store.clone();
            let observed = observed.clone();
            let hook = hook.clone();
            Box::pin(async move {
                match store.pool().read(|db| Ok(store.pending(db)?.len())) {
                    Ok(count) => observed.append(count),
                    // A failed read here would otherwise vanish into a sentinel
                    // and read as a passing chunk boundary.
                    Err(error) => hook.record(error),
                }
            }) as BoxFuture<'static, ()>
        }
    });

    fill(&engine, 250).await;
    engine.drain().await.unwrap();

    assert_eq!(
        hook.failure(),
        None,
        "the mid-push observation itself failed; the counts below prove nothing"
    );
    let sizes: Vec<usize> = transport
        .pushed_batches()
        .into_iter()
        .map(|batch| batch.len())
        .collect();
    assert_eq!(
        sizes,
        [100, 100, 50],
        "the whole queue drains as bounded chunks, in order"
    );
    assert_eq!(
        observed.counts(),
        [250, 150, 50],
        "each chunk ships only after the previous chunk's acks left the journal"
    );

    assert!(
        store.peek_pending().unwrap().is_empty(),
        "every acked chunk leaves the journal"
    );
}

#[tokio::test]
async fn mid_drain_failure_keeps_only_unacked_chunks_pending() {
    let store = store("chunked-drain-failure");
    let transport = StubTransport::new();
    transport.fail_pushes_after(1);
    let engine = engine(store.clone(), transport.clone());

    fill(&engine, 250).await;

    assert!(
        engine.drain().await.is_err(),
        "the second chunk's transport death must surface"
    );

    assert_eq!(
        store.peek_pending().unwrap().len(),
        150,
        "chunk 1's 100 ops acked INCREMENTALLY and left the journal; only the unsent 150 stay — \
         a lost response can no longer strand the whole backlog"
    );
}

// MARK: - DrainBeforePullTests (2)

/// Drain-before-pull: a pending create followed by an immediate
/// pull reaches the wire as push FIRST, pull second — so the echo is in the
/// answer and a response can never clobber writes it never saw.
#[tokio::test]
async fn push_is_observed_before_pull() {
    let store = store("drain-before-pull");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine.pull_once("user").await.unwrap();

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    engine.pull_once("user").await.unwrap();

    let events = transport.events();
    assert_eq!(
        events.len(),
        3,
        "expected the first pull, then a push and a pull, saw {events:?}"
    );
    match &events[1] {
        WireEvent::Push { ids } => assert_eq!(ids.len(), 1),
        other => panic!("the drain must reach the wire before the pull, saw {other:?}"),
    }
    match &events[2] {
        WireEvent::Pull { shard, .. } => assert_eq!(shard, "user"),
        other => panic!("the pull follows the drain, saw {other:?}"),
    }
}

#[tokio::test]
async fn empty_journal_pulls_without_a_push() {
    let store = store("drain-before-pull-empty");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine.pull_once("user").await.unwrap();

    assert_eq!(
        transport.push_count(),
        0,
        "a pure-read pull costs one request, not two"
    );
    assert_eq!(transport.pull_count(), 1);
}

/// Push and pull are independent: a push the server refuses — one bad
/// operation fails the whole request, on every retry — reaches health and
/// never keeps the shard from receiving.
#[tokio::test]
async fn a_push_the_server_refuses_does_not_stop_the_pull() {
    let store = store("drain-before-pull-refused");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine.pull_once("user").await.unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    transport.fail_pushes_with(Some(crate::ReplicaError::Protocol {
        code: "operation data must be an object".into(),
        message: "HTTP 400".into(),
    }));
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n2", "from the server", None)], "c2", false),
    );

    let published = engine
        .pull_until_caught_up(Some(&["user".to_owned()]))
        .await
        .unwrap();

    assert_eq!(published, 1);
    assert_eq!(
        store.peek_snapshot("notes", "n2").unwrap().unwrap().data["title"],
        text("from the server")
    );
    assert_eq!(
        store.peek_pending().unwrap().len(),
        1,
        "the refused submission stays frozen for its own retry"
    );
    assert_eq!(
        engine.health.last_failure().unwrap().operation,
        "push before pull"
    );
}

// MARK: - ConcurrentDrainTests (1)

/// Two drains racing (the scheduled push meeting an explicit drain) must
/// coalesce onto ONE flight. A double-sent `row.create` reaches the server
/// twice: the first is accepted, the second collides → rejected → the
/// revert deletes the freshly-created row out from under the user.
#[tokio::test]
async fn concurrent_drains_push_each_entry_once() {
    let store = store("concurrent-drain");
    let transport = StubTransport::new();
    transport.delay_pushes(Duration::from_millis(50));
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();

    let (first, second) = tokio::join!(engine.drain(), engine.drain());
    let first_verdicts = first.unwrap();
    let second_verdicts = second.unwrap();

    let pushed = transport
        .pushed_ops()
        .into_iter()
        .filter(|op| op.row_id == "n1")
        .count();
    assert_eq!(
        pushed, 1,
        "the same journal entry went to the wire {pushed} times — concurrent drains must share one flight"
    );
    // The joiner is answered by the flight it joined, not by an empty second
    // selection — otherwise a caller reads "nothing was owed".
    // A caller reaching the bulk lane after its publication can observe an
    // empty queue. Every returned result must refer to the one accepted write.
    for verdict in first_verdicts.iter().chain(&second_verdicts) {
        assert_eq!(verdict.outcome, crate::wire::VerdictOutcome::Accepted);
    }
    assert!(!first_verdicts.is_empty() || !second_verdicts.is_empty());
    assert!(store.peek_pending().unwrap().is_empty());
}

// MARK: - DrainGovernorTests (1)
