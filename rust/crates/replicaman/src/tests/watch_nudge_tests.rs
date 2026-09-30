//! The observation plane, transliterated file by file.
//!
//! - `WatchTests` (7)
//! - `ReducerWakeTests` (4)
//! - `NudgeThrottleTests` (3)
//! - `NudgeCoalescingTests` (1)
//! - `NudgeSerialityTests` (1)
//! - `ParkedWatchTests` (1)
//!
//! Upstream polls (`eventually` / `until`) because XCTest cannot see a GRDB
//! `ValueObservation` arm. Rust's watch streams are `futures::Stream`s the test
//! drives itself, so the arming is a POLL here, not a sleep: one poll drives
//! the loop to its parked state, and `assert_quiet` — a single `poll!` at a
//! point where the commit under test has already returned — is how this file
//! spells upstream's "the total is exactly N". Only the nudger suites keep a
//! timed shape, because wall-clock throttling IS their subject.

use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use crate::error::ReplicaError;
use crate::models::RowStream;
use crate::nudger::ReplicaNudger;
use crate::store::JournalRow;
use crate::tests::support::*;

// MARK: - Local fixtures
//
// Anything here that earns a second caller belongs in `support.rs`.

/// The next value a stream yields, or a named failure at the deadline — the
/// bounded-wait discipline of upstream's `eventually`, for observations whose
/// payload IS the assertion.
async fn next_within<S>(stream: &mut S, reason: &str) -> S::Item
where
    S: Stream + Unpin,
{
    match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
        Ok(Some(item)) => item,
        Ok(None) => panic!("the stream ended before it delivered: {reason}"),
        Err(_) => panic!("condition never held: {reason}"),
    }
}

/// The next yielded value matching `predicate` — upstream's
/// `ParkedWatchTests.next(_:timeout:where:)`. Intermediate duplicates and
/// unrelated commits may interleave, so the assertion is "the committed state
/// ARRIVES", never "it is the n-th value".
async fn next_where<S, P>(stream: &mut S, reason: &str, mut predicate: P) -> S::Item
where
    S: Stream + Unpin,
    P: FnMut(&S::Item) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let Ok(next) = tokio::time::timeout(Duration::from_millis(250), stream.next()).await else {
            continue;
        };
        let Some(value) = next else {
            panic!("the stream ended before it delivered: {reason}");
        };
        if predicate(&value) {
            return value;
        }
    }
    panic!("condition never held: {reason}");
}

/// One poll, and it must find nothing. This is how the file spells upstream's
/// exact-count assertions: every commit under test has already committed by the
/// time this runs (a write returns after its transaction, and the pool notifies
/// its watchers on the commit path only), so a signal the engine wrongly
/// produced is READY here, not merely late.
async fn assert_quiet<S>(stream: &mut S, reason: &str)
where
    S: Stream + Unpin,
{
    assert!(futures::poll!(stream.next()).is_pending(), "{reason}");
}

/// `support::until_within` over a synchronous predicate — the counters this
/// file waits on (a tally, a transport count) are all plain reads.
async fn wait_for(reason: &str, timeout: Duration, mut condition: impl FnMut() -> bool) {
    until_within(reason, timeout, &mut || std::future::ready(condition())).await;
}

// MARK: - WatchTests (7)
//
// B8 — `watch`: post-checkpoint change signal per stream. Signals fire only
// after COMMIT — a rolled-back checkpoint fires nothing; the typed watch
// delivers fresh rows.

/// The baseline belongs to the observer that asked for it. A change-only
/// observer must never see one — it would double-count the world it already
/// holds.
#[tokio::test]
async fn signal_can_include_the_committed_baseline_without_changing_the_default() {
    let store = store("watch-baseline");
    let engine = engine(store.clone(), StubTransport::new());

    let mut change_only = Box::pin(engine.watch_signal("notes", false));
    let mut with_initial = Box::pin(engine.watch_signal("notes", true));

    // Arming is observable: one poll drives each loop to its parked state. The
    // include-initial observer answers with the committed baseline; the
    // change-only one must answer with nothing at all.
    assert!(
        matches!(futures::poll!(with_initial.next()), Poll::Ready(Some(()))),
        "the committed baseline was not delivered"
    );
    assert_quiet(
        &mut change_only,
        "a baseline reached a change-only observer",
    )
    .await;

    // One commit, seen by BOTH.
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("one"))]))
        .await
        .unwrap();

    next_within(
        &mut with_initial,
        "the include-initial observer never reported the commit",
    )
    .await;
    next_within(
        &mut change_only,
        "the change-only observer never reported the commit",
    )
    .await;

    // The totals, exactly: 2 and 1. A change-only observer that leaked its
    // baseline would have a second value ready right here.
    assert_quiet(
        &mut with_initial,
        "the include-initial observer reported one commit more than once",
    )
    .await;
    assert_quiet(
        &mut change_only,
        "a baseline reached a change-only observer",
    )
    .await;
}

/// A faulted checkpoint rolls back — it must NOT signal. The committed one that
/// follows must, exactly once.
#[tokio::test]
async fn signal_fires_after_commit_and_not_on_rolled_back_checkpoints() {
    let store = store("watch-rollback");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    // `include_initial` makes ARMING observable: the baseline yield is the
    // proof the observation is live, which a sleep only ever assumed.
    let mut signals = Box::pin(engine.watch_signal("notes", true));
    assert!(
        matches!(futures::poll!(signals.next()), Poll::Ready(Some(()))),
        "the observation never armed"
    );

    engine
        .set_checkpoint_fault(Some(Arc::new(|| {
            Err(ReplicaError::Storage("injected fault".into()))
        })))
        .await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "half", None)], "5:", false),
    );
    assert!(
        engine.pull_once("user").await.is_err(),
        "the injected fault must surface"
    );
    assert_quiet(
        &mut signals,
        "a rolled-back checkpoint fired a signal — consumers would read uncommitted state",
    )
    .await;

    // A committed checkpoint follows: exactly one more signal, no more.
    engine.set_checkpoint_fault(None).await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "landed", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();
    next_within(&mut signals, "commit did not signal the stream").await;
    assert_quiet(
        &mut signals,
        "one committed checkpoint fired more than one signal",
    )
    .await;
}

/// The typed watch hands the consumer models, not a nudge to re-query.
#[tokio::test]
async fn typed_watch_delivers_fresh_rows() {
    let store = store("watch-typed");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let notes = RowStream::<TestNote>::new(engine.clone());

    let mut watch = Box::pin(notes.watch());
    let baseline = next_within(&mut watch, "the typed-watch baseline was not delivered").await;
    assert!(
        baseline.is_empty(),
        "the baseline picture is the empty store"
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "one", None), note("n2", "two", None)],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let seen = next_where(
        &mut watch,
        "the typed watch never saw the imported rows",
        |models: &Vec<TestNote>| models.len() == 2,
    )
    .await;
    assert_eq!(
        seen.iter()
            .map(|model| model.id.clone())
            .collect::<Vec<_>>(),
        ["n1", "n2"]
    );
}

/// A stream's signal is its own: another stream's commit re-evaluates the
/// observation and must yield nothing.
#[tokio::test]
async fn signal_does_not_ring_for_another_streams_commit() {
    let store = store("watch-cross-stream");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    let mut notes_signals = Box::pin(engine.watch_signal("notes", true));
    let mut boards_signals = Box::pin(engine.watch_signal("boards", true));
    assert!(
        matches!(futures::poll!(notes_signals.next()), Poll::Ready(Some(()))),
        "the notes signal baseline was not delivered"
    );
    assert!(
        matches!(futures::poll!(boards_signals.next()), Poll::Ready(Some(()))),
        "the boards signal baseline was not delivered"
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "Trip", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();
    next_within(
        &mut notes_signals,
        "the notes commit did not ring its own signal",
    )
    .await;

    // A second notes commit: by the time IT is delivered, the boards observer
    // has been re-evaluated twice on the same pool and must still hold nothing
    // but its baseline.
    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("again"))]))
        .await
        .unwrap();
    next_within(&mut notes_signals, "the second notes commit did not ring").await;
    assert_quiet(
        &mut boards_signals,
        "a commit on another stream rang this one's signal",
    )
    .await;
}

/// The signal is the durable change sequence, not a content fingerprint: a
/// same-shape rename still rings.
#[tokio::test]
async fn same_weight_content_change_rings_its_own_signal() {
    let store = store("watch-same-weight");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "Trip", None)], "5:", false),
    );
    engine.pull_once("user").await.unwrap();

    let mut signals = Box::pin(engine.watch_signal("notes", true));
    assert!(
        matches!(futures::poll!(signals.next()), Poll::Ready(Some(()))),
        "the signal baseline was not delivered"
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "Trap", None)], "6:", false),
    );
    engine.pull_once("user").await.unwrap();

    next_within(
        &mut signals,
        "the same-weight rename did not ring its stream",
    )
    .await;
}

/// The typed watch is scoped the same way its signal is.
#[tokio::test]
async fn typed_watch_does_not_deliver_for_another_streams_commit() {
    let store = store("watch-typed-cross-stream");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let notes = RowStream::<TestNote>::new(engine.clone());

    let mut watch = Box::pin(notes.watch());
    let baseline = next_within(&mut watch, "the typed-watch baseline was not delivered").await;
    assert!(baseline.is_empty());

    transport.queue_pull(
        "global",
        ScriptedPull::new(
            vec![row_set(
                "assets",
                "a1",
                None,
                fields(&[("kind", text("font"))]),
            )],
            "1:",
            false,
        ),
    );
    engine.pull_once("global").await.unwrap();

    // The global-shard commit happened FIRST. A notes commit follows; once its
    // delivery lands, anything the assets commit wrongly produced has already
    // been delivered too, so the total is exact.
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    let picture = next_within(
        &mut watch,
        "the typed watch never reported its own stream's commit",
    )
    .await;
    assert_eq!(
        picture
            .iter()
            .map(|model| model.id.clone())
            .collect::<Vec<_>>(),
        ["n1"]
    );
    assert_quiet(
        &mut watch,
        "another stream's commit was delivered to this typed watch",
    )
    .await;
}

/// Regression pin for the removed aggregate fingerprint: a peer is a random
/// u64 kept as an i64 bit pattern, so summing two peer values overflows and
/// terminates the observation. The durable stream sequence must signal without
/// inspecting or aggregating those values.
#[tokio::test]
async fn watch_survives_large_peer_docs() {
    let store = store("watch-large-peers");
    let transport = StubTransport::new();
    let mut opts = options(engine_directory(), transport.clone());
    opts.peer_minter = sequential_minter((i64::MAX as u64) - 1);
    let engine = engine_with(store.clone(), OWNER, opts);

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                doc_snapshot("boards", "b1", "stub@1", b"SNAP-1", fields(&[])),
                doc_snapshot("boards", "b2", "stub@1", b"SNAP-2", fields(&[])),
            ],
            "5:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    let first = store.peek_doc("boards", "b1").unwrap().unwrap();
    let second = store.peek_doc("boards", "b2").unwrap().unwrap();
    assert!(
        first.peer > (i64::MAX as u64) / 2 && second.peer > (i64::MAX as u64) / 2,
        "precondition: both peers must be large enough that their sum exceeds i64::MAX"
    );

    let mut signals = Box::pin(engine.watch_signal("boards", true));
    assert!(
        matches!(futures::poll!(signals.next()), Poll::Ready(Some(()))),
        "the observation never armed over the large-peer docs"
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![doc_delta("boards", "b1", 1, "stub@1", b"+d1")],
            "6:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();

    next_within(
        &mut signals,
        "the committed write never signalled — the docs fingerprint overflowed and ended the observation",
    )
    .await;
}

// MARK: - ReducerWakeTests (4)
//
// The sync gate on the path production actually uses: the engine's OWN
// delivery. Every test here that grades the automatic path writes and then
// WAITS — it never calls `drain()` to make its own assertion true.
//
// `flavor = "multi_thread"` is load-bearing wherever a row stays gated: the
// scheduled-push loop is D6-live (see the last case in this group), and on a
// current-thread runtime its yield-free spin would starve the test's own task.

// MARK: - NudgeThrottleTests (3)
//
// The doorbell throttle: during a long server job the signal rate is ~5/s
// per job, and coalescing alone still runs back-to-back pulls. A trailing-edge
// throttle caps doorbell-triggered syncs at ~1 per window; the LAST doorbell
// always lands, so the final state is caught up.

/// A render-storm burst: far more doorbells than windows.
#[tokio::test]
async fn rapid_nudges_cost_at_most_one_pull_per_window() {
    let store = store("nudge-throttle-burst");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    let starts = Arc::new(Mutex::new(Vec::<Instant>::new()));

    let nudger = ReplicaNudger::new(Duration::from_millis(400), Arc::new(TokioSpawner), {
        let engine = engine.clone();
        let starts = starts.clone();
        move || {
            starts.lock().push(Instant::now());
            let engine = engine.clone();
            Box::pin(async move {
                let _ = engine.pull_until_caught_up(None).await;
            })
        }
    });

    for _ in 0..20 {
        nudger.nudge(false);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Quiesce.
    until_within(
        "nudger never went quiet",
        Duration::from_secs(8),
        &mut || {
            let transport = transport.clone();
            async move {
                let before = transport.pull_count();
                if before == 0 {
                    return false;
                }
                futures_timer::Delay::new(Duration::from_millis(500)).await;
                transport.pull_count() == before
            }
        },
    )
    .await;

    let starts = starts.lock().clone();
    let gaps: Vec<Duration> = starts.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert!(
        !gaps.is_empty(),
        "the burst collapsed into a single sync; the trailing edge never ran"
    );
    for gap in gaps {
        assert!(
            gap >= Duration::from_millis(350),
            "one sync per window, not one per doorbell (gap={gap:?})"
        );
    }
}

/// A doorbell INSIDE the window must still produce a sync — late, never lost:
/// the world moved after the last pull.
#[tokio::test]
async fn trailing_edge_always_delivers_the_last_doorbell() {
    let store = store("nudge-throttle-trailing");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    let nudger = ReplicaNudger::new(Duration::from_millis(200), Arc::new(TokioSpawner), {
        let engine = engine.clone();
        move || {
            let engine = engine.clone();
            Box::pin(async move {
                let _ = engine.pull_until_caught_up(None).await;
            })
        }
    });

    // First doorbell syncs immediately and consumes the queued state.
    nudger.nudge(false);
    {
        let transport = transport.clone();
        wait_for("first sync never ran", Duration::from_secs(2), move || {
            transport.pull_count() > 0
        })
        .await;
    }
    let after_first = transport.pull_count();

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "fresh", None)], "9:", false),
    );
    nudger.nudge(false);

    {
        let store = store.clone();
        wait_for(
            "the trailing doorbell was dropped",
            Duration::from_secs(3),
            move || {
                store
                    .peek_snapshot("notes", "n1")
                    .unwrap()
                    .and_then(|row| row.data.get("title").cloned())
                    == Some(text("fresh"))
            },
        )
        .await;
    }
    assert!(transport.pull_count() > after_first);
}

/// A doorbell rung BY A TAP jumps the window. Making the person wait out a
/// throttle window designed for render chatter is how "Stop does nothing" gets
/// reported.
#[tokio::test]
async fn an_immediate_nudge_does_not_wait_out_the_window() {
    let syncs = Tally::new();
    let nudger = ReplicaNudger::new(Duration::from_secs(5), Arc::new(TokioSpawner), {
        let syncs = syncs.clone();
        move || {
            let syncs = syncs.clone();
            Box::pin(async move {
                syncs.bump();
            })
        }
    });

    nudger.nudge(false);
    {
        let syncs = syncs.clone();
        wait_for(
            "the first sync never ran",
            Duration::from_secs(2),
            move || syncs.count() == 1,
        )
        .await;
    }

    // Inside the 5 s window. A polite doorbell would sit here for seconds.
    let started = Instant::now();
    nudger.nudge(true);
    {
        let syncs = syncs.clone();
        wait_for(
            "the tapped doorbell waited out the window",
            Duration::from_secs(2),
            move || syncs.count() == 2,
        )
        .await;
    }
    assert!(started.elapsed() < Duration::from_secs(1));

    // The window is restored for the machine chatter that follows. This wait IS
    // the contract — a throttle is a statement about time, and 0.7 s inside a
    // 5 s window is what "still throttled" means.
    nudger.nudge(false);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(syncs.count(), 2, "an ordinary doorbell is still throttled");
}

// MARK: - NudgeCoalescingTests (1)

/// Doorbell coalescing: N concurrent nudges collapse into at most
/// one in-flight sync plus one queued re-run (<= 2 transport cycles); callers
/// never await.
#[tokio::test]
async fn ten_concurrent_nudges_cost_at_most_two_cycles() {
    let store = store("nudge-coalescing");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.delay_pulls(Duration::from_millis(50));

    let nudger = ReplicaNudger::new(Duration::ZERO, Arc::new(TokioSpawner), {
        let engine = engine.clone();
        move || {
            let engine = engine.clone();
            Box::pin(async move {
                let _ = engine.pull_until_caught_up(None).await;
            })
        }
    });

    for _ in 0..10 {
        nudger.nudge(false);
    }

    // Quiescence, not a settle: the count must stop moving across a window
    // LONGER than the stubbed pull latency, so an in-flight cycle cannot look
    // finished. Bounded, with a named failure.
    until_within(
        "nudger never went quiet",
        Duration::from_secs(8),
        &mut || {
            let transport = transport.clone();
            async move {
                let before = transport.pull_count();
                if before == 0 {
                    return false;
                }
                futures_timer::Delay::new(Duration::from_millis(150)).await;
                transport.pull_count() == before
            }
        },
    )
    .await;

    let shards = schema().shards().len();
    let pulls = transport.pull_count();
    assert!(pulls >= shards, "a nudge really pulls");
    assert!(
        pulls <= 2 * shards,
        "ten doorbells collapse to at most one in-flight sync plus one queued re-run (pulls={pulls})"
    );
}

// MARK: - NudgeSerialityTests (1)

/// The contract CatalogWarmer leans on: a zero-interval nudger is a pure
/// coalescer — a burst of triggers costs one in-flight sync plus at most one
/// queued re-run, and syncs NEVER overlap. Overlap is not just waste there:
/// parallel warm passes race over the same font file mid-move.
#[tokio::test]
async fn a_trigger_burst_never_overlaps_syncs_and_coalesces_to_two() {
    #[derive(Default)]
    struct ProbeState {
        running: usize,
        peak: usize,
        runs: usize,
    }

    #[derive(Default)]
    struct Probe {
        state: Mutex<ProbeState>,
    }

    impl Probe {
        fn enter(&self) {
            let mut state = self.state.lock();
            state.running += 1;
            state.peak = state.peak.max(state.running);
            state.runs += 1;
        }

        fn exit(&self) {
            self.state.lock().running -= 1;
        }

        /// `(running, peak, runs)` — one lock, so the three never disagree.
        fn sample(&self) -> (usize, usize, usize) {
            let state = self.state.lock();
            (state.running, state.peak, state.runs)
        }
    }

    let probe = Arc::new(Probe::default());
    let nudger = ReplicaNudger::new(Duration::ZERO, Arc::new(TokioSpawner), {
        let probe = probe.clone();
        move || {
            let probe = probe.clone();
            Box::pin(async move {
                probe.enter();
                futures_timer::Delay::new(Duration::from_millis(50)).await;
                probe.exit();
            })
        }
    });

    for _ in 0..7 {
        nudger.nudge(false);
    }

    // Quiescence, not a settle: wait until a sync has started and then until
    // none is running and the count has stopped moving. Bounded, and it fails by
    // timing out rather than by asserting too early.
    {
        let probe = probe.clone();
        wait_for(
            "the burst never started a sync at all",
            Duration::from_secs(5),
            move || probe.sample().2 >= 1,
        )
        .await;
    }
    until_within(
        "the nudger never came to rest",
        Duration::from_secs(5),
        &mut || {
            let probe = probe.clone();
            async move {
                let before = probe.sample().2;
                futures_timer::Delay::new(Duration::from_millis(60)).await;
                let (running, _, after) = probe.sample();
                running == 0 && after == before
            }
        },
    )
    .await;

    let (_, peak, runs) = probe.sample();
    assert_eq!(peak, 1, "syncs must never run concurrently");
    assert!(runs >= 1);
    assert!(
        runs <= 2,
        "a burst folds into the in-flight sync plus one re-run (runs={runs})"
    );
}

// MARK: - ParkedWatchTests (1)

/// The refusal ledger as a subscription: a rejected push PARKS the entry and
/// the watcher delivers the committed picture; a discard delivers the picture
/// without it. The consumer never re-queries — a triggered re-read racing the
/// discard it was meant to observe is impossible against commit-ordered values.
#[tokio::test]
async fn a_park_arrives_and_its_discard_clears_it() {
    let store = store("parked-watch");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    reject_all(&transport, "Scenes is invalid");

    let mut parked_stream = Box::pin(engine.watch_parked_ops());
    next_where(
        &mut parked_stream,
        "the empty baseline never arrived",
        Vec::is_empty,
    )
    .await;

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("t"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();

    let parked = next_where(
        &mut parked_stream,
        "the expected parked picture never arrived",
        |rows: &Vec<JournalRow>| !rows.is_empty(),
    )
    .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].parked.as_deref(), Some("Scenes is invalid"));

    engine.discard_ops(&[parked[0].id.clone()]).await.unwrap();
    next_where(
        &mut parked_stream,
        "the discard's empty picture never arrived",
        Vec::is_empty,
    )
    .await;
}

#[tokio::test]
async fn a_corrupt_observed_row_never_becomes_an_empty_picture() {
    let store = store("watch-corruption");
    let engine = engine(store.clone(), StubTransport::new());
    engine
        .save_row(
            "notes",
            "n",
            None,
            &fields(&[("title", text("Saved")), ("rank", text("a"))]),
        )
        .await
        .unwrap();
    let notes = RowStream::<TestNote>::new(engine.clone());
    let mut watch = Box::pin(notes.watch());
    assert_eq!(next_within(&mut watch, "baseline").await.len(), 1);
    store
        .pool()
        .write(|ctx| {
            store.upsert_snapshot(
                ctx,
                "notes",
                "n",
                "user",
                None,
                &fields(&[("title", text("about to corrupt"))]),
            )?;
            ctx.tx.execute(
                "UPDATE snapshots SET data = '{broken' WHERE row_id = 'n'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert_quiet(&mut watch, "corruption was presented as deletion").await;
    store
        .pool()
        .write(|ctx| {
            store.upsert_snapshot(
                ctx,
                "notes",
                "n",
                "user",
                None,
                &fields(&[("title", text("Repaired")), ("rank", text("a"))]),
            )
        })
        .unwrap();
    assert_eq!(
        next_within(&mut watch, "repaired picture").await[0]
            .title
            .as_deref(),
        Some("Repaired")
    );
}
