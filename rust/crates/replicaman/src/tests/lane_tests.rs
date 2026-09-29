//! Lane stickiness, dependency promotion, and the
//! cold window.
//!
//! - `LaneEdgeProbeTests` (5)
//! - `ColdWindowTests` (3)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::schema::ReplicaLane;
use crate::store::ReplicaStateStore;
use crate::tests::support::*;
use crate::value::ReplicaValue;

/// The lane each pending entry sits on, keyed by the row it addresses — the
/// `store.pending` + `store.lane` read the Swift cases do inline.
fn lanes_by_row(store: &ReplicaStateStore) -> HashMap<String, ReplicaLane> {
    store
        .pool()
        .read(|db| {
            let mut by_row = HashMap::new();
            for entry in store.pending(db)? {
                let op = entry.op()?;
                by_row.insert(op.row_id, store.lane(db, &entry.id)?);
            }
            Ok(by_row)
        })
        .expect("read the pending lanes")
}

// MARK: - LaneEdgeProbeTests (5)

/// `testPromotionTerminatesOnAReferenceCycle` — promotion walks op data for
/// values naming pending rows. Two rows naming each other must be followed
/// once, not forever.
#[tokio::test]
async fn promotion_terminates_on_a_reference_cycle() {
    let store = store("lane-cycle");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .save_row("notes", "A", None, &fields(&[("ref", text("B"))]))
        .await
        .expect("save A");
    engine
        .save_row("notes", "B", None, &fields(&[("ref", text("A"))]))
        .await
        .expect("save B");

    engine
        .lane(
            ReplicaLane::Interactive,
            engine.save_row("notes", "C", None, &fields(&[("ref", text("A"))])),
        )
        .await
        .expect("save C interactively");

    let lanes = lanes_by_row(&store);
    assert_eq!(lanes.get("A"), Some(&ReplicaLane::Interactive));
    assert_eq!(
        lanes.get("B"),
        Some(&ReplicaLane::Interactive),
        "the cycle is followed once, not forever"
    );
    assert_eq!(lanes.get("C"), Some(&ReplicaLane::Interactive));
}

/// `testACorruptPendingEntryDoesNotBreakTheUsersNextWrite` — promotion decodes
/// every pending entry's op, so one undecodable journal payload must not blow
/// up a NEW user write.
#[tokio::test]
async fn a_corrupt_pending_entry_does_not_break_the_users_next_write() {
    let store = store("lane-corrupt");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .save_row("notes", "ok", None, &fields(&[("title", text("t"))]))
        .await
        .expect("save the good row");

    // Poison one journal payload the way a partial write or a version skew
    // would.
    store
        .pool()
        .write(|ctx| {
            ctx.tx.execute(
                "UPDATE intents SET payload = ? WHERE id = (SELECT id FROM intents LIMIT 1)",
                rusqlite::params!["{not json at all"],
            )?;
            Ok(())
        })
        .expect("poison a journal payload");

    engine
        .lane(
            ReplicaLane::Interactive,
            engine.save_row(
                "notes",
                "message",
                None,
                &fields(&[("title", text("typed"))]),
            ),
        )
        .await
        .expect("the user's write must still land");

    let pending = store
        .pool()
        .read(|db| Ok(store.pending(db)?.len()))
        .expect("count pending");
    assert_eq!(
        pending, 2,
        "the user's write landed despite the poison entry"
    );
}

/// `testParkedEntriesAreExcludedFromLanesAndDrains` — a parked entry (rejected,
/// awaiting the user) must not be dragged onto a lane or resurrected by a
/// drain.
#[tokio::test]
async fn parked_entries_are_excluded_from_lanes_and_drains() {
    let store = store("lane-parked");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "parked", None, &fields(&[("title", text("x"))]))
        .await
        .expect("save the row that will park");
    store
        .pool()
        .write(|ctx| {
            ctx.tx.execute(
                "UPDATE intents SET state = 'refused', reason = 'refused'",
                [],
            )?;
            Ok(())
        })
        .expect("park every entry");

    engine
        .lane(
            ReplicaLane::Interactive,
            engine.save_row("notes", "live", None, &fields(&[("ref", text("parked"))])),
        )
        .await
        .expect("save the live row");
    engine
        .drain_lane(ReplicaLane::Interactive)
        .await
        .expect("drain the interactive lane");

    assert_eq!(
        transport.pushed_row_ids(),
        vec!["live".to_string()],
        "a parked entry is not resurrected by promotion"
    );
}

/// `testPromotionFindsIdsNestedInsideArraysAndObjects` — a row reference can
/// sit inside an array or a nested object, not only in a flat string column.
#[tokio::test]
async fn promotion_finds_ids_nested_inside_arrays_and_objects() {
    let store = store("lane-nested");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .save_row("notes", "in-array", None, &fields(&[("title", text("a"))]))
        .await
        .expect("save in-array");
    engine
        .save_row("notes", "in-object", None, &fields(&[("title", text("o"))]))
        .await
        .expect("save in-object");

    let nested = fields(&[
        ("refs", ReplicaValue::Array(vec![text("in-array")])),
        (
            "meta",
            ReplicaValue::Object(fields(&[("source", text("in-object"))])),
        ),
    ]);
    engine
        .lane(
            ReplicaLane::Interactive,
            engine.save_row("notes", "doc", None, &nested),
        )
        .await
        .expect("save the nesting row interactively");

    let lanes = lanes_by_row(&store);
    assert_eq!(lanes.get("in-array"), Some(&ReplicaLane::Interactive));
    assert_eq!(lanes.get("in-object"), Some(&ReplicaLane::Interactive));
}

/// `testAStickyPromotionAlsoWalksWhatThePromotedEntryNames` — stickiness pulls
/// a row up by its ROW; what THAT row names must come with it, or the chain
/// breaks one link further down.
#[tokio::test]
async fn a_sticky_promotion_also_walks_what_the_promoted_entry_names() {
    let store = store("lane-sticky-chain");
    let engine = engine(store.clone(), StubTransport::new());

    engine
        .save_row(
            "notes",
            "dependency",
            None,
            &fields(&[("title", text("q"))]),
        )
        .await
        .expect("save the dependency");
    engine
        .save_row(
            "notes",
            "carrier",
            None,
            &fields(&[("ref", text("dependency"))]),
        )
        .await
        .expect("save the carrier");

    // Touch `carrier` from an interactive action: stickiness pulls its queued
    // create up, and `dependency` must follow.
    engine
        .lane(
            ReplicaLane::Interactive,
            engine.save_row(
                "notes",
                "carrier",
                None,
                &fields(&[("title", text("edited"))]),
            ),
        )
        .await
        .expect("touch the carrier interactively");

    let lanes = lanes_by_row(&store);
    assert_eq!(lanes.get("carrier"), Some(&ReplicaLane::Interactive));
    assert_eq!(
        lanes.get("dependency"),
        Some(&ReplicaLane::Interactive),
        "the promoted entry's own dependency cannot be left behind"
    );
}

// MARK: - ColdWindowTests (3)

/// An engine with an explicit cold window — Swift's
/// `Fixture.engine(store:transport:coldWindow:)`.
fn cold_window_engine(
    store: &StoreFixture,
    transport: Arc<StubTransport>,
    cold_window: Duration,
) -> Arc<crate::engine::ReplicaEngine> {
    let mut opts = options(engine_directory(), transport);
    opts.cold_window = cold_window;
    engine_with((*store).clone(), OWNER, opts)
}

/// `testDrainIfWarmSkipsInsideTheColdWindow` — a transport failure marks the
/// wire cold; inside the window `drain_if_warm` does not re-attempt, and the
/// entry stays pending (retryable, never parked).
#[tokio::test]
async fn drain_if_warm_skips_inside_the_cold_window() {
    let store = store("cold-inside");
    let transport = StubTransport::new();
    let engine = cold_window_engine(&store, transport.clone(), Duration::from_secs(60));

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("queued"))]))
        .await
        .expect("queue a write");
    transport.fail_pushes(true);

    engine.drain_if_warm().await.unwrap();
    assert_eq!(
        transport.push_count(),
        1,
        "the first attempt hits the wire and proves it dead"
    );

    engine.drain_if_warm().await.unwrap();
    engine.drain_if_warm().await.unwrap();
    assert_eq!(
        transport.push_count(),
        1,
        "inside the window a known-cold wire is not re-attempted"
    );

    assert!(
        engine.is_cold_for_testing(ReplicaLane::Bulk).await,
        "the skip must come from the stamp, not from an empty journal"
    );
    assert_eq!(
        store.peek_pending().expect("peek pending").len(),
        1,
        "transport failure leaves the entry pending — retryable, never parked"
    );
}

/// `testDrainIfWarmRetriesOnceTheWindowHasElapsed` — a zero window is elapsed
/// the instant it is stamped, so the next `drain_if_warm` retries.
#[tokio::test]
async fn drain_if_warm_retries_once_the_window_has_elapsed() {
    let store = store("cold-elapsed");
    let transport = StubTransport::new();
    let engine = cold_window_engine(&store, transport.clone(), Duration::ZERO);

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("queued"))]))
        .await
        .expect("queue a write");
    transport.fail_pushes(true);

    engine.drain_if_warm().await.unwrap();
    assert_eq!(transport.push_count(), 1);
    assert!(
        !engine.is_cold_for_testing(ReplicaLane::Bulk).await,
        "a zero window is elapsed the instant it is stamped"
    );

    engine.drain_if_warm().await.unwrap();
    assert_eq!(
        transport.push_count(),
        2,
        "past the window the drain retries"
    );
    assert_eq!(store.peek_pending().expect("peek pending").len(), 1);
}

/// `testExplicitDrainAlwaysAttempts` — reconnect's deliberate drains never
/// skip, cold or not, and a success warms the wire again.
#[tokio::test]
async fn explicit_drain_always_attempts() {
    let store = store("cold-explicit");
    let transport = StubTransport::new();
    let engine = cold_window_engine(&store, transport.clone(), Duration::from_secs(60));

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("queued"))]))
        .await
        .expect("queue a write");
    transport.fail_pushes(true);

    engine.drain_if_warm().await.unwrap();
    assert_eq!(transport.push_count(), 1);

    assert!(
        engine.drain().await.is_err(),
        "a dead wire surfaces from the explicit drain"
    );
    assert_eq!(
        transport.push_count(),
        2,
        "explicit drains never skip, cold or not"
    );

    transport.fail_pushes(false);
    assert!(engine.is_cold_for_testing(ReplicaLane::Bulk).await);
    engine.drain().await.expect("drain over a live wire");
    assert_eq!(store.peek_pending().expect("peek pending").len(), 0);
    assert!(
        !engine.is_cold_for_testing(ReplicaLane::Bulk).await,
        "success warms the wire"
    );
}
