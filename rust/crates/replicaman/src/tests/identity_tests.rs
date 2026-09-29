//! The identity-lifecycle group, transliterated file by file.
//!
//! - `OwnerMergeTests` (4)
//! - `IdentityTransitionFenceTests` (5)
//! - `OwnGuestIdentityRebindTests` (4)
//! - `ResidualIdentityTests` (4)
//! - `ResidualBehaviorTests` (2)
//!
//! These cases guard the boundary between two people's worlds, so the
//! assertions about what must NOT be visible after a switch are load-bearing:
//! an unpushed op that survived a retirement would ride the next identity's
//! bearer, and a guest `userId` that survived a merge would be refused (or
//! restored) under the target account's key.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use parking_lot::Mutex;

use crate::error::ReplicaError;
use crate::models::{ReplicaCreateStamp, RowStream};
use crate::preimage::ReplicaPreimage;
use crate::schema::ReplicaLane;
use crate::store::{JournalRow, ReplicaStateStore};
use crate::tests::support::*;
use crate::transport::{BoxFuture, ReplicaTransport};
use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::{ReplicaOp, ReplicaVerdict, verb};

/// The two identities every merge case runs between — upstream's
/// `guestUserId` / `targetUserId`.
const GUEST_USER_ID: i64 = 701;
const TARGET_USER_ID: i64 = 902;

// MARK: - Local fixtures
//
// Everything below is private to this file. The gate is the Rust half of
// upstream's `AsyncGate`.

/// Upstream's `AsyncGate` / `RebindGate`: a hold a transport hook parks in,
/// plus the arrival signal the test waits on. Timing sleeps cannot prove that
/// a seal waited for an outgoing flight; holding the flight AT the transport
/// seam can.
#[derive(Default)]
/// A wire that refuses everything — upstream's `CancellationTransport`. Swift
/// throws `CancellationError`, which is not a `ReplicaError`; the crate's
/// error vocabulary is closed, so the refusal rides `Transport` and the test
/// asserts the exact value instead of the Swift type.
struct CancellationTransport;

impl ReplicaTransport for CancellationTransport {
    fn exchange(
        &self,
        _: crate::ReplicaEndpoint,
        _: Vec<u8>,
    ) -> BoxFuture<'_, crate::ReplicaResult<Vec<u8>>> {
        Box::pin(async { Err(ReplicaError::Transport("cancelled (stub)".into())) })
    }
}

/// A row owner as the wire carries it: a plain JSON number.
fn owner_value(id: i64) -> ReplicaValue {
    ReplicaValue::Number(id as f64)
}

/// The server's copy of a note, owned by `owner`.
fn server_row(id: &str, owner: i64, title: &str) -> ScriptedFrame {
    row_set(
        "notes",
        id,
        None,
        fields(&[("title", text(title)), ("userId", owner_value(owner))]),
    )
}

fn snapshot_owner(store: &ReplicaStateStore, id: &str) -> Option<ReplicaValue> {
    store
        .peek_snapshot("notes", id)
        .expect("read the snapshot")
        .and_then(|row| row.data.get("userId").cloned())
}

fn journal_by_row(rows: Vec<JournalRow>) -> HashMap<String, JournalRow> {
    rows.into_iter()
        .map(|row| (row.op().expect("the entry decodes").row_id.clone(), row))
        .collect()
}

/// The owner named by an entry's revert record — `None` for `absent`.
fn preimage_owner(entry: Option<&JournalRow>) -> Option<ReplicaValue> {
    let raw = entry
        .expect("the entry is present")
        .preimage
        .as_ref()
        .expect("the entry carries a preimage");
    match ReplicaPreimage::parse(raw).expect("the preimage decodes") {
        ReplicaPreimage::Absent => None,
        ReplicaPreimage::Fields { values, .. } => values.get("userId").cloned(),
        ReplicaPreimage::Row { data, .. } => data.get("userId").cloned(),
    }
}

// MARK: - OwnerMergeTests (4)
//
// Guest adoption keeps a recovery source and publishes a separate account copy.

/// `testMergeMovesTheFileAndLeavesNothingBehindUnderTheGuest`
#[tokio::test]
async fn merge_keeps_the_guest_file_as_recovery_and_opens_the_account_copy() {
    let directory = temp_directory("owner-merge-move");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());
    engine.open(GUEST_USER_ID).await.unwrap();
    engine
        .save_row(
            "notes",
            "kept",
            None,
            &fields(&[
                ("title", text("mine")),
                ("userId", owner_value(GUEST_USER_ID)),
            ]),
        )
        .await
        .unwrap();

    engine.seal().await.unwrap();
    engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap();
    engine.unseal().await;

    assert_eq!(engine.owner(), Some(TARGET_USER_ID));
    assert!(
        directory
            .path()
            .join(format!("replica-{TARGET_USER_ID}.sqlite"))
            .exists()
    );
    let guest = directory
        .path()
        .join(format!("replica-{GUEST_USER_ID}.sqlite"));
    assert!(
        guest.exists(),
        "the source remains intact until the host has persisted adoption"
    );
    let notes = RowStream::<TestNote>::new(engine.clone());
    assert_eq!(
        notes.find("kept").unwrap().and_then(|note| note.title),
        Some("mine".to_owned()),
        "a merge preserves the local world"
    );
}

/// `testAWatcherHeldAcrossTheMergeKeepsServingThePreservedWorld` — the
/// preserved-world contract as the person experiences it: they signed in while
/// looking at their project, and it is still on screen after.
#[tokio::test]
async fn a_watcher_held_across_the_merge_keeps_serving_the_preserved_world() {
    let directory = temp_directory("owner-merge-watcher");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());
    engine.open(GUEST_USER_ID).await.unwrap();
    engine
        .save_row(
            "notes",
            "before",
            None,
            &fields(&[("title", text("guest"))]),
        )
        .await
        .unwrap();

    let pictures: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let watcher = tokio::spawn({
        let engine = engine.clone();
        let pictures = pictures.clone();
        async move {
            let mut rows = Box::pin(RowStream::<TestNote>::new(engine).watch());
            while let Some(picture) = rows.next().await {
                let mut ids: Vec<String> = picture.into_iter().map(|note| note.id).collect();
                ids.sort();
                pictures.lock().push(ids);
            }
        }
    });
    let last_is = |expected: &[&str]| -> bool {
        let expected: Vec<String> = expected.iter().map(|id| (*id).to_owned()).collect();
        pictures.lock().last() == Some(&expected)
    };
    until("the watcher never rendered the guest's world", || {
        let held = last_is(&["before"]);
        async move { held }
    })
    .await;

    engine.seal().await.unwrap();
    engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap();
    engine.unseal().await;

    until(
        "the watcher lost the preserved world across the merge",
        || {
            let held = last_is(&["before"]);
            async move { held }
        },
    )
    .await;
    engine
        .save_row(
            "notes",
            "after",
            None,
            &fields(&[("title", text("account"))]),
        )
        .await
        .unwrap();
    until(
        "the watcher never picked up a write made after the merge",
        || {
            let held = last_is(&["after", "before"]);
            async move { held }
        },
    )
    .await;
    assert!(
        !pictures.lock().iter().any(Vec::is_empty),
        "a preserved world must never blink through empty — that is the wipe the merge exists to avoid"
    );
    watcher.abort();
}

/// `testMergeRefusesWithoutTheBarrierAndOnAnUnrelatedSourceOwner`
#[tokio::test]
async fn merge_refuses_without_the_barrier_and_on_an_unrelated_source_owner() {
    let directory = temp_directory("owner-merge-refusal");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());
    engine.open(GUEST_USER_ID).await.unwrap();

    assert_eq!(
        engine
            .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
            .await
            .unwrap_err(),
        ReplicaError::IdentityTransitionRequired,
        "an unsealed engine admitted an identity rewrite"
    );

    engine.seal().await.unwrap();
    let error = engine
        .adopt_merged(12345, TARGET_USER_ID)
        .await
        .unwrap_err();
    assert!(
        matches!(error, ReplicaError::Storage(_)),
        "the merge accepted a source owner this process never held: {error}"
    );
    assert_eq!(
        engine.owner(),
        Some(GUEST_USER_ID),
        "a refused merge leaves the binding where it was"
    );
}

/// `testAReplayedMergeIsANoOp` — recovery replays `completeSessionTransition`
/// after a death anywhere in it. A merge whose work already landed must be a
/// no-op, not a failure — otherwise the replay strands the person behind a boot
/// that can never finish.
#[tokio::test]
async fn a_replayed_merge_is_a_no_op() {
    let directory = temp_directory("owner-merge-replay");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());
    engine.open(GUEST_USER_ID).await.unwrap();
    engine
        .save_row("notes", "kept", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();

    engine.seal().await.unwrap();
    engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap();
    engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap();
    engine.unseal().await;

    assert_eq!(engine.owner(), Some(TARGET_USER_ID));
    assert_eq!(
        RowStream::<TestNote>::new(engine.clone())
            .find("kept")
            .unwrap()
            .and_then(|note| note.title),
        Some("mine".to_owned())
    );
}

// MARK: - IdentityTransitionFenceTests (5)
//
// The identity boundary lives inside the engine because generated CRUD schedules
// transport without passing back through the app host. These cases hold real
// engine flights at the transport seam: timing sleeps cannot prove that
// begin-transition waited, or that an automatic push was covered.

/// `testTransitionWaitsForAutomaticPushAndRejectsPostWipeWritesUntilResume`
#[tokio::test]
async fn transition_waits_for_automatic_push_and_rejects_post_wipe_writes_until_resume() {
    let store = store("fence-automatic-push");
    let transport = StubTransport::new();
    let gate = Gate::new();
    hold_pushes(&transport, &gate);
    // Built like upstream's bare initializer: `automatically_push_writes` is
    // PRODUCTION-true here, so the delivery under test is the engine's own.
    let mut engine_options = options(engine_directory(), transport.clone());
    engine_options.automatically_push_writes = true;
    let engine = engine_with(store.clone(), OWNER, engine_options);

    engine
        .save_row(
            "notes",
            "outgoing",
            None,
            &fields(&[("title", text("must finish under outgoing bearer"))]),
        )
        .await
        .unwrap();
    until("the automatic push never reached the wire", || {
        let arrived = gate.has_arrived();
        async move { arrived }
    })
    .await;

    let transition_finished = Arc::new(AtomicBool::new(false));
    let transition = tokio::spawn({
        let engine = engine.clone();
        let finished = transition_finished.clone();
        async move {
            engine.seal().await.unwrap();
            finished.store(true, Ordering::SeqCst);
        }
    });
    until("engine never closed identity admissions", || {
        let engine = engine.clone();
        async move { engine.is_sealed().await }
    })
    .await;

    assert!(
        !transition_finished.load(Ordering::SeqCst),
        "beginIdentityTransition returned while an automatic push was still on the wire"
    );
    assert_eq!(
        engine
            .save_row(
                "notes",
                "late-before-wipe",
                None,
                &fields(&[("title", text("must not enter outgoing journal"))]),
            )
            .await
            .unwrap_err(),
        ReplicaError::IdentityTransitionInProgress,
        "operation crossed a paused identity boundary"
    );
    assert!(
        store
            .peek_snapshot("notes", "late-before-wipe")
            .unwrap()
            .is_none()
    );

    gate.release();
    transition.await.unwrap();
    assert!(
        store.peek_pending().unwrap().is_empty(),
        "the outgoing flight must settle before the boundary opens"
    );

    engine.unseal().await;
    engine
        .save_row(
            "notes",
            "replacement",
            None,
            &fields(&[("title", text("more work under the same owner"))]),
        )
        .await
        .unwrap();
    until(
        "automatic delivery did not resume after the seal lifted",
        || {
            let count = transport.push_count();
            async move { count == 2 }
        },
    )
    .await;
    assert!(store.peek_pending().unwrap().is_empty());
}

/// `testTransitionWaitsUntilActivePullResponseIsAppliedAndBlocksAnotherPull`
#[tokio::test]
async fn transition_waits_until_active_pull_response_is_applied_and_blocks_another_pull() {
    let store = store("fence-active-pull");
    let transport = StubTransport::new();
    let gate = Gate::new();
    hold_pulls(&transport, &gate);
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("outgoing", "old identity response", None)],
            "1:",
            false,
        ),
    );
    let engine = engine(store.clone(), transport.clone());

    let pull = tokio::spawn({
        let engine = engine.clone();
        async move { engine.pull_once("user").await }
    });
    until("the pull never reached the wire", || {
        let arrived = gate.has_arrived();
        async move { arrived }
    })
    .await;

    let transition_finished = Arc::new(AtomicBool::new(false));
    let transition = tokio::spawn({
        let engine = engine.clone();
        let finished = transition_finished.clone();
        async move {
            engine.seal().await.unwrap();
            finished.store(true, Ordering::SeqCst);
        }
    });
    until("engine never closed identity admissions", || {
        let engine = engine.clone();
        async move { engine.is_sealed().await }
    })
    .await;
    assert!(
        !transition_finished.load(Ordering::SeqCst),
        "beginIdentityTransition returned before the outgoing pull response settled"
    );

    gate.release();
    let applied = pull.await.unwrap().unwrap();
    assert_eq!(applied, 1);
    transition.await.unwrap();
    assert_eq!(
        store
            .peek_snapshot("notes", "outgoing")
            .unwrap()
            .unwrap()
            .data
            .get("title"),
        Some(&text("old identity response")),
        "the boundary returned before the already-started response committed locally"
    );

    // A NEW pull while sealed answers zero and never touches the wire — the
    // same graceful shape as the closed engine, so lifecycle callers (warm,
    // refresh, nudge) stay silent through a transition.
    let pulls_before_sealed_attempt = transport.pull_count();
    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    assert_eq!(
        transport.pull_count(),
        pulls_before_sealed_attempt,
        "a sealed engine must stay off the wire for pulls"
    );

    engine.unseal().await;
    transport.queue_pull("user", ScriptedPull::new(Vec::new(), "2:", false));
    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    assert_eq!(
        transport.pull_count(),
        pulls_before_sealed_attempt + 1,
        "unseal re-admits pulls"
    );
}

/// `testPinnedSourceDrainSealsFirstRejectsConcurrentCRUDAndIsReplaySafe`
#[tokio::test]
async fn pinned_source_drain_seals_first_rejects_concurrent_crud_and_is_replay_safe() {
    let store = store("fence-pinned-drain");
    let ordinary_transport = StubTransport::new();
    let pinned_source_transport = StubTransport::new();
    let gate = Gate::new();
    hold_pushes(&pinned_source_transport, &gate);
    let engine = engine(store.clone(), ordinary_transport.clone());

    engine
        .save_row(
            "notes",
            "outgoing",
            None,
            &fields(&[("title", text("must use pinned source wire"))]),
        )
        .await
        .unwrap();

    let drain = tokio::spawn({
        let engine = engine.clone();
        let pinned: Arc<dyn ReplicaTransport> = pinned_source_transport.clone();
        async move { engine.seal_and_drain(pinned).await }
    });
    until("the pinned source wire never carried the journal", || {
        let arrived = gate.has_arrived();
        async move { arrived }
    })
    .await;

    assert_eq!(
        ordinary_transport.push_count(),
        0,
        "the engine's ordinary/live transport must never carry the frozen source journal"
    );
    assert_eq!(pinned_source_transport.push_count(), 1);
    assert_eq!(
        engine
            .save_row(
                "notes",
                "late",
                None,
                &fields(&[("title", text("must not arrive after drain snapshot"))]),
            )
            .await
            .unwrap_err(),
        ReplicaError::IdentityTransitionInProgress,
        "operation crossed a paused identity boundary"
    );
    assert!(store.peek_snapshot("notes", "late").unwrap().is_none());

    gate.release();
    let verdicts = drain.await.unwrap().unwrap();
    assert_eq!(verdicts.len(), 1);
    assert!(store.peek_pending().unwrap().is_empty());
    assert!(engine.is_sealed().await);

    let replayed = engine
        .seal_and_drain(pinned_source_transport.clone())
        .await
        .unwrap();
    assert!(replayed.is_empty());
    assert_eq!(
        pinned_source_transport.push_count(),
        1,
        "recovery must not resend an entry whose accepted verdict already committed"
    );
}

/// `testPinnedSourceDrainReleasesSingleFlightAfterCancellationError`
#[tokio::test]
async fn pinned_source_drain_releases_single_flight_after_cancellation_error() {
    let store = store("fence-pinned-drain-cancel");
    let engine = engine(store.clone(), StubTransport::new());
    engine
        .save_row(
            "notes",
            "retry-after-cancel",
            None,
            &fields(&[("title", text("still pending"))]),
        )
        .await
        .unwrap();
    engine.seal().await.unwrap();

    let refusal: Arc<dyn ReplicaTransport> = Arc::new(CancellationTransport);
    assert_eq!(
        engine.seal_and_drain(refusal).await.unwrap_err(),
        ReplicaError::Transport("cancelled (stub)".into()),
        "cancellation transport unexpectedly accepted the journal"
    );
    // The internal flight's defer must clear `active_drains` and the wire count.
    assert_eq!(store.peek_pending().unwrap().len(), 1);

    let retry_transport = StubTransport::new();
    let verdicts = engine
        .seal_and_drain(retry_transport.clone())
        .await
        .unwrap();
    assert_eq!(verdicts.len(), 1);
    assert!(store.peek_pending().unwrap().is_empty());
    assert_eq!(retry_transport.push_count(), 1);
}

/// `testASealedEngineStaysOffTheWireForPulls` — pulls WRITE: checkpoint rows and
/// cursors land in the store. A merge rebinds and moves that store while sealed,
/// so a pull that slips in mid-seal races the swap exactly the way local writes
/// would. The seal must keep the engine off the wire for pulls, not just pushes.
#[tokio::test]
async fn a_sealed_engine_stays_off_the_wire_for_pulls() {
    let directory = temp_directory("fence-sealed-pulls");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());
    engine.open(606).await.unwrap();
    engine.seal().await.unwrap();

    let applied = engine.pull_until_caught_up(None).await.unwrap();

    assert_eq!(applied, 0);
    assert_eq!(
        transport.pull_count(),
        0,
        "a sealed engine must not touch the wire for pulls"
    );

    engine.unseal().await;
    engine.pull_once("user").await.unwrap();
    assert_eq!(transport.pull_count(), 1, "unseal re-admits the wire");
}

// MARK: - OwnGuestIdentityRebindTests (4)
//
// An own-guest merge preserves the local world under a replacement server
// identity. Unlike a foreign switch it must not wipe, but preserving guest owner
// fields is also wrong: pending work would be refused under the target bearer,
// and a later rejection could restore a guest-owned preimage over a
// server-recaptured target row.

/// `testRebindRequiresACompletedIdentityFence`
#[tokio::test]
async fn rebind_requires_a_completed_identity_fence() {
    let directory = temp_directory("rebind-fence");
    let store = store("rebind-fence");
    let transport = StubTransport::new();
    let gate = Gate::new();
    hold_pushes(&transport, &gate);
    let mut engine_options = options(directory.path().to_path_buf(), transport.clone());
    engine_options.automatically_push_writes = true;
    let engine = engine_with(store.clone(), GUEST_USER_ID, engine_options);

    engine
        .save_row(
            "notes",
            "guest-row",
            None,
            &fields(&[
                ("title", text("guest")),
                ("userId", owner_value(GUEST_USER_ID)),
            ]),
        )
        .await
        .unwrap();
    until("the automatic push never reached the wire", || {
        let arrived = gate.has_arrived();
        async move { arrived }
    })
    .await;

    assert_eq!(
        engine
            .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
            .await
            .unwrap_err(),
        ReplicaError::IdentityTransitionRequired,
        "an unpaused engine admitted an identity rewrite"
    );

    let transition = tokio::spawn({
        let engine = engine.clone();
        async move { engine.seal().await.unwrap() }
    });
    until("engine never closed identity admissions", || {
        let engine = engine.clone();
        async move { engine.is_sealed().await }
    })
    .await;
    assert_eq!(
        engine
            .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
            .await
            .unwrap_err(),
        ReplicaError::IdentityTransitionRequired,
        "a requested pause is not a held fence while outgoing wire work remains"
    );

    gate.release();
    transition.await.unwrap();
    engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap();
    let merged = engine.store().expect("the merged store");
    assert_eq!(
        merged
            .peek_snapshot("notes", "guest-row")
            .unwrap()
            .unwrap()
            .data
            .get("userId"),
        Some(&owner_value(TARGET_USER_ID))
    );
    engine.unseal().await;
}

/// `testAdoptMergedBlanksCursorsSoTheTargetWorldRebootstraps` — cluster-global
/// xids make the guest's pull cursor NEWER than every row the target already
/// owned on the server, so a cursor carried through the merge hides the target's
/// whole pre-merge world from every future pull. Adoption must blank the cursor
/// so the next pull re-bootstraps the merged world from zero.
#[tokio::test]
async fn adopt_merged_blanks_cursors_so_the_target_world_rebootstraps() {
    let directory = temp_directory("rebind-cursor");
    let store = store("rebind-cursor");
    let transport = StubTransport::new();
    let mut engine_options = options(directory.path().to_path_buf(), transport.clone());
    engine_options.automatically_push_writes = true;
    let engine = engine_with(store.clone(), GUEST_USER_ID, engine_options);

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("guest-row", "guest", None)], "900:", false),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("900:"),
        "the guest's pulls advanced its cursor"
    );

    engine.seal().await.unwrap();
    engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap();
    engine.unseal().await;

    assert!(
        engine.current_cursor("user").await.unwrap().is_none(),
        "an adopted world must re-bootstrap — a carried guest cursor hides every pre-merge target row"
    );
}

/// `testRebindCoversSnapshotsPendingAndParkedJournalWithoutTouchingOtherOwners`
#[tokio::test]
async fn rebind_moves_mutable_authoring_and_preserves_parked_evidence_and_other_owners() {
    let directory = temp_directory("rebind-coverage");
    let fixture = store("rebind-coverage");
    let transport = StubTransport::new();
    let engine = engine_with(
        fixture.clone(),
        GUEST_USER_ID,
        options(directory.path().to_path_buf(), transport.clone()),
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                server_row("preimage", GUEST_USER_ID, "server"),
                server_row("payload", TARGET_USER_ID, "server"),
                server_row("deleted", GUEST_USER_ID, "server"),
                server_row("other-owner", 999, "server"),
                row_set(
                    "notes",
                    "additional-row",
                    None,
                    fields(&[("userId", owner_value(GUEST_USER_ID))]),
                ),
            ],
            "1:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    transport.queue_pull(
        "global",
        ScriptedPull::new(
            vec![row_set(
                "assets",
                "global-guest-value",
                None,
                fields(&[("userId", owner_value(GUEST_USER_ID))]),
            )],
            "1:",
            false,
        ),
    );
    engine.pull_once("global").await.unwrap();

    // Guest in the PATCH PREIMAGE, target already in its payload.
    engine
        .save_row(
            "notes",
            "preimage",
            None,
            &fields(&[
                ("title", text("local")),
                ("userId", owner_value(TARGET_USER_ID)),
            ]),
        )
        .await
        .unwrap();
    // Guest in the PATCH PAYLOAD, target already in its preimage.
    engine
        .save_row(
            "notes",
            "payload",
            None,
            &fields(&[
                ("title", text("local")),
                ("userId", owner_value(GUEST_USER_ID)),
            ]),
        )
        .await
        .unwrap();
    // Guest in a DELETE's whole-row preimage.
    engine.delete_row("notes", "deleted").await.unwrap();
    // Guest in a CREATE snapshot + payload. The nested value deliberately names
    // some other domain actor and must not be treated as row owner.
    engine
        .save_row(
            "notes",
            "created",
            None,
            &fields(&[
                ("title", text("new")),
                ("userId", owner_value(GUEST_USER_ID)),
                (
                    "metadata",
                    ReplicaValue::Object(fields(&[("userId", owner_value(GUEST_USER_ID))])),
                ),
            ]),
        )
        .await
        .unwrap();
    seed_parked_guest_entry(&fixture);

    engine.seal().await.unwrap();
    engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap();
    assert_eq!(
        engine.owner(),
        Some(TARGET_USER_ID),
        "the merge swaps the engine's own binding too"
    );
    // The merge reopens the world at the target's path; every assertion below
    // reads the store the engine is bound to NOW.
    let store = engine.store().expect("the merged store");
    assert_eq!(
        store.path().file_name().and_then(|name| name.to_str()),
        Some(format!("replica-{TARGET_USER_ID}.sqlite").as_str()),
        "the preserved world moves to the target owner's file"
    );

    assert_eq!(
        snapshot_owner(&store, "preimage"),
        Some(owner_value(TARGET_USER_ID))
    );
    assert_eq!(
        snapshot_owner(&store, "payload"),
        Some(owner_value(TARGET_USER_ID))
    );
    assert_eq!(
        snapshot_owner(&store, "created"),
        Some(owner_value(TARGET_USER_ID))
    );
    assert_eq!(
        snapshot_owner(&store, "other-owner"),
        Some(owner_value(999))
    );
    assert_eq!(
        store
            .peek_snapshot("notes", "additional-row")
            .unwrap()
            .unwrap()
            .data
            .get("userId"),
        Some(&owner_value(TARGET_USER_ID)),
        "all rows belonging to the source owner are rebound"
    );
    assert_eq!(
        store
            .peek_snapshot("assets", "global-guest-value")
            .unwrap()
            .unwrap()
            .data
            .get("userId"),
        Some(&owner_value(GUEST_USER_ID)),
        "a same-looking value in the global shard is not this user's local world"
    );
    assert_eq!(
        store
            .peek_snapshot("notes", "created")
            .unwrap()
            .unwrap()
            .data
            .get("metadata")
            .and_then(|value| value.get("userId")),
        Some(&owner_value(GUEST_USER_ID)),
        "only the top-level row-owner column is identity-bound"
    );

    let pending = journal_by_row(store.peek_pending().unwrap());
    assert_eq!(
        pending["payload"]
            .op()
            .unwrap()
            .data
            .and_then(|data| data.get("userId").cloned()),
        Some(owner_value(TARGET_USER_ID))
    );
    assert_eq!(
        pending["created"]
            .op()
            .unwrap()
            .data
            .and_then(|data| data.get("userId").cloned()),
        Some(owner_value(TARGET_USER_ID))
    );
    assert_eq!(
        preimage_owner(pending.get("preimage")),
        Some(owner_value(TARGET_USER_ID))
    );
    assert_eq!(
        preimage_owner(pending.get("payload")),
        Some(owner_value(TARGET_USER_ID))
    );
    assert_eq!(
        preimage_owner(pending.get("deleted")),
        Some(owner_value(TARGET_USER_ID))
    );

    let parked_entries = store.peek_parked().unwrap();
    let parked = parked_entries
        .iter()
        .find(|entry| entry.id == "parked-guest")
        .expect("the parked evidence survives the rebind");
    assert_eq!(
        parked
            .op()
            .unwrap()
            .data
            .and_then(|data| data.get("userId").cloned()),
        Some(owner_value(GUEST_USER_ID))
    );
    assert_eq!(
        preimage_owner(Some(parked)),
        Some(owner_value(GUEST_USER_ID))
    );

    engine.unseal().await;

    // A server merge tail can replace rows while the rebound local ops are still
    // owed. Hold the journal on a dead wire, apply that tail, then reject the
    // patches/delete: no rollback may restore the guest owner.
    transport.fail_pushes(true);
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                server_row("preimage", TARGET_USER_ID, "merged target"),
                server_row("payload", TARGET_USER_ID, "merged target"),
                server_row("deleted", TARGET_USER_ID, "merged target"),
            ],
            "2:",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    transport.fail_pushes(false);
    transport.script_push(|ops| {
        ops.iter()
            .map(|op| {
                if op.verb == verb::ROW_CREATE {
                    ReplicaVerdict::accepted(&op.id)
                } else {
                    ReplicaVerdict::rejected(&op.id, "refused after merge")
                }
            })
            .collect()
    });
    engine.drain().await.unwrap();

    for op in transport.pushed_ops() {
        if let Some(owner) = op.data.as_ref().and_then(|data| data.get("userId")) {
            assert_eq!(
                owner,
                &owner_value(TARGET_USER_ID),
                "guest owner escaped onto target wire"
            );
        }
    }
    for id in ["preimage", "payload", "deleted", "created"] {
        assert_eq!(
            snapshot_owner(&store, id),
            Some(owner_value(TARGET_USER_ID)),
            "a verdict restored the absorbed guest owner for {id}"
        );
    }
}

/// A parked refusal that still names the guest — evidence has to stay
/// internally truthful across the rebind too.
fn seed_parked_guest_entry(store: &ReplicaStateStore) {
    let op = ReplicaOp::new("parked-guest", verb::ROW_PATCH, "notes", "parked")
        .with_data(fields(&[("userId", owner_value(GUEST_USER_ID))]));
    let payload = op.to_json().unwrap();
    let preimage = ReplicaPreimage::Fields {
        values: fields(&[("userId", owner_value(GUEST_USER_ID))]),
        missing: Vec::new(),
    }
    .encoded()
    .unwrap();
    store
        .pool()
        .write(|ctx| {
            store.enqueue(
                ctx,
                &op.id,
                &op.verb,
                &op.stream,
                &op.row_id,
                &payload,
                Some(&preimage),
                ReplicaLane::Bulk,
            )?;
            store.park(ctx, &op.id, "prior refusal")
        })
        .expect("seed the parked guest entry");
}

async fn seal_a_guest_world_with_a_poisoned_entry(
    engine: &crate::ReplicaEngine,
    store: &ReplicaStateStore,
    transport: &StubTransport,
) {
    let durable = server_row("durable", GUEST_USER_ID, "server");
    transport.queue_pull("user", ScriptedPull::new(vec![durable], "1:", false));
    engine.pull_once("user").await.unwrap();
    let guest = fields(&[("userId", owner_value(GUEST_USER_ID))]);
    engine
        .save_row("notes", "valid-entry", None, &guest)
        .await
        .unwrap();
    let poison = ReplicaOp::new("poison", verb::ROW_PATCH, "notes", "poison").with_data(guest);
    let payload = poison.to_json().unwrap();
    store
        .pool()
        .write(|ctx| {
            let preimage = Some(&b"{not-valid-json"[..]);
            store.enqueue(
                ctx,
                "poison",
                verb::ROW_PATCH,
                "notes",
                "poison",
                &payload,
                preimage,
                ReplicaLane::Bulk,
            )
        })
        .expect("seed the poison entry");
    engine.seal().await.unwrap();
}

/// The adoption copy a failed rebind leaves beside the target owner's path.
fn staging_copy(engine: &crate::ReplicaEngine, source: &ReplicaStateStore) -> ReplicaStateStore {
    let source_id = source.pool().read(|db| source.meta(db)).unwrap().store;
    let path = engine
        .store_url(TARGET_USER_ID)
        .with_extension(format!("adopting-{source_id}.sqlite"));
    ReplicaStateStore::open(&path).expect("the staging copy")
}

fn entry_owner(store: &ReplicaStateStore, row_id: &str) -> Option<ReplicaValue> {
    let entries = store.peek_pending().unwrap();
    let entry = entries
        .iter()
        .find(|entry| entry.op().unwrap().row_id == row_id)
        .expect("the entry");
    entry
        .op()
        .unwrap()
        .data
        .and_then(|data| data.get("userId").cloned())
}

/// `testMalformedJournalFailsWithoutPartiallyRebindingSnapshotsOrEntries`
#[tokio::test]
async fn malformed_journal_fails_without_partially_rebinding_snapshots_or_entries() {
    let directory = temp_directory("rebind-malformed");
    let store = store("rebind-malformed");
    let transport = StubTransport::new();
    let guest_options = options(directory.path().to_path_buf(), transport.clone());
    let engine = engine_with(store.clone(), GUEST_USER_ID, guest_options);
    seal_a_guest_world_with_a_poisoned_entry(&engine, &store, &transport).await;

    let error = engine
        .adopt_merged(GUEST_USER_ID, TARGET_USER_ID)
        .await
        .unwrap_err();
    assert!(matches!(error, ReplicaError::Storage(_)), "{error}");
    assert_eq!(engine.owner(), Some(GUEST_USER_ID));
    assert!(!engine.store_url(TARGET_USER_ID).exists());
    let staging = staging_copy(&engine, &store);
    let guest = Some(owner_value(GUEST_USER_ID));
    assert_eq!(snapshot_owner(&staging, "durable"), guest);
    assert_eq!(entry_owner(&staging, "valid-entry"), guest);
    engine.unseal().await;
}

// MARK: - ResidualIdentityTests (4)
//
// The residual an acked-but-reduced entry leaves behind, graded from the RAW
// journal row — never through `pending_ops()`, which is the accessor the engine
// wrote with.

#[tokio::test]
async fn edits_authored_during_upload_are_delivered_or_archived_on_refusal() {
    for accepted in [true, false] {
        let store = store("inflight-document-authoring");
        let transport = StubTransport::new();
        let engine = engine(store.clone(), transport.clone());
        transport.queue_pull(
            "user",
            ScriptedPull::new(
                vec![doc_snapshot(
                    "boards",
                    "b1",
                    "stub@1",
                    b"S",
                    ReplicaFields::new(),
                )],
                "5:",
                false,
            ),
        );
        engine.pull_once("user").await.unwrap();
        engine
            .record_doc_delta("boards", "b1", b"+1")
            .await
            .unwrap();
        let once = Arc::new(AtomicBool::new(false));
        let weak_engine = Arc::downgrade(&engine);
        transport.on_push(move |_| {
            let once = once.clone();
            let weak_engine = weak_engine.clone();
            Box::pin(async move {
                if !once.swap(true, Ordering::SeqCst) {
                    weak_engine
                        .upgrade()
                        .unwrap()
                        .record_doc_delta("boards", "b1", b"+2")
                        .await
                        .unwrap();
                }
            })
        });
        if !accepted {
            reject_all(&transport, "refused");
        }
        engine.drain().await.unwrap();
        assert!(store.peek_pending().unwrap().is_empty());
        if accepted {
            assert!(transport.pushed_ops().iter().any(|op| {
                op.payload
                    .as_ref()
                    .is_some_and(|bytes| String::from_utf8_lossy(bytes).contains("+2"))
            }));
            assert!(store.recovery_records(None, 100).unwrap().is_empty());
        } else {
            let records = store.recovery_records(None, 100).unwrap();
            assert_eq!(records.len(), 1);
            let part = store
                .recovery_parts(&records[0].id, None, 100)
                .unwrap()
                .into_iter()
                .find(|p| p.kind == "document.fold")
                .unwrap();
            let bytes = store
                .recovery_chunk(&records[0].id, &part, 0, 262144)
                .unwrap();
            assert!(String::from_utf8_lossy(&bytes).contains("+1"));
            assert!(String::from_utf8_lossy(&bytes).contains("+2"));
            assert_eq!(store.peek_parked().unwrap().len(), 1);
            assert_eq!(store.fold("boards", "b1").unwrap().unwrap(), b"S");
        }
    }
}

// MARK: - ResidualBehaviorTests (2)
//
// A row-lane delete discards a PARKED create too (the server never had the
// row — nothing to push, nothing to keep as a second copy of the evidence); a
// document this build cannot decode refuses its checkpoint and a per-doc
// resync lever forces the rebuild.

/// `testDeleteOfParkedCreateDiscardsTheEvidenceAndJournalsNothing`
#[tokio::test]
async fn delete_of_parked_create_discards_the_evidence_and_journals_nothing() {
    let store = store("residual-parked-create");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());

    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("mine"))]))
        .await
        .unwrap();
    reject_all(&transport, "refused");
    engine.drain().await.unwrap();
    assert_eq!(
        store.peek_parked().unwrap().len(),
        1,
        "the create parked (and the row reverted)"
    );

    // The user deletes the thing whose create was refused: the parked evidence
    // goes with it, and NO row.delete is journaled — the server never heard the
    // id.
    engine.delete_row("notes", "n1").await.unwrap();
    assert_eq!(store.peek_parked().unwrap().len(), 0);
    assert_eq!(store.peek_pending().unwrap().len(), 0);
}

async fn pull_board(
    engine: &crate::ReplicaEngine,
    transport: &StubTransport,
    codec: &str,
    fold: &[u8],
    cursor: &str,
) -> crate::error::ReplicaResult<usize> {
    let board = doc_snapshot("boards", "b1", codec, fold, ReplicaFields::new());
    transport.queue_pull("user", ScriptedPull::new(vec![board], cursor, false));
    engine.pull_once("user").await
}

/// A snapshot in a codec this build does not know refuses its checkpoint; the
/// resync lever drops the fold and blanks the cursor, and the next pull
/// rebuilds the document from server truth.
#[tokio::test]
async fn unknown_document_codec_refuses_checkpoint_and_resync_rebuilds() {
    let store = store("residual-skipped-delta");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    pull_board(&engine, &transport, "stub@1", b"SNAP", "5:")
        .await
        .unwrap();

    let refused = pull_board(&engine, &transport, "alien@9", b"OPS", "6:").await;
    let unknown = ReplicaError::Codec("No codec registered for alien@9".into());
    assert_eq!(refused, Err(unknown));
    let cursor = engine.current_cursor("user").await.unwrap();
    assert_eq!(cursor.as_deref(), Some("5:"));

    engine.resync_document("boards", "b1").await.unwrap();
    assert!(store.peek_doc("boards", "b1").unwrap().is_none());
    assert!(engine.current_cursor("user").await.unwrap().is_none());
    pull_board(&engine, &transport, "stub@1", b"SNAP2", "7:")
        .await
        .unwrap();
    assert_eq!(store.fold("boards", "b1").unwrap(), Some(b"SNAP2".to_vec()));
}

// MARK: - SealBarrierTests (2) — NEW, no upstream counterpart
//
// Upstream gets this group for free: `ReplicaEngine` is a Swift `actor`, so
// `saveRow`'s whole body is mutually exclusive with `adoptMerged` and the seal
// only has to count WIRE operations. The Rust port has no such isolation — the
// state mutex guards `EngineState` and nothing else, and `admit_local_write`
// releases it before `pool().write(...)`. So the invariant that is structural
// in Swift has to be asserted here.
//
// Both cases park a local write in the ENGINE'S OWN gap: `create_doc` stamps
// (and so reads the injected clock) after the admission check and before the
// pool write. That is the whole window, reproduced with no test seam in
// production code. `multi_thread` is load-bearing — on a current-thread runtime
// the parked write and the seal cannot run at once, which is precisely why R1's
// suite never saw this.

/// A clock that announces the write it is stamping and holds it there.
fn parking_clock(
    admitted: &Arc<Gate>,
    release: &Arc<Gate>,
) -> Arc<dyn Fn() -> SystemTime + Send + Sync> {
    let admitted = admitted.clone();
    let release = release.clone();
    Arc::new(move || {
        admitted.arrive();
        release.block_until_open(Duration::from_secs(5));
        SystemTime::now()
    })
}

/// THE INVARIANT: `seal` is a barrier for local writes, not only for the wire.
/// A write the engine has already admitted must reach the disk before the seal
/// hands the file to `adopt_merged` / `close` / `retire`.
///
/// Ordering, not timing: `seal` raises its flag before it waits, so
/// `is_sealed()` is the exact moment the seal is live, and the two tasks record
/// which of them finished first. A seal that does not count local writes reads
/// `["seal", "write"]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_seal_waits_for_a_local_write_that_already_passed_admission() {
    let directory = temp_directory("seal-covers-local-write");
    let store = store("seal-covers-local-write");
    let transport = StubTransport::new();
    let admitted = Gate::new();
    let release = Gate::new();
    let mut engine_options = options(directory.path().to_path_buf(), transport.clone());
    engine_options.clock = parking_clock(&admitted, &release);
    let engine = engine_with(store.clone(), OWNER, engine_options);

    let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let writer = tokio::spawn({
        let engine = engine.clone();
        let order = order.clone();
        async move {
            let born = engine
                .create_doc(
                    "boards",
                    "b1",
                    b"seed",
                    100,
                    &fields(&[("title", text("a"))]),
                    Some(&ReplicaCreateStamp::standard()),
                )
                .await;
            order.lock().push("write");
            born
        }
    });
    // The write is now past the seal check and has not yet touched the pool.
    admitted.wait_until_arrived().await;

    let sealer = tokio::spawn({
        let engine = engine.clone();
        let order = order.clone();
        async move {
            engine.seal().await.unwrap();
            order.lock().push("seal");
        }
    });
    until("the seal never took effect", || {
        let engine = engine.clone();
        async move { engine.is_sealed().await }
    })
    .await;

    release.release();
    assert!(
        writer.await.expect("the writer task").expect("the write"),
        "the document was never born"
    );
    sealer.await.expect("the sealer task");

    assert_eq!(
        *order.lock(),
        ["write", "seal"],
        "the seal returned while a local write it had already admitted was still in flight — \
         the caller may now hand the file to adopt_merged/close underneath that write"
    );
}

/// THE CONSEQUENCE, on the production path: `close` is `seal` + release the
/// binding. A write the engine admitted must never come back
/// `Storage(\"the store is closed\")` — a refusal shape the Swift barrier does
/// not define, so no caller is written to expect it.
///
/// Several writers, so the barrier is exercised as a COUNT and not as a flag.
/// The worker pool is deliberately wider than `WRITERS`: each parked write
/// holds a thread (the clock seam is synchronous, like the pool write it stands
/// in for), and the close still has to be able to run.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_close_does_not_pull_the_store_out_from_under_admitted_local_writes() {
    const WRITERS: usize = 3;

    let directory = temp_directory("seal-close-race");
    let store = store("seal-close-race");
    let transport = StubTransport::new();
    let admitted = Gate::new();
    let release = Gate::new();
    let arrivals = Tally::new();
    let mut engine_options = options(directory.path().to_path_buf(), transport.clone());
    engine_options.clock = {
        let admitted = admitted.clone();
        let release = release.clone();
        let arrivals = arrivals.clone();
        Arc::new(move || {
            arrivals.bump();
            admitted.arrive();
            release.block_until_open(Duration::from_secs(5));
            SystemTime::now()
        })
    };
    let engine = engine_with(store.clone(), OWNER, engine_options);

    let writers: Vec<_> = (0..WRITERS)
        .map(|index| {
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .create_doc(
                        "boards",
                        &format!("b{index}"),
                        b"seed",
                        100 + index as u64,
                        &fields(&[("title", text("a"))]),
                        Some(&ReplicaCreateStamp::standard()),
                    )
                    .await
            })
        })
        .collect();
    until("not every write reached the admission gap", || {
        let arrivals = arrivals.clone();
        async move { arrivals.count() == WRITERS }
    })
    .await;

    let closer = tokio::spawn({
        let engine = engine.clone();
        async move { engine.close().await.unwrap() }
    });
    until("the close never sealed the engine", || {
        let engine = engine.clone();
        async move { engine.is_sealed().await }
    })
    .await;
    // Every chance for an uncovered seal to run ahead: without the barrier the
    // close is finished microseconds after the flag goes up. With it, the close
    // cannot proceed until these writes land, so this window simply elapses.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let closed_early = store.pool().is_closed();

    release.release();
    let mut refusals = Vec::new();
    for writer in writers {
        match writer.await.expect("the writer task") {
            Ok(true) => {}
            Ok(false) => refusals.push("the birth was silently dropped".to_owned()),
            Err(error) => refusals.push(error.to_string()),
        }
    }
    closer.await.expect("the closer task");

    assert!(
        !closed_early,
        "the close released the store while {WRITERS} local writes it had admitted were still in flight"
    );
    assert!(
        refusals.is_empty(),
        "a local write the engine had already admitted was refused by a store the seal released \
         underneath it: {refusals:?}"
    );
}
