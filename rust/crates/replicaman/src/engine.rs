//! The loop. One engine owns the whole replication surface: checkpoint imports
//! (pull batch → store → cursor, one transaction), the outbound op journal
//! (per-document supersede, verdicts, cold window), and the local write door
//! the generated verbs call.
//!
//! ORDER IS THE CONTRACT (ported from Syncer v1):
//! - A pull batch applies ATOMICALLY with its cursor advance — a crash
//!   mid-batch leaves the previous checkpoint intact.
//! - Drain-before-pull, so the echo is in the answer; `drain_if_warm` skips
//!   inside the cold window so an offline fetch burst doesn't stack timeouts
//!   on a wire that just proved dead.
//! - Journal durability and the client snapshot write share one transaction —
//!   a crash between them cannot exist.
//! - Rejection is a VERDICT (park, keep the reason), transport failure is a
//!   RETRY (entries stay pending) — never conflated.
//!
//! Ported from `Sources/ReplicaMan/ReplicaEngine.swift`. Upstream is a Swift
//! actor; here the actor-isolated members live behind one async mutex, and the
//! members Swift marks `nonisolated` (`docFold`, `docPeer`, binding reads,
//! `openForColdBoot`, `rowFieldStrings`, watch arming) bypass it through the
//! binding's own lock, exactly as upstream.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use event_listener::Event;
mod submissions;
mod transactions;
pub use transactions::ReplicaTransaction;
mod adoption;
mod checkpoints;
use checkpoints::PullStep;
mod integrity;
use parking_lot::Mutex;

use crate::binding::{Bound, ReplicaBinding};
use crate::codec::{ReplicaCodec, ReplicaDocumentMode};
use crate::documents::{
    DocumentCodec, DocumentKey, DocumentPin, HeldDocument, LiveDocuments, ReplicaDocState,
};
use crate::error::{ReplicaError, ReplicaResult};
use crate::id;
use crate::lane_scope::{LaneScope, current_lane};
use crate::models::ReplicaCreateStamp;
use crate::pool::WriteContext;
use crate::preimage::ReplicaPreimage;
use crate::schema::{ReplicaLane, ReplicaSchema, ReplicaStreamSpec, StreamLane};
use crate::spawner::{Spawner, boxed};
use crate::store::{JournalRow, ReplicaStateStore};
use crate::transport::{BoxFuture, ReplicaTransport};
use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::{ReplicaFrame, ReplicaOp, ReplicaVerdict, VerdictOutcome, verb};
use crate::{ReplicaGateHold, SyncChange, SyncChangeKind, SyncGate, SyncGateDecision};

/// A runaway reducer must not spin the flight forever.
pub(crate) const MAX_DRAIN_PASSES: usize = 16;

type RejectionHandler = Arc<dyn Fn(&ReplicaOp, &str) + Send + Sync>;
type CheckpointFault = Arc<dyn Fn() -> ReplicaResult<()> + Send + Sync>;
type PeerMinter = Arc<dyn Fn() -> u64 + Send + Sync>;
type Clock = Arc<dyn Fn() -> SystemTime + Send + Sync>;

type ReconcileWorking = dyn Fn(Option<&crate::DocRow>, i64) -> ReplicaResult<()> + Send + Sync;

struct WorkingEntry {
    generation: u64,
    value: Arc<dyn std::any::Any + Send + Sync>,
    writes: Arc<crate::working::PendingWrites>,
    reconcile: Arc<ReconcileWorking>,
}

/// What a flight leaves behind. Verdicts are the caller's answer; `held` is
/// the SCHEDULER's answer — the entries this flight judged and could not send,
/// because a reducer gated them.
///
/// D6: the gate is derived per drain and never written down, so nothing in the
/// journal distinguishes "owed and sendable" from "owed and gated" —
/// `owes_work` (owed or frozen) answers true for both. The scheduled-push
/// loop needs that distinction or it re-judges a gated row forever. Carrying it
/// on the flight (rather than parking, or a cold stamp) keeps the answer scoped
/// to the one drain that derived it: nothing to invalidate, nothing stale.
#[derive(Clone, Default)]
struct DrainReport {
    verdicts: Vec<ReplicaVerdict>,
    held: HashSet<String>,
}

/// One drain flight. Upstream this is a `Task`; joiners here await the same
/// completion event and read the one result, so a request that joins an
/// in-flight drain is answered by the flight it joined rather than by an empty
/// second selection.
#[derive(Default)]
struct Flight {
    done: Event,
    result: Mutex<Option<ReplicaResult<DrainReport>>>,
}

impl Flight {
    async fn wait(&self) -> ReplicaResult<DrainReport> {
        loop {
            if let Some(result) = self.result.lock().clone() {
                return result;
            }
            let listener = self.done.listen();
            if let Some(result) = self.result.lock().clone() {
                return result;
            }
            listener.await;
        }
    }

    fn complete(&self, result: ReplicaResult<DrainReport>) {
        *self.result.lock() = Some(result);
        self.done.notify(usize::MAX);
    }
}

/// What `seal` waits on.
///
/// Upstream the engine is an `actor`, so `saveRow`'s whole body cannot
/// interleave with `adoptMerged` — mutual exclusion is free and the barrier
/// only has to count WIRE work (the ops that captured the outgoing bearer).
/// Rust has no such isolation: `admit_local_write` releases the state mutex
/// before the pool write, so a write that passed the seal check can still be
/// running when the seal returns and the store closes underneath it
/// (`Storage("the store is closed")` — a refusal shape the Swift barrier never
/// defines). The count restores the invariant.
///
/// A COUNT, not the state guard: holding the async mutex across `pool().write`
/// would serialize every local write behind every other one, and the pool's own
/// writer mutex already serializes the part that must be.
#[derive(Default)]
struct SealBarrier {
    /// Local writes admitted past the seal check and not yet applied.
    /// Incremented UNDER the state lock (so a seal that observes zero has
    /// already excluded every future write); decremented on drop, which is why
    /// it is an atomic and not an `EngineState` field.
    local_writes: AtomicUsize,
    /// Notified when either count reaches zero. `seal` re-checks both.
    released: Event,
}

/// A local write in flight. Its `Drop` is the release — an unwind out of a
/// failed write must not wedge a seal forever.
struct LocalWrite(Arc<SealBarrier>);

impl Drop for LocalWrite {
    fn drop(&mut self) {
        if self.0.local_writes.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.released.notify(usize::MAX);
        }
    }
}

/// An authenticated request owns this admission until its future is dropped.
/// Cancellation is a normal exit path, not just a transport error.
struct WireAdmission<'a> {
    engine: &'a ReplicaEngine,
}

impl Drop for WireAdmission<'_> {
    fn drop(&mut self) {
        self.engine.state.lock().active_wire_operations -= 1;
        self.engine.barrier.released.notify(usize::MAX);
    }
}

struct DrainClaim<'a> {
    engine: &'a ReplicaEngine,
    lane: ReplicaLane,
    flight: Arc<Flight>,
}

impl Drop for DrainClaim<'_> {
    fn drop(&mut self) {
        let mut state = self.engine.state.lock();
        if state
            .active_drains
            .get(&self.lane)
            .is_some_and(|active| Arc::ptr_eq(active, &self.flight))
        {
            state.active_drains.remove(&self.lane);
        }
        drop(state);
        if self.flight.result.lock().is_none() {
            self.flight.complete(Err(ReplicaError::Transport(
                "drain cancelled; pending operations retained".into(),
            )));
        }
    }
}

/// The actor-isolated half.
#[derive(Default)]
struct EngineState {
    /// A sealed engine admits no new local authoring and no authenticated
    /// transport.
    sealed: bool,
    active_wire_operations: usize,
    /// Per LANE, not per engine: a fat bulk batch is what dies on a slow link,
    /// and the one-row message that would have succeeded must not wait out ITS
    /// backoff.
    cold_until: HashMap<ReplicaLane, Instant>,
    /// A task owns this lane's scheduled queue.
    scheduled_pushes: HashSet<ReplicaLane>,
    /// One flight per lane: that is what lets an interactive write leave while
    /// a bulk push is still on the wire.
    active_drains: HashMap<ReplicaLane, Arc<Flight>>,
    /// Entries whose gate was already narrated.
    /// TRUE when ANY page of the last caught-up round reported the server is
    /// still materializing this owner's world.
    /// Deltas intentionally omitted in persisted projection-only mode.
    /// Full replication refuses unsupported or corrupt checkpoints.
    skipped_delta_count: usize,
    /// How many client writes have been undone by rejected verdicts this
    /// session.
    reverted_count: usize,
    on_rejected: Option<RejectionHandler>,
    /// Test seam: thrown between frame apply and cursor advance to prove
    /// checkpoint atomicity.
    checkpoint_fault: Option<CheckpointFault>,
}

/// Everything the engine is configured with. Upstream this is one long
/// initializer with defaults; the defaults are the same.
pub struct ReplicaEngineOptions {
    /// Where the per-owner files live.
    pub directory: PathBuf,
    pub transport: Arc<dyn ReplicaTransport>,
    pub schema: ReplicaSchema,
    pub codecs: Vec<Arc<dyn ReplicaCodec>>,
    pub document_mode: ReplicaDocumentMode,
    pub batch_limit: usize,
    pub cold_window: Duration,
    pub peer_minter: PeerMinter,
    pub clock: Clock,
    pub automatically_push_writes: bool,
    /// The suffix a test process adds so two of them never share a store.
    pub store_suffix: String,
    /// Host-supplied at INIT, per stream — like a cache normalizer. Judged on
    /// EVERY drain; the gate is derived, never stored. Birth-only by design:
    /// the engine self-drains from its first write, so a registration door
    /// would race the first judge.
    pub sync_gates: Vec<Arc<dyn SyncGate>>,
    pub spawner: Arc<dyn Spawner>,
}

impl ReplicaEngineOptions {
    pub fn new(
        directory: impl Into<PathBuf>,
        transport: Arc<dyn ReplicaTransport>,
        schema: ReplicaSchema,
        spawner: Arc<dyn Spawner>,
    ) -> Self {
        Self {
            directory: directory.into(),
            transport,
            schema,
            codecs: Vec::new(),
            document_mode: ReplicaDocumentMode::Replicated,
            batch_limit: 500,
            cold_window: Duration::from_secs(10),
            peer_minter: Arc::new(id::peer),
            clock: Arc::new(SystemTime::now),
            automatically_push_writes: true,
            store_suffix: String::new(),
            sync_gates: Vec::new(),
            spawner,
        }
    }
}

pub struct ReplicaEngine {
    pub health: Arc<crate::ReplicaHealth>,
    commit_identity: String,
    checkpoint_serial: Mutex<()>,
    pull_serial: futures::lock::Mutex<()>,
    push_serial: futures::lock::Mutex<()>,
    /// Which owner's file this process holds open. Everything below reads it;
    /// only `open`/`close`/`retire`/`adopt_merged` write it.
    binding: Arc<ReplicaBinding>,
    directory: PathBuf,
    store_suffix: String,
    schema: ReplicaSchema,
    transport: Arc<dyn ReplicaTransport>,
    codecs: HashMap<String, Arc<dyn ReplicaCodec>>,
    document_mode: ReplicaDocumentMode,
    documents: Arc<Mutex<LiveDocuments>>,
    working: Mutex<HashMap<DocumentKey, WorkingEntry>>,
    working_sealed: AtomicBool,
    batch_limit: usize,
    cold_window: Duration,
    peer_minter: PeerMinter,
    clock: Clock,
    automatically_push_writes: bool,
    sync_gates: Vec<Arc<dyn SyncGate>>,
    gate_tasks: Mutex<Vec<futures::future::AbortHandle>>,
    spawner: Arc<dyn Spawner>,
    state: Mutex<EngineState>,
    barrier: Arc<SealBarrier>,
    weak_self: Weak<ReplicaEngine>,
}

impl ReplicaEngine {
    pub fn new(options: ReplicaEngineOptions) -> Arc<Self> {
        let engine = Arc::new_cyclic(|weak_self| Self {
            health: Arc::new(crate::ReplicaHealth::default()),
            commit_identity: id::ulid(),
            checkpoint_serial: Mutex::new(()),
            pull_serial: futures::lock::Mutex::new(()),
            push_serial: futures::lock::Mutex::new(()),
            binding: Arc::new(ReplicaBinding::default()),
            directory: options.directory,
            store_suffix: options.store_suffix,
            schema: options.schema,
            transport: options.transport,
            codecs: options
                .codecs
                .into_iter()
                .map(|codec| (codec.name().to_owned(), codec))
                .collect(),
            document_mode: options.document_mode,
            documents: Arc::new(Mutex::new(LiveDocuments::default())),
            working: Mutex::new(HashMap::new()),
            working_sealed: AtomicBool::new(false),
            batch_limit: options.batch_limit,
            cold_window: options.cold_window,
            peer_minter: options.peer_minter,
            clock: options.clock,
            automatically_push_writes: options.automatically_push_writes,
            // Armed AT BIRTH: the engine self-drains from its first write, so a
            // registration that arrives by a later call can lose the race — a
            // gated field rode the wire that way live.
            sync_gates: options.sync_gates,
            gate_tasks: Mutex::new(Vec::new()),
            spawner: options.spawner,
            state: Mutex::new(EngineState::default()),
            barrier: Arc::new(SealBarrier::default()),
            weak_self: weak_self.clone(),
        });
        engine.watch_sync_gates();
        engine
    }

    /// Test seam: bind a store the caller already built, without the filesystem
    /// naming. Production has exactly one door, and it is `open(owner)`.
    pub fn with_store(
        store: Arc<ReplicaStateStore>,
        owner: i64,
        options: ReplicaEngineOptions,
    ) -> Arc<Self> {
        let engine = Self::new(options);
        let path = store.path().to_path_buf();
        engine.binding.bind(Bound { owner, store, path });
        engine
    }

    fn me(&self) -> Option<Arc<Self>> {
        self.weak_self.upgrade()
    }

    // MARK: - Lane scope

    /// Every write inside rides one lane, in order. Reads as the fact the
    /// caller actually knows: someone is waiting on this.
    pub fn lane<'a, T>(
        &self,
        lane: ReplicaLane,
        body: impl std::future::Future<Output = T> + Send + 'a,
    ) -> LaneScope<'a, T> {
        LaneScope::new(lane, body)
    }

    // MARK: - Identity boundary

    /// Freeze local write admissions and authenticated transport, then wait for
    /// every already-started push/pull AND every already-admitted local write to
    /// settle. The caller may merge or release the store only after this
    /// returns. Idempotent for the one serialized host transition.
    ///
    /// Local writes are part of the wait because the store closes behind this
    /// barrier: upstream's actor isolation excludes them for free, and here the
    /// count does it. A write that passed the seal check must reach the disk
    /// before `adopt_merged`/`close`/`retire` takes the file away from it.
    pub async fn seal(&self) -> ReplicaResult<()> {
        self.try_seal().await
    }

    pub async fn try_seal(&self) -> ReplicaResult<()> {
        self.seal_working_documents(true);
        if let Err(error) = self.flush_working_documents().await {
            self.seal_working_documents(false);
            return Err(error);
        }
        {
            let mut state = self.state.lock();
            state.sealed = true;
            if Self::settled(&state, &self.barrier) {
                return Ok(());
            }
        }
        loop {
            let listener = self.barrier.released.listen();
            if Self::settled(&*self.state.lock(), &self.barrier) {
                return Ok(());
            }
            listener.await;
        }
    }

    /// Nothing is mid-flight against the store under the outgoing identity.
    fn settled(state: &EngineState, barrier: &SealBarrier) -> bool {
        state.active_wire_operations == 0 && barrier.local_writes.load(Ordering::SeqCst) == 0
    }

    pub async fn is_sealed(&self) -> bool {
        self.state.lock().sealed
    }

    /// Re-admit local authoring and the wire once credentials and the durable
    /// store agree on one identity.
    pub async fn unseal(&self) {
        {
            let mut state = self.state.lock();
            state.sealed = false;
        }
        self.seal_working_documents(false);
        if !self.automatically_push_writes {
            return;
        }
        for lane in ReplicaLane::ALL {
            self.schedule_push_lane(lane).await;
        }
    }

    /// Seal, then push the frozen outgoing journal through a caller-pinned
    /// transport. Deliberately narrower than ordinary `drain`: the engine is
    /// sealed for the whole push, so local CRUD stays refused, and the normal
    /// engine transport is never consulted.
    ///
    /// Replaying after a crash is idempotent: accepted entries were removed by
    /// their verdict transaction; unacknowledged entries remain pending and are
    /// the only entries selected by the next call.
    pub async fn seal_and_drain(
        &self,
        pinned_source_transport: Arc<dyn ReplicaTransport>,
    ) -> ReplicaResult<Vec<ReplicaVerdict>> {
        self.try_seal().await?;
        if self.binding.store().is_none() {
            return Ok(Vec::new());
        }

        // `drain` claims its flight before that task begins its wire operation.
        // If the seal won in that narrow window, let the refused flight release
        // its claim before installing the pinned-source flight below.
        let in_flight: Vec<Arc<Flight>> =
            self.state.lock().active_drains.values().cloned().collect();
        for flight in in_flight {
            match flight.wait().await {
                // Sealing intentionally refuses flights that had not started
                // their request. The pinned flight below owns that same work.
                Err(ReplicaError::IdentityTransitionRequired) | Ok(_) => {}
                Err(error) => return Err(error),
            }
        }

        let flight = {
            let mut state = self.state.lock();
            if !Self::settled(&state, &self.barrier)
                || !state.sealed
                || !state.active_drains.is_empty()
            {
                return Err(ReplicaError::IdentityTransitionRequired);
            }
            let flight = Arc::new(Flight::default());
            // The sign-out flush drains EVERY lane (`lane: None` below): the
            // engine is sealed, nothing can race it, and what it leaves behind
            // the retirement destroys — including the message typed a second
            // ago.
            state
                .active_drains
                .insert(ReplicaLane::Bulk, flight.clone());
            flight
        };
        let _claim = DrainClaim {
            engine: self,
            lane: ReplicaLane::Bulk,
            flight: flight.clone(),
        };
        let outcome = self
            .perform_drain(None, Some(pinned_source_transport), true)
            .await;
        flight.complete(outcome.clone());
        outcome.map(|report| report.verdicts)
    }

    async fn begin_wire_operation(
        &self,
    ) -> ReplicaResult<(Arc<ReplicaStateStore>, WireAdmission<'_>)> {
        let mut state = self.state.lock();
        let Some(store) = self.binding.store() else {
            return Err(ReplicaError::NoOwner);
        };
        if state.sealed {
            return Err(ReplicaError::IdentityTransitionInProgress);
        }
        state.active_wire_operations += 1;
        Ok((store, WireAdmission { engine: self }))
    }

    // MARK: - Recovery levers

    /// The per-doc resync lever: drop the fold and blank its shard's
    /// cursor; the next pull re-bootstraps and the doc is reborn from server
    /// truth (with a fresh peer). The journal is untouched.
    pub async fn resync_document(&self, stream: &str, id: &str) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        let shard = self.schema.shard_of(stream).to_owned();
        self.with_document_reset(stream, id, || {
            store.pool().write(|ctx| {
                store.archive_document(ctx, stream, id, "explicit resync")?;
                store.delete_doc(ctx, stream, id)?;
                store.clear_cursor(ctx, &shard)
            })
        })
    }

    /// Corrupt-fold recovery: REPLACE an unreadable fold with `fold` under a
    /// NEW peer, and blank the shard's cursor so the re-bootstrap merges the
    /// server's true history back in.
    ///
    /// A replacement rather than a drop, because the row must stay WRITABLE: an
    /// edit made between the recovery and the next pull has to have somewhere
    /// to land. The new peer is persisted here — rotating and then forgetting
    /// would walk the next launch straight back into the reused counter range;
    /// `acked` resets to nothing, so the whole recovered history is owed again.
    pub async fn rebuild_document(
        &self,
        stream: &str,
        id: &str,
        fold: &[u8],
        peer: u64,
    ) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        let spec = self.writable_spec(stream)?;
        if spec.lane != StreamLane::Document {
            return Err(ReplicaError::LaneMismatch(stream.into()));
        }
        let codec_name = self.codec_name(&spec)?;
        let codec = self
            .codecs
            .get(&codec_name)
            .ok_or_else(|| ReplicaError::Codec(format!("no codec registered for {codec_name}")))?;
        let replacement = codec.merge(None, fold)?;
        let owed = codec.diff(&replacement, None)?;
        self.with_document_reset(stream, id, || {
            store.pool().write(|ctx| {
                if store
                    .doc(&ctx.tx, stream, id)?
                    .is_some_and(|row| row.peer == peer)
                {
                    return Err(ReplicaError::Codec(
                        "A rebuilt document requires a fresh authoring peer".into(),
                    ));
                }
                store.archive_document(ctx, stream, id, "explicit rebuild")?;
                store.upsert_doc(
                    ctx,
                    stream,
                    id,
                    &spec.shard,
                    &codec_name,
                    &replacement,
                    None,
                    peer,
                )?;
                self.reflect_fold(ctx, stream, id, &store, false)?;
                if !codec.is_empty_diff(&owed) {
                    let entry = store
                        .owed_delta(&ctx.tx, stream, id)?
                        .unwrap_or_else(id::ulid);
                    let op = ReplicaOp::new(entry, verb::DOC_DELTA, stream, id)
                        .with_codec(codec_name.clone())
                        .with_payload(owed.clone());
                    self.enqueue_op(ctx, &op, &store, None, current_lane())?;
                }
                store.clear_cursor(ctx, &spec.shard)
            })
        })?;
        self.schedule_push().await;
        Ok(())
    }

    /// Serialize reset with opening a document and accepting memory-only edits.
    fn with_document_reset<T>(
        &self,
        stream: &str,
        id: &str,
        reset: impl FnOnce() -> ReplicaResult<T>,
    ) -> ReplicaResult<T> {
        let key = DocumentKey::new(stream, id);
        let mut documents = self.documents.lock();
        let mut working = self.working.lock();
        let result = match working.get(&key) {
            Some(entry) => entry.writes.reset_when_saved(reset)?,
            None => reset()?,
        };

        documents.take(&key);
        working.remove(&key);
        Ok(result)
    }

    // MARK: - Owner lifecycle

    /// The owner this process is writing for — `None` while the engine is
    /// closed.
    pub fn owner(&self) -> Option<i64> {
        self.binding.owner()
    }

    /// The bound store — `None` while the engine is closed. Reads go straight
    /// through it (the pool serves concurrent readers); only the engine writes.
    pub fn store(&self) -> Option<Arc<ReplicaStateStore>> {
        self.binding.store()
    }

    pub fn binding(&self) -> &Arc<ReplicaBinding> {
        &self.binding
    }

    pub fn schema(&self) -> &ReplicaSchema {
        &self.schema
    }

    /// Where the bound owner's world lives on disk — `None` while closed.
    pub fn store_path(&self) -> Option<PathBuf> {
        self.binding.current().map(|bound| bound.path)
    }

    /// The byte plane's reference harvest: the string values of the named wire
    /// fields across every row of a stream. Feeds the staged-blob
    /// reference-watch GC — a staged blob no row and no journal op names is
    /// sweepable.
    pub fn row_field_strings(
        &self,
        stream: &str,
        fields: &[String],
    ) -> ReplicaResult<HashSet<String>> {
        let Some(store) = self.binding.store() else {
            return Ok(HashSet::new());
        };
        let wanted: Arc<Vec<String>> = Arc::new(fields.to_vec());
        let harvest = store.materialized_rows::<Vec<String>, _>(stream, None, |_, _, data| {
            Some(
                wanted
                    .iter()
                    .filter_map(|field| {
                        data.get(field)
                            .and_then(ReplicaValue::as_string)
                            .map(str::to_owned)
                    })
                    .collect(),
            )
        })?;
        Ok(harvest.rows.into_iter().flat_map(|row| row.model).collect())
    }

    pub fn store_url(&self, owner: i64) -> PathBuf {
        self.directory
            .join(format!("replica-{owner}{}.sqlite", self.store_suffix))
    }

    /// Open this owner's file — creating it on first sight. Reopening the owner
    /// already bound is a no-op, so a re-minted session for the same guest
    /// keeps its world and its live observations.
    pub async fn open(&self, owner: i64) -> ReplicaResult<()> {
        if self.binding.owner() == Some(owner) {
            self.unseal().await;
            return Ok(());
        }
        self.try_seal().await?;
        self.release_binding(false).await?;
        let path = self.store_url(owner);
        std::fs::create_dir_all(&self.directory)
            .map_err(|error| ReplicaError::Storage(format!("create store directory: {error}")))?;
        let store = Arc::new(ReplicaStateStore::open(&path)?);
        self.heal_cursors(&store)?;
        self.binding.bind(Bound { owner, store, path });
        self.state.lock().sealed = false;
        self.unseal().await;
        Ok(())
    }

    /// Cold boot: bind the identity the keychain already holds BEFORE the first
    /// read, without a hop — a returning user's grid renders from disk on the
    /// first frame, and waiting on an await here would paint them an empty
    /// world first.
    ///
    /// It can only bind from NOTHING. Changing owners is a transition and goes
    /// through `open(owner)`, where the in-flight work is quiesced.
    pub fn open_for_cold_boot(&self, owner: i64) -> ReplicaResult<()> {
        self.binding.bind_if_unbound(|| {
            let path = self.store_url(owner);
            std::fs::create_dir_all(&self.directory).map_err(|error| {
                ReplicaError::Storage(format!("create store directory: {error}"))
            })?;
            let store = Arc::new(ReplicaStateStore::open(&path)?);
            self.heal_cursors(&store)?;
            Ok(Bound { owner, store, path })
        })
    }

    /// The store/cursor invariant, checked where the store is opened rather
    /// than by whoever remembers to ask: an EMPTY replica holding a warm cursor
    /// can never heal — the cursor claims coverage the store does not hold, so
    /// tail pulls serve nothing and every row is stranded server-side. Blank
    /// the cursors and the next pull re-snapshots. The journal is untouched.
    fn heal_cursors(&self, store: &Arc<ReplicaStateStore>) -> ReplicaResult<()> {
        store.require_schema(&self.schema)?;
        store.require_document_mode(self.document_mode)?;
        self.settle_sync_gates(store, None)
    }

    /// An adopted world changed owners, and a cursor is a coverage claim about
    /// ONE owner's pull timeline — the guest's. The account's pre-merge rows sit
    /// below that cursor (xids are cluster-global), so carrying it forward hides
    /// the account's whole prior world from every tail pull, forever. Void
    /// unconditionally; the next pull re-bootstraps, and the reset is safe for
    /// the same reason `reset_cursors` is: the journal survives.
    /// Let this owner's world go: quiesce, then close the pool. The file stays
    /// — a sign-out that keeps the device's world for a later sign-in closes,
    /// it does not retire.
    pub async fn close(&self) -> ReplicaResult<()> {
        self.try_close().await
    }

    pub async fn try_close(&self) -> ReplicaResult<()> {
        self.try_seal().await?;
        self.release_binding(false).await?;
        Ok(())
    }

    /// A foreign identity took the device: close AND delete. Wipe is the file
    /// going away, so there is no wiping pass that could miss a table.
    pub async fn retire(&self) -> ReplicaResult<()> {
        self.try_retire().await
    }

    pub async fn try_retire(&self) -> ReplicaResult<()> {
        self.try_seal().await?;
        self.release_binding(true).await?;
        Ok(())
    }

    async fn release_binding(&self, retiring: bool) -> ReplicaResult<()> {
        let Some(released) = self.binding.current() else {
            return Ok(());
        };
        released.store.close()?;
        self.binding.unbind();
        self.quiesce_in_flight().await;
        self.documents.lock().activate(self.binding.snapshot().1);
        self.working.lock().clear();
        if retiring {
            ReplicaStateStore::remove(&released.path)?;
        }
        Ok(())
    }

    async fn quiesce_in_flight(&self) {
        let mut state = self.state.lock();
        state.scheduled_pushes.clear();
        state.active_drains.clear();
        state.cold_until.clear();
    }

    /// The store, or the closed world's refusal. Every write verb enters here.
    ///
    /// The returned guard is the seal's half of the bargain: admission and the
    /// count happen together under the state lock, so a seal either observes
    /// this write and waits for it, or wins the lock first and refuses it. The
    /// caller must hold the guard across its `pool().write` — dropping it early
    /// re-opens exactly the window this closes.
    async fn admit_local_write(&self) -> ReplicaResult<(Arc<ReplicaStateStore>, LocalWrite)> {
        let state = self.state.lock();
        let Some(store) = self.binding.store() else {
            return Err(ReplicaError::NoOwner);
        };
        if state.sealed {
            return Err(ReplicaError::IdentityTransitionInProgress);
        }
        self.barrier.local_writes.fetch_add(1, Ordering::SeqCst);
        let admitted = LocalWrite(self.barrier.clone());
        drop(state);
        Ok((store, admitted))
    }
}

/// Civil date from a Unix day count — Howard Hinnant's algorithm, so a create
/// stamp can be ISO8601 without a calendar dependency.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

pub(crate) fn iso8601(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0);
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let time_of_day = seconds.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time_of_day / 3600,
        (time_of_day % 3600) / 60,
        time_of_day % 60
    )
}

// MARK: - Pull

impl ReplicaEngine {
    /// One `/pull` request of the shard's round, behind the drain barrier — the
    /// echo of anything owed is in the answer. Answers the frames it published:
    /// none while the round is only staged.
    pub async fn pull_once(&self, shard: &str) -> ReplicaResult<usize> {
        if self.binding.store().is_none() || self.is_sealed().await {
            return Ok(0);
        }
        self.drain_if_warm().await?;
        Ok(self.pull_page(shard).await?.applied)
    }

    /// The named shards (every shard by default) until the server reports
    /// nothing further waiting. Warm-up, foreground and reconnect walk every
    /// shard; the doorbell asks for the one it rang for.
    pub async fn pull_until_caught_up(&self, shards: Option<&[String]>) -> ReplicaResult<usize> {
        if self.binding.store().is_none() || self.is_sealed().await {
            return Ok(0);
        }
        self.drain_if_warm().await?;
        let shards: Vec<String> = shards
            .map(<[String]>::to_vec)
            .unwrap_or_else(|| self.schema.shards().to_vec());
        let mut total = 0;
        for shard in shards {
            // A round the shard could not publish is forgotten once; a second
            // one in the same walk is the caller's to see.
            let mut forgotten: Option<ReplicaError> = None;
            loop {
                let step = self.pull_page(&shard).await?;
                if let Some(reason) = step.forgotten {
                    if forgotten.is_some() {
                        return Err(reason);
                    }
                    forgotten = Some(reason);
                }
                total += step.applied;
                if !step.more {
                    break;
                }
            }
        }
        Ok(total)
    }

    /// The shard's published cursor; a round only staged leaves it unchanged.
    pub async fn current_cursor(&self, shard: &str) -> ReplicaResult<Option<String>> {
        let Some(store) = self.binding.store() else {
            return Ok(None);
        };
        store.pool().read(|db| store.cursor(db, shard))
    }

    /// Blow away every shard's read position — the "rebuild the replica" lever;
    /// the next pull re-snapshots. Safe BECAUSE the journal survives.
    pub async fn reset_cursors(&self) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        store.pool().write(|ctx| {
            for shard in self.schema.shards() {
                store.clear_cursor(ctx, shard)?;
            }
            Ok(())
        })
    }

    async fn pull_page(&self, shard: &str) -> ReplicaResult<PullStep> {
        let (store, _admission) = self.begin_wire_operation().await?;
        self.pull_page_body(shard, &store).await
    }

    async fn pull_page_body(
        &self,
        shard: &str,
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<PullStep> {
        let _flight = self.pull_serial.lock().await;
        self.download_page(shard, store, self.transport.as_ref())
            .await
    }
}

// MARK: - Row lane (local writes)

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowWriteExpectation {
    Absent,
    Present,
    Any,
}

impl ReplicaEngine {
    /// Birth requires absence; it cannot silently patch a colliding identity.
    pub async fn create_row(
        &self,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
    ) -> ReplicaResult<()> {
        self.write_row(
            stream,
            id,
            row_type,
            data,
            None,
            RowWriteExpectation::Absent,
        )
        .await
    }

    /// A generated model's birth: the journal carries `data`, the local row
    /// also keeps the required server-owned values `snapshot` supplies.
    pub(crate) async fn create_model_row(
        &self,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
        snapshot: &ReplicaFields,
    ) -> ReplicaResult<()> {
        self.write_row(
            stream,
            id,
            row_type,
            data,
            Some(snapshot),
            RowWriteExpectation::Absent,
        )
        .await
    }

    /// Updates require a live row; they cannot resurrect a concurrent delete.
    pub async fn update_row(
        &self,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
    ) -> ReplicaResult<()> {
        self.write_row(
            stream,
            id,
            row_type,
            data,
            None,
            RowWriteExpectation::Present,
        )
        .await
    }

    /// Engine seeding/upsert primitive. Client-facing streams intentionally
    /// expose only explicit create/update, matching the newer iOS contract.
    pub async fn save_row(
        &self,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
    ) -> ReplicaResult<()> {
        self.write_row(stream, id, row_type, data, None, RowWriteExpectation::Any)
            .await
    }

    async fn write_row(
        &self,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
        snapshot: Option<&ReplicaFields>,
        expectation: RowWriteExpectation,
    ) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        let spec = self.writable_spec(stream)?;
        if spec.lane != StreamLane::Row {
            return Err(ReplicaError::LaneMismatch(stream.to_owned()));
        }
        let requested_lane = current_lane();
        store.pool().write(|ctx| {
            self.apply_row_write(
                ctx,
                &store,
                &spec,
                stream,
                id,
                row_type,
                data,
                snapshot,
                expectation,
                requested_lane,
            )
        })?;
        self.schedule_push().await;
        Ok(())
    }

    /// The business key a row id may be: nonempty, at most 1024 bytes, no NUL.
    /// Judged at the door — the journal must never carry an address the
    /// server refuses on every retry.
    fn validate_address(stream: &str, id: &str) -> ReplicaResult<()> {
        if id.is_empty() || id.len() > 1024 || id.bytes().any(|byte| byte == 0) {
            return Err(ReplicaError::InvalidRowId {
                stream: stream.to_owned(),
                id: id.to_owned(),
            });
        }
        Ok(())
    }

    fn apply_row_write(
        &self,
        ctx: &mut WriteContext<'_>,
        store: &Arc<ReplicaStateStore>,
        spec: &ReplicaStreamSpec,
        stream: &str,
        id: &str,
        row_type: Option<&str>,
        data: &ReplicaFields,
        snapshot: Option<&ReplicaFields>,
        expectation: RowWriteExpectation,
        requested_lane: ReplicaLane,
    ) -> ReplicaResult<()> {
        Self::validate_address(stream, id)?;
        self.validate_atomic_address(ctx, store, stream, id)?;
        let existing = store.snapshot(&ctx.tx, stream, id)?;
        match (expectation, existing.is_some()) {
            (RowWriteExpectation::Absent, true) => {
                return Err(ReplicaError::RowExists {
                    stream: stream.into(),
                    id: id.into(),
                });
            }
            (RowWriteExpectation::Present, false) => {
                return Err(ReplicaError::UnknownRow {
                    stream: stream.into(),
                    id: id.into(),
                });
            }
            _ => {}
        }
        if let Some(existing) = existing {
            // A missing row field and an explicit JSON null are the same
            // nullable-column value. Generated models encode authored nil
            // as null so nonnil → nil remains observable, while this
            // normalization keeps already-empty optionals out of patches.
            let mut changed: ReplicaFields = data
                .iter()
                .filter(|(key, value)| {
                    existing.data.get(*key).unwrap_or(&ReplicaValue::Null) != *value
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            if changed.is_empty() {
                return Ok(());
            }
            for field in &spec.preconditions {
                if !changed.contains_key(field)
                    && let Some(value) = existing.data.get(field)
                {
                    changed.insert(field.clone(), value.clone());
                }
            }
            let op =
                ReplicaOp::new(id::ulid(), verb::ROW_PATCH, stream, id).with_data(changed.clone());
            let mut prior = ReplicaFields::new();
            let mut missing = Vec::new();
            for key in changed.keys() {
                match existing.data.get(key) {
                    Some(value) => {
                        prior.insert(key.clone(), value.clone());
                    }
                    None => missing.push(key.clone()),
                }
            }
            missing.sort();
            let preimage = ReplicaPreimage::Fields {
                values: prior,
                missing,
            };
            self.enqueue_op(ctx, &op, &store, Some(&preimage.encoded()?), requested_lane)?;
            let mut merged = existing.data;
            for (key, value) in changed {
                merged.insert(key, value);
            }
            store.upsert_snapshot(
                ctx,
                stream,
                id,
                &spec.shard,
                existing.row_type.as_deref().or(row_type),
                &merged,
            )
        } else {
            let op = ReplicaOp::new(id::ulid(), verb::ROW_CREATE, stream, id)
                .with_type(row_type.map(str::to_owned))
                .with_data(data.clone());
            self.enqueue_op(
                ctx,
                &op,
                &store,
                Some(&ReplicaPreimage::Absent.encoded()?),
                requested_lane,
            )?;
            // The journal carries only authored fields. The local birth keeps
            // required server-owned values supplied by the generated model.
            let mut birth = snapshot.cloned().unwrap_or_default();
            birth.extend(data.iter().map(|(key, value)| (key.clone(), value.clone())));
            store.upsert_snapshot(ctx, stream, id, &spec.shard, row_type, &birth)
        }
    }

    /// The generated `delete()`, both lanes. A row the server never heard of
    /// (its create still pending) dies silently — every owed entry discarded,
    /// no delete op; anything else journals `row.delete`. Document lane also
    /// drops the fold and its superseded delta.
    pub async fn delete_row(&self, stream: &str, id: &str) -> ReplicaResult<bool> {
        let (store, _admitted) = self.admit_local_write().await?;
        let spec = self.writable_spec(stream)?;
        let requested_lane = current_lane();
        let queued_delete = store
            .pool()
            .write(|ctx| self.apply_row_delete(ctx, &store, &spec, stream, id, requested_lane))?;
        if spec.lane == StreamLane::Document {
            self.evict_lifetimes(&HashSet::from([(stream.to_owned(), id.to_owned())]));
        }
        if queued_delete {
            self.schedule_push().await;
        }
        Ok(queued_delete)
    }
    fn apply_row_delete(
        &self,
        ctx: &mut WriteContext<'_>,
        store: &Arc<ReplicaStateStore>,
        spec: &ReplicaStreamSpec,
        stream: &str,
        id: &str,
        requested_lane: ReplicaLane,
    ) -> ReplicaResult<bool> {
        Self::validate_address(stream, id)?;
        self.validate_atomic_address(ctx, store, stream, id)?;
        let incarnation = store.incarnation(&ctx.tx, stream, id)?;
        let mut births = Vec::new();
        for entry in store.entries_addressing_verb(&ctx.tx, stream, id, verb::ROW_CREATE)? {
            if entry.op()?.incarnation == incarnation {
                births.push(entry);
            }
        }
        let mut in_flight_birth = false;
        for entry in &births {
            if store.is_frozen(&ctx.tx, &entry.id)? {
                in_flight_birth = true;
                break;
            }
        }
        let displaced = store.snapshot(&ctx.tx, stream, id)?;
        let held_document = if spec.lane == StreamLane::Document {
            store.doc(&ctx.tx, stream, id)?
        } else {
            None
        };

        // Deleting a value that never existed is ordinary CRUD silence.
        if displaced.is_none() && held_document.is_none() && births.is_empty() {
            return Ok(false);
        }

        store.delete_snapshot(ctx, stream, id)?;
        if spec.lane == StreamLane::Document {
            store.delete_doc(ctx, stream, id)?;
        }

        if in_flight_birth {
            // The create may already commit server-side. Keep it ordered
            // ahead of the delete, but drop every now-irrelevant patch or
            // delta addressed at the value.
            store.discard_lifetime(ctx, stream, id, incarnation.as_deref())?;
        } else if !births.is_empty() {
            // The birth was never attempted: the server cannot have heard it,
            // so create + dependent work collapse to nothing.
            store.discard_lifetime(ctx, stream, id, incarnation.as_deref())?;
            store.cancel_unsent_birth(&ctx.tx, stream, id)?;
            return Ok(false);
        } else if spec.lane == StreamLane::Document {
            store.discard_lifetime(ctx, stream, id, incarnation.as_deref())?;
        }

        let op = ReplicaOp::new(id::ulid(), verb::ROW_DELETE, stream, id);
        let preimage = displaced
            .map(|row| {
                ReplicaPreimage::Row {
                    shard: spec.shard.clone(),
                    row_type: row.row_type,
                    data: row.data,
                }
                .encoded()
            })
            .transpose()?;
        self.enqueue_op(ctx, &op, &store, preimage.as_deref(), requested_lane)?;
        Ok(true)
    }
}

// MARK: - Document lane (local writes)

impl ReplicaEngine {
    /// Opt a live editor into in-memory authoring. Call at open, before
    /// exposing the document to editors. Subsequent legacy update doors join
    /// this SAME document and undo manager, then await their durability receipt.
    pub fn open_working_document<S: ReplicaDocState>(
        self: &Arc<Self>,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<Arc<crate::working::WorkingDocument<S::Codec>>> {
        if let Some(working) = self.working_document::<S::Codec>(stream, id) {
            working.state::<S>()?;
            return Ok(working);
        }
        let spec = self.writable_spec(stream)?;
        if spec.lane != StreamLane::Document {
            return Err(ReplicaError::LaneMismatch(stream.into()));
        }
        // Installation and legacy transactional authoring share this gate.
        // A writer that looked up the registry before installation must
        // recheck after acquiring it; two documents may never author one peer.
        let mut live = self.documents.lock();
        let (bound, generation) = self.binding.snapshot();
        let bound = bound.ok_or(ReplicaError::NoOwner)?;
        let (row, sequence, incarnation) = bound.store.pool().read(|db| {
            Ok((
                bound.store.doc(db, stream, id)?,
                bound.store.change_sequence(db, stream)?,
                bound.store.incarnation(db, stream, id)?,
            ))
        })?;
        let row = row.ok_or_else(|| ReplicaError::UnknownDocument {
            stream: stream.into(),
            id: id.into(),
        })?;
        let incarnation = incarnation
            .ok_or_else(|| ReplicaError::Storage("Document has no entity incarnation".into()))?;
        let working = crate::working::WorkingDocument::open::<S>(
            self,
            stream,
            id,
            generation,
            incarnation,
            &row,
            sequence,
        )?;
        let reconcile = Arc::downgrade(&working);
        let key = DocumentKey::new(stream, id);
        let mut held = self.working.lock();
        if self.working_sealed.load(Ordering::Acquire) || self.binding.snapshot().1 != generation {
            return Err(ReplicaError::IdentityTransitionInProgress);
        }
        // A second opener adopts the first author's identity and history.
        if let Some(existing) = held
            .get(&key)
            .filter(|entry| entry.generation == generation)
        {
            let working: Arc<crate::working::WorkingDocument<S::Codec>> = existing
                .value
                .clone()
                .downcast()
                .map_err(|_| ReplicaError::Codec("working codec mismatch".into()))?;
            working.state::<S>()?;
            return Ok(working);
        }
        live.take(&key);
        held.insert(
            key,
            WorkingEntry {
                generation,
                value: working.clone(),
                writes: working.writes.clone(),
                reconcile: Arc::new(move |row, sequence| match reconcile.upgrade() {
                    Some(working) => working.reconcile(row, sequence),
                    None => Ok(()),
                }),
            },
        );
        Ok(working)
    }

    pub fn working_document<C: DocumentCodec>(
        &self,
        stream: &str,
        id: &str,
    ) -> Option<Arc<crate::working::WorkingDocument<C>>> {
        let generation = self.binding.snapshot().1;
        self.working
            .lock()
            .get(&DocumentKey::new(stream, id))
            .filter(|entry| entry.generation == generation)
            .and_then(|entry| entry.value.clone().downcast().ok())
    }

    pub(crate) fn spawn_working(&self, future: crate::SpawnFuture) {
        self.spawner.spawn(future);
    }

    /// Explicit save/close barrier. Network acknowledgement is NOT required.
    pub async fn flush_working_documents(&self) -> ReplicaResult<()> {
        let flushes: Vec<_> = self
            .working
            .lock()
            .values()
            .map(|entry| entry.writes.clone())
            .collect();
        for writes in flushes {
            writes.flush().await?;
        }
        Ok(())
    }

    fn seal_working_documents(&self, sealed: bool) {
        let held = self.working.lock();
        self.working_sealed.store(sealed, Ordering::Release);
        for entry in held.values() {
            entry.writes.set_sealed(sealed);
        }
    }

    /// Background-only committed-data reconciliation; cached readers never
    /// acquire the store's writer/document locks.
    pub fn refresh_working_document(&self, stream: &str, id: &str) -> ReplicaResult<()> {
        let reconcile = self
            .working
            .lock()
            .get(&DocumentKey::new(stream, id))
            .map(|e| e.reconcile.clone());
        if let Some(reconcile) = reconcile {
            let store = self.binding.store().ok_or(ReplicaError::NoOwner)?;
            let (row, sequence) = store.pool().read(|db| {
                Ok((
                    store.doc(db, stream, id)?,
                    store.change_sequence(db, stream)?,
                ))
            })?;
            reconcile(row.as_ref(), sequence)?;
        }
        Ok(())
    }

    pub(crate) async fn record_working_delta(
        &self,
        stream: &str,
        id: &str,
        payload: &[Arc<[u8]>],
        generation: u64,
        incarnation: &str,
        peer: u64,
        confirmed: &[u8],
        lane: ReplicaLane,
    ) -> ReplicaResult<(Vec<u8>, i64)> {
        let (store, _admitted) = self.admit_local_write().await?;
        if self.binding.snapshot().1 != generation {
            return Err(ReplicaError::IdentityTransitionInProgress);
        }
        let saved = store.pool().write(|ctx| {
            let doc =
                store
                    .doc(&ctx.tx, stream, id)?
                    .ok_or_else(|| ReplicaError::UnknownDocument {
                        stream: stream.into(),
                        id: id.into(),
                    })?;
            if store.incarnation(&ctx.tx, stream, id)?.as_deref() != Some(incarnation)
                || doc.peer != peer
            {
                return Err(ReplicaError::Codec(
                    "Working document authoring lease is no longer current".into(),
                ));
            }
            let codec = self
                .codecs
                .get(&doc.codec)
                .ok_or_else(|| ReplicaError::Codec("working codec unavailable".into()))?;
            let version = codec.version(&doc.fold)?;
            if codec.merge_versions(Some(confirmed), &version)? != version {
                return Err(ReplicaError::Codec(
                    "Document was replaced while local edits were pending".into(),
                ));
            }
            let fold = codec.merge_batch(&doc.fold, payload)?;
            let owed = codec.diff(&fold, doc.acked.as_deref())?;
            store.update_doc(ctx, stream, id, Some(&fold), None)?;
            self.reflect_fold(ctx, stream, id, &store, true)?;
            if codec.is_empty_diff(&owed) {
                store.discard_owed_delta(ctx, stream, id)?;
            } else {
                let entry = store
                    .owed_delta(&ctx.tx, stream, id)?
                    .unwrap_or_else(id::ulid);
                let op = ReplicaOp::new(entry, verb::DOC_DELTA, stream, id)
                    .with_codec(doc.codec)
                    .with_payload(owed);
                self.enqueue_op(ctx, &op, &store, None, lane)?;
            }
            Ok((
                codec.version(&fold)?,
                store.change_sequence(&ctx.tx, stream)?,
            ))
        })?;
        self.schedule_push().await;
        Ok(saved)
    }

    /// Birth the document: fold = seed, acked = nothing (the server knows
    /// nothing until the verdict), and the journaled `row.create` carrying
    /// codec + seed. `peer` is the seed's authoring peer — recorded so the app
    /// can keep authoring under it.
    pub async fn create_doc(
        &self,
        stream: &str,
        id: &str,
        seed: &[u8],
        peer: u64,
        data: &ReplicaFields,
        stamp: Option<&ReplicaCreateStamp>,
    ) -> ReplicaResult<bool> {
        let (store, _admitted) = self.admit_local_write().await?;
        Self::validate_address(stream, id)?;
        let spec = self.writable_spec(stream)?;
        if spec.lane != StreamLane::Document {
            return Err(ReplicaError::LaneMismatch(stream.to_owned()));
        }
        let codec_name = self.codec_name(&spec)?;
        self.codecs
            .get(&codec_name)
            .ok_or_else(|| ReplicaError::Codec(format!("no codec registered for {codec_name}")))?
            .merge(None, seed)?;
        let row_data = self.stamped(data, stamp.or(spec.stamp.as_ref()), self.binding.owner());
        let requested_lane = current_lane();
        let inserted = store.pool().write(|ctx| {
            let existing_births =
                store.entries_addressing_verb(&ctx.tx, stream, id, verb::ROW_CREATE)?;
            if store.snapshot(&ctx.tx, stream, id)?.is_some()
                || store.doc(&ctx.tx, stream, id)?.is_some()
                || !existing_births.is_empty()
            {
                return Ok(false);
            }

            let op = ReplicaOp::new(id::ulid(), verb::ROW_CREATE, stream, id)
                .with_codec(codec_name.clone())
                .with_seed(seed.to_vec());
            self.enqueue_op(
                ctx,
                &op,
                &store,
                Some(&ReplicaPreimage::Absent.encoded()?),
                requested_lane,
            )?;
            store.upsert_snapshot(ctx, stream, id, &spec.shard, None, &row_data)?;
            store.upsert_doc(ctx, stream, id, &spec.shard, &codec_name, seed, None, peer)?;
            self.reflect_fold(ctx, stream, id, &store, false)?;
            Ok(true)
        })?;
        if inserted {
            self.schedule_push().await;
        }
        Ok(inserted)
    }

    /// A local edit: merged into the fold, then SUPERSEDED into the one owed
    /// `doc.delta` per document — the entry's payload is always
    /// `diff(fold, since: acked)`, so consecutive edits fold into a single op
    /// under a stable id. Frozen and refused deltas are never edited: the next
    /// edit starts a new owed one.
    pub async fn record_doc_delta(
        &self,
        stream: &str,
        id: &str,
        payload: &[u8],
    ) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        let spec = self.writable_spec(stream)?;
        if spec.lane != StreamLane::Document {
            return Err(ReplicaError::LaneMismatch(stream.to_owned()));
        }
        let requested_lane = current_lane();
        store.pool().write(|ctx| {
            let Some(doc) = store.doc(&ctx.tx, stream, id)? else {
                return Err(ReplicaError::UnknownDocument {
                    stream: stream.to_owned(),
                    id: id.to_owned(),
                });
            };
            let Some(codec) = self.codecs.get(&doc.codec) else {
                return Err(ReplicaError::Codec(format!(
                    "no codec registered for {}",
                    doc.codec
                )));
            };
            let fold = codec.merge(Some(&doc.fold), payload)?;
            store.update_doc(ctx, stream, id, Some(&fold), None)?;
            self.reflect_fold(ctx, stream, id, &store, true)?;

            let owed = codec.diff(&fold, doc.acked.as_deref())?;
            if codec.is_empty_diff(&owed) {
                store.discard_owed_delta(ctx, stream, id)
            } else {
                let entry = store
                    .owed_delta(&ctx.tx, stream, id)?
                    .unwrap_or_else(id::ulid);
                let op = ReplicaOp::new(entry, verb::DOC_DELTA, stream, id)
                    .with_codec(doc.codec.clone())
                    .with_payload(owed);
                self.enqueue_op(ctx, &op, &store, None, requested_lane)
                    .map(|_| ())
            }
        })?;
        self.schedule_push().await;
        Ok(())
    }

    pub(crate) fn document_codec<C: DocumentCodec>(&self) -> ReplicaResult<&C> {
        self.codecs
            .get(C::CODEC_NAME)
            .and_then(|codec| (codec.as_ref() as &dyn std::any::Any).downcast_ref())
            .ok_or_else(|| {
                ReplicaError::Codec(format!("no document codec registered as {}", C::CODEC_NAME))
            })
    }

    /// Synchronous, version-memoized first read. A corrupt stored fold is
    /// reported without replacing it. All reads reconcile with committed data,
    /// including changes made through older raw-delta and checkpoint doors.
    pub fn document_state<S: ReplicaDocState>(
        &self,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<Option<Arc<S>>> {
        if let Some(working) = self.working_document::<S::Codec>(stream, id) {
            return working.state::<S>().map(Some);
        }
        self.read_document_state::<S>(stream, id, false)
    }

    /// Peek without opening a cold document. A held document is still checked
    /// against the durable fold, so deletion/reset never serves stale state.
    pub fn document_held_state<S: ReplicaDocState>(
        &self,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<Option<Arc<S>>> {
        if let Some(working) = self.working_document::<S::Codec>(stream, id) {
            return working.state::<S>().map(Some);
        }
        self.read_document_state::<S>(stream, id, true)
    }

    fn read_document_state<S: ReplicaDocState>(
        &self,
        stream: &str,
        id: &str,
        held_only: bool,
    ) -> ReplicaResult<Option<Arc<S>>> {
        let codec = self.document_codec::<S::Codec>()?;
        let mut live = self.documents.lock();
        let (bound, generation) = self.binding.snapshot();
        live.activate(generation);
        let key = DocumentKey::new(stream, id);
        let held = live.take(&key);
        let Some(bound) = bound else { return Ok(None) };
        if held_only && held.is_none() {
            return Ok(None);
        }
        let Some(row) = bound
            .store
            .pool()
            .read(|db| bound.store.doc(db, stream, id))?
        else {
            return Ok(None);
        };
        let mut held = HeldDocument::reconcile(held, &row, codec)?;
        let state = held.state::<S>(codec)?;
        live.put(key, held);
        Ok(Some(state))
    }

    pub fn pin_document(&self, stream: &str, id: &str) -> DocumentPin {
        LiveDocuments::pin(
            &self.documents,
            || self.binding.snapshot().1,
            DocumentKey::new(stream, id),
        )
    }

    /// Mutate the held document and commit its fold/journal atomically. The
    /// closure is synchronous and must not call engine APIs or retain the
    /// document. No reader sees a half-edit. Error/unwind drops the held copy;
    /// SQLite rolls back and the next read opens the last committed fold.
    pub async fn update_document<C: DocumentCodec>(
        &self,
        stream: &str,
        id: &str,
        body: impl FnOnce(&mut C::Document) -> ReplicaResult<()> + Send,
    ) -> ReplicaResult<bool> {
        if let Some(working) = self.working_document::<C>(stream, id) {
            let receipt = working.edit(body)?;
            working.wait_saved(receipt.sequence).await?;
            return Ok(receipt.changed);
        }
        let probe_start = std::time::Instant::now();
        let (store, _admitted) = self.admit_local_write().await?;
        let admission_ms = probe_start.elapsed().as_secs_f64() * 1000.0;
        let spec = self.writable_spec(stream)?;
        if spec.lane != StreamLane::Document {
            return Err(ReplicaError::LaneMismatch(stream.into()));
        }
        let codec = self.document_codec::<C>()?;
        let requested_lane = current_lane();
        let mut stages = [0.0_f64; 5];
        let gate_start = std::time::Instant::now();
        let gate_ms;
        let transaction_ms;
        let mut closure_ms = 0.0;
        let mut joined = None;
        let changed = {
            let mut live = self.documents.lock();
            gate_ms = gate_start.elapsed().as_secs_f64() * 1000.0;
            if let Some(working) = self.working_document::<C>(stream, id) {
                let receipt = working.edit(body)?;
                joined = Some((working, receipt));
                transaction_ms = 0.0;
                false
            } else {
                let (bound, generation) = self.binding.snapshot();
                if !bound.is_some_and(|bound| Arc::ptr_eq(&bound.store, &store)) {
                    return Err(ReplicaError::IdentityTransitionInProgress);
                }
                live.activate(generation);
                let key = DocumentKey::new(stream, id);
                let mut previous = live.take(&key);
                let transaction_start = std::time::Instant::now();
                let (held, changed) = store.pool().write(|ctx| {
                    let closure_start = std::time::Instant::now();
                    let row = store.doc(&ctx.tx, stream, id)?.ok_or_else(|| {
                        ReplicaError::UnknownDocument {
                            stream: stream.into(),
                            id: id.into(),
                        }
                    })?;
                    let mut held = HeldDocument::reconcile(previous.take(), &row, codec)?;
                    stages[0] = closure_start.elapsed().as_secs_f64() * 1000.0;
                    let edit_start = std::time::Instant::now();
                    let document = held.document_mut::<C>()?;
                    let before = codec.document_version(document);
                    body(document)?;
                    if before == codec.document_version(document) {
                        stages[1] = edit_start.elapsed().as_secs_f64() * 1000.0;
                        closure_ms = closure_start.elapsed().as_secs_f64() * 1000.0;
                        return Ok((held, false));
                    }
                    stages[1] = edit_start.elapsed().as_secs_f64() * 1000.0;
                    let snapshot_start = std::time::Instant::now();
                    let fold = codec.document_snapshot(document)?;
                    stages[2] = snapshot_start.elapsed().as_secs_f64() * 1000.0;
                    let delta_start = std::time::Instant::now();
                    let owed = codec.export_document_delta(document, row.acked.as_deref())?;
                    stages[3] = delta_start.elapsed().as_secs_f64() * 1000.0;
                    let journal_start = std::time::Instant::now();
                    store.update_doc(ctx, stream, id, Some(&fold), None)?;
                    self.reflect_fold(ctx, stream, id, &store, true)?;
                    if codec.is_empty_diff(&owed) {
                        store.discard_owed_delta(ctx, stream, id)?;
                    } else {
                        let entry = store
                            .owed_delta(&ctx.tx, stream, id)?
                            .unwrap_or_else(id::ulid);
                        let op = ReplicaOp::new(entry, verb::DOC_DELTA, stream, id)
                            .with_codec(row.codec)
                            .with_payload(owed);
                        self.enqueue_op(ctx, &op, &store, None, requested_lane)?;
                    }
                    held.fold = fold;
                    stages[4] = journal_start.elapsed().as_secs_f64() * 1000.0;
                    closure_ms = closure_start.elapsed().as_secs_f64() * 1000.0;
                    Ok((held, true))
                })?;
                transaction_ms = transaction_start.elapsed().as_secs_f64() * 1000.0;
                live.put(key, held);
                changed
            }
        };
        if let Some((working, receipt)) = joined {
            // All synchronous gates have been released before durability.
            working.wait_saved(receipt.sequence).await?;
            return Ok(receipt.changed);
        }
        let schedule_start = std::time::Instant::now();
        if changed {
            self.schedule_push().await
        }
        // Control thread only, after releasing the document/store locks. No
        // document contents or plugin state enter this temporary timing trace.
        log::info!(target: "replicaman::control_probe",
            "[control-probe] stage=document_save stream={stream} document={id} changed={changed} admission_ms={admission_ms:.3} gate_ms={gate_ms:.3} read_reconcile_ms={:.3} edit_ms={:.3} snapshot_ms={:.3} delta_ms={:.3} journal_ms={:.3} transaction_wait_commit_ms={:.3} schedule_ms={:.3} total_ms={:.3}",
            stages[0], stages[1], stages[2], stages[3], stages[4],
            (transaction_ms - closure_ms).max(0.0), schedule_start.elapsed().as_secs_f64() * 1000.0,
            probe_start.elapsed().as_secs_f64() * 1000.0);
        Ok(changed)
    }

    pub async fn undo_document<C: DocumentCodec>(
        &self,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<bool> {
        let codec = self.document_codec::<C>()?;
        self.update_document::<C>(stream, id, |document| codec.undo(document).map(|_| ()))
            .await
    }

    pub async fn redo_document<C: DocumentCodec>(
        &self,
        stream: &str,
        id: &str,
    ) -> ReplicaResult<bool> {
        let codec = self.document_codec::<C>()?;
        self.update_document::<C>(stream, id, |document| codec.redo(document).map(|_| ()))
            .await
    }

    /// Create-stamps: `userId` as the bound owner, ISO8601 timestamps from the
    /// injected clock.
    fn stamped(
        &self,
        data: &ReplicaFields,
        stamp: Option<&ReplicaCreateStamp>,
        user_id: Option<i64>,
    ) -> ReplicaFields {
        let (Some(stamp), Some(user_id)) = (stamp, user_id) else {
            return data.clone();
        };
        let mut result = data.clone();
        let timestamp = iso8601((self.clock)());
        if let Some(field) = &stamp.user_id {
            result.insert(field.clone(), ReplicaValue::signed_integer(user_id));
        }
        if let Some(field) = &stamp.created_at {
            result.insert(field.clone(), ReplicaValue::string(timestamp.clone()));
        }
        if let Some(field) = &stamp.updated_at {
            result.insert(field.clone(), ReplicaValue::string(timestamp));
        }
        result
    }

    /// The journal write half of a local op: claim the lane, keep the two
    /// invariants that make overtaking safe, then enqueue.
    ///
    ///   STICKINESS — a row's later ops join the lane its pending ops are on.
    ///     Otherwise a bulk patch passes the interactive create of its own row
    ///     and the server refuses an update to a row it has never seen.
    ///   PROMOTION — an interactive op naming a pending bulk row pulls that row
    ///     (and what IT names, transitively) onto the interactive lane. Ids are
    ///     unique minted strings, so matching op data values against pending
    ///     row ids needs no foreign-key schema; a false match costs one row
    ///     shipping sooner.
    fn enqueue_op(
        &self,
        ctx: &mut WriteContext<'_>,
        op: &ReplicaOp,
        store: &Arc<ReplicaStateStore>,
        preimage: Option<&[u8]>,
        requested: ReplicaLane,
    ) -> ReplicaResult<ReplicaLane> {
        let identified = store.identify(
            &ctx.tx,
            op,
            &self.schema,
            op.verb == verb::ROW_CREATE,
            preimage,
        )?;
        let op = &identified;
        if ctx.atomic_entries.is_some() {
            self.validate_atomic_admission(ctx, store, op, preimage)?;
            let lane = self.journal_op(ctx, op, store, preimage, requested)?;
            if let Some(entries) = &mut ctx.atomic_entries {
                entries.push(op.id.clone());
            }
            return Ok(lane);
        }
        if !self.admit_gate(ctx, op, store, preimage)? {
            return Ok(requested);
        }
        self.journal_op(ctx, op, store, preimage, requested)
    }

    fn journal_op(
        &self,
        ctx: &mut WriteContext<'_>,
        op: &ReplicaOp,
        store: &Arc<ReplicaStateStore>,
        preimage: Option<&[u8]>,
        requested: ReplicaLane,
    ) -> ReplicaResult<ReplicaLane> {
        let identified;
        let op = if op.incarnation.is_none() {
            identified = store.identify(&ctx.tx, op, &self.schema, false, preimage)?;
            &identified
        } else {
            op
        };
        let queued = store.pending_entries(&ctx.tx, &op.stream, &op.row_id)?;
        let mut lane = queued.first().map_or(requested, |entry| entry.lane);
        let mut also_walk: Vec<ReplicaOp> = Vec::new();
        if requested == ReplicaLane::Interactive && lane == ReplicaLane::Bulk {
            let ids: Vec<String> = queued.iter().map(|entry| entry.id.clone()).collect();
            store.promote(ctx, &ids)?;
            lane = ReplicaLane::Interactive;
            // Those entries were pulled up by their ROW, so nothing has walked
            // what THEY name yet.
            also_walk = queued
                .iter()
                .map(|entry| ReplicaOp::from_json(&entry.payload))
                .collect::<ReplicaResult<_>>()?;
        }
        store.enqueue(
            ctx,
            &op.id,
            &op.verb,
            &op.stream,
            &op.row_id,
            &op.to_json()?,
            preimage,
            lane,
        )?;
        if lane == ReplicaLane::Interactive {
            let mut frontier = vec![op.clone()];
            frontier.extend(also_walk);
            self.promote_dependencies(ctx, frontier, store)?;
        }
        Ok(lane)
    }

    /// Every id a value mentions, at any depth — a row reference can sit inside
    /// an array or a nested document, not only in a top-level string column.
    fn named_ids(value: &ReplicaValue, found: &mut HashSet<String>) {
        match value {
            ReplicaValue::String(text) => {
                found.insert(text.clone());
            }
            ReplicaValue::Array(items) => {
                for item in items {
                    Self::named_ids(item, found);
                }
            }
            ReplicaValue::Object(fields) => {
                for field in fields.values() {
                    Self::named_ids(field, found);
                }
            }
            _ => {}
        }
    }

    fn promote_dependencies(
        &self,
        ctx: &mut WriteContext<'_>,
        ops: Vec<ReplicaOp>,
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<()> {
        let mut frontier = ops;
        let mut promoted: Vec<String> = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();
        while let Some(current) = frontier.pop() {
            let mut named: HashSet<String> = HashSet::new();
            for value in current.data.unwrap_or_default().values() {
                Self::named_ids(value, &mut named);
            }
            let fresh: Vec<String> = named.difference(&visited).cloned().collect();
            if fresh.is_empty() {
                continue;
            }
            visited.extend(fresh.iter().cloned());
            for entry in store.pending_bulk_entries(&ctx.tx, &fresh)? {
                // An entry whose bytes will not decode is the drain's problem to
                // park, never a reason to fail the write the user just made.
                let Ok(entry_op) = entry.op() else { continue };
                promoted.push(entry.id);
                // Transitive: what the dependency itself names must come with
                // it, or the chain breaks one link further down.
                frontier.push(entry_op);
            }
        }
        store.promote(ctx, &promoted)
    }
}

// MARK: - Scheduled delivery

impl ReplicaEngine {
    /// Something was written — kick whichever lanes now owe work. The entry may
    /// have landed on a lane the caller did NOT request (stickiness joins its
    /// row's lane; promotion pulls dependencies up), so the caller's own lane
    /// is not a reliable answer to "what needs draining".
    async fn schedule_push(&self) {
        if !self.automatically_push_writes {
            return;
        }
        let Some(store) = self.binding.store() else {
            return;
        };
        {
            let state = self.state.lock();
            if state.sealed || state.scheduled_pushes.len() >= ReplicaLane::ALL.len() {
                return;
            }
        }
        let owed = store
            .pool()
            .read(|db| store.lanes_owed(db))
            .unwrap_or_default();
        for lane in owed {
            self.schedule_push_lane(lane).await;
        }
    }

    /// CRUD commits schedule their own delivery, PER LANE — a bulk backlog
    /// stuck on the wire must not hold an interactive write behind it. One task
    /// owns each lane's queue and runs it to empty after successful pushes; a
    /// transport failure leaves the durable entries for the foreground/
    /// reconnect lifecycle.
    ///
    /// The return type is spelled out rather than inferred: this call and
    /// `push_scheduled_writes` are mutually recursive (a trailing check
    /// re-schedules), and an `async fn` cycle has no fixed point for the
    /// compiler's `Send` inference.
    fn schedule_push_lane(&self, lane: ReplicaLane) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if !self.automatically_push_writes || self.binding.store().is_none() {
                return;
            }
            {
                let mut state = self.state.lock();
                if state.sealed || state.scheduled_pushes.contains(&lane) {
                    return;
                }
                state.scheduled_pushes.insert(lane);
            }
            let Some(engine) = self.me() else { return };
            let weak = Arc::downgrade(&engine);
            drop(engine);
            self.spawner.spawn(boxed(async move {
                if let Some(engine) = weak.upgrade() {
                    engine.push_scheduled_writes(lane).await;
                }
            }));
        })
    }

    /// D6 — THE TERMINATION RULE. This loop is what has to come to rest: a
    /// gated entry is pending-and-unparked by design, so `owes_work` answers
    /// true for as long as the gate holds (minutes, for an upload), and every
    /// pass re-derives the same gate. The loop therefore asks for work the last
    /// drain has NOT already judged and withheld — `held` starts empty, so the
    /// first drain of a scheduled push always runs (a schedule IS an external
    /// wake: a write, an unseal, a lifecycle event), and a drain that comes to
    /// rest with everything gated ends it.
    ///
    /// The trailing check uses the same subtraction, which is what keeps the
    /// "arrived after our empty read" race closed: a new entry is not in `held`,
    /// so it re-arms; the gated ones are, so they do not.
    fn push_scheduled_writes(self: Arc<Self>, lane: ReplicaLane) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let mut transport_failed = false;
            let mut held: HashSet<String> = HashSet::new();
            loop {
                {
                    let state = self.state.lock();
                    if Self::is_cold_in(&state, lane, self.cold_window) || state.sealed {
                        break;
                    }
                }
                if self.binding.store().is_none() {
                    break;
                }
                let has_work = match self.has_sendable_work(lane, &held) {
                    Ok(has_work) => has_work,
                    Err(error) => {
                        // No awaiting caller owns automatic work. Publish the
                        // failure and keep the durable journal for a later retry.
                        self.health.record("read scheduled journal", error);
                        transport_failed = true;
                        break;
                    }
                };
                if !has_work {
                    break;
                }
                match self.drain_lane_reporting(lane).await {
                    Ok(report) => held = report.held,
                    Err(error) => {
                        self.health.record("scheduled push", error);
                        transport_failed = true;
                        break;
                    }
                }
            }

            // Clear ownership before the trailing check. A write that interleaves
            // here schedules its own task; otherwise this task catches the narrow
            // "arrived after our empty read" race itself.
            {
                let mut state = self.state.lock();
                state.scheduled_pushes.remove(&lane);
                if transport_failed
                    || Self::is_cold_in(&state, lane, self.cold_window)
                    || state.sealed
                {
                    return;
                }
            }
            if self.binding.store().is_none() {
                return;
            }
            match self.has_sendable_work(lane, &held) {
                Ok(true) => self.schedule_push_lane(lane).await,
                Ok(false) => {}
                Err(error) => self.health.record("read trailing journal", error),
            }
        })
    }

    /// Does this lane owe anything the last drain did not already judge and
    /// withhold? With an empty `held` this is exactly `owes_work` — the same
    /// question, one query — so the wake paths are unchanged.
    fn has_sendable_work(&self, lane: ReplicaLane, held: &HashSet<String>) -> ReplicaResult<bool> {
        let Some(store) = self.binding.store() else {
            return Ok(false);
        };
        if held.is_empty() {
            return store.pool().read(|db| store.owes_work(db, lane));
        }
        store.pool().read(|db| {
            Ok(store
                .pending_entry_ids(db, lane)?
                .iter()
                .any(|id| !held.contains(id)))
        })
    }

    fn is_cold_in(state: &EngineState, lane: ReplicaLane, _window: Duration) -> bool {
        state
            .cold_until
            .get(&lane)
            .is_some_and(|until| Instant::now() < *until)
    }

    /// Test seam mirroring upstream's `isColdForTesting`.
    pub async fn is_cold_for_testing(&self, lane: ReplicaLane) -> bool {
        let state = self.state.lock();
        Self::is_cold_in(&state, lane, self.cold_window)
    }

    /// Synchronous on purpose (pool reads never touch engine state): the doc
    /// plane loads folds without a hop.
    pub fn doc_fold(&self, stream: &str, id: &str) -> ReplicaResult<Option<Vec<u8>>> {
        let Some(store) = self.binding.store() else {
            return Ok(None);
        };
        store
            .pool()
            .read(|db| Ok(store.doc(db, stream, id)?.map(|doc| doc.fold)))
    }

    pub fn doc_peer(&self, stream: &str, id: &str) -> ReplicaResult<Option<u64>> {
        let Some(store) = self.binding.store() else {
            return Ok(None);
        };
        store
            .pool()
            .read(|db| Ok(store.doc(db, stream, id)?.map(|doc| doc.peer)))
    }
}

// MARK: - Journal / drain

impl ReplicaEngine {
    /// Both lanes, interactive first — the compatibility path (lifecycle
    /// wake-ups, tests, the identity fence) where "everything owed" is meant.
    pub async fn drain(&self) -> ReplicaResult<Vec<ReplicaVerdict>> {
        if self.binding.store().is_none() {
            return Ok(Vec::new());
        }
        let mut verdicts = self.drain_lane(ReplicaLane::Interactive).await?;
        verdicts.extend(self.drain_lane(ReplicaLane::Bulk).await?);
        Ok(verdicts)
    }

    /// One lane's queue. Single-flight PER LANE: a bulk push already on the
    /// wire does not hold this one, which is the entire point.
    pub async fn drain_lane(&self, lane: ReplicaLane) -> ReplicaResult<Vec<ReplicaVerdict>> {
        self.drain_lane_reporting(lane)
            .await
            .map(|report| report.verdicts)
    }

    /// `drain_lane` plus what only the scheduler needs: which entries this
    /// flight judged and could not send. A joiner is answered by the flight it
    /// joined, held set included — the flight ran the joiner's owed pass, so
    /// its answer is the joiner's answer.
    async fn drain_lane_reporting(&self, lane: ReplicaLane) -> ReplicaResult<DrainReport> {
        if self.binding.store().is_none() {
            return Ok(DrainReport::default());
        }
        let mut joined = Vec::new();
        loop {
            let claim = {
                let mut state = self.state.lock();
                if state.sealed {
                    return Err(ReplicaError::IdentityTransitionInProgress);
                }
                if let Some((active_lane, active)) = state.active_drains.iter().next() {
                    Err((*active_lane, active.clone()))
                } else {
                    let flight = Arc::new(Flight::default());
                    state.active_drains.insert(lane, flight.clone());
                    Ok(flight)
                }
            };
            match claim {
                Err((active_lane, active)) => {
                    let result = active.wait().await?;
                    if active_lane == lane {
                        joined.extend(result.verdicts);
                    }
                }
                Ok(flight) => {
                    let _claim = DrainClaim {
                        engine: self,
                        lane,
                        flight: flight.clone(),
                    };
                    let outcome = self.perform_drain(Some(lane), None, false).await;
                    flight.complete(outcome.clone());
                    let mut report = outcome?;
                    joined.append(&mut report.verdicts);
                    report.verdicts = joined;
                    return Ok(report);
                }
            }
        }
    }

    /// `lane: None` means EVERY lane — the sign-out flush, which owes the
    /// server whatever the journal holds before the wipe destroys it.
    async fn perform_drain(
        &self,
        lane: Option<ReplicaLane>,
        selected_transport: Option<Arc<dyn ReplicaTransport>>,
        sealed_flush: bool,
    ) -> ReplicaResult<DrainReport> {
        let (store, _admission) = if sealed_flush {
            let mut state = self.state.lock();
            let bound = self.binding.store();
            match (state.sealed, state.active_wire_operations, bound) {
                (true, 0, Some(store)) => {
                    state.active_wire_operations += 1;
                    (store, WireAdmission { engine: self })
                }
                _ => {
                    drop(state);
                    // `drain` claimed the single-flight task before suspending.
                    self.state
                        .lock()
                        .active_drains
                        .remove(&lane.unwrap_or(ReplicaLane::Bulk));
                    return Err(ReplicaError::IdentityTransitionRequired);
                }
            }
        } else {
            match self.begin_wire_operation().await {
                Ok(store) => store,
                Err(error) => {
                    // If a seal won the engine between that claim and this
                    // task's first turn, release the claim as well as refusing
                    // the wire.
                    self.state
                        .lock()
                        .active_drains
                        .remove(&lane.unwrap_or(ReplicaLane::Bulk));
                    return Err(error);
                }
            }
        };

        self.drain_body(
            lane,
            selected_transport.as_ref().unwrap_or(&self.transport),
            &store,
        )
        .await
    }

    async fn drain_body(
        &self,
        lane: Option<ReplicaLane>,
        transport: &Arc<dyn ReplicaTransport>,
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<DrainReport> {
        let _flight = self.push_serial.lock().await;
        let result = self
            .transmit_submissions(store, lane, transport.as_ref())
            .await;
        let mut state = self.state.lock();
        match &result {
            Ok(_) => {
                state.cold_until.clear();
            }
            Err(ReplicaError::Transport(_)) => {
                let until = Instant::now() + self.cold_window;
                for priority in ReplicaLane::ALL {
                    state.cold_until.insert(priority, until);
                }
            }
            Err(_) => state.cold_until.clear(),
        }
        result
    }

    pub async fn drain_if_warm(&self) -> ReplicaResult<()> {
        // Lanes share the frozen submissions. A failure must not retry those
        // same submissions through the other lane in this barrier.
        for lane in ReplicaLane::ALL {
            let cold = {
                let state = self.state.lock();
                Self::is_cold_in(&state, lane, self.cold_window)
            };
            if !cold {
                if let Err(error) = self.drain_lane(lane).await {
                    if matches!(error, ReplicaError::IdentityTransitionInProgress) {
                        return Err(error);
                    }
                    // Push and pull are independent: whatever stops the push —
                    // the wire, the server's refusal, the journal — is reported,
                    // its submissions stay for their own retry, and the shard
                    // keeps receiving.
                    self.health.record("push before pull", error);
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

// MARK: - Sync gate

impl ReplicaEngine {
    /// The reduction pass. Laws:
    /// - a gated entry holds every LATER entry of the same row (a patch must
    ///   never overtake its row's gated create);
    /// - a withheld field superseded by a LATER pending entry of the same row
    ///   is DEAD — dropped from the residual so an old value can never overtake
    ///   a newer one;
    /// - clear/absent fields are the reducer's business, not the engine's.
    fn reject_row(
        &self,
        ctx: &mut WriteContext<'_>,
        entry: &JournalRow,
        op: &ReplicaOp,
        reason: &str,
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<usize> {
        if op.verb == verb::ROW_CREATE {
            // Rollback removes dependent edits, so preserve the branch first.
            store.archive_entity(&ctx.tx, &op.stream, &op.row_id, reason)?;
        }
        let owed: Vec<_> = store
            .entries_addressing(&ctx.tx, &op.stream, &op.row_id)?
            .into_iter()
            .filter(|e| e.parked.is_none())
            .collect();
        let index = owed.iter().position(|e| e.id == entry.id).ok_or_else(|| {
            ReplicaError::Storage(format!("Refused intent is missing: {}", entry.id))
        })?;
        let current = &owed[index];
        let later = &owed[index + 1..];
        for pending in later.iter().rev() {
            if let Some(raw) = &pending.preimage {
                self.revert(
                    ctx,
                    &pending.op()?,
                    &ReplicaPreimage::parse(raw)?,
                    store,
                    false,
                )?;
            }
        }
        store.park(ctx, &entry.id, reason)?;
        if let Some(raw) = &current.preimage {
            let preimage = ReplicaPreimage::parse(raw)?;
            self.revert(ctx, op, &preimage, store, true)?;
        }
        let survivors: Vec<JournalRow> = store
            .entries_addressing(&ctx.tx, &op.stream, &op.row_id)?
            .into_iter()
            .filter(|pending| later.iter().any(|next| next.id == pending.id))
            .collect();
        self.rebase_entries(
            ctx,
            &op.stream,
            &op.row_id,
            self.schema.shard_of(&op.stream),
            store,
            &survivors,
        )?;
        Ok(usize::from(current.preimage.is_some()))
    }

    /// The undo, inside the verdict transaction.
    fn revert(
        &self,
        ctx: &mut WriteContext<'_>,
        op: &ReplicaOp,
        preimage: &ReplicaPreimage,
        store: &Arc<ReplicaStateStore>,
        cascading: bool,
    ) -> ReplicaResult<()> {
        match (op.verb.as_str(), preimage) {
            (verb::ROW_CREATE, ReplicaPreimage::Absent) => {
                store.delete_snapshot(ctx, &op.stream, &op.row_id)?;
                if !cascading {
                    return Ok(());
                }
                if self.schema.lane_of(&op.stream) == StreamLane::Document {
                    store.delete_doc(ctx, &op.stream, &op.row_id)?;
                }
                // A refused birth takes every dependent patch/delta/delete with
                // it on either lane. Its own parked entry stays as the evidence.
                store.discard_entries(ctx, &op.stream, &op.row_id, Some(&op.id))
            }
            (verb::ROW_PATCH, ReplicaPreimage::Fields { values, missing }) => {
                let Some(row) = store.snapshot(&ctx.tx, &op.stream, &op.row_id)? else {
                    return Ok(());
                };
                let mut data = row.data;
                for (key, value) in values {
                    data.insert(key.clone(), value.clone());
                }
                for key in missing {
                    data.remove(key);
                }
                store.upsert_snapshot(
                    ctx,
                    &op.stream,
                    &op.row_id,
                    self.schema.shard_of(&op.stream),
                    row.row_type.as_deref(),
                    &data,
                )
            }
            (
                verb::ROW_DELETE | verb::ROW_CREATE,
                ReplicaPreimage::Row {
                    shard,
                    row_type,
                    data,
                },
            ) => {
                store.upsert_snapshot(
                    ctx,
                    &op.stream,
                    &op.row_id,
                    shard,
                    row_type.as_deref(),
                    data,
                )?;
                if cascading
                    && self.schema.lane_of(&op.stream) == StreamLane::Document
                    && store
                        .base_row(&ctx.tx, &op.stream, &op.row_id)?
                        .is_none_or(|base| base.fold.is_none())
                {
                    // The row returns and its document comes back from the base;
                    // a document the base never held only a re-bootstrap can rebuild.
                    let target = self
                        .schema
                        .spec(&op.stream)
                        .map_or(shard.as_str(), |spec| spec.shard.as_str());
                    store.clear_cursor(ctx, target)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn advance_acked(
        &self,
        ctx: &mut WriteContext<'_>,
        op: &ReplicaOp,
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<()> {
        let Some(payload) = op.payload.as_ref().or(op.seed.as_ref()) else {
            return Ok(());
        };
        // A later local delete may already have removed this authoring fold.
        let Some(doc) = store.doc(&ctx.tx, &op.stream, &op.row_id)? else {
            return Ok(());
        };
        let codec = self
            .codecs
            .get(&doc.codec)
            .ok_or_else(|| ReplicaError::Codec(format!("No codec registered for {}", doc.codec)))?;
        let version = codec.payload_version(payload)?;
        let acked = codec.merge_versions(doc.acked.as_deref(), &version)?;
        store.update_doc(ctx, &op.stream, &op.row_id, None, Some(&acked))
    }
}

// MARK: - Observability

impl ReplicaEngine {
    pub async fn pending_ops(&self) -> ReplicaResult<Vec<JournalRow>> {
        let Some(store) = self.binding.store() else {
            return Ok(Vec::new());
        };
        store.pool().read(|db| store.pending(db))
    }

    pub async fn parked_ops(&self) -> ReplicaResult<Vec<JournalRow>> {
        let Some(store) = self.binding.store() else {
            return Ok(Vec::new());
        };
        store.pool().read(|db| store.parked(db))
    }

    /// Abandon owed or refused entries by id — the discard path (a row thrown
    /// away before the server ever heard its id must stop owing anything).
    /// Frozen entries stay until their verdict.
    pub async fn discard_ops(&self, ids: &[String]) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        store.pool().write(|ctx| {
            for id in ids {
                store.discard(ctx, id)?;
            }
            Ok(())
        })
    }

    /// The GC read seam: string values of `fields` across every journal entry
    /// (pending AND parked) of `stream`.
    pub async fn pending_field_strings(
        &self,
        stream: &str,
        fields: &[String],
    ) -> ReplicaResult<Vec<String>> {
        let Some(store) = self.binding.store() else {
            return Ok(Vec::new());
        };
        let rows = store
            .pool()
            .read(|db| store.entries_for_stream(db, stream))?;
        Ok(rows
            .iter()
            .map(|entry| entry.op())
            .collect::<ReplicaResult<Vec<_>>>()?
            .into_iter()
            .flat_map(|op| {
                let data = op.data.unwrap_or_default();
                fields
                    .iter()
                    .filter_map(|field| {
                        data.get(field)
                            .and_then(ReplicaValue::as_string)
                            .map(str::to_owned)
                    })
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    /// Row ids with ANY journal entry (pending or parked) on `stream`.
    pub async fn pending_row_ids(&self, stream: &str) -> ReplicaResult<Vec<String>> {
        let Some(store) = self.binding.store() else {
            return Ok(Vec::new());
        };
        let rows = store
            .pool()
            .read(|db| store.entries_for_stream(db, stream))?;
        let mut ids: Vec<_> = rows
            .iter()
            .map(|entry| entry.op())
            .collect::<ReplicaResult<Vec<_>>>()?
            .into_iter()
            .map(|op| op.row_id)
            .collect();
        ids.extend(
            store
                .pool()
                .read(|db| store.gate_holds(db))?
                .into_iter()
                .filter(|hold| hold.stream == stream)
                .map(|hold| hold.row_id),
        );
        Ok(ids)
    }

    pub async fn skipped_delta_count(&self) -> usize {
        self.state.lock().skipped_delta_count
    }

    pub async fn reverted_count(&self) -> usize {
        self.state.lock().reverted_count
    }

    /// The rejection seam: fired once per rejected op AFTER the verdict
    /// transaction (revert included) commits.
    pub async fn set_rejection_handler(&self, handler: Option<RejectionHandler>) {
        self.state.lock().on_rejected = handler;
    }

    /// Test seam: thrown between frame apply and cursor advance to prove
    /// checkpoint atomicity.
    pub async fn set_checkpoint_fault(&self, fault: Option<CheckpointFault>) {
        self.state.lock().checkpoint_fault = fault;
    }

    // MARK: - Plumbing

    fn writable_spec(&self, stream: &str) -> ReplicaResult<ReplicaStreamSpec> {
        let Some(spec) = self.schema.spec(stream) else {
            return Err(ReplicaError::UnknownStream(stream.to_owned()));
        };
        if spec.readonly {
            return Err(ReplicaError::ReadonlyStream(stream.to_owned()));
        }
        if self.document_mode == ReplicaDocumentMode::ProjectionsOnly
            && spec.lane == StreamLane::Document
        {
            return Err(ReplicaError::ReadonlyStream(stream.to_owned()));
        }
        Ok(spec.clone())
    }

    fn codec_name(&self, spec: &ReplicaStreamSpec) -> ReplicaResult<String> {
        if let Some(name) = &spec.codec {
            return Ok(name.clone());
        }
        if self.codecs.len() == 1
            && let Some(only) = self.codecs.keys().next()
        {
            return Ok(only.clone());
        }
        Err(ReplicaError::Codec(format!(
            "stream {} declares no codec",
            spec.name
        )))
    }
}

// MARK: - Watch
//
// GRDB's `ValueObservation` becomes: arm on the store's post-commit doorbell,
// re-read the tracked value, and yield only when it differs (`removeDuplicates`).
// Writes bump `stream_meta.change_seq` exactly where upstream does, so a
// journal-only commit never wakes a row observer, and a rolled-back checkpoint
// fires nothing because the doorbell rings after the commit.

/// What survives one arming of a watch loop.
struct WatchArming {
    was_armed: bool,
    baseline_done: bool,
    last_sequence: Option<i64>,
    last_parked: Option<Vec<JournalRow>>,
}

struct WatchLoop {
    health: Arc<crate::ReplicaHealth>,
    binding: Arc<ReplicaBinding>,
    stream: String,
    include_initial: bool,
    armed: bool,
    generation: u64,
    arming: Option<WatchArming>,
}

impl WatchLoop {
    fn new(
        binding: Arc<ReplicaBinding>,
        health: Arc<crate::ReplicaHealth>,
        stream: &str,
        include_initial: bool,
    ) -> Self {
        Self {
            binding,
            health,
            stream: stream.to_owned(),
            include_initial,
            armed: false,
            generation: u64::MAX,
            arming: None,
        }
    }

    /// Whichever side wins, the next arming waits for a store that is actually
    /// different: re-arming on the same dead pool would spin, and re-arming on
    /// a live one would duplicate the picture it is already delivering.
    async fn park(&self, generation: u64, listener: Option<event_listener::EventListener>) {
        match listener {
            Some(listener) => {
                let mut commit = Box::pin(listener);
                let mut rebind = Box::pin(self.binding.wait_for_change(generation));
                futures::future::select(&mut commit, &mut rebind).await;
            }
            None => self.binding.wait_for_change(generation).await,
        }
    }

    fn rearm_if_rebound(&mut self, generation: u64) {
        if self.generation != generation {
            self.generation = generation;
            self.arming = None;
        }
    }
}

impl ReplicaEngine {
    /// Document values over the same durable sequence as row watches. Initial
    /// read is synchronous through `document_state`; this stream can include
    /// that baseline or deliver only subsequent changes. Errors are values,
    /// never silently replaced with an empty project.
    pub fn watch_document<S: ReplicaDocState>(
        self: &Arc<Self>,
        stream: &str,
        id: &str,
        include_initial: bool,
    ) -> impl futures::Stream<Item = ReplicaResult<Option<Arc<S>>>> + Send + use<S> {
        struct State<S: ReplicaDocState> {
            engine: Arc<ReplicaEngine>,
            stream: String,
            id: String,
            generation: u64,
            sequence: Option<ReplicaResult<i64>>,
            last: Option<ReplicaResult<Option<Arc<S>>>>,
        }
        let state = State::<S> {
            engine: self.clone(),
            stream: stream.into(),
            id: id.into(),
            generation: u64::MAX,
            sequence: None,
            last: None,
        };
        futures::stream::unfold(state, move |mut state| async move {
            loop {
                let (bound, generation) = state.engine.binding.snapshot();
                if state.generation != generation {
                    state.generation = generation;
                    state.sequence = None;
                }
                let store = bound.map(|bound| bound.store);
                if store.as_ref().is_some_and(|store| store.pool().is_closed()) {
                    state.engine.binding.wait_for_change(generation).await;
                    continue;
                }
                let listener = store.as_ref().map(|store| store.pool().watch().listen());
                let sequence = match store.as_ref() {
                    Some(store) => store
                        .pool()
                        .read(|db| store.change_sequence(db, &state.stream)),
                    None => Ok(-1),
                };
                if state.sequence.as_ref() != Some(&sequence) {
                    state.sequence = Some(sequence.clone());
                    let value = sequence
                        .and_then(|_| {
                            state
                                .engine
                                .refresh_working_document(&state.stream, &state.id)
                        })
                        .and_then(|_| state.engine.document_state::<S>(&state.stream, &state.id));
                    if state.engine.binding.snapshot().1 != generation {
                        continue;
                    }
                    if state.last.as_ref() != Some(&value) {
                        let first = state.last.is_none();
                        state.last = Some(value.clone());
                        if !first || include_initial {
                            return Some((value, state));
                        }
                    }
                }
                match listener {
                    Some(listener) => {
                        let mut commit = Box::pin(listener);
                        let mut rebind = Box::pin(state.engine.binding.wait_for_change(generation));
                        futures::future::select(&mut commit, &mut rebind).await;
                    }
                    None => state.engine.binding.wait_for_change(generation).await,
                }
            }
        })
    }

    /// Post-commit signal for one stream — an observation over its durable
    /// `stream_meta.change_seq`, so consumers hang off committed state only: a
    /// rolled-back checkpoint never fires. Change-only is the default;
    /// state-owning consumers can request the committed baseline to close the
    /// observer-arming race.
    ///
    /// The signal outlives its store: a watcher armed before anyone owned the
    /// process, or held across a foreign switch, re-arms on the owner that
    /// arrives instead of observing a pool nobody writes to any more.
    pub fn watch_signal(
        &self,
        stream: &str,
        include_initial: bool,
    ) -> impl futures::Stream<Item = ()> + Send + use<> {
        let state = WatchLoop::new(
            self.binding.clone(),
            self.health.clone(),
            stream,
            include_initial,
        );
        futures::stream::unfold(state, |mut state| async move {
            loop {
                let (bound, generation) = state.binding.snapshot();
                state.rearm_if_rebound(generation);
                let Some(bound) = bound else {
                    if state.arming.is_none() {
                        // The baseline belongs to the OBSERVER, not to the
                        // store: a watcher that already reported one world sees
                        // the next owner's first picture as a CHANGE, not as
                        // another baseline.
                        let should_yield = state.include_initial && !state.armed;
                        state.armed = true;
                        state.arming = Some(WatchArming {
                            was_armed: true,
                            baseline_done: true,
                            last_sequence: None,
                            last_parked: None,
                        });
                        if should_yield {
                            return Some(((), state));
                        }
                    }
                    state.park(generation, None).await;
                    continue;
                };
                if state.arming.is_none() {
                    state.arming = Some(WatchArming {
                        was_armed: state.armed,
                        baseline_done: false,
                        last_sequence: None,
                        last_parked: None,
                    });
                    state.armed = true;
                }
                let store = bound.store;
                if store.pool().is_closed() {
                    state.park(generation, None).await;
                    continue;
                }
                let listener = store.pool().watch().listen();
                let sequence = match store
                    .pool()
                    .read(|db| store.change_sequence(db, &state.stream))
                {
                    Ok(sequence) => sequence,
                    Err(error) => {
                        // Observers retain their last committed value on a read
                        // failure. Report it instead of publishing an empty value.
                        state.health.record("watch stream", error);
                        state.park(generation, Some(listener)).await;
                        continue;
                    }
                };
                let arming = state.arming.as_mut().expect("armed above");
                if arming.last_sequence != Some(sequence) {
                    arming.last_sequence = Some(sequence);
                    let baseline = !arming.baseline_done;
                    arming.baseline_done = true;
                    let should_yield = !baseline || arming.was_armed || state.include_initial;
                    if should_yield {
                        return Some(((), state));
                    }
                }
                state.park(generation, Some(listener)).await;
            }
        })
    }

    /// Post-commit VALUES of the journal's parked entries — the refusal ledger
    /// as a subscription. Same identity-rebinding loop as `watch_signal`, but
    /// the yielded vector IS the state (baseline included): a consumer owns its
    /// picture by assignment, never by re-query, so a drain's park and a
    /// discard both arrive as committed pictures in commit order.
    pub fn watch_parked_ops(&self) -> impl futures::Stream<Item = Vec<JournalRow>> + Send + use<> {
        let state = WatchLoop::new(self.binding.clone(), self.health.clone(), "", false);
        futures::stream::unfold(state, |mut state| async move {
            loop {
                let (bound, generation) = state.binding.snapshot();
                state.rearm_if_rebound(generation);
                let Some(bound) = bound else {
                    if state.arming.is_none() {
                        state.armed = true;
                        state.arming = Some(WatchArming {
                            was_armed: true,
                            baseline_done: true,
                            last_sequence: None,
                            last_parked: Some(Vec::new()),
                        });
                        // No owner: the committed picture is "nothing parked".
                        return Some((Vec::new(), state));
                    }
                    state.park(generation, None).await;
                    continue;
                };
                if state.arming.is_none() {
                    state.arming = Some(WatchArming {
                        was_armed: state.armed,
                        baseline_done: false,
                        last_sequence: None,
                        last_parked: None,
                    });
                    state.armed = true;
                }
                let store = bound.store;
                if store.pool().is_closed() {
                    state.park(generation, None).await;
                    continue;
                }
                let listener = store.pool().watch().listen();
                let parked = match store.pool().read(|db| store.parked(db)) {
                    Ok(parked) => parked,
                    Err(error) => {
                        // Keep the last committed refusal ledger and expose
                        // the failed read through the engine's health surface.
                        state.health.record("watch parked operations", error);
                        state.park(generation, Some(listener)).await;
                        continue;
                    }
                };
                let arming = state.arming.as_mut().expect("armed above");
                if arming.last_parked.as_ref() != Some(&parked) {
                    arming.last_parked = Some(parked.clone());
                    return Some((parked, state));
                }
                state.park(generation, Some(listener)).await;
            }
        })
    }
}

include!("engine/commits.rs");

include!("engine/projections.rs");

include!("engine/gates.rs");
