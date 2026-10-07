//! The suite's fixtures — the Rust half of
//! `Tests/ReplicaManTests/Support/TestSupport.swift`. Also serves codec crates'
//! suites through the `test-support` feature.
#![allow(dead_code)]

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use crate::codec::ReplicaCodec;
use crate::engine::{ReplicaEngine, ReplicaEngineOptions};
use crate::error::{ReplicaError, ReplicaResult};
use crate::models::{ReplicaRowModel, ReplicaWritableRowModel};
use crate::schema::{ReplicaSchema, ReplicaStreamSpec};
use crate::spawner::{SpawnFuture, Spawner};
use crate::store::ReplicaStateStore;
use crate::transport::{BoxFuture, ReplicaTransport};
use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::{ReplicaOp, ReplicaVerdict};

mod protocol_fixture;

/// A scratch path that deletes itself when the handle drops.
///
/// Upstream never needs one: `ReplicaHost` mints a fresh store per process
/// under test (`storeSuffix: "-test-<UUID>"`) and the simulator throws the
/// whole container away afterwards. We have no container, so the fixture owns
/// its files. `Drop` is the entire point — it runs on the unwind out of a
/// failed assertion too, so a test that dies half-way still takes its sqlite
/// store with it, which a `remove_dir_all` in a setup function would not.
pub struct Scratch {
    path: PathBuf,
}

impl Scratch {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Deref for Scratch {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for Scratch {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // sqlite parks `-wal` and `-shm` beside the database: siblings of the
        // path we minted, not children, so sweep them by name.
        for suffix in ["", "-wal", "-shm"] {
            let mut name = self.path.clone().into_os_string();
            name.push(suffix);
            let path = PathBuf::from(name);
            // Errors are swallowed on purpose: a test that is already failing
            // must not have its verdict replaced by a cleanup panic.
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(&path);
            } else {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

/// A per-test file under the OS temp directory. Upstream uses a UUID; a
/// process-unique counter plus the pid is the same guarantee without a new
/// dependency.
pub fn temp_path(name: &str, extension: &str) -> Scratch {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push("replica-man-tests");
    path.push(format!("{name}-{}-{unique}{extension}", std::process::id()));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("temp directory");
    }
    Scratch { path }
}

/// A per-test home for per-owner files — the shape the app host uses.
pub fn temp_directory(name: &str) -> Scratch {
    let scratch = temp_path(name, "");
    std::fs::create_dir_all(scratch.path()).expect("temp directory");
    scratch
}

// MARK: - Stub codec (loro-free doc plumbing)

/// Enough codec to exercise the ENGINE's document plumbing without loro:
/// fold = concatenated payloads, version = byte count. The real merge
/// semantics live in the loro suite; this one keeps the core suite rows-only
/// (matrix 12).
pub struct StubCodec;

fn count(bytes: Option<&[u8]>) -> usize {
    bytes
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|text| text.parse::<usize>().ok())
        .unwrap_or(0)
}

impl ReplicaCodec for StubCodec {
    fn name(&self) -> &str {
        "stub@1"
    }

    fn merge(&self, fold: Option<&[u8]>, payload: &[u8]) -> ReplicaResult<Vec<u8>> {
        let mut merged = fold.unwrap_or_default().to_vec();
        merged.extend_from_slice(payload);
        Ok(merged)
    }

    fn diff(&self, fold: &[u8], since: Option<&[u8]>) -> ReplicaResult<Vec<u8>> {
        Ok(fold[count(since).min(fold.len())..].to_vec())
    }

    fn version(&self, fold: &[u8]) -> ReplicaResult<Vec<u8>> {
        Ok(fold.len().to_string().into_bytes())
    }

    fn payload_version(&self, payload: &[u8]) -> ReplicaResult<Vec<u8>> {
        Ok(payload.len().to_string().into_bytes())
    }

    fn merge_versions(&self, a: Option<&[u8]>, b: &[u8]) -> ReplicaResult<Vec<u8>> {
        Ok(count(a).max(count(Some(b))).to_string().into_bytes())
    }

    fn is_empty_diff(&self, payload: &[u8]) -> bool {
        payload.is_empty()
    }
}

/// A codec whose payloads carry history with causal dependencies: token `N`
/// depends on token `N-1`, so a fold cannot absorb a payload whose
/// predecessor it never saw — the document's own `MissingCausalDeps`. Folds
/// and payloads are the sorted, comma-separated tokens they carry.
pub struct CausalCodec;

impl CausalCodec {
    pub fn payload(tokens: impl IntoIterator<Item = u32>) -> Vec<u8> {
        let mut tokens: Vec<u32> = tokens.into_iter().collect();
        tokens.sort_unstable();
        tokens
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",")
            .into_bytes()
    }

    pub fn tokens(bytes: &[u8]) -> std::collections::BTreeSet<u32> {
        std::str::from_utf8(bytes)
            .unwrap_or_default()
            .split(',')
            .filter(|token| !token.is_empty())
            .map(|token| token.parse().expect("a causal token"))
            .collect()
    }
}

impl ReplicaCodec for CausalCodec {
    fn name(&self) -> &str {
        "causal@1"
    }

    fn merge(&self, fold: Option<&[u8]>, payload: &[u8]) -> ReplicaResult<Vec<u8>> {
        let mut seen = fold.map(Self::tokens).unwrap_or_default();
        for token in Self::tokens(payload) {
            if token > 1 && !seen.contains(&(token - 1)) {
                return Err(ReplicaError::MissingCausalDeps);
            }
            seen.insert(token);
        }
        Ok(Self::payload(seen))
    }

    fn diff(&self, fold: &[u8], since: Option<&[u8]>) -> ReplicaResult<Vec<u8>> {
        let acked = since.map(Self::tokens).unwrap_or_default();
        Ok(Self::payload(
            Self::tokens(fold)
                .into_iter()
                .filter(|token| !acked.contains(token)),
        ))
    }

    fn version(&self, fold: &[u8]) -> ReplicaResult<Vec<u8>> {
        Ok(fold.to_vec())
    }

    fn payload_version(&self, payload: &[u8]) -> ReplicaResult<Vec<u8>> {
        Ok(payload.to_vec())
    }

    fn merge_versions(&self, a: Option<&[u8]>, b: &[u8]) -> ReplicaResult<Vec<u8>> {
        let mut tokens = a.map(Self::tokens).unwrap_or_default();
        tokens.extend(Self::tokens(b));
        Ok(Self::payload(tokens))
    }

    fn is_empty_diff(&self, payload: &[u8]) -> bool {
        payload.is_empty()
    }
}

// MARK: - Stub transport

/// A frame a test scripts. The fixture server stamps its lifetime and revision
/// when it serves it, as the real server's capture decides them: a birth the
/// server accepted keeps the incarnation its client minted.
#[derive(Clone, Debug, PartialEq)]
pub enum ScriptedFrame {
    RowSet {
        stream: String,
        id: String,
        row_type: Option<String>,
        data: ReplicaFields,
    },
    RowDelete {
        stream: String,
        id: String,
    },
    DocDelta {
        stream: String,
        id: String,
        seq: i64,
        codec: String,
        payload: Vec<u8>,
    },
    DocSnapshot {
        stream: String,
        id: String,
        codec: String,
        snapshot: Vec<u8>,
        data: ReplicaFields,
    },
}

impl ScriptedFrame {
    pub fn stream(&self) -> &str {
        match self {
            Self::RowSet { stream, .. }
            | Self::RowDelete { stream, .. }
            | Self::DocDelta { stream, .. }
            | Self::DocSnapshot { stream, .. } => stream,
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::RowSet { id, .. }
            | Self::RowDelete { id, .. }
            | Self::DocDelta { id, .. }
            | Self::DocSnapshot { id, .. } => id,
        }
    }
}

/// What the server delivers after the request's cursor. The fixture server pages
/// it by the request's `limit` and answers `reset` from the request's cursor, as
/// the real server does.
#[derive(Clone, Debug, PartialEq)]
pub struct ScriptedPull {
    pub frames: Vec<ScriptedFrame>,
    pub cursor: String,
    pub more: bool,
}

impl ScriptedPull {
    pub fn new(frames: Vec<ScriptedFrame>, cursor: impl Into<String>, more: bool) -> Self {
        Self {
            frames,
            cursor: cursor.into(),
            more,
        }
    }
}

/// Canned wire: scripted pull responses per shard, scripted push verdicts,
/// injectable failures, and a full event log — matrix assertions about ORDER
/// (drain-before-pull) and COUNT (cold window, nudge coalescing) read the log.
#[derive(Clone, Debug, PartialEq)]
pub enum WireEvent {
    Pull {
        shard: String,
        cursor: Option<String>,
    },
    Push {
        ids: Vec<String>,
    },
}

type PushScript = Arc<dyn Fn(&[ReplicaOp]) -> Vec<ReplicaVerdict> + Send + Sync>;
type PushHook = Arc<dyn Fn(Vec<ReplicaOp>) -> BoxFuture<'static, ()> + Send + Sync>;
type PullHook = Arc<dyn Fn(String) -> BoxFuture<'static, ()> + Send + Sync>;
type VerdictFault = Box<dyn FnOnce(&mut Vec<ReplicaVerdict>) + Send>;

#[derive(Default)]
struct StubState {
    events: Vec<WireEvent>,
    pushed_batches: Vec<Vec<ReplicaOp>>,
    pull_queues: std::collections::HashMap<String, std::collections::VecDeque<ScriptedPull>>,
    push_script: Option<PushScript>,
    pull_fails: bool,
    push_fails: bool,
    push_failure: Option<ReplicaError>,
    push_success_budget: Option<i64>,
    lost_replies: usize,
    verdict_fault: Option<VerdictFault>,
    pull_delay: Duration,
    push_delay: Duration,
    push_hook: Option<PushHook>,
    pull_hook: Option<PullHook>,
}

#[derive(Default)]
pub struct StubTransport {
    state: Mutex<StubState>,
    protocol: protocol_fixture::ProtocolFixture,
}

impl StubTransport {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn queue_pull(&self, shard: &str, response: ScriptedPull) {
        self.state
            .lock()
            .pull_queues
            .entry(shard.to_owned())
            .or_default()
            .push_back(response);
    }

    /// Decide each fresh operation's verdict. A refused member refuses its
    /// whole group, as the server's savepoint does.
    pub fn script_push(
        &self,
        script: impl Fn(&[ReplicaOp]) -> Vec<ReplicaVerdict> + Send + Sync + 'static,
    ) {
        self.state.lock().push_script = Some(Arc::new(script));
    }

    pub fn fail_pulls(&self, fail: bool) {
        self.state.lock().pull_fails = fail;
    }

    pub fn fail_pushes(&self, fail: bool) {
        self.state.lock().push_fails = fail;
    }

    /// Every push request fails with this error before the server reads it —
    /// the server's own refusal of a request (HTTP 400), not the wire's.
    pub fn fail_pushes_with(&self, error: Option<ReplicaError>) {
        self.state.lock().push_failure = error;
    }

    /// Succeed the first `calls` pushes, then fail — the chunked-drain
    /// mid-flight transport death.
    pub fn fail_pushes_after(&self, calls: i64) {
        self.state.lock().push_success_budget = Some(calls);
    }

    /// The server commits the next `count` pushes, but their replies are lost.
    pub fn lose_push_replies(&self, count: usize) {
        self.state.lock().lost_replies = count;
    }

    /// Rewrite the next push answer after the server committed it — the
    /// proxy's malformed-verdict faults.
    pub fn corrupt_next_verdicts(
        &self,
        fault: impl FnOnce(&mut Vec<ReplicaVerdict>) + Send + 'static,
    ) {
        self.state.lock().verdict_fault = Some(Box::new(fault));
    }

    /// A restored server: every request carrying the old dataset is fenced.
    pub fn rotate_dataset(&self, dataset: &str) {
        self.protocol.rotate_dataset(dataset);
    }

    /// The server no longer recognizes any cursor it issued.
    pub fn forget_cursors(&self) {
        self.protocol.forget_cursors();
    }

    /// Awaited inside `push` BEFORE verdicts return — the observation seam for
    /// "what had already happened when this chunk went on the wire".
    pub fn on_push(
        &self,
        hook: impl Fn(Vec<ReplicaOp>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) {
        self.state.lock().push_hook = Some(Arc::new(hook));
    }

    /// Awaited inside `pull` after the request is observable but before its
    /// response is returned.
    pub fn on_pull(&self, hook: impl Fn(String) -> BoxFuture<'static, ()> + Send + Sync + 'static) {
        self.state.lock().pull_hook = Some(Arc::new(hook));
    }

    pub fn delay_pulls(&self, delay: Duration) {
        self.state.lock().pull_delay = delay;
    }

    pub fn delay_pushes(&self, delay: Duration) {
        self.state.lock().push_delay = delay;
    }

    pub fn events(&self) -> Vec<WireEvent> {
        self.state.lock().events.clone()
    }

    /// The operations the server executed, one batch per request that carried
    /// any; stored-verdict replays execute nothing.
    pub fn pushed_batches(&self) -> Vec<Vec<ReplicaOp>> {
        self.state.lock().pushed_batches.clone()
    }

    /// Every op the server has executed, flattened — the suite's commonest read.
    pub fn pushed_ops(&self) -> Vec<ReplicaOp> {
        self.state
            .lock()
            .pushed_batches
            .iter()
            .flatten()
            .cloned()
            .collect()
    }

    pub fn pushed_row_ids(&self) -> Vec<String> {
        self.pushed_ops().into_iter().map(|op| op.row_id).collect()
    }

    /// The operation ids of every push request, failed ones included.
    pub fn push_requests(&self) -> Vec<Vec<String>> {
        self.state
            .lock()
            .events
            .iter()
            .filter_map(|event| match event {
                WireEvent::Push { ids } => Some(ids.clone()),
                WireEvent::Pull { .. } => None,
            })
            .collect()
    }

    pub fn pull_count(&self) -> usize {
        self.state
            .lock()
            .events
            .iter()
            .filter(|event| matches!(event, WireEvent::Pull { .. }))
            .count()
    }

    pub fn push_count(&self) -> usize {
        self.state
            .lock()
            .events
            .iter()
            .filter(|event| matches!(event, WireEvent::Push { .. }))
            .count()
    }

    pub fn pulled_shards(&self) -> Vec<String> {
        self.state
            .lock()
            .events
            .iter()
            .filter_map(|event| match event {
                WireEvent::Pull { shard, .. } => Some(shard.clone()),
                WireEvent::Push { .. } => None,
            })
            .collect()
    }
}

impl StubTransport {
    /// Every pull request is logged and may fail before the server reads it.
    pub(super) fn admit_pull<'a>(
        &'a self,
        shard: &'a str,
        cursor: Option<&'a str>,
    ) -> BoxFuture<'a, ReplicaResult<()>> {
        Box::pin(async move {
            let (fails, hook, delay) = {
                let mut state = self.state.lock();
                state.events.push(WireEvent::Pull {
                    shard: shard.to_owned(),
                    cursor: cursor.map(str::to_owned),
                });
                (state.pull_fails, state.pull_hook.clone(), state.pull_delay)
            };
            if fails {
                return Err(ReplicaError::Transport("pull refused (stub)".into()));
            }
            if let Some(hook) = hook {
                hook(shard.to_owned()).await;
            }
            if !delay.is_zero() {
                futures_timer::Delay::new(delay).await;
            }
            Ok(())
        })
    }

    pub(super) fn scripted_pull(&self, shard: &str) -> Option<ScriptedPull> {
        self.state
            .lock()
            .pull_queues
            .get_mut(shard)
            .and_then(std::collections::VecDeque::pop_front)
    }

    /// The rest of an answer the server paged: what it serves next.
    pub(super) fn requeue_pull(&self, shard: &str, rest: ScriptedPull) {
        self.state
            .lock()
            .pull_queues
            .entry(shard.to_owned())
            .or_default()
            .push_front(rest);
    }

    /// The server holds changes for the shard that no pull has served.
    pub(super) fn holds_pull(&self, shard: &str) -> bool {
        self.state
            .lock()
            .pull_queues
            .get(shard)
            .is_some_and(|queue| !queue.is_empty())
    }

    /// Every push request is logged and may fail before the server reads it.
    pub(super) fn admit_push(&self, ids: Vec<String>) -> ReplicaResult<()> {
        let mut state = self.state.lock();
        state.events.push(WireEvent::Push { ids });
        if state.push_fails {
            return Err(ReplicaError::Transport("push refused (stub)".into()));
        }
        if let Some(error) = &state.push_failure {
            return Err(error.clone());
        }
        if let Some(budget) = state.push_success_budget {
            if budget <= 0 {
                return Err(ReplicaError::Transport(
                    "push budget exhausted (stub)".into(),
                ));
            }
            state.push_success_budget = Some(budget - 1);
        }
        Ok(())
    }

    /// The operations the server executes for the first time.
    pub(super) fn scripted_push(&self, ops: Vec<ReplicaOp>) -> BoxFuture<'_, Vec<ReplicaVerdict>> {
        Box::pin(async move {
            let hook = self.state.lock().push_hook.clone();
            if let Some(hook) = hook {
                hook(ops.clone()).await;
            }
            let delay = {
                let mut state = self.state.lock();
                state.pushed_batches.push(ops.clone());
                state.push_delay
            };
            if !delay.is_zero() {
                futures_timer::Delay::new(delay).await;
            }
            let script = self.state.lock().push_script.clone();
            match script {
                Some(script) => script(&ops),
                None => ops
                    .iter()
                    .map(|op| ReplicaVerdict::accepted(&op.id))
                    .collect(),
            }
        })
    }

    /// The reply of a committed push: lost, corrupted, or delivered.
    pub(super) fn answer_push(
        &self,
        mut verdicts: Vec<ReplicaVerdict>,
    ) -> ReplicaResult<Vec<ReplicaVerdict>> {
        let mut state = self.state.lock();
        if state.lost_replies > 0 {
            state.lost_replies -= 1;
            return Err(ReplicaError::Transport(
                "HTTP 503: server committed; reply lost (stub)".into(),
            ));
        }
        if let Some(fault) = state.verdict_fault.take() {
            fault(&mut verdicts);
        }
        Ok(verdicts)
    }
}

impl ReplicaTransport for StubTransport {
    fn exchange(
        &self,
        endpoint: crate::ReplicaEndpoint,
        body: Vec<u8>,
    ) -> BoxFuture<'_, ReplicaResult<Vec<u8>>> {
        Box::pin(async move { self.protocol.exchange(self, endpoint, &body).await })
    }
}

/// A hold that many pushes may await (`XCTestExpectation` may only be waited on
/// once, and a duplicate push is exactly what some of these tests hunt), plus
/// the ARRIVAL half of upstream's `AsyncGate`: `release`/`wait` says "the hold
/// is lifted", `arrive`/`wait_until_arrived` says "the flight is provably on
/// the wire". They are different questions and a test that needs both used to
/// need two gates — `arrive_and_wait` is the pair, as upstream writes it.
///
/// Every wait is DEADLINED (upstream's `wait(seconds: 5)`): a wedged runner
/// must fail the case with a reason, not hang the suite.
#[derive(Default)]
pub struct Gate {
    opened: std::sync::atomic::AtomicBool,
    arrived: std::sync::atomic::AtomicBool,
    event: event_listener::Event,
}

/// Upstream's `wait(seconds: 5)`.
const GATE_TIMEOUT: Duration = Duration::from_secs(5);

impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn release(&self) {
        self.opened.store(true, std::sync::atomic::Ordering::SeqCst);
        self.event.notify(usize::MAX);
    }

    pub fn is_open(&self) -> bool {
        self.opened.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Announce that the held code is running — the observation a test needs
    /// before it can assert anything about "while the flight is in flight".
    pub fn arrive(&self) {
        self.arrived
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.event.notify(usize::MAX);
    }

    pub fn has_arrived(&self) -> bool {
        self.arrived.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Awaited INSIDE the held code: announce, then hold until the test opens
    /// the gate. Upstream's `AsyncGate.arriveAndWait()`.
    pub async fn arrive_and_wait(&self) {
        self.arrive();
        self.wait().await;
    }

    pub async fn wait(&self) {
        self.wait_within(GATE_TIMEOUT, "the gate never opened")
            .await;
    }

    pub async fn wait_until_arrived(&self) {
        self.await_flag(
            GATE_TIMEOUT,
            "the gate was never reached",
            Self::has_arrived,
        )
        .await;
    }

    pub async fn wait_within(&self, timeout: Duration, reason: &str) {
        self.await_flag(timeout, reason, Self::is_open).await;
    }

    async fn await_flag(&self, timeout: Duration, reason: &str, flag: fn(&Self) -> bool) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if flag(self) {
                return;
            }
            let listener = self.event.listen();
            if flag(self) {
                return;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                panic!("gate deadline expired: {reason}");
            }
            futures::future::select(
                listener,
                std::pin::pin!(futures_timer::Delay::new(remaining)),
            )
            .await;
        }
    }

    /// The same hold from a BLOCKING context — a synchronous seam (a commit
    /// hook, an injected clock) has no executor to yield to.
    pub fn block_until_open(&self, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        while !self.is_open() {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                panic!("gate deadline expired while blocking");
            }
            event_listener::Listener::wait_timeout(self.event.listen(), remaining);
        }
    }
}

/// An `on_push` hook that holds every flight at `gate`, announcing arrival.
pub fn hold_pushes(transport: &StubTransport, gate: &Arc<Gate>) {
    let gate = gate.clone();
    transport.on_push(move |_ops| {
        let gate = gate.clone();
        Box::pin(async move { gate.arrive_and_wait().await })
    });
}

/// The same for `on_pull` — a pull page holds the seal too.
pub fn hold_pulls(transport: &StubTransport, gate: &Arc<Gate>) {
    let gate = gate.clone();
    transport.on_pull(move |_shard| {
        let gate = gate.clone();
        Box::pin(async move { gate.arrive_and_wait().await })
    });
}

/// `TestSupport.HookOutcome` — captures an error raised inside a NON-throwing
/// transport hook so the test can assert it never happened. Swallowing it
/// would be a hidden green, and a hook is the one place a test is tempted to.
#[derive(Default)]
pub struct HookOutcome {
    failure: Mutex<Option<String>>,
}

impl HookOutcome {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record(&self, error: impl std::fmt::Display) {
        *self.failure.lock() = Some(error.to_string());
    }

    pub fn failure(&self) -> Option<String> {
        self.failure.lock().clone()
    }
}

/// A thread-safe tally for observation tests.
#[derive(Default)]
pub struct Tally {
    value: std::sync::atomic::AtomicUsize,
}

impl Tally {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn bump(&self) {
        self.value.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn count(&self) -> usize {
        self.value.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// The mutable set a sync reducer consults FRESH on every judge. The engine's
/// contract is that the gate is DERIVED per drain, never cached per entry, so
/// the test's release is visible to the very next judge — including one that
/// happens inside a flight already on the wire.
#[derive(Default)]
pub struct ReleaseLedger {
    keys: Mutex<std::collections::HashSet<String>>,
    signal: Arc<crate::SyncGateSignal>,
}

impl ReleaseLedger {
    pub fn new(released: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            keys: Mutex::new(released.iter().map(|key| (*key).to_owned()).collect()),
            signal: crate::SyncGateSignal::new(),
        })
    }

    pub fn land(&self, key: &str) {
        self.keys.lock().insert(key.to_owned());
        self.signal.fire();
    }

    pub fn contains(&self, key: &str) -> bool {
        self.keys.lock().contains(key)
    }
}

/// The production default (`automatically_push_writes = true`) — this fixture
/// turns it off, and the cases grading the engine's OWN delivery turn it back
/// on and never call `drain()`.
pub fn self_delivering_engine(
    store: Arc<ReplicaStateStore>,
    transport: Arc<StubTransport>,
) -> Arc<ReplicaEngine> {
    let mut options = options(engine_directory(), transport);
    options.automatically_push_writes = true;
    engine_with(store, OWNER, options)
}

/// Bounded wait for an async condition — the suite's hang defense: every wait
/// has a deadline, and a missed one fails the test instead of wedging the
/// runner. `reason` is required, not optional.
pub async fn until<F, Fut>(reason: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    until_within(reason, Duration::from_secs(5), &mut condition).await;
}

pub async fn until_within<F, Fut>(reason: &str, timeout: Duration, condition: &mut F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if condition().await {
            return;
        }
        futures_timer::Delay::new(Duration::from_millis(5)).await;
    }
    panic!("condition never held: {reason}");
}

// MARK: - Typed test model (hand-written; codegen's shape in miniature)

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TestNote {
    pub id: String,
    pub title: Option<String>,
    pub rank: Option<String>,
}

impl TestNote {
    pub fn new(id: &str, title: Option<&str>, rank: Option<&str>) -> Self {
        Self {
            id: id.to_owned(),
            title: title.map(str::to_owned),
            rank: rank.map(str::to_owned),
        }
    }
}

impl ReplicaRowModel for TestNote {
    fn stream_name() -> &'static str {
        "notes"
    }

    fn decode(id: &str, row_type: Option<&str>, data: &ReplicaFields) -> Option<Self> {
        if row_type.is_some_and(|row_type| row_type != "Note") {
            return None;
        }
        Some(Self {
            id: id.to_owned(),
            title: data
                .get("title")
                .and_then(ReplicaValue::as_string)
                .map(str::to_owned),
            rank: data
                .get("rank")
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
        if let Some(rank) = &self.rank {
            data.insert("rank".into(), ReplicaValue::string(rank));
        }
        data
    }
}

impl ReplicaWritableRowModel for TestNote {}

// MARK: - Fixtures

/// The suite's owner: every fixture engine is bound to user 42, which is what
/// a create stamps.
pub const OWNER: i64 = 42;

/// A spawner backed by the dev-dependency runtime — the host's job in
/// production, the test runner's here.
pub struct TokioSpawner;

impl Spawner for TokioSpawner {
    fn spawn(&self, future: SpawnFuture) {
        tokio::spawn(future);
    }
}

/// notes: writable row · boards: document (stub codec) · jobs: readonly row ·
/// assets: row on the global shard.
pub fn schema() -> ReplicaSchema {
    ReplicaSchema::new(vec![
        ReplicaStreamSpec::row("notes"),
        ReplicaStreamSpec::document("boards").codec("stub@1"),
        ReplicaStreamSpec::row("jobs").readonly(true),
        ReplicaStreamSpec::row("assets").shard("global"),
    ])
}

/// The fixture schema with its document stream on the causal codec.
pub fn causal_schema() -> ReplicaSchema {
    ReplicaSchema::new(vec![
        ReplicaStreamSpec::row("notes"),
        ReplicaStreamSpec::document("boards").codec("causal@1"),
        ReplicaStreamSpec::row("jobs").readonly(true),
        ReplicaStreamSpec::row("assets").shard("global"),
    ])
}

/// The fixture store together with the scratch file it lives in. Hold the
/// handle for the length of the test: `Deref` hands out the `Arc` the engine
/// wants, and the store's files go away when the handle does.
pub struct StoreFixture {
    store: Arc<ReplicaStateStore>,
    _scratch: Scratch,
}

impl Deref for StoreFixture {
    type Target = Arc<ReplicaStateStore>;

    fn deref(&self) -> &Arc<ReplicaStateStore> {
        &self.store
    }
}

pub fn store(name: &str) -> StoreFixture {
    let scratch = temp_path(name, ".sqlite");
    let store = Arc::new(ReplicaStateStore::open(scratch.path()).expect("open the fixture store"));
    StoreFixture {
        store,
        _scratch: scratch,
    }
}

/// Deterministic peer minter: 100, 101, 102…
/// A store write that bypasses the engine: fault injection and direct fixture state.
pub fn write<T>(
    store: &ReplicaStateStore,
    body: impl FnOnce(&mut crate::WriteContext<'_>) -> ReplicaResult<T>,
) -> ReplicaResult<T> {
    store.pool().write(body)
}

pub fn reconcile<C: crate::DocumentCodec>(
    working: &crate::working::WorkingDocument<C>,
    row: Option<&crate::DocRow>,
    sequence: i64,
) -> ReplicaResult<()> {
    working.reconcile(row, sequence)
}

#[allow(clippy::too_many_arguments)]
pub async fn record_working_delta(
    engine: &ReplicaEngine,
    stream: &str,
    id: &str,
    payload: &[Arc<[u8]>],
    generation: u64,
    incarnation: &str,
    peer: u64,
    confirmed: &[u8],
    lane: crate::ReplicaLane,
) -> ReplicaResult<(Vec<u8>, i64)> {
    engine
        .record_working_delta(
            stream,
            id,
            payload,
            generation,
            incarnation,
            peer,
            confirmed,
            lane,
        )
        .await
}

/// The entity incarnation the store holds for an address.
pub fn incarnation(
    store: &ReplicaStateStore,
    stream: &str,
    id: &str,
) -> ReplicaResult<Option<String>> {
    store.pool().read(|db| store.incarnation(db, stream, id))
}

pub fn sequential_minter(from: u64) -> Arc<dyn Fn() -> u64 + Send + Sync> {
    let next = Arc::new(std::sync::atomic::AtomicU64::new(from));
    Arc::new(move || next.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
}

/// `automatically_push_writes` is PRODUCTION-`true`. This fixture defaults it
/// to `false`, as upstream's does, and it is a field on the returned options:
/// suites that grade an explicit `drain()` keep `false` so their pending counts
/// stay deterministic; the suites that grade the engine's OWN delivery set it
/// `true` and never call `drain()` at all.
pub fn options(
    directory: PathBuf,
    transport: Arc<dyn crate::transport::ReplicaTransport>,
) -> ReplicaEngineOptions {
    let mut options =
        ReplicaEngineOptions::new(directory, transport, schema(), Arc::new(TokioSpawner));
    options.codecs = vec![Arc::new(StubCodec) as Arc<dyn ReplicaCodec>];
    options.peer_minter = sequential_minter(100);
    options.automatically_push_writes = false;
    options.spawner = Arc::new(TokioSpawner);
    options
}

/// An engine bound to a store the caller already built.
///
/// The directory only matters for the path-deriving lifecycle verbs
/// (`adopt_merged` moves the file to `<directory>/replica-<owner>.sqlite`), and
/// the caller holds no `Scratch` to sweep it. Rooting it under the same
/// `replica-man-tests` directory every `Scratch` lives in keeps those derived
/// stores inside the swept tree instead of loose in `$TMPDIR` — a bare
/// `temp_dir()` here is how a future author refills the disk.
pub fn engine(store: Arc<ReplicaStateStore>, transport: Arc<StubTransport>) -> Arc<ReplicaEngine> {
    let options = options(engine_directory(), transport);
    ReplicaEngine::with_store(store, OWNER, options)
}

/// The home the path-deriving lifecycle verbs write into — see `engine`.
pub fn engine_directory() -> PathBuf {
    let mut directory = std::env::temp_dir();
    directory.push("replica-man-tests");
    std::fs::create_dir_all(&directory).expect("temp directory");
    directory
}

pub fn engine_with(
    store: Arc<ReplicaStateStore>,
    owner: i64,
    options: ReplicaEngineOptions,
) -> Arc<ReplicaEngine> {
    ReplicaEngine::with_store(store, owner, options)
}

/// An engine that owns its own files: nothing is bound until `open(owner)`.
pub fn unopened_engine(directory: PathBuf, transport: Arc<StubTransport>) -> Arc<ReplicaEngine> {
    ReplicaEngine::new(options(directory, transport))
}

pub fn note(id: &str, title: &str, rank: Option<&str>) -> ScriptedFrame {
    let mut data = ReplicaFields::new();
    data.insert("title".into(), ReplicaValue::string(title));
    if let Some(rank) = rank {
        data.insert("rank".into(), ReplicaValue::string(rank));
    }
    ScriptedFrame::RowSet {
        stream: "notes".into(),
        id: id.into(),
        row_type: None,
        data,
    }
}

pub fn row_set(
    stream: &str,
    id: &str,
    row_type: Option<&str>,
    data: ReplicaFields,
) -> ScriptedFrame {
    ScriptedFrame::RowSet {
        stream: stream.into(),
        id: id.into(),
        row_type: row_type.map(str::to_owned),
        data,
    }
}

pub fn doc_snapshot(
    stream: &str,
    id: &str,
    codec: &str,
    snapshot: &[u8],
    data: ReplicaFields,
) -> ScriptedFrame {
    ScriptedFrame::DocSnapshot {
        stream: stream.into(),
        id: id.into(),
        codec: codec.into(),
        snapshot: snapshot.to_vec(),
        data,
    }
}

pub fn doc_delta(stream: &str, id: &str, seq: i64, codec: &str, payload: &[u8]) -> ScriptedFrame {
    ScriptedFrame::DocDelta {
        stream: stream.into(),
        id: id.into(),
        seq,
        codec: codec.into(),
        payload: payload.to_vec(),
    }
}

pub fn row_delete(stream: &str, id: &str) -> ScriptedFrame {
    ScriptedFrame::RowDelete {
        stream: stream.into(),
        id: id.into(),
    }
}

/// `["title": .string(...)]` in one call — the suite writes a lot of these.
pub fn fields(pairs: &[(&str, ReplicaValue)]) -> ReplicaFields {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), value.clone()))
        .collect()
}

pub fn text(value: &str) -> ReplicaValue {
    ReplicaValue::string(value)
}

/// Verdict scripts the suite reuses.
pub fn reject_all(transport: &StubTransport, reason: &'static str) {
    transport.script_push(move |ops| {
        ops.iter()
            .map(|op| ReplicaVerdict::rejected(&op.id, reason))
            .collect()
    });
}

pub fn accept_all(transport: &StubTransport) {
    transport.script_push(|ops| {
        ops.iter()
            .map(|op| ReplicaVerdict::accepted(&op.id))
            .collect()
    });
}

pub fn blob_gate(released: Arc<ReleaseLedger>) -> Arc<dyn crate::SyncGate> {
    struct BlobGate(Arc<ReleaseLedger>);
    impl crate::SyncGate for BlobGate {
        fn id(&self) -> &str {
            "blob-upload"
        }
        fn stream(&self) -> Option<&str> {
            Some("notes")
        }
        fn changes(&self) -> Option<Arc<crate::SyncGateSignal>> {
            Some(self.0.signal.clone())
        }
        fn judge(&self, change: &crate::SyncChange) -> crate::SyncGateDecision {
            if let Some(key) = change.local.get("blob").and_then(ReplicaValue::as_string) {
                if !key.is_empty() && !self.0.contains(key) {
                    return crate::SyncGateDecision::Hold(format!("blob {key} is uploading"));
                }
            }
            crate::SyncGateDecision::Push
        }
    }
    Arc::new(BlobGate(released))
}
