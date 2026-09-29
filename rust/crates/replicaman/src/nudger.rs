//! The doorbell's landing pad, ported verbatim from Syncer v1 via
//! `Sources/ReplicaMan/ReplicaNudger.swift`.
//!
//! Every "server state moved" signal — a realtime event, app foregrounding, a
//! drained create — calls `nudge()`; the nudger coalesces them into AT MOST
//! one in-flight sync (drain + pull-until-caught-up) plus one queued re-run.
//! Data never rides the signal itself; the pull is always the carrier, so a
//! lost doorbell costs latency, never correctness. Callers never await.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_timer::Delay;
use parking_lot::Mutex;

use crate::spawner::{Spawner, boxed};
use crate::transport::BoxFuture;

type SyncBody = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Default)]
struct NudgerState {
    in_flight: bool,
    queued: bool,
    last_start: Option<Instant>,
    skip_window: bool,
}

pub struct ReplicaNudger {
    sync: SyncBody,
    /// The trailing-edge throttle window: doorbell storms (a render signals
    /// ~5/s per cook) collapse to at most one sync per window, and the LAST
    /// doorbell always lands. Zero = pure coalescer.
    interval: Duration,
    spawner: Arc<dyn Spawner>,
    state: Mutex<NudgerState>,
}

impl ReplicaNudger {
    pub fn new(
        interval: Duration,
        spawner: Arc<dyn Spawner>,
        sync: impl Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            sync: Arc::new(sync),
            interval,
            spawner,
            state: Mutex::new(NudgerState::default()),
        })
    }

    /// `immediate` skips the throttle window for this run. The window exists to
    /// absorb machine chatter; a doorbell rung BY a tap is the opposite — the
    /// person is watching the affordance that the pulled row will flip, and a
    /// second of politeness reads as a dead button.
    pub fn nudge(self: &Arc<Self>, immediate: bool) {
        {
            let mut state = self.state.lock();
            if immediate {
                state.skip_window = true;
            }
            if state.in_flight {
                state.queued = true;
                return;
            }
            state.in_flight = true;
        }
        let engine = Arc::downgrade(self);
        self.spawner.spawn(boxed(async move {
            if let Some(nudger) = engine.upgrade() {
                nudger.run().await;
            }
        }));
    }

    async fn run(&self) {
        loop {
            // Trailing edge: never start a sync inside the window of the
            // previous one; doorbells arriving during the wait fold into this
            // run.
            let wait = {
                let state = self.state.lock();
                if !self.interval.is_zero()
                    && !state.skip_window
                    && let Some(last_start) = state.last_start
                {
                    self.interval.checked_sub(last_start.elapsed())
                } else {
                    None
                }
            };
            if let Some(wait) = wait.filter(|wait| !wait.is_zero()) {
                Delay::new(wait).await;
            }
            {
                let mut state = self.state.lock();
                state.skip_window = false;
                state.queued = false;
                state.last_start = Some(Instant::now());
            }
            (self.sync)().await;
            let mut state = self.state.lock();
            if !state.queued {
                state.in_flight = false;
                return;
            }
        }
    }
}
