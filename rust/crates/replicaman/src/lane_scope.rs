//! The lane a current action claims.
//!
//! Upstream this is a Swift `@TaskLocal`, so a helper called inside
//! `lane { }` — a mutation helper, a journal writer — inherits it without
//! knowing lanes exist; forgetting to thread a parameter is the failure mode
//! the design exists to remove. Rust has no task-locals without a runtime, so
//! this is the same mechanism a runtime's task-local uses: a thread-local set
//! on poll-entry and restored on poll-exit, wrapped around the scoped future.
//! Nesting therefore works (a `bulk` scope inside an `interactive` one RESETS
//! the lane), and a suspension inside the scope cannot leak the claim to
//! whatever the executor polls next.
//!
//! The scoped future is hand-boxed — the house async pattern, and what keeps
//! this `Pin` projection free of `unsafe`.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::schema::ReplicaLane;

thread_local! {
    static CURRENT_LANE: Cell<ReplicaLane> = const { Cell::new(ReplicaLane::Bulk) };
}

/// The lane the current action claims — background by default.
pub fn current_lane() -> ReplicaLane {
    CURRENT_LANE.with(Cell::get)
}

/// Every write inside rides one lane, in order. Not a transaction: writes land
/// locally as they happen, the server applies each op in its own savepoint,
/// and a refusal is per op. It is a routing and ordering scope, nothing more.
pub struct LaneScope<'a, T> {
    lane: ReplicaLane,
    inner: Pin<Box<dyn Future<Output = T> + Send + 'a>>,
}

impl<'a, T> LaneScope<'a, T> {
    pub fn new(lane: ReplicaLane, inner: impl Future<Output = T> + Send + 'a) -> Self {
        Self {
            lane,
            inner: Box::pin(inner),
        }
    }
}

impl<T> Future for LaneScope<'_, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let lane = self.lane;
        let previous = CURRENT_LANE.with(|cell| cell.replace(lane));
        let outcome = self.inner.as_mut().poll(cx);
        CURRENT_LANE.with(|cell| cell.set(previous));
        outcome
    }
}
