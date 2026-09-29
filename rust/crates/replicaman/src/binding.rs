//! The engine's ONE identity fact: which owner's file this process holds open,
//! and the store over it. `None` is the whole closed world — no owner, no
//! file, no pool, nothing a write could land in.
//!
//! Read without the engine's async mutex (`store`, `owner`, `database` are all
//! non-isolated upstream, and the doc plane loads folds without a hop), so the
//! state lives under its own lock. Every WRITE to it goes through the engine,
//! so a write and a rebind can never interleave.
//!
//! Ported from `Sources/ReplicaMan/ReplicaBinding.swift`.

use std::path::PathBuf;
use std::sync::Arc;

use event_listener::Event;
use parking_lot::Mutex;

use crate::error::ReplicaResult;
use crate::store::ReplicaStateStore;

#[derive(Clone)]
pub struct Bound {
    pub owner: i64,
    pub store: Arc<ReplicaStateStore>,
    pub path: PathBuf,
}

#[derive(Default)]
struct BindingState {
    bound: Option<Bound>,
    /// Bumped whenever the bound store changes. Watchers ride it: an
    /// observation is only ever valid for one pool, so every change has to
    /// re-arm the ones that were riding the old one.
    generation: u64,
}

#[derive(Default)]
pub struct ReplicaBinding {
    state: Mutex<BindingState>,
    event: Event,
}

impl ReplicaBinding {
    pub fn current(&self) -> Option<Bound> {
        self.state.lock().bound.clone()
    }

    pub fn owner(&self) -> Option<i64> {
        self.state.lock().bound.as_ref().map(|bound| bound.owner)
    }

    pub fn store(&self) -> Option<Arc<ReplicaStateStore>> {
        self.state
            .lock()
            .bound
            .as_ref()
            .map(|bound| bound.store.clone())
    }

    /// The bound store plus the generation it was read at — a watcher arms on
    /// the pair so it can park for the NEXT store without missing one that
    /// arrived while it was arming.
    pub fn snapshot(&self) -> (Option<Bound>, u64) {
        let state = self.state.lock();
        (state.bound.clone(), state.generation)
    }

    pub fn bind(&self, replacement: Bound) {
        self.mutate(|state| state.bound = Some(replacement));
    }

    pub fn unbind(&self) -> Option<Bound> {
        let mut released = None;
        self.mutate(|state| released = state.bound.take());
        released
    }

    /// The cold-boot bind: build and publish under the lock, so two first
    /// touches from different threads cannot each open the file.
    pub fn bind_if_unbound(
        &self,
        make: impl FnOnce() -> ReplicaResult<Bound>,
    ) -> ReplicaResult<()> {
        let mut state = self.state.lock();
        if state.bound.is_some() {
            return Ok(());
        }
        let made = make()?;
        state.bound = Some(made);
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        self.event.notify(usize::MAX);
        Ok(())
    }

    /// The merge's swap: one store out, one store in, ONE generation. A
    /// watcher must never see the unbound moment in between — an empty picture
    /// mid-sign-in is the wipe the preserved world exists to avoid.
    pub fn replace(&self, replacement: Bound) {
        self.mutate(|state| state.bound = Some(replacement));
    }

    pub fn generation(&self) -> u64 {
        self.state.lock().generation
    }

    /// Parks until the bound store differs from the one read at `generation`.
    pub async fn wait_for_change(&self, generation: u64) {
        loop {
            if self.generation() != generation {
                return;
            }
            let listener = self.event.listen();
            if self.generation() != generation {
                return;
            }
            listener.await;
        }
    }

    fn mutate(&self, body: impl FnOnce(&mut BindingState)) {
        {
            let mut state = self.state.lock();
            body(&mut state);
            state.generation = state.generation.wrapping_add(1);
        }
        self.event.notify(usize::MAX);
    }
}
