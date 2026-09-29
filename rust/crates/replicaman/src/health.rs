use std::sync::Arc;
use std::time::SystemTime;

use event_listener::Event;
use parking_lot::Mutex;

use crate::ReplicaError;

/// A failure from background work without an awaiting caller. Explicit storage
/// and synchronization operations still return their errors directly.
#[derive(Clone, Debug)]
pub struct ReplicaFailure {
    pub operation: String,
    pub error: ReplicaError,
    pub occurred_at: SystemTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaFailureKind {
    Transport,
    Storage,
    UpgradeRequired,
    RecoveryRequired,
    Other,
}

impl ReplicaFailure {
    pub fn kind(&self) -> ReplicaFailureKind {
        match &self.error {
            ReplicaError::Transport(_) => ReplicaFailureKind::Transport,
            ReplicaError::Storage(_) => ReplicaFailureKind::Storage,
            ReplicaError::Protocol { code, .. } => match code.as_str() {
                "UpgradeRequired" => ReplicaFailureKind::UpgradeRequired,
                "DatasetChanged" | "NamespaceChanged" | "MutationChanged" => {
                    ReplicaFailureKind::RecoveryRequired
                }
                _ => ReplicaFailureKind::Other,
            },
            _ => ReplicaFailureKind::Other,
        }
    }
}

#[derive(Default)]
pub struct ReplicaHealth {
    latest: Mutex<(u64, Option<ReplicaFailure>)>,
    changed: Event,
}

impl ReplicaHealth {
    pub fn last_failure(&self) -> Option<ReplicaFailure> {
        self.latest.lock().1.clone()
    }

    /// Observe the latest failure. Slow observers coalesce intervening failures;
    /// keeping an observer alive never creates an unbounded event queue.
    pub fn failures(
        self: &Arc<Self>,
    ) -> impl futures::Stream<Item = ReplicaFailure> + Send + use<> {
        futures::stream::unfold((self.clone(), None), |(health, mut seen)| async move {
            loop {
                let listener = health.changed.listen();
                let (version, failure) = health.latest.lock().clone();
                if seen != Some(version) {
                    seen = Some(version);
                    if let Some(failure) = failure {
                        return Some((failure, (health, seen)));
                    }
                }
                listener.await;
            }
        })
    }

    /// Record a host-owned background failure that has no awaiting caller.
    pub fn record(&self, operation: &str, error: ReplicaError) {
        log::error!("[health] {operation}: {error}");
        let mut latest = self.latest.lock();
        latest.0 = latest.0.wrapping_add(1);
        latest.1 = Some(ReplicaFailure {
            operation: operation.to_owned(),
            error,
            occurred_at: SystemTime::now(),
        });
        drop(latest);
        self.changed.notify(usize::MAX);
    }
}
