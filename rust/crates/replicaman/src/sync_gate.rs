//! Write-time synchronization policy. Holds survive restarts without replaying
//! every intermediate local edit; a released row sends its current state.
use crate::ReplicaFields;
use event_listener::Event;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncChangeKind {
    Create,
    Patch,
    Delete,
    Document,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncChange {
    pub stream: String,
    pub row_id: String,
    pub kind: SyncChangeKind,
    pub local: ReplicaFields,
    pub previous: ReplicaFields,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncGateDecision {
    Push,
    Hold(String),
    Discard,
}

pub trait SyncGate: Send + Sync {
    /// Stable across launches. A gate without a stream is a global policy;
    /// its held rows do not make other streams wait for them.
    fn id(&self) -> &str;
    fn stream(&self) -> Option<&str> {
        None
    }
    fn judge(&self, change: &SyncChange) -> SyncGateDecision;
    fn changes(&self) -> Option<Arc<SyncGateSignal>> {
        None
    }
}

#[derive(Debug, Default)]
pub struct SyncGateSignal {
    sequence: AtomicU64,
    event: Event,
}
impl SyncGateSignal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn fire(&self) {
        self.sequence.fetch_add(1, Ordering::SeqCst);
        self.event.notify(usize::MAX);
    }
    pub(crate) async fn after(&self, previous: u64) -> u64 {
        loop {
            let listener = self.event.listen();
            let current = self.sequence.load(Ordering::SeqCst);
            if current != previous {
                return current;
            }
            listener.await;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaGateHold {
    pub stream: String,
    pub row_id: String,
    pub gate_id: String,
    pub reason: String,
    pub sequence: i64,
    pub server_knows: bool,
    pub(crate) preimage: Vec<u8>,
}
