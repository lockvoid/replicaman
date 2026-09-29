//! ReplicaMan — the row + document replication engine, client half.
//!
//! Keeps a device replica convergent with the rails server
//! (`vendor/replica_man`): pull frames by shard cursor into an atomic
//! checkpoint store, journal outbound ops, per-document delta supersede,
//! verdicts on push. The store (rusqlite, WAL) is the app's raw truth; any
//! native projection is rebuilt FROM it.
//!
//! The core is codec-agnostic and links no CRDT; the Loro codec is the
//! separate `replicaman-loro` crate, so a rows-only consumer builds without it.

pub mod binding;
pub mod codec;
pub mod document_value;
pub mod document_value_decoding;
pub mod documents;
pub mod engine;
pub mod error;
pub mod gzip;
pub mod health;
pub mod id;
pub mod lane_scope;
pub mod models;
pub mod nudger;
pub mod pool;
pub mod preimage;
pub mod reference;
pub mod row_cache;
pub mod schema;
pub use reference::{ReplicaReference, ReplicaReferenceSpec};
mod protocol;
pub mod spawner;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
mod sync_store;
pub mod transport;
mod transport_retry;
pub mod value;
pub mod value_coding;
pub mod wire;
pub mod working;

pub use binding::{Bound, ReplicaBinding};
pub use codec::{ReplicaCodec, ReplicaDocumentMode};
pub use document_value::{DocumentEntry, DocumentFields, DocumentValue};
pub use documents::{DocumentCodec, DocumentPin, ReplicaDocState};
pub use engine::{ReplicaEngine, ReplicaEngineOptions, ReplicaTransaction};
pub use error::{ReplicaError, ReplicaResult};
pub use health::{ReplicaFailure, ReplicaFailureKind, ReplicaHealth};
pub use lane_scope::current_lane;
pub use models::{
    DocumentStream, ReadonlyDocumentStream, ReadonlyRowStream, ReplicaCreateStamp, ReplicaDocModel,
    ReplicaReads, ReplicaRowModel, ReplicaWritableRowModel, RowStream,
};
pub use nudger::ReplicaNudger;
pub use pool::{SqlitePool, WriteContext};
pub use preimage::ReplicaPreimage;
pub use row_cache::{MaterializedRow, RowMaterialization, RowRecord};
pub use schema::{ReplicaLane, ReplicaReflection, ReplicaSchema, ReplicaStreamSpec, StreamLane};
pub use spawner::{SpawnFuture, Spawner};
pub use store::{DocRow, JournalRow, ReplicaStateStore, SnapshotRow};
pub use transport::{
    BoxFuture, HttpClient, HttpReplicaTransport, NoWireTransport, ReplicaEndpoint, ReplicaTransport,
};
pub use value::{ReplicaFields, ReplicaValue};
pub use wire::{
    ReplicaFrame, ReplicaJson, ReplicaOp, ReplicaPullResponse, ReplicaVerdict, VerdictOutcome, verb,
};

#[cfg(test)]
mod tests;

mod recovery;
mod recovery_export;
pub use recovery::{ReplicaRecoveryPart, ReplicaRecoveryRecord};

mod commit;
pub use commit::ReplicaCommitSession;

mod sync_gate;
pub use sync_gate::{
    ReplicaGateHold, SyncChange, SyncChangeKind, SyncGate, SyncGateDecision, SyncGateSignal,
};

mod sync_status;
mod integrity;
pub use sync_status::ReplicaSyncStatus;
