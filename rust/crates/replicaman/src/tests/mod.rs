//! The transliterated suite. In-crate on purpose: upstream's XCTest targets
//! use `@testable import ReplicaMan`, and the engine's test seams
//! (`checkpointFault`, `isColdForTesting`, the store-binding initializer, the
//! raw journal readers) are internal there too.

pub(crate) use crate::testing as support;

mod drain_tests;
mod gzip_tests;
mod http_transport_tests;
mod identity_tests;
mod lane_tests;
mod pull_tests;
mod replica_value_coding_tests;
mod store_binding_tests;
mod store_migration_tests;
mod watch_nudge_tests;
mod wire_byte_parity_tests;
mod write_tests;

mod commit_tests;

mod gate_tests;

mod recovery_tests;

mod reference_tests;

mod adoption_tests;

mod sync_status_tests;

mod rebirth_tests;

mod atomic_write_tests;

mod protocol_tests;

mod intent_tests;
