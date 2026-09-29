# ReplicaMan for Rust

Durable SQLite replicas with atomic local writes, field-patch synchronization,
write-time sync gates, and an optional `loro` document codec. The Rails server,
Swift/Kotlin clients, shared generator and protocol tests live in the ReplicaMan
monorepo beside this package.

Start with the monorepo's [installation guide](../../../docs/INSTALLATION.md#rust),
[Rust setup](../../../docs/SETUP.md#rust), and
[mutation guide](../../../docs/MUTATIONS.md). The
[Notes example](../../../examples/notes/README.md) is a runnable offline consumer.

Documents need the separate `replicaman-loro` crate. `ReplicaEngineOptions::new` requires an executor
(`Spawner`) so background persistence cannot silently disappear. Ordinary
asynchronous writes finish after durable commit. For the working-document API,
await each save receipt or `wait_saved` before showing “saved”. Lifecycle methods
return errors when pending working edits cannot be persisted.

The package is pre-release. Version 0.1.0 does not read previous private-beta
stores. See the monorepo's `docs/PROTOCOL.md` and `e2e/README.md` for integration and
verification contracts.
