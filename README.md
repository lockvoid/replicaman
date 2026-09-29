# ReplicaMan

**Durable local data and synchronization for Rails applications, with native Swift, Kotlin, and Rust clients.**

- **Offline reads and writes.** Read from SQLite and save changes locally, with durable outbound work that survives app restarts.
- **Rails as the authority.** Define streams over ActiveRecord models, keep authorization and business rules on the server, and capture changes in the same database transaction.
- **Row and document synchronization.** Use field patches for records and the optional Loro plugin for collaborative documents.
- **Typed application code.** Generate native models, stream handles, and document shapes from one server manifest.
- **Consistent local state.** Pull each account's buckets in bounded pages and publish a complete round atomically, together with its cursor.
- **Retry and recovery.** Retry immutable submissions after lost replies; retain refused or displaced work for inspection and explicit recovery.
- **Independent packages, one contract.** Use the clients separately while keeping the server, codecs, generator, and shared tests together.

## Quick start

The [Notes example](./examples/notes/README.md) creates and edits a note, queues an atomic action, and verifies that the data and pending writes survive reopening:

```sh
mise run examples   # the example in Swift, Kotlin and Rust
```

The examples run offline and need no Rails server or Loro dependency. For an application, continue with [Installation](./docs/INSTALLATION.md) and [Setup](./docs/SETUP.md).

## Keynotes

How local writes, server decisions, and pull rounds fit together: **[Keynotes](./docs/KEYNOTES.md)**.

A successful save means the local transaction committed. Server acceptance and visibility on another device are later steps, exposed through synchronization status.

## Documentation

- **[Installation](./docs/INSTALLATION.md)**
- **[Setup](./docs/SETUP.md)**
- **[Models and code generation](./docs/MODELS.md)**
- **[Queries and observation](./docs/QUERIES.md)**
- **[Mutations and transactions](./docs/MUTATIONS.md)**
- **[Documents and Loro](./docs/DOCUMENTS.md)**
- **[Synchronization](./docs/SYNC.md)**
- **[Sync gates](./docs/GATES.md)**
- **[Storage and account lifecycle](./docs/STORAGE.md)**
- **[Recovery and diagnostics](./docs/RECOVERY.md)**
- **[Rails server](./docs/SERVER.md)**
- **[Command responses](./docs/COMMITS.md)**

The [documentation index](./docs/README.md) groups the guides by task. Packages are currently consumed from this checkout; registry publication is a separate release step.

## Development

Every suite runs through [mise](https://mise.jdx.dev): `mise run test` for the unit and integration suites, `mise run e2e` for the real HTTP scenarios (see [Testing](./e2e/README.md)). The [protocol specification](./docs/PROTOCOL.md) is the contract for implementers.

| Directory | Contents |
| --- | --- |
| `protocol` | Shared storage schema and conformance fixtures |
| `swift` | Swift package: `ReplicaMan`, optional `ReplicaManLoro`, tests, E2E worker, example |
| `kotlin` | Gradle build: JVM/Android client, optional `replicaman-loro`, Loro JNI binding, E2E worker, example |
| `rust` | Cargo workspace: Rust client, optional `replicaman-loro`, E2E worker, example |
| `ruby` | `replica_man` Rails engine, with the `loro` Ruby extension in `vendor/loro` |
| `codegen` | Shared manifest validator and native generators |
| `e2e` | Real HTTP scenarios across the three clients |
| `tools` | Repository tooling (schema embedding, fixtures, notices, packaging) |
| `examples` | Shared example manifest and guide |

## License

[MIT](./LICENSE). Dependencies retain their own licenses; see the [dependency record](./docs/DEPENDENCIES.md).
