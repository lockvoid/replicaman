# Typed offline notes

Follow [Setup](../../docs/SETUP.md) for an HTTP-connected engine and
[Mutations](../../docs/MUTATIONS.md) for the transaction APIs used here.

These are runnable row-only consumers of the public generator and native
libraries. Each generates its model from the same small manifest, creates and
edits a note, queues a two-note atomic server action, closes the store, reopens it,
and checks both the note and unsent journal. They use fresh temporary directories and remove only their own files.

From the repository root:

```sh
swift run replicaman-notes-example
./gradlew :notes-example:run
cargo run -p replicaman-notes-example
```

Swift and Kotlin demonstrate the transaction closure. Rust demonstrates its typed
create/update methods, each a durable transaction. No Loro dependency or server
is needed for these examples. Their offline transport always refuses network
work; it never fabricates an acknowledgement. Replace it with
`HTTPReplicaTransport` / `HttpReplicaTransport`, supply the application's bearer
token, and mount the Rails stream described in the server README to synchronize.

To regenerate or verify the model output:

```sh
ruby tools/generate_fixtures.rb
ruby tools/generate_fixtures.rb --check
```

The root `mise run examples` compiles and runs these examples
(Swift on macOS; Kotlin and Rust on macOS/Linux). The real online counterpart is
`e2e/e2e.rb`, which starts a disposable Rails server and three native clients.

An ordinary write closure is one **local** transaction. Its operations can be
accepted independently by the server. Use `engine.writeAtomically` (Rust:
`write_atomically`) for a row action requiring all-or-nothing server execution.
The examples queue two notes this way. The action is frozen before the local
transaction returns; later edits cannot change its bytes.

A held or discarded member, an earlier unfrozen dependency, more than 100
operations, or more than 32 MiB refuses the entire atomic action locally.
Use ordinary writes for drafts that should remain editable while waiting on
uploads. A network batch is not an atomic action. See the
[contract](../../docs/PROTOCOL.md) for the distinction and recovery behavior.
