# Mutations and transactions

Write through the engine so the visible value, outbound operation, and rollback information commit together. A successful write is saved locally and can be read immediately, including while offline.

The server validates the write later. Use [sync status](./SYNC.md#sync-status) to distinguish local saving from server acceptance.

## Create and update

### Swift

```swift
try await replica.write { tx in
    try tx.notes.create(
        Note(id: "first-note", title: "Draft", userId: 42)
    )

    try tx.notes.update("first-note") { note in
        note.title = "Saved offline"
    }
}
```

### Kotlin

```kotlin
replica.writeAsync { tx ->
    tx.notes.create(
        Note(id = "first-note", title = "Draft", userId = 42)
    )

    tx.notes.update("first-note") { note ->
        note.copy(title = "Saved offline")
    }
}
```

Import the generated transaction extensions, such as `example.generated.*`. Each closure above is one local transaction: an exception rolls back both the rows and their outbound work.

`create` requires a new local identity. `update` reads the current model inside the transaction and throws if it is missing or cannot be decoded. It records changed fields, so an edit to `title` does not overwrite an unrelated field.

Generate a new ID with `ReplicaID.ulid()` for ordinary user-created records. The fixed IDs here make the examples easy to follow.

### Rust

```rust
replica
    .notes()
    .create(&Note::new("first-note", "Draft", 42))
    .await?;

replica
    .notes()
    .update(&Note::new("first-note", "Saved offline", 42))
    .await?;
```

Each typed Rust call is its own durable transaction. `update` compares the supplied model with the current local row and sends the difference. A stale whole model can therefore express changes you did not intend; use a transaction when a write depends on a fresh read.

## Delete

```swift
try await replica.write { tx in
    try tx.notes.delete("first-note")
}
```

```kotlin
replica.writeAsync { tx ->
    tx.notes.delete("first-note")
}
```

```rust
replica.notes().delete("first-note").await?;
```

Deletion becomes visible locally with its durable outbound operation. The protocol keeps the deleted entity's incarnation so a delayed edit cannot recreate that lifetime by accident.

## Local transactions and atomic server actions

| API | Local outcome | Server outcome |
| --- | --- | --- |
| Swift/Kotlin `write`; Rust `write` | One SQLite transaction | Independent operations can receive different verdicts |
| Swift/Kotlin `writeAtomically`; Rust `write_atomically` | One SQLite transaction, with a frozen action | Every row operation in the action succeeds or fails together |
| Several ordinary writes delivered in one HTTP request | Their original transactions | Transport batching adds no atomicity |

Use an atomic server action when partial acceptance would violate the meaning of the action:

```swift
try replica.engine.writeAtomically { tx in
    try tx.notes.create(Note(id: "group-a", title: "First", userId: 42))
    try tx.notes.create(Note(id: "group-b", title: "Second", userId: 42))
}
```

```kotlin
replica.engine.writeAtomically { tx ->
    tx.notes.create(Note(id = "group-a", title = "First", userId = 42))
    tx.notes.create(Note(id = "group-b", title = "Second", userId = 42))
}
```

These Swift/Kotlin atomic closures run synchronously; call them from a worker context for substantial work. The [Rust example](../rust/examples/notes/src/main.rs) shows the equivalent `write_atomically` call using `ReplicaRowModel::encode` and transaction `create`.

An atomic action supports **at most 100 row operations** and **32 MiB of encoded payload**. All members must pass their sync gates and dependency checks. A held or discarded member, or an earlier unfrozen dependency, refuses the entire transaction. Its bytes are frozen before the call returns.

Use ordinary writes for drafts or records waiting on uploads. Atomic actions cannot remain editable under a hold. These APIs cover row groups; they do not promise a mixed row/document server transaction.

## Keep transactions small

Read, decide, and write inside the closure. Perform network requests, media work, and other long-running tasks before it. Do not retain the transaction object or reenter the engine through a different write API from inside the closure.

For Swift and Kotlin, prefer the asynchronous transaction entry point from UI code. Synchronous reads and writes still perform database work even though the data is local.

## Conflicts

Row patches apply only their named fields. Concurrent changes to different fields compose. Competing changes to the same field follow server transaction order unless the stream's normalizer applies a domain rule or refuses the write. Device timestamps do not decide the winner.

A declared precondition field is not an automatic compare-and-swap mechanism. Implement version comparison or another business invariant in the server normalizer under the transaction lock.

An ordinary create that collides with an existing server identity is refused. For deliberately shared deterministic IDs, implement the explicit `create_existing` normalizer hook and define how incoming data merges.

## Refusals and network failures

A transport failure leaves work retryable. A server refusal is a recorded decision: the engine reconciles the optimistic value and preserves the refusal evidence. Retrying connectivity must not turn that refusal into a new business action.

Inspect refused operations through `parkedOps()` / `parked_ops()`, expose the reason to the user, and let a deliberate new edit express a corrected action. Work displaced by a changed lifetime or account history is available through [Recovery](./RECOVERY.md).

## Next steps

- [Sync gates](./GATES.md) — delay eligible writes until application dependencies are ready.
- [Documents](./DOCUMENTS.md) — edit collaborative content through a managed codec.
- [Synchronization](./SYNC.md) — deliver changes and observe their status.
