# Queries and observation

Read through a generated stream handle. Reads use the local SQLite replica, including local edits, and do not make a network request. Synchronization updates that store and notifies observers after a committed change.

The examples use the `NotesReplica` from [Setup](./SETUP.md).

## Find a record

```swift
let note = try replica.notes.find("first-note")
```

```kotlin
val note = replica.notes.find("first-note")
```

```rust
let note = replica.notes().find("first-note")?;
```

A missing record returns `nil`, `null`, or `None`. A generated row model can also be unavailable when stored data has an unknown subtype or lacks a required field; the raw record stays in storage. Malformed stored JSON raises an error. Do not turn a failed read into an empty successful result.

## Read a collection

```swift
let notes = try replica.notes.list(limit: 50)
```

```kotlin
val notes = replica.notes.list(limit = 50)
```

```rust
let notes = replica.notes().all()?;
```

Swift and Kotlin support a limit on the indexed query API. Rust's `all()` loads the whole stream; it does not expose the same limit or ordering API. Use it for bounded collections.

## Indexed queries

First declare the indexes from [Models](./MODELS.md#indexes) and regenerate. These examples require a B-tree index on `user_id` and `title`, plus an FTS5 index on `title`.

### Swift

```swift
let notes = try replica.notes.list(
    .eq(.userId, 42),
    order: [.ascending(.title)],
    limit: 50
)

let matches = try replica.notes.list(
    .match(.title, "meeting notes"),
    limit: 20
)
```

### Kotlin

```kotlin
val notes = replica.notes.list(
    predicate = ReplicaPredicate.eq(Note.Field.USER_ID, 42),
    order = listOf(ReplicaOrder(Note.Field.TITLE)),
    limit = 50,
)

val matches = replica.notes.list(
    predicate = ReplicaPredicate.match(Note.Field.TITLE, "meeting notes"),
    limit = 20,
)
```

Generated Kotlin field enums use upper snake case. Import your generated model and `io.replicaman.*`.

| Query | Required structure | Behavior |
| --- | --- | --- |
| Equality, one-of, null | B-tree index | Matches a declared scalar field |
| String prefix | B-tree index | Case-sensitive binary string prefix |
| Text match | FTS5 index | Tokenized word-prefix search; accepts ordinary search text |
| Ordering | B-tree index | Sorts by declared fields, with row ID as the tie-breaker |
| ID / ID prefixes | Primary key | Needs no field index declaration |

An empty text query matches all rows; an empty one-of or ID-prefix set matches none. Negation follows SQL null semantics: a null field does not satisfy either an equality predicate or its negation. Use an explicit null predicate when that distinction matters.

Rust provides `where_equals(&ReplicaFields)` for field equality. The generated Swift/Kotlin indexed predicate DSL is not currently part of the Rust API.

## Observe changes

### Swift

Keep the returned watch for as long as the screen needs updates:

```swift
let watch = replica.notes.watch(includeInitial: true) { notes in
    // Delivered on the main actor.
    render(notes)
}

await watch.hold()
```

Run this from the screen's task. `hold()` keeps the observation alive and cancels it when that task is canceled. You can also retain the handle yourself and call `watch.cancel()`.

`includeInitial` defaults to `false` for the callback API. Set it to `true` for an initial result followed by changes. A scoped watch accepts the same predicate, order, and limit as `list()`.

### Kotlin

The whole-stream API returns a `Flow`:

```kotlin
replica.notes.watch().collect { notes ->
    render(notes)
}
```

Collect from a lifecycle-owned coroutine. Cancellation stops collection. For an indexed query, the callback overload returns a `ReplicaWatch`:

```kotlin
val watch = replica.notes.watch(
    predicate = ReplicaPredicate.eq(Note.Field.USER_ID, 42),
    includeInitial = true,
) { notes ->
    publishNotes(notes)
}

try {
    watch.hold()
} finally {
    watch.cancel()
}
```

Dispatch callback results into your application's UI state as appropriate. `render` and `publishNotes` in these examples are application functions.

### Rust

```rust
use futures::StreamExt;

let watch = replica.notes().watch();
futures::pin_mut!(watch);

while let Some(notes) = watch.next().await {
    render(notes);
}
```

Dropping the stream ends the observation. Keep the task that consumes it within the active account's lifetime.

## Read before writing

A read used for display can happen outside a write transaction. A read that decides a write must share that write's transaction, or another local change can occur between them.

Swift and Kotlin expose typed transaction reads and updates. Rust exposes raw transaction reads alongside its typed row methods. See [Mutations](./MUTATIONS.md).

## Next steps

- [Mutations](./MUTATIONS.md) — edit records and handle server decisions.
- [Synchronization](./SYNC.md) — refresh local data from Rails.
- [Recovery](./RECOVERY.md) — observe failures and inspect retained data.
