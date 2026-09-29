# Recovery and diagnostics

ReplicaMan retains work that cannot safely continue in its original context. Examples include an edit to a replaced entity lifetime, a refused submission, or a submission bound to an earlier account or dataset.

A connection failure alone leaves the original submission retryable. It does not require a new operation id or a recovery branch.

## Start with status and health

Read [sync status](./SYNC.md#sync-status) to distinguish queued work, holds, submitted groups, accepted overlays, refusals, and recovery branches. Observe the engine's health surface for failures from automatic work.

Keep the original error and operation name in diagnostics. A storage failure should not become “no records found”, and a refused business action should not become a connectivity retry.

## List retained branches

Use the opened engine's state store:

```swift
let records = try store.recoveryRecords(limit: 50)
```

```kotlin
val records = store.recoveryRecords(limit = 50)
```

```rust
let records = store.recovery_records(None, 50)?;
```

Each record identifies the stream, row, incarnation, reason, and creation time. Paginate using the last returned record ID. Rescan from the beginning when refreshing the list to discover records added during a previous traversal.

`recoveryParts` / `recovery_parts` lists the retained byte sections. `recoveryChunk` / `recovery_chunk` reads bounded chunks of up to **256 KiB**, so an inspector need not load a whole document history into memory.

## Export a branch

Export preserves original bytes, including malformed JSON that cannot be decoded. Run file I/O outside the UI thread. The destination must be a new file chosen by the application; publish or share it only after the export and file close succeed.

### Swift

Here `destination` is an open `FileHandle` owned by the caller:

```swift
try store.exportRecovery(id: record.id) { bytes in
    try destination.write(contentsOf: bytes)
}
```

The caller is responsible for closing the handle and propagating close errors. An export error leaves the source branch retained.

### Kotlin

Here `destination` is an application-owned `File`:

```kotlin
destination.outputStream().use { output ->
    store.exportRecovery(record.id) { bytes ->
        output.write(bytes)
    }
}
```

The stream closes through `use`; write and close failures propagate.

### Rust

Here `destination` is an open file or another fallible byte sink:

```rust
use std::io::Write;
use replicaman::ReplicaError;

store.export_recovery(&record.id, |bytes| {
    destination
        .write_all(bytes)
        .map_err(|error| ReplicaError::Storage(error.to_string()))
})?;
```

Flush and finalize the destination before exposing it to another process or user. The export callback must only write bytes; it must not reenter the state store. Export reads one consistent SQLite snapshot.

## Export format

An export is newline-delimited UTF-8 JSON: record metadata, ordered parts, checksummed byte chunks, and a final completion record. A missing completion record means the file is incomplete even if every preceding line parses.

The archive can contain document folds, outbound intent, preimages, hold metadata, frozen submissions, accepted overlays, and identity context. Treat it as private application data. Part keys are opaque identifiers, not filesystem paths.

The full format and validation rules are in the [recovery export contract](./RECOVERY_EXPORT.md).

## Verify stored state

After a successful pull, explicitly check the stored authoritative base against the server's buckets at the same cursor:

```swift
try await engine.verifyIntegrity(shard: "user")
```

```kotlin
engine.verifyIntegrity(shard = "user")
```

```rust
engine.verify_integrity("user").await?;
```

This performs a full local scan plus an authenticated server comparison. Schedule it during diagnosis or periodic maintenance, not after every edit.

Local seals detect changed stored base bytes. The server comparison checks membership, incarnations, and revisions. It does not compare a canonical merged Loro document or prove that every domain write was captured correctly.

| Result | Next step |
| --- | --- |
| Success | The checked base and the server agree within those checks |
| `CursorBehind` | The server moved on since the last pull: pull, then verify again |
| `ReplicaDiverged` or a local seal failure | Preserve data and investigate; choose an explicit recovery action |
| Authorization or dataset failure | Resolve the account/history boundary before continuing |

Verification does not rewrite rows or acknowledge unsent work.

## Recover deliberately

An archive is evidence for recovery, not a command queue to replay into any account. Confirm the principal, dataset, entity lifetime, and intended business action before producing a new authorized edit.

For document diagnosis, `resyncDocument` / `resync_document` archives the outgoing document and requests a fresh baseline. `rebuildDocument` / `rebuild_document` accepts an explicit replacement and validates it before installation. Settle working edits first, and keep the archive until the outcome is understood.

Exporting does not delete the source branch. `removeRecoveryRecord` / `remove_recovery_record` is a separate destructive operation for an explicit application decision. Do not call it merely because an export started or a later sync succeeded.

## Next steps

- [Storage](./STORAGE.md) — protect account and store boundaries.
- [Synchronization](./SYNC.md) — classify delivery failures.
- [Server restore](./SERVER.md#restore-authoritative-history) — restore PostgreSQL with a new dataset epoch.
