# Storage and account lifecycle

The local database holds saved application data, pending writes, server results, document history, and recovery records. Keep it in persistent application storage for the account that owns it.

## One store per owner

Opening owner `42` uses `replica-42.sqlite` under the engine's configured directory. Another owner uses a different file. Use **one authoring engine per owner file**; an operating-system lease prevents a second writer from opening it concurrently.

Choose a stable owner ID that corresponds to the authenticated server principal. Separate deployments or replica namespaces also need separate directories. A token change does not change the ownership of data already in a file.

## When a write is saved

A row transaction saves the materialized value, outbound intent, references, and rollback information together. A document edit saves its fold, reflected fields, and outbound history together.

SQLite uses WAL with `synchronous=FULL`; Apple stores also enable `fullfsync`. A successful ordinary write returns after the local commit. Flush behavior still depends on the operating system and hardware.

Rust's optional working-document editor has an additional stage: an edit may be visible in memory before its persistence receipt completes. Await that receipt or `wait_saved` before displaying “saved”. Ordinary asynchronous document mutations already wait for persistence.

## Open and close

```swift
try await engine.open(owner: 42)

// Use the replica while this account is active.

try await engine.close()
```

```kotlin
engine.open(42)

// Use the replica while this account is active.

engine.close()
```

```rust
engine.open(42).await?;

// Use the replica while this account is active.

engine.close().await?;
```

Closing stops admissions and releases the active binding. It preserves the owner file and pending work for reopening. It does not mean all work reached the server.

Treat lifecycle failures as failures. In particular, do not proceed with a credential switch after a failed close or seal. Rust may need to persist pending working-document edits during that boundary.

## Switch accounts

Serialize account transitions in the application:

1. Stop admitting new application work for the outgoing account.
2. Seal or close its engine and wait for already admitted operations to settle.
3. Replace credentials only after that boundary completes.
4. Open the incoming owner's store with matching credentials.
5. Start the incoming account's observers and sync schedule.

Closing already seals the engine. Use `seal()` separately when a longer application transition must keep the current store bound while refusing new writes and authenticated requests. `unseal()` resumes admissions after the store and credentials agree again.

An ordinary access-token refresh for the same principal is different from switching principals. Keep token access thread-safe and let the transport read the current token when sending a request.

Use `close()` when preserving an account for later sign-in. The native engine's `retire()` method closes and **deletes the local owner database**, including retained work. Reserve it for an explicit decision to remove that account's local data.

## Long-running application work

Cancel account-owned jobs on a transition and capture the intended owner before asynchronous work begins. A generated stream handle follows its engine; retaining the handle does not pin the old account.

Where available, the engine's local-session APIs fence delayed local work to the captured binding. Command responses use `commitSession` / `commit_session` for the same reason. See [Command responses](./COMMITS.md).

## Database access

Use generated reads and engine transactions for application data. Public SQL access is read-only. Writing ReplicaMan tables directly would bypass outbound intent, document bookkeeping, and rollback state.

SQLite data may still reside in the WAL. Do not copy or replace a live database file as a backup strategy. Close the owning engine first or use an appropriate consistent SQLite backup facility, and preserve the original until recovery is verified.

## Media retention

Your application owns image, video, and other blob storage. A local row that is temporarily absent from the current server view is not enough evidence that its media can be deleted.

Pending intent, holds, frozen submissions, and accepted overlays can still reference media. The state store exposes `containsUnsettledOperation` / `contains_unsettled_operation` to inspect delivery stages. Combine that with current rows and any recovery retention policy before collecting blobs. A decoding error must fail the retention decision rather than count as “no references”.

## Storage growth

Use [sync status](./SYNC.md#sync-status) to measure journal, submitted, accepted, download, document, and recovery bytes separately. This distinguishes a stopped upload, a held record, and a branch waiting for a user decision.

Do not delete pending work or recovery records by age. Server-side transfer cleanup and payload GC have different rules and live in the [server operations guide](./SERVER.md#maintenance).

## Format changes

This release initializes fresh stores for the former private-beta format. It does not migrate those earlier embedded databases. Unsupported formats fail explicitly; do not treat an open failure as permission to erase newly authored data.

## Next steps

- [Recovery](./RECOVERY.md) — export retained work before an explicit recovery decision.
- [Documents](./DOCUMENTS.md) — manage editor and peer lifetimes.
- [Synchronization](./SYNC.md) — monitor delivery independently of local saving.
