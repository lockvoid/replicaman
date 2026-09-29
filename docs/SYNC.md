# Synchronization

ReplicaMan saves locally before it synchronizes. Delivery sends durable outbound work to Rails; pulling fetches the account's buckets and publishes them into the local store.

## Push and pull

For an explicit refresh, send eligible local work and then pull:

```swift
try await engine.drain()
try await engine.pullUntilCaughtUp()
```

```kotlin
engine.drain()
engine.pullUntilCaughtUp()
```

```rust
engine.drain().await?;
engine.pull_until_caught_up(None).await?;
```

`drain` returns server verdicts. A rejected operation is a verdict, so inspect the result or the durable refused-work state when your UI needs to explain rejection. A transport or protocol failure throws or returns an error.

`pullUntilCaughtUp` follows a pull round's pages for the selected shards. It returns the number of applied changes; it is not a promise that no other server write can occur afterward. Pass explicit shards when only part of the replica needs refreshing:

```swift
try await engine.pullUntilCaughtUp(shards: ["user"])
```

```kotlin
engine.pullUntilCaughtUp(shards = listOf("user"))
```

```rust
let shards = vec!["user".to_owned()];
engine.pull_until_caught_up(Some(&shards)).await?;
```

`user` is the default shard. A shard groups streams that must appear together in one published round. Publication is atomic within a shard; several shards do not form one cross-shard transaction.

## Schedule synchronization

Automatic pushing is enabled by default and schedules delivery after local writes. Set `automaticallyPushWrites: false` in Swift, `automaticallyPushWrites = false` in Kotlin, or `automatically_push_writes = false` in Rust when the application owns the entire schedule.

Schedule refreshes when the app becomes active, connectivity returns, a server notification arrives, or the user requests refresh. Coalesce repeated signals, and keep background work within the active account's lifetime.

A server doorbell is a hint to pull. It carries no authoritative row data, and losing one notification must not prevent the next foreground or scheduled refresh from catching up. ReplicaMan does not install an application WebSocket connection or an operating-system background job for you.

## What happens after a save

1. The local value and outbound intent commit together.
2. Eligible intent freezes into an immutable submission; every operation gets its own id.
3. Rails records the business outcome and each operation's verdict in its transaction.
4. The client saves the verdicts locally before it sends the next batch.
5. An accepted overlay stays visible until a pull round that started after the acceptance publishes.

A lost reply repeats the same submission with the same operation ids; the server answers with the recorded verdicts and does not run the action again. Later edits become later work, even if an earlier submission is still uncertain.

## Sync status

Read status from the opened engine's `ReplicaStateStore`. These calls read one SQLite snapshot:

```swift
let status = try store.syncStatus()
```

```kotlin
val status = store.syncStatus()
```

```rust
let status = store.sync_status()?;
```

Kotlin store helpers are extensions; import `io.replicaman.*` or the individual extension functions.

| Swift / Kotlin field | Meaning |
| --- | --- |
| `queuedOperations` | Intent waiting for delivery |
| `heldEntities` | Records whose gates defer delivery |
| `submittedGroups` | Frozen submissions awaiting reconciliation |
| `acceptedOperations` | Accepted work waiting for a pull round |
| `rejectedOperations` | Refused operations retained for inspection |
| `recoveryBranches` | Archived work requiring an explicit recovery decision |
| `oldestIntentAt` / `oldestDownloadAt` | Age information for pending work or staged downloads |
| `journalBytes`, `submittedBytes`, `acceptedBytes`, `downloadBytes`, `recoveryBytes`, `documentBytes` | Storage used by the different stages |

Rust uses snake_case names for the same fields. `hasUnsettledWork` / `has_unsettled_work()` includes refusals and recovery branches, not just network backlog. Do not label an account “fully synced” merely because the editable journal is empty.

## Failure handling

| Situation | What ReplicaMan does | Application response |
| --- | --- | --- |
| Offline, timeout, HTTP 429 or 5xx | Keeps retryable work | Report connectivity state and schedule another attempt |
| Domain refusal | Reconciles the optimistic write and keeps its reason | Show the refusal; let the user make a corrected edit |
| `UpgradeRequired` | Refuses an unsupported contract | Require a compatible client/server deployment |
| `DatasetChanged` or `NamespaceChanged` | Stops the invalid exchange and retains local bytes | Enter a deliberate recovery flow |
| `CursorInvalid` | Discards staging and starts a baseline round | Nothing; the next pull rebuilds the base |
| Corrupt or incomplete pull page | Publishes no partial round | Surface the error and preserve the current store |
| Local storage failure | Fails the save or reports background health | Do not report the edit as saved |

The native HTTP transports honor valid `Retry-After` advice after HTTP 429 or 5xx. Waiting is cancellable. The failure still reaches the caller and acknowledges no work.

Never mint new operation ids for a failed request, and never wipe the database as an automatic retry strategy. A restored server history and an ordinary connection outage require different handling.

## Background health

Explicit calls propagate their failures. Automatic work has no awaiting caller, so observe the engine's health surface as well:

| Client | Observe | Latest failure |
| --- | --- | --- |
| Swift | `engine.health.failures()` | `engine.health.lastFailure` |
| Kotlin | `engine.health.failure` (`StateFlow`) | `engine.health.failure.value` |
| Rust | `engine.health.failures()` | `engine.health.last_failure()` |

Failures include the operation, original error, and time. Connect them to application diagnostics and sync UI. Health is bounded diagnostic state, not a durable history of every failure.

## Large protocol payloads

A pull page holds about **256 KiB** but always at least one frame. An entity can be up to **32 MiB** and travels inline in its frame; larger encoded entities are refused before the server commits them.

Application media uploads, storage credentials, and blob retention policy remain application responsibilities. [Sync gates](./GATES.md) let row delivery wait for those media uploads.

## Next steps

- [Keynotes](./KEYNOTES.md) — understand pull rounds and retry behavior.
- [Recovery](./RECOVERY.md) — inspect retained branches and verify integrity.
- [Command responses](./COMMITS.md) — refresh after application commands.
