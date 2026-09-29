# Sync gates

A sync gate decides whether a local change can leave the device. Use one when a record depends on a media upload, an application setting, or another condition that must be satisfied before the server sees it.

The local write still goes through the engine transaction. A hold is durable and survives reopening.

## Decisions

| Decision | Effect |
| --- | --- |
| Push | The change becomes eligible for delivery |
| Hold | Keep the entity local and defer its outbound work until the gate allows it |
| Discard | Keep the local value without sending this change; not allowed for a birth that later work may depend on |

Swift names these cases `.push`, `.gate(reason)`, and `.discard`. A gate receives a `SyncChange` with the stream, row ID, kind, changed values, and previous values. Deletion has no new field values; consult its previous values when checking dependencies.

## Declare gates before opening

This Swift example pauses delivery when cloud backup is disabled. `isEnabled` must read thread-safe application state:

```swift
struct BackupGate: SyncGate {
    let id = "cloud-backup"
    let stream: String? = nil

    let isEnabled: @Sendable () -> Bool
    let signal: SyncGateSignal

    var changes: AsyncStream<Void> {
        signal.stream
    }

    func judge(_ change: SyncChange) -> SyncVerdict {
        isEnabled() ? .push : .gate("Cloud backup is disabled")
    }
}
```

Pass the gate to `ReplicaEngine(syncGates:)` at construction. After changing the setting, call `signal.fire()` so the engine rechecks its holds.

Kotlin accepts `List<SyncGate>` through `syncGates`; Rust accepts `Vec<Arc<dyn SyncGate>>` through `sync_gates`. Each client has the same write-time decision model. Give a gate a stable identifier across launches so its persisted holds remain attributable to the same rule.

## Global and stream gates

A gate with no stream is a global delivery policy. A stream-specific gate can establish a dependency barrier for declared children of a held parent. A global backup policy does not establish that parent/child barrier.

Declare the relationships in the [server manifest](./MODELS.md#references-and-lifetimes). Do not infer dependency ordering from coincidentally matching IDs in unrelated streams.

## Release a hold

Gates are evaluated when a change is written. Their change signal, a later write, or reopening the store triggers reevaluation. A network drain does not repeatedly poll unchanged gate conditions.

When a hold releases, ReplicaMan sends the current allowed row state or document history. It does not replay every intermediate local edit made during the hold. Keep gate evaluation short and deterministic; initiate the media upload elsewhere, then signal the gate when its result is available.

You can explicitly recheck gates through `refreshSyncGates` / `refresh_sync_gates`. Use this for a host-owned event that has no direct gate signal.

## Atomic actions

`writeAtomically` / `write_atomically` requires every member to be eligible immediately. A held or discarded member refuses and rolls back the entire local action. Use ordinary writes for work that should remain editable while waiting on uploads.

## Inspect and retain

Held entities appear in sync status and through `heldRows()` / `held_rows()`. Expose the reason when a user needs to know why delivery is waiting.

Media referenced by held work must remain available. Once a gate releases, the work can still be pending, submitted, or accepted-but-not-visible. Retention must account for those stages as well; see [Storage](./STORAGE.md#media-retention).

## Next steps

- [Mutations](./MUTATIONS.md) — choose the appropriate transaction boundary.
- [Synchronization](./SYNC.md) — inspect delivery progress.
- [Recovery](./RECOVERY.md) — inspect held work displaced by a lifetime or access change.
