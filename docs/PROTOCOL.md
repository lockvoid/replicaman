# Protocol and durability contract

This is the implemented ReplicaMan contract, protocol version 2. Rails/PostgreSQL
is authoritative; Swift, Kotlin and Rust maintain owner-specific SQLite replicas.
The server keeps no per-device state: no writers, sessions or read views.

## Local durability and ownership

A row save commits its materialized value, outbound intent, identity/references
and rejection preimage in one SQLite transaction. A document save commits its
merged fold, reflected fields and outstanding history together. A successful
save means local durability, not server acceptance.

Writers use WAL with `synchronous=FULL`; Apple stores enable `fullfsync`.
These settings depend on the operating system and hardware honoring flushes.
SIGKILL/reopen tests do not certify physical power-loss behavior.

One writable engine owns an OS-backed store lease. Owner stores are distinct.
Open rotates authoring peers and fences stale document handles. Seal blocks new
authoring/wire admissions and waits for admitted work; switch credentials only
after sealing. Close, seal and adoption remain fallible. Do not hide a failure
and proceed with an identity change.

Rust working documents distinguish in-memory acceptance from a durable receipt.
Await the receipt or `wait_saved` before reporting an edit as saved. Ordinary
async document mutations await persistence. Read handles are detached from
managed authoring; retained editors cannot write into a replaced lifetime.

## Requests

Every request is a JSON object with `protocol` (2), `namespace`, `schema` and
`dataset`. A client that has never synchronized sends `dataset: null` on its first
pull and stores the server's value; afterwards a different dataset fails with
`DatasetChanged`. Old clients stop with `DatasetChanged`, preserving local bytes;
they never replay old business actions into restored history.

Counters and revisions use canonical decimal strings; opaque business keys are
UTF-8 strings of 1 to 1,024 bytes without NUL. Errors answer
`{error: code, message}`: `UpgradeRequired`, `NamespaceChanged`, `DatasetChanged`,
`CursorInvalid`, `CursorBehind`, `MutationChanged`, HTTP 401 for authentication,
429/5xx with optional `Retry-After` for retryable failures.

## Buckets and cursors

The server captures every synchronized row into one bucket: `<shard>:<owner>`, or
`<shard>:*` for rows of a stream's declared shared owner. A transaction claims the
positions of all its captures at once, as its last act before COMMIT, taking the
bucket counters in one order; a counter row stays locked until that commit, so
positions become visible in commit order without gaps, and a transaction holding a
counter never waits on a row.
A row lives in the bucket it was born in: a write that would change its owner is
refused.

A principal reads its own bucket of a shard and, when a stream of that shard
declares a shared owner, the shared bucket. A cursor is opaque to clients: the
position reached in each of those buckets.

## Pull

`POST /pull {shard, cursor, limit}` (`cursor` null for a full baseline, `limit`
1..1000, default 500) answers:

```json
{"protocol": 2, "namespace": "…", "dataset": "…", "schema": 1,
 "shard": "user", "reset": false, "frames": [ … ], "cursor": "…", "more": false}
```

Frames arrive in position order:

- `row.set {stream, id, incarnation, revision, type, data}` — the row's state.
- `row.delete {stream, id, incarnation, revision}` — the lifetime ended.
- `doc.snapshot {stream, id, incarnation, revision, codec, snapshot, data}` — a full
  document baseline (base64 fold) with its reflected fields.
- `doc.delta {stream, id, incarnation, seq, codec, payload}` — history the client
  lacks, always followed in the same response by the document's `row.set`.

A document born, reborn or compacted after the cursor arrives as `doc.snapshot`.
A baseline pull (`cursor` null, `reset: true`) omits deletions; on publishing it
the client replaces the shard's base.

Each response reads one database snapshot. `more: true` means the bucket heads were
ahead of the returned cursor: the client stages the frames and continues with the
new cursor. It publishes only a round whose last response says `more: false`, in one
SQLite transaction: the staged frames in order, the rebased local authoring, the
cursor, and the removal of accepted overlays the round covers. A round survives
process death through its staged pages. `CursorInvalid` (malformed, foreign or
ahead of the server) discards staging and starts a baseline round.

A page holds at most `limit` entities and about 256 KiB, but always at least one
entity; a document's `doc.delta` frames and its `row.set` count as one entity, so a
page can carry more frames than `limit`. An entity is at most 32 MiB.

## Push

Local intent remains editable until eligible for transmission. Gates run before
freezing. Freezing creates one immutable submission in the same SQLite
transaction, ordered by a local sequence. Every operation carries a UUID (version 7
by default) minted by the client; the operations of one `writeAtomically` action
share a `group` UUID and stay contiguous.

`POST /push {ops}` sends at most 100 operations of the oldest submissions, never
splitting one. The server applies them in order in one transaction. For each
operation or group it first claims the id in `operations` (id, author, body SHA-256,
outcome, and the refusal reason when rejected), then runs it in a savepoint:

- A claimed id returns its stored verdict without running the mutation again; the
  same id with different bytes fails the request with `MutationChanged`.
- A declared refusal rolls back the group and records `rejected` with its reason.
- An infrastructure failure rolls back the whole request, claims included.

The answer is `{verdicts: [{id, outcome: "accepted"|"rejected", reason?}]}` in
request order. The client validates it completely before changing local state; a
missing, duplicate or foreign verdict acknowledges nothing. Accepted submissions
move to overlays; rejected ones become recovery evidence. The client sends the
next batch only after applying the previous answer, and retries a failed request
with the same operations.

An accepted overlay stays until a pull round that started after its acceptance
publishes: the round records the highest accepted local sequence when it starts
(`visible`) and removes overlays up to it. The server remembers operation ids
permanently; a periodic job may later delete old ones if volume ever requires it.

## Local transactions and atomic server actions

`write` gives local atomicity. Its independent operations may receive independent
server outcomes. Use `writeAtomically` (Rust `write_atomically`) for one row action
that must succeed or fail together remotely.

All members must pass their gates and dependency checks. A held/discarded member,
an earlier unfrozen dependency, more than 100 operations, or an encoded payload
over 32 MiB refuses the entire local transaction. The successful action freezes
as one submission before returning.

## Stored-base verification

After synchronizing, call `verifyIntegrity(shard:)` in Swift/Kotlin or
`verify_integrity(shard)` in Rust to scan the authoritative base. It streams one
SQLite snapshot, excluding local intent, accepted overlays and materialized
optimistic reads.

Each saved base row has a local SHA-256 seal over its exact stored fields/bytes.
The seal uses domain `replicaman-base\0` followed by stream, ID, shard,
incarnation, decimal revision, nullable type, stored JSON text, nullable codec
and nullable fold. Fields use an unsigned 64-bit big-endian byte length followed
by bytes; the all-ones length represents null. A missing or mismatched seal fails
verification without rewriting the row.

`POST /verify {shard, cursor}` answers `{shard, cursor, count, digest}` for the
current heads only; a client whose cursor is behind gets `CursorBehind` and pulls
first. The digest uses domain `replicaman-view\0`; live members sort by binary
UTF-8 stream then ID and contribute stream, ID, incarnation and canonical decimal
revision with the same length framing. Different membership reports
`ReplicaDiverged`. Verification never removes or acknowledges unsent authoring.

## Network backpressure

HTTP requests have bounded bodies and cancellable I/O. On HTTP 429 or 5xx,
native transports retain Retry-After advice shared across their endpoints.
Integer seconds and the preferred HTTP date format are supported. Deadlines use
monotonic time, add up to 255 ms jitter, and cap one advice interval at 24 hours.
Missing or invalid advice leaves the normal failure cooldown in force; the HTTP
failure still propagates and acknowledges no local work.

## Lifetimes, membership and conflicts

A row's owner decides its bucket and never changes. Pending and held work on a
deleted or replaced lifetime becomes an inspectable recovery branch.

An ordinary recreation names `replaces`, the previous deleted incarnation, and
chooses a fresh incarnation. Unknown or stale predecessors are refused. Compact
tombstones remain for the dataset's lifetime, even after their payload is
collected. A derived child's lifetime comes from its declared parent binding.

Row patches change only their named fields. Distinct fields compose; competing
values follow server transaction order unless domain normalization or explicit
preconditions refuse them. Device wall clocks do not choose the winner.

Document history merges through its registered codec. It must reject unsupported
codecs, missing baselines and unsatisfied causal dependencies. The framework never
substitutes an empty document or silently drops a failed delta.

## Holds and recovery

A sync gate chooses push, hold or discard at write time. Holds persist separately
from the outbound journal. Release sends the current allowed row state or document
history. A birth cannot be discarded while later work could depend on it.

Recovery copies raw bytes within the storage transaction before removing authoring
from the active view. Only explicit `removeRecoveryRecord` /
`remove_recovery_record` discards an archive. [Recovery exports](./RECOVERY.md)
define the portable JSON Lines format.

## Server capture and operations

Authentication supplies the principal; authorization runs inside the admitted
transaction or pull snapshot. A push may only capture its target row into the
principal's own bucket. Domain normalization and capture commit together.
Ordinary writes and `Replica.transaction` bulk writes are captured in the same
transaction: a trigger records every change to a replicated row in
`replica_man_changes`, capture clears it, and a deferred check refuses
the commit if anything is left.
A capture that leaves a row's projection, type, incarnation and bucket unchanged
takes no new revision or position and sends no frame. A deadlock, serialization
failure or lock timeout answers 503 `Contention` with `Retry-After: 1`.

External work that must survive belongs in a job enqueued inside the mutation's
transaction, on a queue stored in the same database. The host remains responsible
for HTTPS, credentials, domain validation, scheduling, quotas, backups, restore
fencing and monitoring.
