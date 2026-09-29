# Command responses

An application command can return a ReplicaMan refresh hint alongside its domain
result. The hint names affected shards and binds them to the namespace, schema,
protocol, and dataset. Applying it requests ordinary verified checkpoints.

## Capture a command

```ruby
commit = AppReplica.capture(user: current_user) do
  report = CreateReport.call(user: current_user, input: params.fetch(:report))

  { report_id: report.id }
end

render json: commit.value.merge(replica_commit: commit.encode)
```

`CreateReport` is application code. Capture collects changes through the normal
capture callbacks; the application command still owns its transaction.
`commit.value` is the block's result.

Call `commit.encode` after the command commits. Encoding inside an open database
transaction raises `ReplicaMan::Commit::Uncommitted`. The successful encoding is
memoized. Its Base64 payload contains protocol identity and a list of shard names;
it contains no row frames or document snapshots.

## Apply on the client

Capture a session before the command's HTTP request:

```swift
let session = try await engine.commitSession()
let answer = try await api.createReport(input)

if let hint = answer.replicaCommit {
    try await engine.apply(commit: hint, session: session)
}
```

Kotlin uses `engine.commitSession()` and `engine.apply(commit, session)`. Rust uses
`engine.commit_session().await?` and `engine.apply_commit(hint, &session).await?`.
The session binds the reply to the current engine/owner store. A response from an
outgoing account cannot refresh an incoming account's store.

The client validates the hint's identity, invalidates stale download staging for
its shards, and pulls through the normal checkpoint path. Authorization,
accepted overlays, local intent, publication atomicity, and cursor updates follow
the same rules as ordinary synchronization.

## Preserve command success

Once the command returns its domain ID, a refresh failure is a sync failure.
Retain the domain result, report the error, and schedule another refresh. Do not
repeat the command merely because applying its hint failed.

If encoding the hint fails after the command has committed, preserve the domain
response, return a null hint, and report the encoding error. This is a deliberate
exception at the response boundary: the business operation already succeeded,
and ordinary synchronization can recover its visible state. Do not hide command,
storage, or transaction failures under that exception.

See the monorepo's [command response guide](../../../../docs/COMMITS.md) for
Swift, Kotlin, and Rust examples, or return to the [server guide](../README.md).
