# Command responses

An application command can change replicated data outside the client's normal write queue. For example, a server command may create a report and return its ID. Include a ReplicaMan refresh hint so the client can fetch the affected shards immediately after the command succeeds.

The API calls this a **commit**, but its payload is a dataset-bound refresh hint. Applying it performs an ordinary pull.

## Capture the server command

```ruby
commit = NotesReplica.capture(user: current_user) do
  report = CreateReport.call(user: current_user, input: params.fetch(:report))

  { report_id: report.id }
end

render json: commit.value.merge(replica_commit: commit.encode)
```

`CreateReport` is application code and owns its transaction and business rules. Capture collects affected stream addresses through the normal capture callbacks. It does not wrap the whole command in a new database transaction.

Encode the hint only after the command commits. `commit.encode` inside an open SQL transaction raises `ReplicaMan::Commit::Uncommitted`. The encoded value is an opaque Base64 string containing protocol identity and the affected shard names; do not import it as row data.

## Bind the response to the account

Capture a commit session before starting the command request. Apply the returned hint under that session:

```swift
let session = try await engine.commitSession()
let answer = try await api.createReport(input)

if let hint = answer.replicaCommit {
    try await engine.apply(commit: hint, session: session)
}
```

```kotlin
val session = engine.commitSession()
val answer = api.createReport(input)

answer.replicaCommit?.let { hint ->
    engine.apply(commit = hint, session = session)
}
```

```rust
let session = engine.commit_session().await?;
let answer = api.create_report(input).await?;

if let Some(hint) = answer.replica_commit.as_deref() {
    engine.apply_commit(hint, &session).await?;
}
```

`api`, `input`, and the response types belong to your application. The session prevents a response started for one account/store binding from being applied to another. The hint also validates the namespace, schema, and dataset.

Applying a valid hint pulls the affected shards. Pending local work, atomic publication, and cursor updates follow the same path as normal sync.

## Keep command success separate from refresh success

Once a command returns its domain ID, keep that result even if refreshing the replica fails. Report the refresh failure and schedule normal sync; do not repeat a paid or otherwise consequential command to recover a failed pull.

Likewise, if hint encoding fails after the server command has committed, preserve the successful domain response, return a null hint, and report the encoding error. This is an intentional response-boundary exception: the command already succeeded, and ordinary synchronization can fetch its data. Do not rescue the command itself as though it were merely an optional hint failure.

## Next steps

- [Synchronization](./SYNC.md) — the pull path used by hints.
- [Storage](./STORAGE.md#switch-accounts) — serialize account transitions.
- [Rails server](./SERVER.md#capture-application-writes) — capture domain changes reliably.
