# Setup

Create one engine for the active account, give it a persistent directory and a transport, then open the account's store. Reuse that engine throughout the application.

The examples below use `NotesReplica` and `Note` from the [Notes manifest](../examples/notes/manifest.json). Generate your own types with [Models and code generation](./MODELS.md). The server must expose the matching namespace, schema, and streams.

## Swift

```swift
import Foundation
import ReplicaMan

func openNotes(
    home: URL,
    baseURL: URL,
    owner: Int,
    token: @escaping @Sendable () -> String?
) async throws -> NotesReplica {
    let transport = HTTPReplicaTransport(
        baseURL: baseURL,
        token: token
    )

    let engine = ReplicaEngine(
        home: home,
        transport: transport,
        schema: NotesReplica.schema
    )

    try await engine.open(owner: owner)

    return NotesReplica(engine: engine)
}
```

Use an Application Support directory for `home` and the mounted Rails URL, such as `https://api.example.com/replica`, for `baseURL`. The token closure reads your application's current bearer token and must be safe to call from background work.

The returned replica exposes generated stream handles:

```swift
let notes = try replica.notes.list()

try await replica.write { tx in
    try tx.notes.create(
        Note(id: ReplicaID.ulid(), title: "First note", userId: 42)
    )
}
```

Here and below, owner `42` is the signed-in server user in the example. Use your actual principal ID.

## Kotlin

```kotlin
import example.generated.NotesReplica
import io.replicaman.HTTPReplicaTransport
import io.replicaman.ReplicaEngine
import java.io.File
import okhttp3.HttpUrl

suspend fun openNotes(
    home: File,
    baseURL: HttpUrl,
    owner: Long,
    token: () -> String?,
): NotesReplica {
    val transport = HTTPReplicaTransport(
        baseURL = baseURL,
        token = token,
    )

    val engine = ReplicaEngine(
        home = home,
        transport = transport,
        schema = NotesReplica.schema,
    )

    engine.open(owner)

    return NotesReplica(engine)
}
```

On Android, pass an application-owned persistent directory such as `File(context.filesDir, "replica")`. Convert a configured URL with OkHttp's `toHttpUrl()` extension. Import your generated package when using its transaction extensions:

```kotlin
import example.generated.*
import io.replicaman.ReplicaID

val notes = replica.notes.list()

replica.writeAsync { tx ->
    tx.notes.create(
        Note(id = ReplicaID.ulid(), title = "First note", userId = 42)
    )
}
```

`writeAsync` moves the transaction off the caller's dispatcher. `write` runs synchronously on the caller's thread; use it when you already own a suitable worker context.

## Rust

Rust accepts a `ReplicaTransport` and a spawner supplied by the host. This example uses Tokio with its `rt-multi-thread` feature enabled:

```rust
use std::path::PathBuf;
use std::sync::Arc;

use replicaman::transport::ReplicaTransport;
use replicaman::{ReplicaEngine, ReplicaEngineOptions, ReplicaResult};

use crate::generated::NotesReplica;

async fn open_notes(
    directory: PathBuf,
    owner: i64,
    transport: Arc<dyn ReplicaTransport>,
) -> ReplicaResult<NotesReplica> {
    let options = ReplicaEngineOptions::new(
        directory,
        transport,
        NotesReplica::schema(),
        Arc::new(|future| {
            tokio::spawn(future);
        }),
    );

    let engine = ReplicaEngine::new(options);
    engine.open(owner).await?;

    Ok(NotesReplica::new(engine))
}
```

Call this while the runtime is active and keep the runtime alive for the engine's lifetime. The [runnable Rust example](../rust/examples/notes/src/main.rs) uses `NoWireTransport` for offline work.

For HTTP, implement `replicaman::transport::HttpClient` and pass it to `HttpReplicaTransport::new(base_url, client, token, headers)`. The token and header sources are boxed closures. Your adapter must:

- Return `HttpResponse { status, body, retry_after }`, preserving `Retry-After`.
- Enforce the supplied response limit while reading the body.
- Cancel I/O when its future is dropped and apply a request deadline.
- Propagate HTTP transport and decoding failures.

The engine owns synchronization retries and durable submissions. The adapter sends the supplied bytes and returns the response.

## Synchronize

Opening a store makes local data available. To send eligible writes and pull server changes:

```swift
try await replica.engine.drain()
try await replica.engine.pullUntilCaughtUp()
```

```kotlin
replica.engine.drain()
replica.engine.pullUntilCaughtUp()
```

```rust
replica.engine.drain().await?;
replica.engine.pull_until_caught_up(None).await?;
```

Automatic pushing is enabled by default. The application schedules pulls on foregrounding, reconnect, notifications, or an explicit refresh. See [Synchronization](./SYNC.md) for status and error handling.

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `home` / Rust `directory` | Platform configuration; explicit in Rust | Parent directory for owner databases |
| `schema` | Required | Generated streams, namespace, and schema version |
| `codecs` | Empty | Register document codecs such as Loro |
| `automaticallyPushWrites` / `automatically_push_writes` | `true` | Schedule delivery after local writes |
| `coldWindow` / `cold_window` | 10 seconds | Cooldown used after transport failures |
| `syncGates` / `sync_gates` | Empty | Application rules for holding outbound work |
| `documentMode` / `document_mode` | Replicated | Full document replication or an explicitly read-only projection store |

Supply codecs and gates when creating the engine, before opening it. A projection-only store has a distinct persisted mode; do not use it to edit documents.

## Next steps

- [Queries](./QUERIES.md) — read and observe the local store.
- [Mutations](./MUTATIONS.md) — choose local or remote atomicity.
- [Storage](./STORAGE.md) — close stores and switch accounts safely.
