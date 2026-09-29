# Documents and Loro

Use document streams for content whose edits should merge across devices: structured editors, collaborative text, lists, and field-based registries. ReplicaMan's optional Loro plugin manages the codec, local history, and synchronization boundary.

Use row streams for ordinary records that need server-ordered field patches. Both kinds share the same account, authorization, and pull model.

## Enable the codec

Add the optional packages described in [Installation](./INSTALLATION.md). Register a codec when constructing the engine:

```swift
import ReplicaManLoro

let engine = ReplicaEngine(
    home: home,
    transport: transport,
    schema: AppReplica.schema,
    codecs: [LoroReplicaCodec()]
)
```

```kotlin
import io.replicaman.loro.LoroReplicaCodec

val engine = ReplicaEngine(
    home = home,
    transport = transport,
    schema = AppReplica.schema,
    codecs = listOf(LoroReplicaCodec()),
)
```

```rust
use replicaman_loro::LoroReplicaCodec;

options.codecs.push(Arc::new(LoroReplicaCodec::new()));
let engine = ReplicaEngine::new(options);
```

Here `AppReplica` is generated from a manifest containing your document streams. Rust's `options` is the `ReplicaEngineOptions` from [Setup](./SETUP.md#rust), with the `replicaman-loro` crate added.

## Define a server document

Assume `Board` has a string primary key, `user_id`, and `name`. Register `Streams::Boards` on your replica and install `ReplicaMan::Loro`:

```ruby
class BoardNormalizer < ReplicaMan::Normalizer::Document
  def project(doc, op: nil)
    op ? { user_id: op.user.id } : {}
  end
end

class Streams::Boards < ReplicaMan::Stream
  scope ->(user) { { user_id: user.id } }
  door BoardNormalizer

  attribute :user_id, :name

  document do
    attribute :name
  end
end

AppReplica.use ReplicaMan::Loro
AppReplica.stream Streams::Boards
```

`name` belongs to document history and is reflected into the row projection. Scalar document fields live under the `meta` root. The row remains useful for lists and indexes; edits to document-owned fields must go through the document.

Export and regenerate the manifest after adding the stream. Use `--document-out` for generated typed document shapes.

## Create a document

Create a fresh authoring peer, seed the document, and pass its snapshot to the engine. These examples create the `boards` stream above for owner `42`.

### Swift

```swift
let codec = LoroReplicaCodec()
let peer = ReplicaID.peer()
let draft = try codec.open(fold: nil, peer: peer)

try draft.write(.string("Plans"), at: ["meta", "name"])

try await engine.createDoc(
    stream: "boards",
    id: "first-board",
    seed: try codec.snapshot(draft),
    peer: peer,
    data: ["userId": .signedInteger(42)]
)
```

### Kotlin

```kotlin
val codec = LoroReplicaCodec()
val peer = ReplicaID.peer()
val draft = codec.open(fold = null, peer = peer)

codec.write(ReplicaValue.Str("Plans"), listOf("meta", "name"), draft)

engine.createDoc(
    stream = "boards",
    id = "first-board",
    seed = codec.snapshot(draft),
    peer = peer,
    data = mapOf("userId" to ReplicaValue.Integer(42)),
)
```

### Rust

```rust
use replicaman::{DocumentCodec, DocumentValue, ReplicaFields, ReplicaValue};

let codec = LoroReplicaCodec::new();
let peer = replicaman::id::peer();
let mut draft = codec.open_document(None, peer)?;

draft.write_map_field("meta", "name", &DocumentValue::String("Plans".into()))?;

let seed = codec.document_snapshot(&draft)?;
let data = ReplicaFields::from([
    ("userId".into(), ReplicaValue::Integer(42)),
]);

engine
    .create_doc("boards", "first-board", &seed, peer, &data, None)
    .await?;
```

The seed peer identifies the history you just authored. Do not hard-code a shared peer or copy an active editor identity into another store. ReplicaMan manages subsequent editor peers and fences stale handles on reopen or lifetime changes.

## Edit through a managed scope

Once the document exists, use the engine's edit boundary:

```swift
try await engine.updateDocument(
    stream: "boards",
    id: "first-board",
    codec: LoroReplicaCodec.self
) { document in
    try document.write(.string("Release plans"), at: ["meta", "name"])
}
```

```kotlin
engine.updateDocument("boards", "first-board", codec) { document ->
    codec.write(
        ReplicaValue.Str("Release plans"),
        listOf("meta", "name"),
        document,
    )
}
```

```rust
engine
    .update_document::<LoroReplicaCodec>("boards", "first-board", |document| {
        document.write_map_field(
            "meta",
            "name",
            &DocumentValue::String("Release plans".into()),
        )
    })
    .await?;
```

These methods return whether the document changed and complete after persistence. A missing document is an error. If editing or persistence fails, report that failure; do not keep mutating the escaped document object.

Generated document stream handles provide `findDoc`, `watchDoc`, `updateDoc`, `undoDoc`, and `redoDoc` where the native API supports the corresponding typed state. Rust uses snake_case names and explicit codec/state type parameters. Kotlin supplies its codec/state adapter explicitly.

## Read and observe

Use a document state projection for rendering, and a document watch for local edits and pulled history. Read access is detached from managed authoring; changing a read handle does not save an edit.

Keep editor objects inside their managed edit scope. A handle retained across account switching, document replacement, or engine reopening is not a valid new authoring session.

## Undo and registries

Use the managed `undoDocument` / `redoDocument` APIs or their generated stream equivalents so undo changes are persisted and synchronized. Rust uses `undo_document` / `redo_document`. Undo history belongs to the current editor session; do not assume it survives every close and reopen.

The plugin also provides map and registry helpers. They let an application change named fields while preserving concurrent additions and untouched fields. Use generated document writers or those helpers instead of rebuilding an entire shared registry from a stale UI snapshot.

## Failures and projections

Invalid document bytes, missing causal dependencies, or an unavailable codec are explicit failures. ReplicaMan does not replace an unreadable document with an empty one. An explicit resync or rebuild archives the outgoing state first; see [Recovery](./RECOVERY.md).

A store opened in projection-only document mode is read-only for documents and remembers that mode on disk. It can consume projected row fields without a full editor, but it cannot silently become a document-writing store.

## Next steps

- [Queries](./QUERIES.md) — use row projections for lists.
- [Sync gates](./GATES.md) — hold a document while dependencies are unavailable.
- [Recovery](./RECOVERY.md) — preserve and inspect failed or displaced history.
