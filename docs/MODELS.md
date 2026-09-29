# Models and code generation

Define the replication surface in Rails, export a manifest, and generate native types from it. The manifest describes streams, fields, permissions, indexes, references, and document shapes.

## Define a stream

Assume an ActiveRecord `Note` with a string `id`, a non-null string `title`, and a non-null integer `user_id`:

```ruby
class Streams::Notes < ReplicaMan::Stream
  scope ->(user) { { user_id: user.id } }
  door ReplicaMan::Normalizer::Row

  attribute :title, :user_id
end

class NotesReplica < ReplicaMan::Replica
  namespace 'example.notes'
  schema_version 1

  stream Streams::Notes
end
```

Add the authentication and dataset configuration from [Rails server](./SERVER.md) before using this definition over HTTP.

Only declared attributes enter the manifest. New database columns stay private until you expose them. Wire names use lower camel case, so `user_id` becomes `userId` in Swift and Kotlin; Rust generates the corresponding native field name.

## Export the manifest

From your Rails application:

```sh
bin/rails runner 'ReplicaMan::Manifest.new(NotesReplica).write("config/replica-manifest.json")'
```

Commit this file. It is the input shared by every client and makes generation independent of a running server. The HTTP manifest endpoint is disabled in production by default.

## Generate native models

Run from the application repository, adjusting output paths to your targets:

```sh
ruby ../replicaman/codegen/bin/replica-codegen \
  --language swift --manifest config/replica-manifest.json \
  --out Generated --name NotesReplica

ruby ../replicaman/codegen/bin/replica-codegen \
  --language kotlin --manifest config/replica-manifest.json \
  --out src/main/kotlin/example/generated \
  --package example.generated --name NotesReplica

ruby ../replicaman/codegen/bin/replica-codegen \
  --language rust --manifest config/replica-manifest.json \
  --out src/generated --name NotesReplica
```

For Rust, declare `mod generated;` in your crate. Swift and Kotlin output must belong to the application target or source set that uses it.

The Notes manifest produces a `Note` model, a `NotesReplica` container, and typed access to the `notes` stream. Swift and Kotlin also generate transaction accessors such as `tx.notes`. Read-only streams omit authoring accessors.

## Keep generated code current

Run the same command with `--check` in CI:

```sh
ruby ../replicaman/codegen/bin/replica-codegen \
  --language swift --manifest config/replica-manifest.json \
  --out Generated --name NotesReplica --check
```

`--check` compares generated output without modifying the destination. Normal generation tracks the files it owns and removes stale generated files while preserving neighboring handwritten files. Do not edit generated source; change the stream definition, manifest, or generator configuration.

## Field directions

| Declaration | Behavior |
| --- | --- |
| `attribute :title` | Exposes the model field using the stream's normal write/read rules |
| `attribute :created_at, push: false` | Server-owned field; clients receive it but cannot author it |
| No `door` | The whole stream is read-only to clients |
| Computed `pull:` | Derives a field for replication; declare dependencies when it reads another model |
| `precondition: true` | Requires the field on applicable writes; business compare-and-swap rules still belong in the normalizer |

Changing a field's direction or document ownership is a schema change. The generated API helps express the rules, and the server validates them again.

## Indexes

Declare indexes on fields that Swift and Kotlin clients query or sort:

```ruby
class Streams::Notes < ReplicaMan::Stream
  scope ->(user) { { user_id: user.id } }
  door ReplicaMan::Normalizer::Row

  attribute :title, :user_id

  index :user_id
  index :title
  index :title, kind: :fts5
end
```

Regeneration adds indexed field cases to the native model. Use B-tree indexes for equality, ordering, and string prefixes; use FTS5 for text matching. These declarations describe client query indexes, so add any server database indexes separately.

The small checked-in Notes example has no indexes. Add the declarations above and regenerate before using `Note.Field.title` or `Note.Field.userId`. Rust currently exposes `find`, `all`, and `where_equals` rather than the Swift/Kotlin indexed query DSL.

## References and lifetimes

Declare relationships that outbound changes depend on:

```ruby
reference :notebook, stream: 'notebooks', field: :notebook_id
```

The referenced stream must exist in the same replica. ReplicaMan binds the reference to the parent's current incarnation, so a queued child cannot silently attach to a newly created parent that reused an ID. The default reference is required; use `optional: true` only when the relationship can be absent.

For a derived record whose entire lifetime belongs to that parent, also declare:

```ruby
lifetime_from :notebook
```

This is stronger than an ordinary relationship. Use it for derived data that should belong to the replacement parent's lifetime when the parent is recreated.

## Document shapes

Add `--document-out` when generating typed document shapes. `--ts-out` generates TypeScript declarations for those shapes; it does not provide a JavaScript replication client.

Optional `--config` JSON supports model and variant naming overrides and application-specific document projection adapters. The generator validates the common semantic model before rendering each language. See [Generator options](../codegen/README.md) and [Documents](./DOCUMENTS.md).

## Schema identity

Use a stable namespace for one replication application and a deliberate schema version for its contract. An unsupported schema or a different namespace is an explicit synchronization failure. Regenerate and deploy the affected clients together when changing a contract they must understand.

## Next steps

- [Queries](./QUERIES.md) — use the generated read API.
- [Mutations](./MUTATIONS.md) — author through typed transactions.
- [Rails server](./SERVER.md) — enforce authorization and domain rules.
