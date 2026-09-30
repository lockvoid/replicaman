# ReplicaMan for Rails

ReplicaMan exposes owned row and CRDT document streams to durable Swift, Kotlin
and Rust replicas. The Rails engine owns change capture, bucketed pulls,
idempotent operations, normalization and the schema manifest. Your application
owns authentication, domain validation and jobs.

The [Rails guide](../docs/SERVER.md) is the application integration guide.
Continue with [client setup](../docs/SETUP.md),
[models and generation](../docs/MODELS.md), or
[documents](../docs/DOCUMENTS.md). The wire contract is
[PROTOCOL.md](../docs/PROTOCOL.md).

## Install in a Rails application

```ruby
# Gemfile
gem 'replica_man', path: '../replicaman/ruby'
# Add only for document streams:
gem 'loro', path: '../replicaman/ruby/vendor/loro'
```

Use Ruby 3.4+ and PostgreSQL; the optional Ruby Loro binding requires Ruby 4.0.
The verified environment is Ruby 4.0.5, Rails 8, PostgreSQL 18. Install the
engine migration before serving requests:

```sh
bin/rails replica_man:install:migrations
bin/rails db:migrate
```

The package uses PostgreSQL transaction snapshots, partitioned tables and
transaction IDs; SQLite is a client store, not a supported server substitute.

## Define a row stream

Assume `Note` has a string primary key, integer `user_id`, and string `title`.
Declare attributes explicitly; new database columns stay private by default.

```ruby
class Streams::Notes < ReplicaMan::Stream
  owner :user_id
  door ReplicaMan::Normalizer::Row
  attribute :title, :user_id
end

class AppReplica < ReplicaMan::Replica
  namespace 'my-application'
  schema_version 1
  # Configure REPLICAMAN_DATASET_EPOCH outside database backups.
  authenticate { |request| Session.authenticate_bearer(request) }
  authorize do |request:, user:, operation:|
    # Recheck credentials inside the actual push transaction or pull snapshot.
    # Returning nil or a different principal refuses the request.
    Session.authenticate_bearer(request)
  end
  stream Streams::Notes
end

# config/routes.rb
mount AppReplica => '/replica'
```

`Session.authenticate_bearer` is application code. A row's owner decides its
bucket (`<shard>:<owner id>`) and never changes; `owner :user_id, shared: -> { id }`
puts rows owned by that shared principal in the shard's shared bucket, which
every principal reads. A stream without a `door` refuses client writes and emits
read-only generated clients. Subclass `ReplicaMan::Normalizer::Row` for domain
validation through `refuse?(op)` and normalization through `normalize(attributes)`.

A push runs its operations in order, in one transaction, each operation or atomic
group in a savepoint. Every operation id is claimed in `replica_man_operations`
with its verdict: a retry returns the stored verdict without running the mutation
again. By default a new create colliding with an existing identity is rejected,
including when a custom normalizer overrides `create`.

Some domains deliberately share deterministic identities between an offline
client and the server. For those row streams, explicitly implement
`create_existing(replica, stream, op, record)`. The framework locks `record` and
checks its owner first. The handler must validate and persist the incoming write
according to the domain's merge rules; returning without applying it must be a
documented domain rule, never an assumption that a different create was an exact
retry.

## Document streams

Use `AppReplica.use ReplicaMan::Loro` and a normalizer derived from
`ReplicaMan::Normalizer::Document`. Declare the codec's document shape/defaults
and reflected row columns on the stream. The core package does not require Loro
for row-only applications. The runnable engine fixture under `test/dummy` shows
a small document stream, row stream, owners and a normalized projection.

Document deltas merge under the fold lock. The normalizer can author corrective
edits and project the merged state onto its ActiveRecord row. Missing causal
dependencies or invalid bytes fail the operation; they do not become an empty
document. Full history is retained through compaction.

The optional plugin also provides `ReplicaMan::Loro::Writer.write_registry(doc,
root, entries, base: previous_entries)` and `write_fields(map, fields, base: previous_fields)`.
Registries are hashes of entry keys to field hashes. These helpers preserve
concurrent additions and untouched fields. They do not commit and propagate
refused writes; discard the edited document if the enclosing action fails.

## Jobs and command responses

```ruby
AppReplica.after_apply do |operations:, user:, origin:|
  # Runs once per push, inside its transaction, for newly applied operations.
  NotebookIndexJob.perform_later(user_id: user.id)
end

AppReplica.doorbell do |shard:, captures:|
  # Called after commit. Delivery is a hint; clients still pull durable state.
  ReplicaNotificationJob.perform_later(shard: shard)
end
```

Enqueue external work as jobs on a queue stored in the same database (Solid Queue
with `enqueue_after_transaction_commit` off), so a job commits or rolls back with
the operations; never call an external service inside `after_apply`. For ordinary
application commands, `AppReplica.capture(user:)` returns a dataset-bound refresh
hint. See [the command response contract](docs/COMMITS.md).

## Generate clients and operate the server

```sh
bin/rails runner 'ReplicaMan::Manifest.new(AppReplica).write("config/replica-manifest.json")'
ruby ../replicaman/codegen/bin/replica-codegen --language swift \
  --manifest config/replica-manifest.json --out Generated --name AppReplica
```

`GET /manifest` is disabled in production by default. The same generator supports
Kotlin and Rust. Commit the manifest and generated output, and run `--check` in CI.

Migrations create each stream's partitions and change-capture triggers.
After adding, changing or removing a stream, `bin/rails g replica_man:migration`
writes the migration from the difference between the declarations and the
migrated database. Keep `schema_format = :sql`, and assert in a test that
`ReplicaMan::Schema::Plan.new([AppReplica]).changes` is empty.

```ruby
ReplicaMan::Backfill.call(AppReplica)
ReplicaMan::Reconcile.call(AppReplica)
AppReplica.gc(window: 30.days, limit: 500)
```

Backfill existing rows in bounded batches. Normal ActiveRecord writes are captured
automatically. Put raw SQL and `insert_all` inside `AppReplica.transaction`; database
hooks refuse a commit that leaves authoritative changes uncaptured. A write that
leaves a row's projection unchanged takes no new revision and sends no frame.

GC releases old tombstone payloads and document deltas while retaining entity
lifetime fences; schedule another call when a returned count reaches the limit.
Rotate the externally configured dataset epoch before serving a restored or
forked database.

Each published row/document must fit the 32 MiB entity limit. The server checks
the encoded entity before its domain transaction commits, so a later bootstrap
cannot become impossible merely because a document grew too large. A deadlock,
serialization failure or lock timeout answers 503 with `Retry-After`; the client
retries the same operations.

Use the root `mise run test:rails` to run real PostgreSQL integration tests in a
disposable database. The root end-to-end suite drives all three native clients
over HTTP. No application database is required for package verification.

A computed pull that reads another model declares the dependency on the child
stream, for example
`depends_on 'Notebook', via: :notebook_id, fields: [:title]`. Parent saves
recapture affected children in the same transaction. Bulk updates use
`YourReplica.transaction { Notebook.where(...).update_all(...) }`; committing
uncaptured dependency changes fails at the database boundary. Index the child
foreign key for large collections. Dependency capture does not modify the child's
domain row or manufacture an `updated_at` change.

## Restoring authoritative history

Stop every application server and background job before restoring. Use a new,
empty PostgreSQL database; restore all domain and ReplicaMan tables from the
same backup. The packaged command refuses a nonempty target:

```sh
DATABASE_URL=postgresql:///restored_app bundle exec replicaman-restore \
  --backup app.dump --epoch-file /etc/app/replicaman-epoch --service-stopped
```

Set `REPLICAMAN_DATASET_EPOCH_FILE=/etc/app/replicaman-epoch` on every server,
remove any explicit `dataset_epoch` declaration, and then resume service.
The file must be outside database backups and contain the same generation on
every application node. The restore command empties the fence before importing
and installs a fresh epoch only after `pg_restore --single-transaction` succeeds.
A failed import leaves synchronization unavailable. An invalid or missing file
never falls back to the previous epoch.

Existing clients retain their local data and refuse synchronization with
`DatasetChanged`. Recover or export their local branches explicitly; do not
automatically replay old business mutations into restored history. Deployments
using a secret/configuration service can continue using
`REPLICAMAN_DATASET_EPOCH`, but their restore procedure must rotate that value
before any server or job resumes.
