# Rails server

ReplicaMan exposes authorized streams over ActiveRecord models. Rails remains the authority for membership, validation, and business effects. The engine captures committed changes and serves them to native replicas through bucketed pulls.

## Install

Add the gem and run its migrations as described in [Installation](./INSTALLATION.md#rails). The server requires **Rails 8+**, **Ruby 3.4+**, and **PostgreSQL**. Document streams additionally use the Ruby Loro binding, which requires Ruby 4.0.

Install every engine migration before serving requests. PostgreSQL transaction snapshots, partitions, and capture guards are part of the contract.

## Define a row stream

Assume `Note` has a string primary key, non-null `title`, and non-null integer `user_id`:

```ruby
# app/models/streams/notes.rb
class Streams::Notes < ReplicaMan::Stream
  scope ->(user) { { user_id: user.id } }
  door ReplicaMan::Normalizer::Row

  attribute :title, :user_id
end
```

The stream name follows the model table: `Streams::Notes` exposes `notes` over `Note`. Attributes are explicit, so a new database column does not automatically become public.

The `scope` defines the current principal's membership and is checked on reads and writes. Omitting `door` makes a stream read-only to clients.

## Authenticate and mount

```ruby
# app/models/notes_replica.rb
class NotesReplica < ReplicaMan::Replica
  namespace 'example.notes'
  schema_version 1

  authenticate do |request|
    Session.authenticate_bearer(request)
  end

  authorize do |request:, user:, operation:|
    Session.authenticate_bearer(request)
  end

  stream Streams::Notes
end

# config/routes.rb
mount NotesReplica => '/replica'
```

`Session.authenticate_bearer` is your application's authentication method. It must return the authenticated user or refuse the request; it is not provided by ReplicaMan.

`authenticate` identifies the principal. `authorize` revalidates access inside the actual admitted transaction or pull snapshot. Returning no principal or a different principal refuses the request. Use `operation` when application policy differs by request kind.

Clients send bearer credentials to the mounted URL. Do not expose an unauthenticated fixture server as a production integration.

## Configure the dataset epoch

Set `REPLICAMAN_DATASET_EPOCH` to a stable, deployment-owned identifier, or configure `REPLICAMAN_DATASET_EPOCH_FILE` with a file containing it. Generate the value once when provisioning a new authoritative history:

```sh
ruby -rsecurerandom -e 'puts SecureRandom.uuid'
```

Store that output in your deployment configuration. Keep the same value on every application server and across ordinary restarts. Keep it outside database backups and rotate it when restoring or forking authoritative history.

The namespace identifies the replication application; the schema version identifies its data contract; the dataset epoch identifies this server history. They serve different purposes.

## Validate and normalize writes

The row normalizer applies the supplied fields to an ActiveRecord model and saves it. Use normal model validations for constraints such as required titles:

```ruby
class Note < ApplicationRecord
  validates :title, presence: true, length: { maximum: 500 }
end
```

For replication-specific rules, subclass `ReplicaMan::Normalizer::Row` and assign it with `door`:

| Hook | Purpose |
| --- | --- |
| `refuse?(op)` | Return a reason or truthy value for a declared domain refusal; return false/nil to continue |
| `normalize(attributes)` | Return the attributes to persist after domain normalization |
| `create_existing(replica, stream, op, record)` | Define an explicit merge for a deliberately shared deterministic identity |

Ordinary creates colliding with existing identities are refused. Implement `create_existing` only when your domain deliberately allows that collision. The framework locks the record and checks membership before calling it; your handler must validate and persist the intended result.

Required data should use `fetch` so a missing value fails at its source. Let unexpected storage and infrastructure exceptions propagate. A declared refusal is committed as a business verdict; an infrastructure failure rolls back the whole request, and the client retries the same operations.

## Capture application writes

Ordinary supported ActiveRecord writes are captured automatically. Domain changes and replica capture commit together, so clients cannot receive a successfully committed domain write without its corresponding replication state.

Wrap bulk writes or raw SQL that changes replicated data in the replica transaction:

```ruby
NotesReplica.transaction do
  Note.where(user_id: user.id).update_all(title: 'Archived')
end
```

Database guards refuse a commit that leaves required capture incomplete. Keep bulk updates bounded for your application's workload.

If a computed pull reads another model, declare the dependency on the child stream. For example, an `entries` stream that projects fields from `Notebook` could declare:

```ruby
depends_on 'Notebook', via: :notebook_id, fields: [:title]
```

Changes to the declared parent fields recapture affected children in the same transaction. Index the child foreign key. This projection dependency is separate from a mutation [reference](./MODELS.md#references-and-lifetimes).

## Effects and notifications

Use `after_apply` for work that must commit with newly applied operations:

```ruby
NotesReplica.after_apply do |operations:, user:, origin:|
  NotebookIndexJob.perform_later(user_id: user.id, operation_ids: operations.map(&:id))
end
```

It runs once per push, inside its transaction, for operations applied by that request; a retried push whose operations were already applied does not run it again. External work belongs in a job on a queue stored in the same database (for example Solid Queue with `enqueue_after_transaction_commit` off), so the job commits or rolls back with the operations. Network calls inside the callback cannot roll back.

Notify clients after commit through `doorbell`:

```ruby
NotesReplica.doorbell do |shard:, captures:|
  ReplicaNotificationJob.perform_later(shard: shard)
end
```

The notification job is application code. Its delivery is a hint; pull remains the durable data source. A missed notification is recovered by a later pull.

For a separate application command, return a refresh hint with `capture(user:)`. See [Command responses](./COMMITS.md).

## Document streams

Install `ReplicaMan::Loro`, use a `ReplicaMan::Normalizer::Document`, and declare the document-owned fields and shapes. The normalizer merges history and projects the result onto its ActiveRecord row under the document lock.

See [Documents and Loro](./DOCUMENTS.md) for a complete small stream. Row-only applications need no Loro dependency.

## Export the schema

```sh
bin/rails runner 'ReplicaMan::Manifest.new(NotesReplica).write("config/replica-manifest.json")'
```

Commit the manifest and generate each client from it. The production manifest endpoint is disabled by default. See [Models](./MODELS.md) for generation and drift checks.

## Maintenance

`install!` creates a partition per stream and the change-capture triggers. It takes table locks, so run it from the migration step, never on boot:

```ruby
# lib/tasks/replica.rake
task replica_install: :environment do
  NotesReplica.install!
end

task 'db:schema:dump' => 'replica_install'
Rake::Task['db:migrate'].enhance { Rake::Task['replica_install'].invoke }
```

`NotesReplica.uninstalled` lists what `install!` would still create; assert it is empty in a test so the schema dump stays complete. Backfill existing rows before clients depend on them:

```ruby
ReplicaMan::Backfill.call(NotesReplica)
```

Operational jobs can reconcile capture and release old payloads in bounded batches:

```ruby
ReplicaMan::Reconcile.call(NotesReplica)
NotesReplica.gc(window: 30.days, limit: 500)
```

GC releases old tombstone payloads and document deltas while keeping entity lifetime fences. Schedule another pass when a returned count reaches the limit. Operation verdicts are kept permanently; a periodic job may delete old ones if their volume ever requires it.

Each encoded entity must fit the **32 MiB** limit. The server checks this before committing its domain transaction so an accepted document cannot later become impossible to bootstrap.

## Restore authoritative history

Stop every application server and background job before restoring. Restore domain and ReplicaMan tables together into a new, empty database. The packaged command refuses a nonempty target:

```sh
DATABASE_URL=postgresql:///restored_app bundle exec replicaman-restore \
  --backup app.dump \
  --epoch-file /etc/app/replicaman-epoch \
  --service-stopped
```

Configure every application node with `REPLICAMAN_DATASET_EPOCH_FILE=/etc/app/replicaman-epoch` and remove any explicit `dataset_epoch` declaration. The file must live outside database backups and expose the same value to every node.

The command fences sync before import and installs a fresh epoch only after a successful single-transaction restore. A failed import leaves sync unavailable. Resume service only after restore and epoch installation complete.

Existing clients stop with `DatasetChanged` and preserve their local data. Recover or export their branches explicitly; do not automatically replay old business actions into restored history. Deployments using `REPLICAMAN_DATASET_EPOCH` in a configuration service must rotate that value through their equivalent restore procedure.

## Next steps

- [Setup](./SETUP.md) — connect native clients.
- [Models](./MODELS.md) — expose fields, indexes, and references.
- [Command responses](./COMMITS.md) — connect ordinary application commands to sync.
- [Testing](../e2e/README.md) — verify with disposable PostgreSQL and real clients.
