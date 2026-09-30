class CreateReplicaManTables < ActiveRecord::Migration[8.1]
  IDENTIFIER = "'^[a-zA-Z0-9_.:-]{1,128}$'".freeze

  def up
    execute 'CREATE SEQUENCE replica_man_revision_seq'
    create_enum :replica_man_outcome, %w[accepted rejected]
    create_snapshots
    create_deltas
    create_partition_defaults
    create_revision_trigger
    create_claim_guard
    create_buckets
    create_operations
    create_changes
    create_capture_functions
  end

  def down
    raise ActiveRecord::IrreversibleMigration
  end

  private

  def create_snapshots
    create_table :replica_man_snapshots, primary_key: [:namespace, :stream, :row_id],
                 options: 'PARTITION BY LIST (stream)' do |t|
      t.string :namespace, null: false
      t.string :stream, null: false
      t.string :row_id, null: false
      t.string :incarnation, null: false, default: -> { 'gen_random_uuid()::text' }
      t.string :row_type
      t.jsonb :data, null: false, default: {}
      t.string :codec
      t.binary :document
      t.string :bucket
      t.bigint :position
      t.bigint :document_position
      t.bigint :revision, null: false
      t.datetime :deleted_at
      t.timestamps default: -> { 'now()' }

      t.check_constraint "namespace ~ #{IDENTIFIER}", name: 'replica_man_snapshot_namespace'
      t.check_constraint "stream ~ #{IDENTIFIER}", name: 'replica_man_snapshot_stream'
      t.check_constraint "incarnation ~ #{IDENTIFIER}", name: 'replica_man_snapshot_incarnation'
      t.check_constraint 'octet_length(row_id) BETWEEN 1 AND 1024', name: 'replica_man_snapshot_row_id'
      t.check_constraint '(bucket IS NULL) = (position IS NULL)', name: 'replica_man_snapshot_bucket_position'
      t.check_constraint 'position = -1 OR position > 0', name: 'replica_man_snapshot_position'
      t.check_constraint 'document IS NULL OR codec IS NOT NULL', name: 'replica_man_snapshot_document_codec'
      t.check_constraint 'document_position IS NULL OR document IS NOT NULL OR deleted_at IS NOT NULL',
                         name: 'replica_man_snapshot_document_position'
      t.check_constraint 'deleted_at IS NOT NULL OR position IS NOT NULL OR (document IS NOT NULL AND document_position IS NULL)',
                         name: 'replica_man_snapshot_owned'

      t.index [:namespace, :bucket, :position], name: 'replica_man_snapshot_positions'
      t.index [:namespace, :bucket, :position], name: 'replica_man_snapshot_live_positions', where: 'deleted_at IS NULL'
      t.index [:namespace, :bucket, :revision], name: 'replica_man_snapshot_unclaimed', where: 'position = -1'
      t.index [:namespace, :stream, :row_id, :deleted_at], name: 'replica_man_snapshot_garbage',
                                                         where: "deleted_at IS NOT NULL AND (data <> '{}'::jsonb OR document IS NOT NULL)"
    end
  end

  def create_deltas
    create_table :replica_man_deltas, primary_key: [:stream, :id],
                 options: 'PARTITION BY LIST (stream)' do |t|
      t.string :namespace, null: false
      t.string :stream, null: false
      t.bigserial :id, null: false
      t.string :row_id, null: false
      t.bigint :seq, null: false
      t.binary :payload, null: false
      t.bigint :position
      t.datetime :created_at, null: false, default: -> { 'now()' }

      t.check_constraint 'seq > 0', name: 'replica_man_delta_seq'
      t.check_constraint 'octet_length(payload) > 0', name: 'replica_man_delta_payload'

      t.index [:namespace, :stream, :row_id, :seq], unique: true, name: 'replica_man_delta_address'
      t.index [:namespace, :stream, :row_id, :position], name: 'replica_man_delta_positions'
    end
  end

  def create_partition_defaults
    execute 'CREATE TABLE replica_man_snapshots_default PARTITION OF replica_man_snapshots DEFAULT'
    execute 'CREATE TABLE replica_man_deltas_default PARTITION OF replica_man_deltas DEFAULT'
  end

  def create_revision_trigger
    execute <<~SQL
      CREATE FUNCTION replica_man_stamp_revision() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        NEW.revision := nextval('replica_man_revision_seq');
        RETURN NEW;
      END;
      $$;
      CREATE TRIGGER replica_man_revision BEFORE INSERT OR UPDATE ON replica_man_snapshots
      FOR EACH ROW EXECUTE FUNCTION replica_man_stamp_revision();
    SQL
  end

  def create_claim_guard
    execute <<~SQL
      CREATE FUNCTION replica_man_refuse_unclaimed() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF EXISTS (SELECT 1 FROM replica_man_snapshots
                   WHERE namespace = NEW.namespace AND stream = NEW.stream AND row_id = NEW.row_id AND position = -1) THEN
          RAISE EXCEPTION 'replica_man: %/% committed without a claimed position', NEW.stream, NEW.row_id;
        END IF;
        RETURN NULL;
      END;
      $$;
      CREATE CONSTRAINT TRIGGER replica_man_position_claimed AFTER INSERT OR UPDATE OF position ON replica_man_snapshots
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.position = -1) EXECUTE FUNCTION replica_man_refuse_unclaimed();
    SQL
  end

  def create_buckets
    create_table :replica_man_buckets, primary_key: [:namespace, :bucket] do |t|
      t.string :namespace, null: false
      t.string :bucket, null: false
      t.bigint :head, null: false, default: 0

      t.check_constraint 'head >= 0', name: 'replica_man_bucket_head'
    end
  end

  def create_operations
    create_table :replica_man_operations, id: false do |t|
      t.uuid :op_id, null: false, primary_key: true
      t.string :namespace, null: false
      t.string :author, null: false
      t.binary :body_sha256, null: false
      t.enum :outcome, enum_type: :replica_man_outcome, null: false
      t.text :reason
      t.datetime :created_at, null: false, default: -> { 'now()' }

      t.check_constraint 'octet_length(body_sha256) = 32', name: 'replica_man_operation_body_sha256'
      t.check_constraint "(outcome = 'rejected') = (reason IS NOT NULL)", name: 'replica_man_operation_reason'

      t.index :created_at, name: 'replica_man_operation_age'
    end
  end

  def create_changes
    create_table :replica_man_changes,
                 primary_key: [:namespace, :stream, :row_id, :transaction_id] do |t|
      t.string :namespace, null: false
      t.string :stream, null: false
      t.string :row_id, null: false
      t.column :transaction_id, 'xid8', null: false
      t.boolean :inserted, null: false, default: false
    end
  end

  def create_capture_functions
    execute <<~SQL
      CREATE FUNCTION replica_man_lock_entity(namespace text, stream text, row_id text)
      RETURNS void LANGUAGE sql AS $$
        SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(namespace, stream, row_id)::text, 137));
      $$;

      CREATE FUNCTION replica_man_record_change() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE
        address text;
      BEGIN
        IF TG_OP = 'DELETE' THEN
          address := to_jsonb(OLD) ->> TG_ARGV[2];
        ELSE
          address := to_jsonb(NEW) ->> TG_ARGV[2];
        END IF;

        IF TG_OP = 'UPDATE' AND (to_jsonb(OLD) ->> TG_ARGV[2]) IS DISTINCT FROM address THEN
          RAISE EXCEPTION 'ReplicaMan entity addresses are immutable';
        END IF;

        PERFORM replica_man_lock_entity(TG_ARGV[0], TG_ARGV[1], address);

        INSERT INTO replica_man_changes (namespace, stream, row_id, transaction_id, inserted)
        VALUES (TG_ARGV[0], TG_ARGV[1], address, pg_current_xact_id(), TG_OP = 'INSERT')
        ON CONFLICT (namespace, stream, row_id, transaction_id) DO UPDATE
        SET inserted = replica_man_changes.inserted OR EXCLUDED.inserted;

        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
      END;
      $$;

      CREATE FUNCTION replica_man_require_capture() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM replica_man_changes
          WHERE namespace = NEW.namespace AND stream = NEW.stream
          AND row_id = NEW.row_id AND transaction_id = NEW.transaction_id
        ) THEN
          RAISE EXCEPTION 'ReplicaMan uncaptured change: %/%; use the replica transaction API', NEW.stream, NEW.row_id
            USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
      END;
      $$;

      CREATE CONSTRAINT TRIGGER replica_man_capture_required
      AFTER INSERT OR UPDATE ON replica_man_changes
      DEFERRABLE INITIALLY DEFERRED
      FOR EACH ROW EXECUTE FUNCTION replica_man_require_capture();
    SQL
  end
end
