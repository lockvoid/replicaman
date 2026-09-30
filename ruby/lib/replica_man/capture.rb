module ReplicaMan
  class Capture
    UNCLAIMED = -1

    class << self
      def install(stream)
        registry[[stream.replica.namespace, stream.stream_name]] = stream
        return unless installed.add?([stream.replica.namespace, stream.model, stream.stream_name])

        stream.model.before_save { EntityFence.lock(stream, stream.row_key(self)) }
        stream.model.before_destroy { EntityFence.lock(stream, stream.row_key(self)) }
        stream.model.after_save { Capture.record(stream, self) }
        stream.model.after_touch { Capture.record(stream, self) }
        stream.model.after_destroy { Capture.record(stream, self, deleted: true) }
        install_flush(stream.model)
        stream.projection_dependencies.each { install_flush(it.parent) }
        stream.model.after_rollback { Capture.discard(stream, self) }
      end

      def install_flush(model)
        @flush_models ||= Set.new
        model.before_commit { Capture.flush } if @flush_models.add?(model)
      end

      def record(stream, record, deleted: false)
        row_id = stream.row_key(record)
        buffer[[stream.replica.namespace, stream.stream_name, row_id]] = {
          replica: stream.replica,
          stream: stream.stream_name,
          stream_ref: stream,
          shard: stream.shard,
          row_id: row_id,
          model_id: record.id.to_s,
          type: stream.sti ? stream.wire_type(record.class) : nil,
          data: stream.serialize(record),
          bucket: deleted ? nil : stream.bucket_for(record),
          document: stream.document?,
          deleted: deleted,
          origin: op_origin(stream.stream_name, row_id),
          incarnation: op_incarnation(stream.stream_name, row_id),
          user: op_user(stream.stream_name, row_id),
        }
        if (commit = Commit.current)
          ActiveRecord.after_all_transactions_commit { commit.record(stream.stream_name, row_id) }
        end
      end

      def record_deletion(stream, row_id)
        buffer[[stream.replica.namespace, stream.stream_name, row_id]] = deleted_entry(stream, row_id)
      end

      def with_op_origin(origin, stream:, row_id:, incarnation: nil, user: nil)
        ActiveSupport::IsolatedExecutionState[:replica_man_capture_op] =
          { origin: origin, stream: stream.to_s, row_id: row_id.to_s, incarnation: incarnation, user: user }
        yield
      ensure
        ActiveSupport::IsolatedExecutionState[:replica_man_capture_op] = nil
      end

      # Explicit replica transactions coalesce notifications. Each fragment is
      # removed on savepoint rollback before the outer transaction can deliver it.
      def batch
        return yield if notification_batch

        entries = []
        ActiveSupport::IsolatedExecutionState[:replica_man_notifications] = entries
        begin
          result = yield
          ring_after_commit(entries.flatten(1))
          result
        ensure
          ActiveSupport::IsolatedExecutionState[:replica_man_notifications] = nil
        end
      end

      def flush
        collect_changes
        entries = buffer.values
        buffer.clear

        # One canonical order for every writer: these upserts row-lock snapshots, and two transactions
        # locking the same rows in opposite orders deadlock.
        entries = References.capture_order(entries)

        written = entries.select do |entry|
          refresh(entry) unless entry.fetch(:deleted)
          prepare_birth(entry) if entry.fetch(:birth, false)
          changed = upsert(entry)
          Frames.validate_snapshot!(entry.fetch(:replica), entry.fetch(:stream), entry.fetch(:row_id)) if changed
          CaptureHooks.clear(entry)
          changed
        end
        schedule_notifications(written)
      end

      def discard(stream, record)
        buffer.delete([stream.replica.namespace, stream.stream_name, stream.row_key(record)])
        # A savepoint can undo a second edit while an earlier outer edit is
        # still owed to capture. Read the surviving database state after the
        # rollback; the rolled-back model instance still carries stale fields.
        # Outer rollback must leave no capture waiting for a later transaction.
        return unless record.class.connection.transaction_open?

        fresh = stream.model.unscoped.find_by(id: record.id)
        self.record(stream, fresh) if fresh
      end

      private

      def registry
        @registry ||= {}
      end

      def collect_changes
        CaptureHooks.changes(Snapshot.connection).each do |change|
          stream = registry.fetch([change.fetch('namespace'), change.fetch('stream')])
          row_id = change.fetch('row_id')
          key = [stream.replica.namespace, stream.stream_name, row_id]

          unless buffer.key?(key)
            record = stream.locate(row_id)

            if record
              self.record(stream, record)
            else
              buffer[key] = deleted_entry(stream, row_id)
            end
          end

          buffer.fetch(key)[:birth] = change.fetch('inserted')
        end
      end

      def deleted_entry(stream, row_id)
        {
          replica: stream.replica, stream_ref: stream, stream: stream.stream_name,
          shard: stream.shard, row_id: row_id, model_id: row_id,
          type: nil, data: {}, bucket: nil, document: stream.document?,
          deleted: true, origin: nil, user: nil
        }
      end

      def prepare_birth(entry)
        return if entry.fetch(:deleted)

        scope = { namespace: entry.fetch(:replica).namespace, stream: entry.fetch(:stream), row_id: entry.fetch(:row_id) }

        if entry.fetch(:document)
          fold = Snapshot.where(scope).where(document_position: nil).where.not(document: nil).exists?
          raise 'Create documents through ReplicaMan.document so the fold and row commit together' unless fold

          Delta.where(scope).delete_all
        end

        entry[:incarnation] ||= References.captured_incarnation(entry.fetch(:stream_ref), entry.fetch(:row_id), entry.fetch(:data)) || SecureRandom.uuid
      end

      def installed
        @installed ||= Set.new
      end

      # Serialize a fresh read, not the saved instance: Rails writes only dirty columns, so a stale
      # instance would snapshot a state the row never held, and tails keep it until the next write.
      def refresh(entry)
        stream = entry.fetch(:stream_ref)
        EntityFence.lock(stream, entry.fetch(:row_id))
        fresh = stream.model.unscoped.find_by(id: entry.fetch(:model_id))
        if fresh.nil?
          entry[:deleted] = true
          entry[:bucket] = nil
          return
        end

        entry[:type] = stream.sti ? stream.wire_type(fresh.class) : nil
        entry[:data] = stream.serialize(fresh)
        entry[:bucket] = stream.bucket_for(fresh)
      end

      def op_origin(stream_name, row_id)
        op_field(stream_name, row_id, :origin)
      end

      def op_incarnation(stream_name, row_id)
        op_field(stream_name, row_id, :incarnation)
      end

      def op_user(stream_name, row_id)
        op_field(stream_name, row_id, :user)
      end

      def op_field(stream_name, row_id, field)
        op = ActiveSupport::IsolatedExecutionState[:replica_man_capture_op]
        op.fetch(field) if op && op.fetch(:stream) == stream_name && op.fetch(:row_id) == row_id.to_s
      end

      def buffer
        buffers = ActiveSupport::IsolatedExecutionState[:replica_man_capture] ||= {}
        buffers[Snapshot.connection.object_id] ||= {}
      end

      def notification_batch
        ActiveSupport::IsolatedExecutionState[:replica_man_notifications]
      end

      def schedule_notifications(entries)
        batch = notification_batch
        return ring_after_commit(entries) unless batch

        batch << entries
        Snapshot.current_transaction.after_rollback do
          batch.delete_if { it.equal?(entries) }
        end
      end

      def ring_after_commit(entries)
        entries = entries.reverse.uniq { [it.fetch(:replica), it.fetch(:stream), it.fetch(:row_id)] }.reverse
        rings = entries.group_by { [it.fetch(:replica), it.fetch(:shard)] }
        return if rings.empty?

        ActiveRecord.after_all_transactions_commit do
          rings.each do |(replica, shard), captured|
            replica.doorbell&.call(
              shard: shard,
              captures: captured.map { it.slice(:stream, :row_id, :data, :deleted, :origin, :bucket) }
            )
          end
        end
      end

      def upsert(entry)
        address = address(entry)
        previous = Snapshot.lock.find_by(address)
        settle_owner!(entry, previous)
        return false if entry.fetch(:deleted) && previous.nil?

        authorize!(entry)
        return false if unchanged?(entry, previous, address)

        write(entry, entry.fetch(:bucket) && UNCLAIMED)
        claim_at_commit if entry.fetch(:bucket)
        true
      end

      def claim_at_commit
        transaction = Snapshot.connection.current_transaction
        return if ActiveSupport::IsolatedExecutionState[:replica_man_claiming].equal?(transaction)

        ActiveSupport::IsolatedExecutionState[:replica_man_claiming] = transaction
        transaction.before_commit { claim_positions }
      end

      def claim_positions
        unclaimed = Snapshot.connection.select_rows(<<~SQL, 'ReplicaMan unclaimed')
          SELECT namespace, bucket, stream, row_id FROM replica_man_snapshots
          WHERE position = #{UNCLAIMED} ORDER BY namespace, bucket, revision
        SQL
        positions = Buckets.claim(unclaimed.map { it.first(2) })
        unclaimed.zip(positions).each { |(namespace, _, stream, row_id), position| stamp(namespace, stream, row_id, position) }
      end

      def stamp(namespace, stream, row_id, position)
        Snapshot.connection.exec_update(<<~SQL, 'ReplicaMan claim', [namespace, stream, row_id, position])
          UPDATE replica_man_snapshots SET position = $4,
            document_position = CASE WHEN document_position = #{UNCLAIMED} THEN $4 ELSE document_position END
          WHERE namespace = $1 AND stream = $2 AND row_id = $3
        SQL
        Delta.where(namespace: namespace, stream: stream, row_id: row_id, position: nil).update_all(position: position)
      end

      def address(entry)
        { namespace: entry.fetch(:replica).namespace, stream: entry.fetch(:stream), row_id: entry.fetch(:row_id) }
      end

      # A row whose owner reads nil is outside every replica: it leaves like a deletion.
      def settle_owner!(entry, previous)
        entry.merge!(deleted: true, data: {}) if entry.fetch(:bucket).nil? && !entry.fetch(:deleted)
        if entry.fetch(:deleted)
          entry[:bucket] = previous&.bucket
          entry[:data] = previous.data if entry.fetch(:data).empty? && previous&.data
        elsif previous&.bucket && previous.deleted_at.nil? && previous.bucket != entry.fetch(:bucket)
          raise Refused, "a row's owner cannot change: #{entry.fetch(:stream)}/#{entry.fetch(:row_id)}"
        end
      end

      def write(entry, position)
        Snapshot.connection.exec_query(<<~SQL, 'ReplicaMan::Capture', binds(entry, position))
          INSERT INTO replica_man_snapshots
            (namespace, stream, row_id, incarnation, row_type, data, deleted_at, bucket, position)
          VALUES ($1, $2, $3, COALESCE($4, gen_random_uuid()::text), $5, $6, $7, $8, $9)
          ON CONFLICT (namespace, stream, row_id) DO UPDATE
          SET incarnation = COALESCE($4, replica_man_snapshots.incarnation), row_type = EXCLUDED.row_type,
              data = EXCLUDED.data, deleted_at = EXCLUDED.deleted_at, bucket = EXCLUDED.bucket,
              position = EXCLUDED.position, document_position = #{document_position(entry)}, updated_at = now()
        SQL
      end

      def document_position(entry)
        column = 'replica_man_snapshots.document_position'
        entry.fetch(:document) ? "COALESCE(#{column}, EXCLUDED.position, 0)" : column
      end

      def unchanged?(entry, previous, address)
        return false if previous.nil? || previous.deleted_at || previous.position.nil? || entry.fetch(:deleted)
        return false unless previous.bucket == entry.fetch(:bucket) && previous.row_type == entry.fetch(:type)
        return false unless [nil, previous.incarnation].include?(entry.fetch(:incarnation, nil))
        return false if entry.fetch(:document) && (previous.document_position.nil? || Delta.where(address).where(position: nil).exists?)

        previous.data == JSON.parse(entry.fetch(:data).to_json)
      end

      def authorize!(entry)
        user = entry.fetch(:user, nil)
        return if user.nil? || entry.fetch(:bucket) == entry.fetch(:stream_ref).own_bucket(user)

        raise Refused, 'row is outside your replica'
      end

      def binds(entry, position)
        [
          entry.fetch(:replica).namespace,
          entry.fetch(:stream),
          entry.fetch(:row_id),
          entry.fetch(:incarnation, nil),
          entry.fetch(:type),
          entry.fetch(:data).to_json,
          entry.fetch(:deleted) ? Time.current : nil,
          entry.fetch(:bucket),
          position
        ]
      end
    end
  end
end
