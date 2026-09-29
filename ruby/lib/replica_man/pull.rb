require 'base64'

module ReplicaMan
  # Reads the principal's buckets for one shard from the client's cursor. Each
  # page is one database snapshot; `more` tells the client to continue before
  # it publishes, so the published state is a consistent point of the shard.
  class Pull
    def self.call(replica, user:, body:, &admit)
      new(replica, user, body).call(&admit)
    end

    def initialize(replica, user, body)
      @replica = replica
      @user = user
      @body = body
    end

    def call(&admit)
      validate_request!
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      result = ActiveRecord::Base.uncached do
        ActiveRecord::Base.transaction(isolation: :repeatable_read) do
          @user = admit.call if admit
          read
        end
      end

      elapsed = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1000).round
      Rails.logger.info("[replica_man] pull shard=#{@shard} frames=#{result.fetch(:frames).size} ms=#{elapsed} bootstrap=#{result.fetch(:reset)}")
      result
    end

    private

    def validate_request!
      Protocol.validate!(@replica, @body, initial: true)
      RequestBody.require_fields!(@body, %w[shard])

      @shard = Protocol.identifier!(@body.fetch('shard'), 'shard')
      @limit = Integer(@body.fetch('limit', 500), exception: false)

      unless @limit && (1..1000).cover?(@limit)
        raise InvalidRequest, 'limit must be an integer from 1 to 1000'
      end
    end

    def read
      buckets = @replica.buckets(@user, @shard)
      cursor = Cursor.decode(@body.fetch('cursor', nil), buckets)
      heads = @heads = Buckets.heads(@replica.namespace, buckets)
      raise Protocol::Error.new('CursorInvalid') if buckets.any? { cursor.fetch(it) > heads.fetch(it) }

      @frames = []
      @entities = 0
      @bytes = 0
      reached = cursor.dup
      buckets.each do |bucket|
        break if full?

        reached[bucket] = read_bucket(bucket, cursor.fetch(bucket))
      end

      Protocol.header(@replica).merge(
        shard: @shard,
        reset: @body.fetch('cursor', nil).nil?,
        frames: @frames,
        cursor: Cursor.encode(reached),
        more: buckets.any? { reached.fetch(it) < heads.fetch(it) }
      )
    end

    def full?
      @entities >= @limit || @bytes >= Protocol::PAGE_BYTES
    end

    # Returns the last position emitted. A bootstrap reads live rows only, so
    # once they run out the rest of the bucket up to its head is tombstones.
    def read_bucket(bucket, since)
      reached = since

      loop do
        entries = changes(bucket, reached, @limit - @entities, live: since.zero?)
        return since.zero? ? @heads.fetch(bucket) : reached if entries.empty?

        entries.each do |entry|
          emit(entry, since)
          reached = entry.position
          return reached if full?
        end
      end
    end

    def changes(bucket, after, limit, live:)
      scope = Snapshot.where(namespace: @replica.namespace, bucket: bucket).where('position > ?', after)
      scope = scope.where(deleted_at: nil) if live
      scope.select(*(Snapshot.column_names - %w[document data]), small_data).order(:position).limit(limit).to_a
    end

    def small_data
      Arel.sql(<<~SQL.squish)
        CASE WHEN octet_length(replica_man_snapshots.data::text) <= #{Protocol::PAGE_BYTES / 2}
          THEN replica_man_snapshots.data END AS data
      SQL
    end

    def emit(entry, since)
      bootstrap = since.zero?

      @entities += 1
      if entry.deleted_at
        return append(frame: 'row.delete', stream: entry.stream, id: entry.row_id,
                      incarnation: entry.incarnation, revision: entry.revision.to_s)
      end

      entry.data = payload(entry, :data) if entry.data.nil?
      return append(row_set(entry)) unless @replica.streams.fetch(entry.stream.to_sym).document?

      baseline = bootstrap || entry.document_position > since
      return append(doc_snapshot(entry)) if baseline

      deltas(entry, since).each { append(it) }
      append(row_set(entry))
    end

    def row_set(entry)
      {
        frame: 'row.set', stream: entry.stream, id: entry.row_id, incarnation: entry.incarnation,
        revision: entry.revision.to_s, type: entry.row_type, data: entry.data
      }
    end

    def doc_snapshot(entry)
      {
        frame: 'doc.snapshot', stream: entry.stream, id: entry.row_id, incarnation: entry.incarnation,
        revision: entry.revision.to_s, codec: entry.codec, data: entry.data,
        snapshot: Base64.strict_encode64(payload(entry, :document))
      }
    end

    def deltas(entry, since)
      Delta.where(namespace: entry.namespace, stream: entry.stream, row_id: entry.row_id)
        .where('position > ?', since).order(:seq).map do |delta|
        {
          frame: 'doc.delta', stream: entry.stream, id: entry.row_id, incarnation: entry.incarnation,
          seq: delta.seq, codec: entry.codec, payload: Base64.strict_encode64(delta.payload)
        }
      end
    end

    def payload(entry, column)
      Snapshot.where(namespace: entry.namespace, stream: entry.stream, row_id: entry.row_id).pick(column) ||
        raise("Snapshot has no durable #{column}")
    end

    def append(frame)
      @frames << frame
      @bytes += JSON.generate(frame).bytesize
      true
    end
  end
end
