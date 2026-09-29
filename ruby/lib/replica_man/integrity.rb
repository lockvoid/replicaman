require 'digest'

module ReplicaMan
  # Digest of a shard's live members for the principal, comparable with the
  # client's stored base. It answers only for the current head: a client whose
  # cursor is behind pulls first.
  class Integrity
    def self.call(replica, user:, body:, &admit)
      Protocol.validate!(replica, body)
      RequestBody.require_fields!(body, %w[shard cursor])
      shard = Protocol.identifier!(body.fetch('shard'), 'shard')

      ActiveRecord::Base.transaction(isolation: :repeatable_read) do
        user = admit.call if admit
        buckets = replica.buckets(user, shard)
        cursor = Cursor.decode(body.fetch('cursor'), buckets)
        raise Protocol::Error.new('CursorBehind') unless cursor == Buckets.heads(replica.namespace, buckets)

        Protocol.header(replica).merge(shard: shard, cursor: body.fetch('cursor'), **summarize(replica, buckets))
      end
    end

    # C collation agrees with SQLite's UTF-8 binary order.
    def self.summarize(replica, buckets)
      scope = Snapshot.where(namespace: replica.namespace, bucket: buckets, deleted_at: nil)
      order = [Arel.sql('stream COLLATE "C"'), Arel.sql('row_id COLLATE "C"')]

      digest(Enumerator.new do |entries|
        DatabaseCursor.each(scope.select(:stream, :row_id, :incarnation, :revision), batch_size: 1000, order: order) do |entry|
          entries << [entry.stream, entry.row_id, entry.incarnation, entry.revision.to_s]
        end
      end)
    end

    def self.digest(entries)
      digest = Digest::SHA256.new
      digest << "replicaman-view\0"
      count = 0

      entries.each do |fields|
        fields.each do |value|
          bytes = value.encode(Encoding::UTF_8)
          digest << [bytes.bytesize].pack('Q>') << bytes
        end
        count += 1
      end

      { digest: digest.hexdigest, count: count.to_s }
    end
  end
end
