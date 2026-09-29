module ReplicaMan
  class Backfill
    # The progress callback runs only after a batch has committed. Persist its
    # cursor to resume an interrupted scan; replaying a batch is harmless.
    def self.call(replica, batch_size: 100, after: {}, &progress)
      unless batch_size.is_a?(Integer) && (1..1000).cover?(batch_size)
        raise ArgumentError, 'backfill batch_size must be between 1 and 1000'
      end

      cursor = after.stringify_keys
      replica.streams.values.reject(&:document?).to_h do |stream|
        count = 0
        scope = stream.model.unscoped
        previous = cursor.fetch(stream.stream_name, nil)
        scope = scope.where(stream.model.arel_table[stream.model.primary_key].gt(previous)) if previous

        scope.find_in_batches(batch_size: batch_size) do |records|
          records.each do |candidate|
            replica.transaction do
              EntityFence.lock(stream, stream.row_key(candidate))
              fresh = stream.model.unscoped.find_by(id: candidate.id)
              next unless fresh

              Capture.record(stream, fresh)
              count += 1
            end
          end

          cursor[stream.stream_name] = records.last.id
          progress&.call(cursor.dup)
        end

        [stream.stream_name.to_sym, count]
      end
    end
  end
end
