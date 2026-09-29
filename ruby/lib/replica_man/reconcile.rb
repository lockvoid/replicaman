module ReplicaMan
  class Reconcile
    def self.call(replica)
      replica.streams.values.to_h { [it.stream_name.to_sym, new(it).call] }
    end

    def initialize(stream)
      @stream = stream
    end

    def call
      counts = { recaptured: 0, tombstoned: 0, missing_fold: 0 }

      @stream.model.find_in_batches do |records|
        known = snapshots.where(row_id: records.map { @stream.row_key(it).to_s }).index_by(&:row_id)

        records.each do |record|
          snapshot = known.fetch(@stream.row_key(record).to_s, nil)

          next if snapshot && snapshot.deleted_at.nil? && current?(snapshot, record)

          outcome = recapture(@stream.row_key(record))
          counts[outcome] = counts.fetch(outcome) + 1 if outcome
        end
      end

      snapshots.where(deleted_at: nil).find_in_batches do |batch|
        live = @stream.model.where(@stream.key => batch.map(&:row_id)).pluck(@stream.key).to_set(&:to_s)

        batch.each do |snapshot|
          next if live.include?(snapshot.row_id)

          counts[:tombstoned] = counts.fetch(:tombstoned) + 1 if tombstone(snapshot)
        end
      end

      report(counts)
      counts
    end

    private

    def snapshots
      Snapshot.where(namespace: @stream.replica.namespace, stream: @stream.stream_name)
    end

    def current?(snapshot, record)
      snapshot.data == JSON.parse(@stream.serialize(record).to_json) &&
        snapshot.row_type == (@stream.sti ? @stream.wire_type(record.class) : nil)
    end

    def recapture(row_id)
      ActiveRecord::Base.transaction do
        EntityFence.lock(@stream, row_id)
        fresh = @stream.model.unscoped.find_by(@stream.key => row_id)
        next unless fresh

        snapshot = snapshots.find_by(row_id: row_id)
        next :missing_fold if @stream.document? && (snapshot.nil? || snapshot.document.nil?)
        next if snapshot && snapshot.deleted_at.nil? && current?(snapshot, fresh)

        Capture.record(@stream, fresh)
        Capture.flush
        :recaptured
      end
    end

    def tombstone(snapshot)
      ActiveRecord::Base.transaction do
        EntityFence.lock(@stream, snapshot.row_id)
        next if @stream.model.unscoped.exists?(@stream.key => snapshot.row_id)

        next false if snapshots.where(row_id: snapshot.row_id).pick(:deleted_at)

        Capture.record_deletion(@stream, snapshot.row_id)
        Capture.flush
        true
      end
    end

    def report(counts)
      Rails.logger.info(
        "[replica_man] reconcile stream=#{@stream.stream_name} recaptured=#{counts.fetch(:recaptured)} " \
        "tombstoned=#{counts.fetch(:tombstoned)} missing_fold=#{counts.fetch(:missing_fold)}"
      )
    end
  end
end
