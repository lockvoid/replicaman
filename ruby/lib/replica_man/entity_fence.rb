module ReplicaMan
  # Rails and SQL updates lock the domain row before their capture callback.
  # Follow that order everywhere, then fence creation at absent addresses.
  module EntityFence
    def self.lock(stream, row_id)
      connection = stream.model.connection
      raise 'entity fence requires a transaction' unless connection.transaction_open?

      rows = stream.model.unscoped.where(stream.key => row_id.to_s)
      existing = rows.lock.pick(stream.model.primary_key)

      connection.exec_query(
        'SELECT replica_man_lock_entity($1::text, $2::text, $3::text)',
        'ReplicaMan entity fence',
        [stream.replica.namespace, stream.stream_name, row_id.to_s]
      )

      # An insert may have committed while we waited for the absent-address
      # fence. Do not wait for its row while holding the fence: an ordinary
      # update could already hold that row and be waiting for our fence.
      # NOWAIT raises a retryable database error; the entire transaction rolls
      # back, including its result, so the client's frozen submission can retry.
      rows.lock('FOR UPDATE NOWAIT').pick(stream.model.primary_key) unless existing
    end
  end
end
