module ReplicaMan
  module CaptureHooks
    def self.changes(connection)
      connection.select_all(<<~SQL)
        SELECT namespace, stream, row_id, inserted
        FROM replica_man_changes
        WHERE transaction_id = pg_current_xact_id()
        ORDER BY namespace, stream, row_id
      SQL
    end

    def self.clear(entry)
      Snapshot.connection.exec_query(<<~SQL, 'ReplicaMan capture completed', [entry.fetch(:replica).namespace, entry.fetch(:stream), entry.fetch(:row_id)])
        DELETE FROM replica_man_changes
        WHERE namespace = $1 AND stream = $2 AND row_id = $3
        AND transaction_id = pg_current_xact_id()
      SQL
    end
  end
end
