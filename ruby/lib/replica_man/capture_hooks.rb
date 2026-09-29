module ReplicaMan
  module CaptureHooks
    def self.install(replica)
      connection = Snapshot.connection
      return unless connection.data_source_exists?('replica_man_changes')

      replica.streams.each_value do |stream|
        table = connection.quote_table_name(stream.model.table_name)
        trigger = connection.quote_column_name(trigger_name(replica, stream))
        arguments = [replica.namespace, stream.stream_name, stream.key].map { connection.quote(it) }.join(', ')

        connection.execute(<<~SQL)
          CREATE OR REPLACE TRIGGER #{trigger}
          BEFORE INSERT OR UPDATE OR DELETE ON #{table}
          FOR EACH ROW EXECUTE FUNCTION replica_man_record_change(#{arguments})
        SQL
      end
      ProjectionDependencies.install(replica, connection)
    end

    def self.trigger_name(replica, stream)
      "replica_capture_#{Digest::SHA256.hexdigest([replica.namespace, stream.stream_name].join(':'))[0, 16]}"
    end

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
