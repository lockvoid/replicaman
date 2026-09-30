module ReplicaMan
  module Schema
    # Records every write to a stream's table, so a transaction that bypasses capture cannot commit.
    CaptureTrigger = Data.define(:namespace, :stream, :table, :key) do
      def self.statement
        :replica_capture
      end

      def name
        "replica_capture_#{Digest::SHA256.hexdigest([namespace, stream].join(':'))[0, 16]}"
      end

      def create(connection)
        arguments = [namespace, stream, key].map { connection.quote(it) }.join(', ')
        connection.execute(<<~SQL)
          CREATE TRIGGER #{connection.quote_column_name(name)}
          BEFORE INSERT OR UPDATE OR DELETE ON #{connection.quote_table_name(table)}
          FOR EACH ROW EXECUTE FUNCTION replica_man_record_change(#{arguments})
        SQL
      end

      def drop(connection)
        connection.execute("DROP TRIGGER #{connection.quote_column_name(name)} ON #{connection.quote_table_name(table)}")
      end

      def to_ruby
        "#{stream.to_sym.inspect}, namespace: '#{namespace}', table: #{table.to_sym.inspect}, key: #{key.to_sym.inspect}"
      end
    end
  end
end
