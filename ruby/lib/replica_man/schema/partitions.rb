module ReplicaMan
  module Schema
    # A stream's partition of the snapshot table and of the delta table.
    Partitions = Data.define(:stream) do
      def self.statement
        :replica_partitions
      end

      def create(connection)
        PARENTS.each { carve(connection, it) }
      end

      def drop(connection)
        PARENTS.each { connection.execute("DROP TABLE IF EXISTS #{partition(it)}") }
      end

      def to_ruby
        stream.to_sym.inspect
      end

      private

      def partition(parent)
        "#{parent}_#{stream}"
      end

      def carve(connection, parent)
        return if connection.select_value("SELECT to_regclass(#{connection.quote(partition(parent))})::text")

        connection.transaction { move_into_partition(connection, parent) }
      end

      # Rows captured before the partition existed sit in the default partition; they move unchanged.
      def move_into_partition(connection, parent)
        value = connection.quote(stream)
        lagged = connection.select_value("SELECT EXISTS (SELECT FROM #{parent}_default WHERE stream = #{value})")
        if lagged
          connection.execute(<<~SQL)
            CREATE TEMP TABLE #{partition(parent)}_carve ON COMMIT DROP AS
            SELECT * FROM #{parent}_default WHERE stream = #{value}
          SQL
          connection.execute("DELETE FROM #{parent}_default WHERE stream = #{value}")
        end
        connection.execute("CREATE TABLE #{partition(parent)} PARTITION OF #{parent} FOR VALUES IN (#{value})")
        connection.execute("INSERT INTO #{parent} SELECT * FROM #{partition(parent)}_carve") if lagged
      end
    end
  end
end
