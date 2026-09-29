module ReplicaMan
  module DatabaseCursor
    # One query plan and bounded batches. Transaction rollback closes the cursor
    # if fetching or the caller fails; the original exception must propagate.
    def self.each(scope, batch_size:, order: nil)
      connection = scope.model.connection
      name = "replica_cursor_#{SecureRandom.hex(8)}"
      order ||= Array(scope.model.primary_key).map { scope.model.arel_table[it] }

      connection.execute('SET LOCAL cursor_tuple_fraction = 1.0')
      connection.execute("DECLARE #{name} NO SCROLL CURSOR FOR #{scope.reorder(*order).to_sql}")

      loop do
        rows = connection.exec_query("FETCH FORWARD #{batch_size} FROM #{name}")
        break if rows.empty?

        rows.each { yield scope.model.instantiate(it) }
      end

      connection.execute("CLOSE #{name}")
    end
  end
end
