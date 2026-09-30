module ReplicaMan
  module Schema
    # Recaptures a stream's rows when parent fields its projection reads change, bulk SQL included.
    # The function comment keeps the definition, so a plan can drop it after the declaration is gone.
    DependencyTrigger = Data.define(:namespace, :stream, :table, :key, :parent, :parent_key, :via, :fields) do
      def self.statement
        :replica_dependency
      end

      def self.prefix(namespace)
        "replica_dependency_#{Digest::SHA256.hexdigest(namespace).first(12)}_"
      end

      def name
        self.class.prefix(namespace) + Digest::SHA256.hexdigest([stream, parent, via].join(':')).first(16)
      end

      def create(connection)
        function = connection.quote_column_name(name)
        connection.execute(function_sql(connection, function))
        connection.execute(trigger_sql(connection, function))
        connection.execute("COMMENT ON FUNCTION #{function}() IS #{connection.quote(to_h.to_json)}")
      end

      def drop(connection)
        function = connection.quote_column_name(name)
        connection.execute("DROP TRIGGER #{function} ON #{connection.quote_table_name(parent)}")
        connection.execute("DROP FUNCTION #{function}()")
      end

      def to_ruby
        "#{stream.to_sym.inspect}, namespace: '#{namespace}', table: #{table.to_sym.inspect}, " \
          "key: #{key.to_sym.inspect}, parent: #{parent.to_sym.inspect}, parent_key: #{parent_key.to_sym.inspect}, " \
          "via: #{via.to_sym.inspect}, fields: %i[#{fields.join(' ')}]"
      end

      private

      def function_sql(connection, function)
        <<~SQL
          CREATE FUNCTION #{function}() RETURNS trigger LANGUAGE plpgsql AS $dependency$
          BEGIN
            INSERT INTO replica_man_changes (namespace, stream, row_id, transaction_id, inserted)
            SELECT #{connection.quote(namespace)}, #{connection.quote(stream)},
                   child.#{connection.quote_column_name(key)}::text, pg_current_xact_id(), false
            FROM #{connection.quote_table_name(table)} child
            WHERE child.#{connection.quote_column_name(via)} = NEW.#{connection.quote_column_name(parent_key)}
            ON CONFLICT (namespace, stream, row_id, transaction_id) DO NOTHING;
            RETURN NEW;
          END;
          $dependency$
        SQL
      end

      def trigger_sql(connection, function)
        columns = fields.map { connection.quote_column_name(it) }
        changed = columns.map { "OLD.#{it} IS DISTINCT FROM NEW.#{it}" }.join(' OR ')
        <<~SQL
          CREATE TRIGGER #{function}
          AFTER UPDATE OF #{columns.join(', ')} ON #{connection.quote_table_name(parent)}
          FOR EACH ROW WHEN (#{changed}) EXECUTE FUNCTION #{function}()
        SQL
      end
    end
  end
end
