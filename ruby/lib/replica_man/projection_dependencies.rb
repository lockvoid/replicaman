module ReplicaMan
  # A projection may read another model. Queue affected children for capture in the
  # parent's transaction, including bulk SQL writes that bypass Rails callbacks.
  module ProjectionDependencies
    class Dependency
      attr_reader :via, :fields

      def initialize(parent, via:, fields:)
        @parent = parent
        @via = via.to_s
        @fields = Array(fields).map(&:to_s).uniq.freeze
        raise Stream::Invalid, 'a projection dependency needs changed fields' if @fields.empty?
      end

      def parent
        @parent.is_a?(String) ? @parent.constantize : @parent
      end

      def validate!(stream)
        unless parent.is_a?(Class) && parent < ActiveRecord::Base && parent.primary_key.is_a?(String)
          raise Stream::Invalid, 'a projection dependency needs an ActiveRecord model with one primary key'
        end

        missing = fields - parent.column_names
        raise Stream::Invalid, "unknown dependency fields: #{missing.join(', ')}" unless missing.empty?
        unless stream.model.column_names.include?(via)
          raise Stream::Invalid, "unknown dependency foreign key: #{stream.model}.#{via}"
        end
      end
    end

    module_function

    def install(replica, connection)
      prefix = 'replica_dependency_' + Digest::SHA256.hexdigest(replica.namespace).first(12) + '_'
      expected = Set.new

      replica.streams.each_value do |stream|
        stream.projection_dependencies.each do |dependency|
          dependency.validate!(stream)
          suffix = Digest::SHA256.hexdigest([stream.stream_name, dependency.parent.table_name, dependency.via].join(':')).first(16)
          name = prefix + suffix
          raise Stream::Invalid, "duplicate projection dependency on #{stream.stream_name}" unless expected.add?(name)

          install_dependency(connection, stream, dependency, name)
        end
      end

      remove_obsolete(connection, prefix, expected)
    end

    def install_dependency(connection, stream, dependency, name)
      quote = ->(value) { connection.quote_column_name(value) }
      function = quote.call(name)
      parent = connection.quote_table_name(dependency.parent.table_name)
      child = connection.quote_table_name(stream.model.table_name)
      fields = dependency.fields.map(&quote)
      changed = fields.map { "OLD.#{it} IS DISTINCT FROM NEW.#{it}" }.join(' OR ')
      parent_key = quote.call(dependency.parent.primary_key)

      connection.execute(<<~SQL)
        CREATE OR REPLACE FUNCTION #{function}() RETURNS trigger LANGUAGE plpgsql AS $dependency$
        BEGIN
          INSERT INTO replica_man_changes (namespace, stream, row_id, transaction_id, inserted)
          SELECT #{connection.quote(stream.replica.namespace)}, #{connection.quote(stream.stream_name)},
                 child.#{quote.call(stream.key)}::text, pg_current_xact_id(), false
          FROM #{child} child WHERE child.#{quote.call(dependency.via)} = NEW.#{parent_key}
          ON CONFLICT (namespace, stream, row_id, transaction_id) DO NOTHING;
          RETURN NEW;
        END;
        $dependency$;

        CREATE OR REPLACE TRIGGER #{function}
        AFTER UPDATE OF #{fields.join(', ')} ON #{parent}
        FOR EACH ROW WHEN (#{changed}) EXECUTE FUNCTION #{function}();
      SQL
    end

    # Reconfiguration must not leave a removed declaration silently capturing
    # children forever. Names are scoped to this namespace, never another replica.
    def remove_obsolete(connection, prefix, expected)
      triggers = connection.select_all(<<~SQL)
        SELECT t.tgname, n.nspname, c.relname, p.proname, pn.nspname AS function_schema
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_proc p ON p.oid = t.tgfoid
        JOIN pg_namespace pn ON pn.oid = p.pronamespace
        WHERE NOT t.tgisinternal AND left(t.tgname, #{prefix.length}) = #{connection.quote(prefix)}
      SQL
      triggers.each do |trigger|
        next if expected.include?(trigger.fetch('tgname'))

        table = [trigger.fetch('nspname'), trigger.fetch('relname')].map { connection.quote_column_name(it) }.join('.')
        function = [trigger.fetch('function_schema'), trigger.fetch('proname')].map { connection.quote_column_name(it) }.join('.')
        connection.execute("DROP TRIGGER #{connection.quote_column_name(trigger.fetch('tgname'))} ON #{table}")
        connection.execute("DROP FUNCTION #{function}()")
      end
    end
  end
end
