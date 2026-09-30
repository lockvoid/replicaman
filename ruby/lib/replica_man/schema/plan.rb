module ReplicaMan
  module Schema
    # The migration commands that make the database carry exactly the objects the declared streams need.
    class Plan
      class Unreadable < StandardError; end

      Change = Data.define(:command, :object) do
        def to_ruby
          "#{command} #{object.to_ruby}"
        end
      end

      def initialize(replicas, connection: Snapshot.connection)
        @replicas = replicas
        @connection = connection
      end

      def changes
        drops + creates
      end

      private

      def drops
        [installed_dependencies - dependencies, installed_captures - captures, present_partitions - partitions]
          .flat_map { |objects| objects.sort_by(&:to_ruby).map { change(:drop, it) } }
      end

      def creates
        [partitions - complete_partitions, captures - installed_captures, dependencies - installed_dependencies]
          .flat_map { |objects| objects.sort_by(&:to_ruby).map { change(:create, it) } }
      end

      def change(verb, object)
        Change.new(command: :"#{verb}_#{object.class.statement}", object: object)
      end

      def streams
        @replicas.flat_map { it.streams.values }
      end

      def namespaces
        @replicas.map(&:namespace)
      end

      def partitions
        streams.map { Partitions.new(stream: it.stream_name) }.uniq
      end

      def captures
        streams.map do
          CaptureTrigger.new(namespace: it.replica.namespace, stream: it.stream_name, table: it.model.table_name, key: it.key)
        end
      end

      def dependencies
        triggers = streams.flat_map { |stream| stream.projection_dependencies.map { dependency_trigger(stream, it) } }
        duplicate = triggers.group_by(&:name).values.find { it.size > 1 }
        raise Stream::Invalid, "duplicate projection dependency on #{duplicate.first.stream}" if duplicate

        triggers
      end

      def dependency_trigger(stream, dependency)
        DependencyTrigger.new(
          namespace: stream.replica.namespace, stream: stream.stream_name, table: stream.model.table_name,
          key: stream.key, parent: dependency.parent.table_name, parent_key: dependency.parent.primary_key,
          via: dependency.via, fields: dependency.fields
        )
      end

      def present_partitions
        partitioned_streams.values.reduce(:|).map { Partitions.new(stream: it) }
      end

      def complete_partitions
        partitioned_streams.values.reduce(:&).map { Partitions.new(stream: it) }
      end

      def partitioned_streams
        @partitioned_streams ||= PARENTS.to_h do |parent|
          [parent, @connection.select_values(<<~SQL).to_set { it.delete_prefix("#{parent}_") }]
            SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = #{@connection.quote(parent)}::regclass AND pg_get_expr(c.relpartbound, c.oid) <> 'DEFAULT'
          SQL
        end
      end

      def installed_captures
        @installed_captures ||= @connection.select_rows(<<~SQL).filter_map { capture_trigger(*it) }
          SELECT t.tgrelid::regclass::text, t.tgnargs, encode(t.tgargs, 'escape')
          FROM pg_trigger t WHERE NOT t.tgisinternal AND starts_with(t.tgname, 'replica_capture_')
        SQL
      end

      def capture_trigger(table, count, arguments)
        namespace, stream, key = arguments.split('\\000').first(count.to_i)
        CaptureTrigger.new(namespace:, stream:, table:, key:) if namespaces.include?(namespace)
      end

      def installed_dependencies
        @installed_dependencies ||= @connection.select_rows(<<~SQL).filter_map { installed_dependency(*it) }
          SELECT t.tgname, obj_description(t.tgfoid, 'pg_proc')
          FROM pg_trigger t WHERE NOT t.tgisinternal AND starts_with(t.tgname, 'replica_dependency_')
        SQL
      end

      def installed_dependency(name, definition)
        return unless namespaces.any? { name.start_with?(DependencyTrigger.prefix(it)) }
        raise Unreadable, "#{name} keeps no definition; drop the trigger and its function by hand" if definition.nil?

        DependencyTrigger.new(**JSON.parse(definition, symbolize_names: true))
      end
    end
  end
end
