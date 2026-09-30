module ReplicaMan
  module Schema
    # Migration statements for the objects a declared stream needs; each create has a drop with the
    # same arguments, so a `change` migration reverses.
    module Statements
      def create_replica_partitions(stream)
        Partitions.new(stream: stream.to_s).create(self)
      end

      def drop_replica_partitions(stream)
        Partitions.new(stream: stream.to_s).drop(self)
      end

      def create_replica_capture(stream, **definition)
        replica_capture_trigger(stream, definition).create(self)
      end

      def drop_replica_capture(stream, **definition)
        replica_capture_trigger(stream, definition).drop(self)
      end

      def create_replica_dependency(stream, **definition)
        replica_dependency_trigger(stream, definition).create(self)
      end

      def drop_replica_dependency(stream, **definition)
        replica_dependency_trigger(stream, definition).drop(self)
      end

      private

      def replica_capture_trigger(stream, definition)
        CaptureTrigger.new(stream: stream.to_s, **definition.transform_values(&:to_s))
      end

      def replica_dependency_trigger(stream, definition)
        DependencyTrigger.new(
          stream: stream.to_s, **definition.except(:fields).transform_values(&:to_s),
          fields: definition.fetch(:fields).map(&:to_s)
        )
      end
    end
  end
end
