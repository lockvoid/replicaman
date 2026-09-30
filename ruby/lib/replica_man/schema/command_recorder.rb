module ReplicaMan
  module Schema
    # Lets `change` migrations record the stream statements and reverse each into its counterpart.
    module CommandRecorder
      INVERSES = {
        create_replica_partitions: :drop_replica_partitions,
        create_replica_capture: :drop_replica_capture,
        create_replica_dependency: :drop_replica_dependency,
      }.then { it.merge(it.invert) }.freeze

      INVERSES.each do |command, inverse|
        define_method(command) do |*args, **kwargs, &block|
          args << Hash.ruby2_keywords_hash(kwargs) unless kwargs.empty?
          record(command, args, &block)
        end

        define_method(:"invert_#{command}") do |args|
          [inverse, args]
        end
      end
    end
  end
end
