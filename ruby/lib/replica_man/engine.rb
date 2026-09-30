module ReplicaMan
  class Engine < ::Rails::Engine
    isolate_namespace ReplicaMan

    initializer 'replica_man.schema_statements' do
      ActiveSupport.on_load(:active_record_postgresqladapter) { include ReplicaMan::Schema::Statements }
      ActiveSupport.on_load(:active_record) { ActiveRecord::Migration::CommandRecorder.include(ReplicaMan::Schema::CommandRecorder) }
    end
  end
end
