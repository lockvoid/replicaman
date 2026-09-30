# Runs the statements `bin/rails g replica_man:migration` would write, the way the written migration runs them.
module StreamMigration
  def stream_changes
    ReplicaMan::Schema::Plan.new([DummyReplica]).changes.map(&:to_ruby)
  end

  def migrate_streams!
    lines = stream_changes
    migration = Class.new(ActiveRecord::Migration[ActiveRecord::Migration.current_version]) do
      define_method(:change) { lines.each { instance_eval(it) } }
    end
    migration.new.migrate(:up)
  end

  def rolled_back
    ActiveRecord::Base.transaction do
      yield ActiveRecord::Base.connection
      raise ActiveRecord::Rollback
    end
  end
end

ActiveSupport::TestCase.include(StreamMigration)
