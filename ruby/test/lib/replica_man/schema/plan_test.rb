require 'test_helper'

class PlanTest < ActiveSupport::TestCase
  test 'a migrated database needs no change' do
    assert_empty stream_changes
  end

  test 'a missing partition and capture trigger come back as creates' do
    rolled_back do |connection|
      capture('jobs', key: 'id').drop(connection)
      connection.execute('DROP TABLE replica_man_deltas_jobs')

      assert_equal ['create_replica_partitions :jobs', "create_replica_capture #{capture('jobs', key: 'id').to_ruby}"], stream_changes
    end
  end

  test 'the objects of an undeclared stream come back as drops' do
    rolled_back do |connection|
      ReplicaMan::Schema::Partitions.new(stream: 'ghosts').create(connection)
      capture('ghosts', key: 'id').create(connection)

      assert_equal ["drop_replica_capture #{capture('ghosts', key: 'id').to_ruby}", 'drop_replica_partitions :ghosts'], stream_changes
    end
  end

  test 'a changed key replaces the capture trigger' do
    rolled_back do |connection|
      capture('jobs', key: 'id').drop(connection)
      capture('jobs', key: 'user_id').create(connection)

      assert_equal ["drop_replica_capture #{capture('jobs', key: 'user_id').to_ruby}",
                    "create_replica_capture #{capture('jobs', key: 'id').to_ruby}"], stream_changes
    end
  end

  test 'another namespace keeps its triggers' do
    rolled_back do |connection|
      ReplicaMan::Schema::CaptureTrigger.new(namespace: 'elsewhere', stream: 'ghosts', table: 'jobs', key: 'id').create(connection)

      assert_empty stream_changes
    end
  end

  test 'a dependency trigger that keeps no definition is refused' do
    rolled_back do |connection|
      name = "#{ReplicaMan::Schema::DependencyTrigger.prefix('replicaman-test')}0000000000000000"
      connection.execute("CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$")
      connection.execute("CREATE TRIGGER #{name} AFTER UPDATE ON users FOR EACH ROW EXECUTE FUNCTION #{name}()")

      assert_raises(ReplicaMan::Schema::Plan::Unreadable) { stream_changes }
    end
  end

  private

  def capture(stream, key:)
    ReplicaMan::Schema::CaptureTrigger.new(namespace: 'replicaman-test', stream: stream, table: 'jobs', key: key)
  end
end
