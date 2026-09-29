require 'test_helper'

class PartitionTest < ActiveSupport::TestCase
  test 'one partition per declared stream plus a default, on both system tables' do
    %w[replica_man_snapshots replica_man_deltas].each do |table|
      partitions = ActiveRecord::Base.connection.select_values(<<~SQL)
        SELECT c.relname FROM pg_class c JOIN pg_inherits i ON c.oid = i.inhrelid
        WHERE i.inhparent = '#{table}'::regclass
      SQL

      expected = DummyReplica.streams.keys.map { "#{table}_#{it}" } + ["#{table}_default"]
      assert_equal expected.sort, partitions.sort, "#{table} partition set"
    end
  end

  test 'a declared stream lands in its partition; an undeclared stream lands in the default' do
    user = User.create!(id: 'u1', name: 'D')
    Job.create!(id: 'j1', user: user, state: 'queued')

    assert_equal 'replica_man_snapshots_jobs', partition_of('jobs', 'j1')

    ActiveRecord::Base.connection.execute(<<~SQL)
      INSERT INTO replica_man_snapshots (namespace, stream, row_id, bucket, position)
      VALUES ('replicaman-test', 'ghosts', 'g1', 'user:u1', 1)
    SQL
    assert_equal 'replica_man_snapshots_default', partition_of('ghosts', 'g1'),
                 'an undeclared stream is caught, never refused — declaration lag must not eat writes'
  end

  test 'uninstalled names the partitions and triggers install! would create' do
    connection = ActiveRecord::Base.connection
    trigger = ReplicaMan::CaptureHooks.trigger_name(DummyReplica, DummyReplica.streams.fetch(:jobs))
    assert_empty DummyReplica.uninstalled

    connection.execute("DROP TRIGGER #{connection.quote_column_name(trigger)} ON jobs")
    connection.execute('DROP TABLE replica_man_deltas_jobs')

    assert_equal ['replica_man_deltas_jobs', "#{trigger} on jobs"], DummyReplica.uninstalled
  ensure
    DummyReplica.install!
  end

  test 'install! is idempotent' do
    user = User.create!(id: 'u1', name: 'D')
    Job.create!(id: 'j1', user: user, state: 'queued')
    position = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').position

    2.times { DummyReplica.install! }

    assert_empty DummyReplica.uninstalled
    assert_equal position, ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').position
    assert_equal 1, capture_triggers_on('jobs')
  end

  def capture_triggers_on(table)
    ActiveRecord::Base.connection.select_value(<<~SQL)
      SELECT count(*) FROM pg_trigger WHERE tgrelid = '#{table}'::regclass AND tgname LIKE 'replica_capture_%'
    SQL
  end

  test 'carving a stream whose rows landed in the default partition reclaims them, never crashes' do
    connection = ActiveRecord::Base.connection
    %w[replica_man_snapshots replica_man_deltas].each { connection.execute("DROP TABLE #{it}_jobs") }
    connection.execute(<<~SQL)
      INSERT INTO replica_man_snapshots (namespace, stream, row_id, bucket, position)
      VALUES ('replicaman-test', 'jobs', 'lagged', 'user:u1', 7);
      INSERT INTO replica_man_deltas (namespace, stream, row_id, seq, payload)
      VALUES ('replicaman-test', 'jobs', 'lagged', 1, '\\x00');
    SQL

    assert_nothing_raised { DummyReplica.install! }

    assert_equal 'replica_man_snapshots_jobs', partition_of('jobs', 'lagged')
    assert_equal 7, connection.select_value("SELECT position FROM replica_man_snapshots WHERE row_id = 'lagged'"),
                 'the move must not re-stamp — no cursor may notice'
    assert_equal 'replica_man_deltas_jobs', connection.select_value(<<~SQL)
      SELECT tableoid::regclass::text FROM replica_man_deltas WHERE row_id = 'lagged'
    SQL
  ensure
    %w[replica_man_snapshots replica_man_deltas].each do |table|
      connection.execute("DELETE FROM #{table}_default WHERE stream = 'jobs'")
    end
    DummyReplica.install!
  end

  test 'partition setup propagates an unavailable configured database' do
    ReplicaMan::Snapshot.define_singleton_method(:table_exists?) do
      raise ActiveRecord::ConnectionNotEstablished, 'connection to server at "127.0.0.1", port 5432 failed'
    end

    assert_raises(ActiveRecord::ConnectionNotEstablished) { DummyReplica.install! }
  ensure
    ReplicaMan::Snapshot.singleton_class.send(:remove_method, :table_exists?)
  end

  private

  def partition_of(stream, row_id)
    ActiveRecord::Base.connection.select_value(<<~SQL)
      SELECT tableoid::regclass::text FROM replica_man_snapshots
      WHERE stream = #{ActiveRecord::Base.connection.quote(stream)} AND row_id = #{ActiveRecord::Base.connection.quote(row_id)}
    SQL
  end
end
