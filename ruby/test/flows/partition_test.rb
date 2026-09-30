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

  private

  def partition_of(stream, row_id)
    ActiveRecord::Base.connection.select_value(<<~SQL)
      SELECT tableoid::regclass::text FROM replica_man_snapshots
      WHERE stream = #{ActiveRecord::Base.connection.quote(stream)} AND row_id = #{ActiveRecord::Base.connection.quote(row_id)}
    SQL
  end
end
