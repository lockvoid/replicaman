require 'test_helper'

class PartitionsTest < ActiveSupport::TestCase
  PROBE = 'carve_probe'.freeze

  setup do
    @connection = ReplicaMan::Snapshot.connection
  end

  teardown do
    ReplicaMan::Schema::PARENTS.each { @connection.execute("DROP TABLE IF EXISTS #{it}_#{PROBE}") }
  end

  test 'create carves the stream a partition of the snapshot and the delta table' do
    ReplicaMan::Schema::Partitions.new(stream: PROBE).create(@connection)

    assert_equal %W[replica_man_snapshots_#{PROBE} replica_man_deltas_#{PROBE}], partitions
  end

  test 'create moves rows captured before the partition existed, unchanged' do
    rolled_back do
      @connection.execute(<<~SQL)
        INSERT INTO replica_man_snapshots (namespace, stream, row_id, bucket, position)
        VALUES ('replicaman-test', '#{PROBE}', 'lagged', 'user:u1', 7);
        INSERT INTO replica_man_deltas (namespace, stream, row_id, seq, payload)
        VALUES ('replicaman-test', '#{PROBE}', 'lagged', 1, '\\x00');
      SQL

      ReplicaMan::Schema::Partitions.new(stream: PROBE).create(@connection)

      assert_equal ["replica_man_snapshots_#{PROBE}", 7], @connection.select_rows(<<~SQL).first
        SELECT tableoid::regclass::text, position FROM replica_man_snapshots WHERE row_id = 'lagged'
      SQL
      assert_equal "replica_man_deltas_#{PROBE}", @connection.select_value(<<~SQL)
        SELECT tableoid::regclass::text FROM replica_man_deltas WHERE row_id = 'lagged'
      SQL
    end
  end

  test 'create keeps an existing partition and carves the missing one' do
    @connection.execute("CREATE TABLE replica_man_snapshots_#{PROBE} PARTITION OF replica_man_snapshots FOR VALUES IN ('#{PROBE}')")

    ReplicaMan::Schema::Partitions.new(stream: PROBE).create(@connection)

    assert_equal %W[replica_man_snapshots_#{PROBE} replica_man_deltas_#{PROBE}], partitions
  end

  test 'drop removes both partitions' do
    partitions = ReplicaMan::Schema::Partitions.new(stream: PROBE)
    partitions.create(@connection)

    partitions.drop(@connection)

    assert_empty self.partitions
  end

  test 'a migration names the stream' do
    assert_equal ':jobs', ReplicaMan::Schema::Partitions.new(stream: 'jobs').to_ruby
  end

  private

  def partitions
    ReplicaMan::Schema::PARENTS.map { "#{it}_#{PROBE}" }
      .select { @connection.select_value("SELECT to_regclass(#{@connection.quote(it)})::text") }
  end
end
