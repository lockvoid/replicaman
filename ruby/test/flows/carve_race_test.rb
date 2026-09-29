require 'test_helper'

class CarveRaceTest < ActiveSupport::TestCase
  PROBE = 'carve_race_probe'.freeze

  teardown do
    ReplicaMan::Snapshot.connection.execute("DROP TABLE IF EXISTS replica_man_snapshots_#{PROBE}")
  end

  def poison_create(connection, failures:)
    state = { attempts: 0 }
    connection.singleton_class.prepend(Module.new do
      define_method(:execute) do |sql, *args|
        if sql.to_s.include?('PARTITION OF') && sql.to_s.include?(PROBE) && (state[:attempts] += 1) <= failures
          raise ActiveRecord::CheckViolation, 'PG::CheckViolation: updated partition constraint for default partition "replica_man_snapshots_default" would be violated by some row'
        end
        super(sql, *args)
      end
    end)
    state
  end

  test 'a writer racing the carve costs one retry, not a dead boot' do
    connection = ReplicaMan::Snapshot.connection
    state = poison_create(connection, failures: 1)

    DummyReplica.send(:carve, connection, 'replica_man_snapshots', PROBE)

    assert_equal 2, state[:attempts], 'the carve must re-run after the lost race'
    assert connection.select_value("SELECT to_regclass('replica_man_snapshots_#{PROBE}')::text"),
           'the retry re-sweeps and carves'
  end

  test 'a twice-lost race leaves the stream on the default partition — never a raise' do
    connection = ReplicaMan::Snapshot.connection
    state = poison_create(connection, failures: 2)

    assert_nothing_raised do
      DummyReplica.send(:carve, connection, 'replica_man_snapshots', PROBE)
    end

    assert_equal 2, state[:attempts], 'exactly one retry — a persistent racer must not loop the boot'
    assert_nil connection.select_value("SELECT to_regclass('replica_man_snapshots_#{PROBE}')::text")
  end
end
