require 'test_helper'

class SchemaConstraintsTest < ActiveSupport::TestCase
  test 'a live row outside every bucket is refused by the database itself' do
    assert_raises(ActiveRecord::CheckViolation) do
      execute("INSERT INTO replica_man_snapshots (namespace, stream, row_id) VALUES ('replicaman-test', 'jobs', 'j1')")
    end
  end

  test 'a row id longer than the protocol allows is refused' do
    assert_raises(ActiveRecord::CheckViolation) do
      execute(<<~SQL)
        INSERT INTO replica_man_snapshots (namespace, stream, row_id, bucket, position)
        VALUES ('replicaman-test', 'jobs', repeat('x', 1025), 'user:u1', 1)
      SQL
    end
  end

  test 'a position is either unclaimed or a real counter value' do
    assert_raises(ActiveRecord::CheckViolation) { snapshot(0) }
  end

  test 'an unclaimed position cannot outlive its transaction' do
    error = assert_raises(ActiveRecord::StatementInvalid) { ActiveRecord::Base.transaction { snapshot(-1) } }

    assert_match 'committed without a claimed position', error.message
    refute ReplicaMan::Snapshot.exists?(row_id: 'j1')
  end

  test 'a refusal needs its reason and an acceptance has none' do
    assert_raises(ActiveRecord::CheckViolation) { operation('rejected', nil) }
    assert_raises(ActiveRecord::CheckViolation) { operation('accepted', 'why') }
  end

  test 'an operation body digest is a SHA-256' do
    assert_raises(ActiveRecord::CheckViolation) { operation('accepted', nil, digest: '\\x00') }
  end

  private

  def snapshot(position)
    execute(<<~SQL)
      INSERT INTO replica_man_snapshots (namespace, stream, row_id, bucket, position)
      VALUES ('replicaman-test', 'jobs', 'j1', 'user:u1', #{position})
    SQL
  end

  def operation(outcome, reason, digest: "\\x#{'ab' * 32}")
    execute(<<~SQL)
      INSERT INTO replica_man_operations (op_id, namespace, author, body_sha256, outcome, reason)
      VALUES (gen_random_uuid(), 'replicaman-test', '["User","u1"]', '#{digest}', '#{outcome}',
              #{reason.nil? ? 'NULL' : "'#{reason}'"})
    SQL
  end

  def execute(sql)
    ActiveRecord::Base.connection.execute(sql)
  end
end
