require 'test_helper'

class PushFailuresTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
  end

  def tally(id, name, status: 'open')
    { id: name, op: 'row.create', stream: 'tallies', row_id: id,
      data: { userId: @user.id, version: 0, status: status, count: 1 } }
  end

  def with_door_raising(error, message)
    normalizer = DummyReplica.streams[:tallies].normalizer
    normalizer.define_singleton_method(:refuse?) { |_op| raise error, message }
    yield
  ensure
    normalizer.singleton_class.send(:remove_method, :refuse?)
  end

  test 'a failure the same operation would repeat is its verdict; the rest of the push lands' do
    verdicts = push_ops(DummyReplica, user: @user, ops: [tally('t1', 'bad', status: nil), tally('t2', 'good')])[:verdicts]

    assert_equal %w[rejected accepted], verdicts.map { it[:outcome] }
    assert_match(/\AActiveRecord::NotNullViolation: /, verdicts.first[:reason])
    assert Tally.exists?('t2')
    refute Tally.exists?('t1')
    assert_equal 2, ReplicaMan::Operation.count

    again = push_ops(DummyReplica, user: @user, ops: [tally('t1', 'bad', status: nil), tally('t2', 'good')])[:verdicts]
    assert_equal verdicts, again, 'a retry gets the recorded verdicts'
  end

  test 'a host exception inside the door is the operation verdict' do
    verdicts = with_door_raising(KeyError, 'key not found: :required') do
      push_ops(DummyReplica, user: @user, ops: [tally('t1', 'bad')])[:verdicts]
    end

    assert_equal [{ id: 'bad', outcome: 'rejected', reason: 'KeyError: key not found: :required' }], verdicts
    refute Tally.exists?('t1')
  end

  test 'a transient failure rolls the whole request back for a retry' do
    with_door_raising(ActiveRecord::Deadlocked, 'deadlock detected') do
      assert_raises(ActiveRecord::Deadlocked) { push_ops(DummyReplica, user: @user, ops: [tally('t1', 'later')]) }
    end

    assert_equal 0, ReplicaMan::Operation.count, 'no claim survives a request that rolls back'
    assert_equal 'accepted', push_ops(DummyReplica, user: @user, ops: [tally('t1', 'later')])[:verdicts].first[:outcome]
  end
end
