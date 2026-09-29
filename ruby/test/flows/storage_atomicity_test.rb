require 'test_helper'

class StorageAtomicityTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'One')
    create_board!(id: 'b1', user: @user, name: 'Board')
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'Original', label: 'idea')
  end

  test 'a concurrent patch reads its baseline after obtaining the row lock' do
    pid = Queue.new
    worker = nil
    Item.transaction do
      held = Items::TextItem.lock.find('i1')
      worker = Thread.new do
        ActiveRecord::Base.connection_pool.with_connection do |connection|
          pid << connection.select_value('SELECT pg_backend_pid()')
          push_ops(DummyReplica, user: @user, ops: [{ id: 'patch', op: 'row.patch', stream: 'items', row_id: 'i1', data: { body: 'New body' } }])
        end
      end
      backend = pid.pop(timeout: 5) || flunk('worker did not connect')
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      until ActiveRecord::Base.connection.select_value("SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = #{Integer(backend)}")
        flunk('push did not reach the locked row') if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        ActiveRecord::Base.connection.execute('SELECT pg_stat_clear_snapshot()')
        sleep 0.01
      end
      held.update!(label: 'task')
    end
    assert worker.join(5), 'push did not finish after lock release'
    assert_equal 'accepted', worker.value[:verdicts].first[:outcome]
    assert_equal 'task', Items::TextItem.find('i1').label, 'patching body must preserve the concurrent label edit in the same JSON column'
    assert_equal 'New body', Items::TextItem.find('i1').body
    assert_equal 'task', ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').data['label']
  ensure
    worker&.kill if worker&.alive?
  end
end
