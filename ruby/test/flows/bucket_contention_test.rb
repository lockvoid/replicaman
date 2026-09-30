require 'test_helper'

class BucketContentionTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'One')
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Board') }
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'Device')
    Items::TextItem.create!(id: 'i2', board_id: 'b1', rank: 'b', body: 'Server')
    @previous = DummyReplica.after_apply
  end

  teardown do
    DummyReplica.instance_variable_set(:@after_apply, @previous)
  end

  test 'a push whose hook waits on a server write commits beside it without a deadlock' do
    server_holds_row, push_applied = Queue.new, Queue.new
    DummyReplica.after_apply do |**|
      push_applied << true
      Items::TextItem.find('i2').update!(body: 'Hook')
    end
    server = Thread.new { ActiveRecord::Base.connection_pool.with_connection { hold_then_commit(server_holds_row, push_applied) } }
    server_holds_row.pop(timeout: 5) || flunk('the server write never started')

    receipt = push_ops(DummyReplica, user: @user, ops: [patch('i1', 'Pushed')])

    assert server.join(10), 'the server write never finished'
    assert_equal 'accepted', receipt[:verdicts].first[:outcome]
    assert_equal %w[Pushed Hook], Items::TextItem.order(:id).map(&:body)
    assert_equal ReplicaMan::Buckets.heads('replicaman-test', ['user:u1']).fetch('user:u1'), last_position
  ensure
    server&.kill if server&.alive?
  end

  private

  def patch(row_id, body)
    { id: "patch-#{row_id}", op: 'row.patch', stream: 'items', row_id: row_id, data: { body: body } }
  end

  def hold_then_commit(holding, released)
    ActiveRecord::Base.transaction do
      Items::TextItem.find('i2').update!(body: 'Job')
      holding << true
      released.pop(timeout: 5) || raise('the push never applied')
    end
  end

  def last_position
    ReplicaMan::Snapshot.where(namespace: 'replicaman-test', bucket: 'user:u1').maximum(:position)
  end
end
