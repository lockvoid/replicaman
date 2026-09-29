require 'test_helper'

class PushReplayTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'One')
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Board') }
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'Original')
  end

  def patch(id, body)
    { id: id, op: 'row.patch', stream: 'items', row_id: 'i1', data: { body: body } }
  end

  test 'a delayed create retry cannot resurrect a deleted row' do
    create = { id: 'birth', op: 'row.create', stream: 'items', row_id: 'i2', type: 'TextItem',
               data: { boardId: 'b1', rank: 'b', body: 'Created' } }
    first = push_ops(DummyReplica, user: @user, ops: [create])
    assert_equal 'accepted', first[:verdicts].first[:outcome]
    deletion = { id: 'delete', op: 'row.delete', stream: 'items', row_id: 'i2' }
    push_ops(DummyReplica, user: @user, ops: [deletion])

    assert_equal first, push_ops(DummyReplica, user: @user, ops: [create])
    refute Items::TextItem.exists?('i2')
  end

  test 'reusing an operation id for different bytes does not acknowledge unapplied data' do
    push_ops(DummyReplica, user: @user, ops: [patch('stable-id', 'One')])
    error = assert_raises(ReplicaMan::Protocol::Error) do
      push_ops(DummyReplica, user: @user, ops: [patch('stable-id', 'Two')])
    end
    assert_equal 'MutationChanged', error.code
    assert_equal 'One', Items::TextItem.find('i1').body
  end

  test 'results are scoped to the authenticated principal' do
    original = patch('same-id', 'Accepted')
    push_ops(DummyReplica, user: @user, ops: [original])
    other = User.create!(id: 'u2', name: 'Other')
    result = push_ops(DummyReplica, user: other, ops: [original])
    assert_equal 'rejected', result[:verdicts].first[:outcome]
  end

  test 'after_apply sees only newly applied operations and rolls back with results' do
    previous = DummyReplica.after_apply
    calls = []
    DummyReplica.after_apply { |operations:, **| calls << operations.map(&:id) }
    original = patch('original', 'One')
    push_ops(DummyReplica, user: @user, ops: [original])
    push_ops(DummyReplica, user: @user, ops: [original, patch('later', 'Two')])
    assert_equal [[DomainClient.uuid('original')], [DomainClient.uuid('later')]], calls

    DummyReplica.after_apply { |**| raise 'domain transaction failed' }
    assert_raises(RuntimeError) { push_ops(DummyReplica, user: @user, ops: [patch('retryable', 'Three')]) }
    assert_equal 'Two', Items::TextItem.find('i1').body
    DummyReplica.after_apply { |**| }
    push_ops(DummyReplica, user: @user, ops: [patch('retryable', 'Three')])
    assert_equal 'Three', Items::TextItem.find('i1').body,
                 'a failed transaction must not leave an accepted result'
  ensure
    DummyReplica.instance_variable_set(:@after_apply, previous)
  end

  test 'concurrent duplicate delivery runs its domain transaction once' do
    previous = DummyReplica.after_apply
    arrived = Queue.new
    release = Queue.new
    calls = Queue.new
    DummyReplica.after_apply do |**|
      calls << true
      arrived << true
      release.pop
    end
    original = patch('concurrent', 'One')
    first = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection { push_ops(DummyReplica, user: @user, ops: [original]) }
    end
    arrived.pop(timeout: 5) || flunk('first mutation never arrived')
    second = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection { push_ops(DummyReplica, user: @user, ops: [original]) }
    end
    release << true
    assert first.join(5), 'first delivery did not finish'
    assert second.join(5), 'retry did not finish'
    assert_equal first.value, second.value
    assert_equal 1, calls.size
  ensure
    release << true if release
    [first, second].compact.each { it.kill if it.alive? }
    DummyReplica.instance_variable_set(:@after_apply, previous)
  end
end
