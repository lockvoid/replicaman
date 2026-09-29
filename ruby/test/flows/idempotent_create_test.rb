require 'test_helper'

class IdempotentCreateTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Mine') }
  end

  def create_op(id: 'op1', body: 'hello')
    { id: id, op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
      data: { boardId: 'b1', rank: 'a', body: body } }
  end

  test 'an identical retried operation returns its original acceptance' do
    original = create_op
    first = push_ops(DummyReplica, user: @user, ops: [original])[:verdicts]
    assert_equal 'accepted', first.first[:outcome]
    assert_equal first, push_ops(DummyReplica, user: @user, ops: [original])[:verdicts]
    assert_equal 'hello', Items::TextItem.find('i1').body
  end

  test 'a new operation cannot acknowledge a colliding row identity without saving its data' do
    push_ops(DummyReplica, user: @user, ops: [create_op])
    collision = create_op(id: 'different-operation', body: 'must not silently disappear')
    verdict = push_ops(DummyReplica, user: @user, ops: [collision])[:verdicts].first
    assert_equal 'rejected', verdict[:outcome]
    assert_equal 'hello', Items::TextItem.find('i1').body
  end

  test 'a new document create cannot acknowledge a seed it never imported' do
    doc = Loro::Doc.new(peer_id: 987)
    doc.get_map('meta').set('name', 'unimported authoring')
    doc.commit
    collision = { id: 'new-birth', op: 'row.create', stream: 'boards', row_id: 'b1', codec: 'loro@1',
                  seed: Base64.strict_encode64(doc.export_snapshot) }
    verdict = push_ops(DummyReplica, user: @user, ops: [collision])[:verdicts].first
    assert_equal 'rejected', verdict[:outcome]
    assert_equal 'Mine', Board.find('b1').name
  end

  test 'a custom normalizer cannot silently acknowledge a colliding create' do
    push_ops(DummyReplica, user: @user, ops: [create_op])
    normalizer = DummyReplica.streams.fetch(:items).normalizer
    original = normalizer.method(:create)
    normalizer.define_singleton_method(:create) do |_, stream, op|
      stream.member!(op.user, stream.serialize(stream.locate(op.row_id)))
    end

    verdict = push_ops(DummyReplica, user: @user, ops: [create_op(id: 'custom', body: 'unsaved')])[:verdicts].first
    assert_equal 'rejected', verdict[:outcome]
    assert_equal 'hello', Items::TextItem.find('i1').body
  ensure
    normalizer.define_singleton_method(:create, original) if original
  end

  test 'a collision on another unique field is rejected when the requested row was not created' do
    connection = ActiveRecord::Base.connection
    index = 'replica_test_unique_board_rank'
    connection.add_index(:items, [:board_id, :rank], unique: true, name: index)

    original = create_op
    conflicting = create_op(id: 'op2').merge(row_id: 'i2')
    verdicts = push_ops(DummyReplica, user: @user, ops: [original, conflicting])[:verdicts]

    assert_equal 'accepted', verdicts.first[:outcome]
    assert_equal 'rejected', verdicts.last[:outcome],
                 'acknowledging a missing row causes clients to forget an undelivered create'
    assert Items::TextItem.exists?('i1')
    refute Items::TextItem.exists?('i2')
  ensure
    connection.remove_index(:items, name: index) if connection.index_exists?(:items, [:board_id, :rank], name: index)
  end

  test 'an existing entity is changed by a patch and replay cannot overwrite later edits' do
    push_ops(DummyReplica, user: @user, ops: [create_op])
    merge = { id: 'domain-merge', op: 'row.patch', stream: 'items', row_id: 'i1', data: { body: 'merged' } }
    first = push_ops(DummyReplica, user: @user, ops: [merge])
    assert_equal 'accepted', first[:verdicts].first[:outcome]
    assert_equal 'merged', Items::TextItem.find('i1').body
    assert_equal 'merged', ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').data.fetch('body')

    Items::TextItem.find('i1').update!(body: 'later')
    assert_equal first, push_ops(DummyReplica, user: @user, ops: [merge])
    assert_equal 'later', Items::TextItem.find('i1').body

    stranger = User.create!(id: 'other', name: 'Other')
    refused = push_ops(DummyReplica, user: stranger, ops: [merge])[:verdicts].first
    assert_equal 'rejected', refused[:outcome]
    assert_equal 'later', Items::TextItem.find('i1').body
  end
end
