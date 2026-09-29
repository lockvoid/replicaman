require 'test_helper'

class PushMembershipHoistTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @board = create_board!(id: 'b0', user: @user, name: 'Existing')
  end

  test 'each operation is authorized against the row it writes' do
    other = User.create!(id: 'u2', name: 'Other')
    create_board!(id: 'b9', user: other, name: 'Theirs')
    Items::TextItem.create!(id: 'mine', board: @board, rank: 'a', body: 'x')
    Items::TextItem.create!(id: 'theirs', board_id: 'b9', rank: 'a', body: 'x')

    result = push_ops(DummyReplica, user: @user, ops: [
      { 'id' => 'p1', 'op' => 'row.patch', 'stream' => 'items', 'row_id' => 'mine', 'data' => { 'body' => 'ok' } },
      { 'id' => 'p2', 'op' => 'row.patch', 'stream' => 'items', 'row_id' => 'theirs', 'data' => { 'body' => 'no' } },
      { 'id' => 'p3', 'op' => 'row.patch', 'stream' => 'items', 'row_id' => 'mine', 'data' => { 'boardId' => 'b9' } }
    ])

    assert_equal %w[accepted rejected rejected], result[:verdicts].map { it[:outcome] }
    assert_equal %w[b0 ok], Item.find('mine').then { [it.board_id, it.body] }
    assert_equal 'x', Item.find('theirs').body
  end

  test 'a create that changes membership mid-batch is seen by later ops — the memo must not go stale' do
    doc = Loro::Doc.new(peer_id: 3)
    doc.get_map('meta').set('name', 'Fresh board')
    doc.commit

    Items::TextItem.create!(id: 'i0', board: @board, rank: 'a', body: 'x')
    ops = [
      { 'id' => 'op1', 'op' => 'row.patch', 'stream' => 'items', 'row_id' => 'i0', 'data' => { 'body' => 'seen' } },
      { 'id' => 'op2', 'op' => 'row.create', 'stream' => 'boards', 'row_id' => 'b1',
        'codec' => 'loro@1', 'seed' => Base64.strict_encode64(doc.export_snapshot) },
      { 'id' => 'op3', 'op' => 'row.create', 'stream' => 'items', 'row_id' => 'i9',
        'type' => 'TextItem', 'data' => { 'boardId' => 'b1', 'rank' => 'a', 'body' => 'x' } }
    ]

    result = push_ops(DummyReplica, user: @user, ops: ops)

    assert(result[:verdicts].all? { |v| v[:outcome] == 'accepted' }, result[:verdicts].inspect)
  end
end
