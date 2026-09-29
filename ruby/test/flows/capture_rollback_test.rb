require 'test_helper'

class CaptureRollbackTest < ActiveSupport::TestCase
  test 'rollback in one stream cannot discard another stream with the same model id' do
    user = User.create!(id: 'owner', name: 'Owner')
    board = create_board!(id: 'same', user: user, name: 'Before')
    item = Items::TextItem.create!(id: 'same', board: board, rank: '1', body: 'Before')
    Board.transaction do
      board.update!(name: 'Committed board')
      Item.transaction(requires_new: true) do
        item.update!(body: 'Rolled back item')
        raise ActiveRecord::Rollback
      end
    end
    assert_equal 'Committed board', ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'same').data['name']
    assert_equal 'Before', ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'same').data['body']
  end

  test 'savepoint rollback retains the earlier write in the outer transaction' do
    user = User.create!(id: 'owner', name: 'Owner')
    board = create_board!(id: 'b', user: user, name: 'Before')
    Board.transaction do
      board.update!(name: 'Outer write')
      Board.transaction(requires_new: true) do
        board.update!(name: 'Rolled back write')
        raise ActiveRecord::Rollback
      end
    end
    assert_equal 'Outer write', board.reload.name
    assert_equal 'Outer write', ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b').data['name']
  end

  test 'outer rollback leaves no capture to leak into the next transaction' do
    user = User.create!(id: 'owner', name: 'Owner')
    board = create_board!(id: 'b', user: user, name: 'Before')
    Board.transaction do
      board.update!(name: 'Rolled back')
      raise ActiveRecord::Rollback
    end
    other = create_board!(id: 'next', user: user, name: 'Next')
    assert_equal 'Before', ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b').data['name']
    assert_equal other.name, ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'next').data['name']
  end
end
