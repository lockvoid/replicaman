class AddMembershipIndexes < ActiveRecord::Migration[8.1]
  def change
    add_index :replica_man_snapshots, "(data ->> 'userId')", name: 'idx_replica_man_snapshots_user_id'
    add_index :replica_man_snapshots, "(data ->> 'boardId')", name: 'idx_replica_man_snapshots_board_id'
  end
end
