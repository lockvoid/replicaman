class CreateDummyTables < ActiveRecord::Migration[8.1]
  def change
    create_table :users, id: :string do |t|
      t.string :name
    end

    create_table :boards, id: :string do |t|
      t.string :user_id, null: false
      t.string :name
    end

    create_table :items, id: :string do |t|
      t.string :board_id, null: false
      t.string :type, null: false
      t.string :rank, null: false
      t.jsonb :metadata, null: false, default: {}
    end

    create_table :jobs, id: :string do |t|
      t.string :user_id, null: false
      t.string :state, null: false
    end
  end
end
