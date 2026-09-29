class CreateTallies < ActiveRecord::Migration[8.1]
  def change
    create_table :tallies, id: :string do |t|
      t.string :user_id, null: false
      t.integer :version, null: false, default: 0
      t.string :status, null: false
      t.integer :count, null: false, default: 0
    end
  end
end
