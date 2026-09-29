class CreateTickets < ActiveRecord::Migration[8.1]
  def change
    create_table :tickets do |t|
      t.string :code, null: false, index: { unique: true }
      t.string :user_id, null: false
      t.string :note
    end
  end
end
