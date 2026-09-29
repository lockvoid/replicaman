class AddPriorityToJobs < ActiveRecord::Migration[8.1]
  def change
    add_column :jobs, :priority, :string, null: false, default: 'low'
  end
end
