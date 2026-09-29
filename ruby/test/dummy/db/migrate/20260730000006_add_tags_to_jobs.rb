class AddTagsToJobs < ActiveRecord::Migration[8.1]
  def change
    add_column :jobs, :tags, :string, array: true, null: false, default: []
  end
end
