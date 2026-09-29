class AddPayloadToJobs < ActiveRecord::Migration[8.1]
  def change
    add_column :jobs, :payload, :jsonb
  end
end
