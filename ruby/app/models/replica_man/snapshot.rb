module ReplicaMan
  class Snapshot < ApplicationRecord
    self.table_name = 'replica_man_snapshots'
    self.primary_key = [:namespace, :stream, :row_id]
  end
end
