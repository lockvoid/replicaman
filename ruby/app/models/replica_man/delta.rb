module ReplicaMan
  class Delta < ApplicationRecord
    self.table_name = 'replica_man_deltas'

    self.primary_key = [:stream, :id]
  end
end
