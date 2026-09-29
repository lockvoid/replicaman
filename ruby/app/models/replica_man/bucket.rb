module ReplicaMan
  class Bucket < ApplicationRecord
    self.table_name = 'replica_man_buckets'
    self.primary_key = [:namespace, :bucket]
  end
end
