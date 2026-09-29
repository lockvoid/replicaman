module ReplicaMan
  class Operation < ApplicationRecord
    self.table_name = 'replica_man_operations'
    self.primary_key = :op_id

    enum :outcome, { accepted: 'accepted', rejected: 'rejected' }, validate: true
  end
end
