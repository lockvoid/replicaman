module ReplicaMan
  # A bucket's position counter row stays locked until the capturing transaction
  # commits, so positions become visible in commit order and without gaps.
  module Buckets
    def self.advance(namespace, bucket)
      Bucket.connection.exec_query(<<~SQL, 'ReplicaMan::Buckets', [namespace, bucket]).rows.first.first
        INSERT INTO replica_man_buckets (namespace, bucket, head) VALUES ($1, $2, 1)
        ON CONFLICT (namespace, bucket) DO UPDATE SET head = replica_man_buckets.head + 1
        RETURNING head
      SQL
    end

    def self.heads(namespace, buckets)
      stored = Bucket.where(namespace: namespace, bucket: buckets).pluck(:bucket, :head).to_h
      buckets.to_h { [it, stored.fetch(it, 0)] }
    end
  end
end
