module ReplicaMan
  # A bucket's position counter row stays locked until the capturing transaction
  # commits, so positions become visible in commit order and without gaps.
  module Buckets
    # One claim per transaction, its last act before COMMIT, counters in one order:
    # a transaction holding a counter never waits on a row, so writers cannot deadlock.
    def self.claim(keys)
      after = keys.tally.sort.to_h { |(namespace, bucket), count| [[namespace, bucket], advance(namespace, bucket, count) - count] }
      keys.map { after[it] += 1 }
    end

    def self.heads(namespace, buckets)
      stored = Bucket.where(namespace: namespace, bucket: buckets).pluck(:bucket, :head).to_h
      buckets.to_h { [it, stored.fetch(it, 0)] }
    end

    def self.advance(namespace, bucket, count)
      Bucket.connection.exec_query(<<~SQL, 'ReplicaMan::Buckets', [namespace, bucket, count]).rows.first.first
        INSERT INTO replica_man_buckets (namespace, bucket, head) VALUES ($1, $2, $3)
        ON CONFLICT (namespace, bucket) DO UPDATE SET head = replica_man_buckets.head + EXCLUDED.head
        RETURNING head
      SQL
    end
    private_class_method :advance
  end
end
