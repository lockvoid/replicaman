class Streams::Jobs < ReplicaMan::Stream
  owner :user_id

  attribute :id, :user_id, :state, :priority, :tags
  attribute :payload, JobPayloads::Polymorphic
  attribute :summary, JobPayloads::Summary,
            pull: ->(job) { { 'state' => job.state, 'active' => job.state != 'done', 'metricsByKey' => {} } }
end
