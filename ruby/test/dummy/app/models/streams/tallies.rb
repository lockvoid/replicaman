class Streams::Tallies < ReplicaMan::Stream
  owner :user_id
  door ReplicaMan::Normalizer::Row

  attribute :user_id
  attribute :count
  attribute :version, precondition: true
  attribute :status, precondition: true
end
