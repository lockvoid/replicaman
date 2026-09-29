class Streams::Exports < ReplicaMan::Stream
  owner :user_id
  door ReplicaMan::Normalizer::Row

  attribute :user_id, :progress, :markers, :details
  attribute :result, ExportResults::Polymorphic
end
