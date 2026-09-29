class Streams::Boards < ReplicaMan::Stream
  owner :user_id
  door BoardNormalizer

  attribute :id
  attribute :user_id
  attribute :name

  document do
    attribute :name
  end
end
