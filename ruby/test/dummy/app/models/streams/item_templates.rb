class Streams::ItemTemplates < ReplicaMan::Stream
  owner :user_id
  door ReplicaMan::Normalizer::Row

  attribute :user_id, :name

  variant ItemTemplates::PhotoItem do
    attribute :tone, :aspect
  end

  variant ItemTemplates::TextItem do
    attribute :tone, :body
  end
end
