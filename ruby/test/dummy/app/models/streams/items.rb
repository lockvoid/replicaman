class Streams::Items < ReplicaMan::Stream
  owner ->(item) { item.board&.user_id }
  door ReplicaMan::Normalizer::Row

  attribute :id, :board_id, :rank
  index :board_id
  attribute :label
  attribute :annotation, :string, pull: false
  attribute :rank_badge, :string, pull: ->(item) { "##{item.rank}" }

  variant Items::PhotoItem do
    attribute :caption, :width
  end

  variant Items::TextItem do
    attribute :body
  end
end
