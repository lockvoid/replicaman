class Streams::Decks < ReplicaMan::Stream
  owner :user_id
  door ReplicaMan::Normalizer::Document

  attribute :change_seq, :created_at, :updated_at, :user_id
  attribute :slide_count, :integer, pull: ->(deck) { deck.slides.size }
  index :updated_at

  document do
    attribute :slides
    attribute :layouts
    attribute :settings
  end
end
