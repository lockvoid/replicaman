class Streams::Themes < ReplicaMan::Stream
  owner :user_id
  door ReplicaMan::Normalizer::Row

  attribute :name, :description, :logo_ref, :pinned
  attribute :colors, { model: ThemeShapes::Color, collection: :array }
  attribute :heading, ThemeShapes::Heading
  attribute :logo_url, :string, pull: ->(theme) { theme.logo_ref && "https://media.example.test/#{theme.logo_ref}" }
  attribute :created_at, :updated_at, :user_id, push: false
end
