module WorkflowShapes
  class Step
    include StoreModel::Model

    attribute :kind, :string
    attribute :options, ActiveRecord::Type::Json.new, default: -> { {} }

    validates :kind, presence: true
    validates :options, exclusion: { in: [nil] }
  end

  class Link
    include StoreModel::Model

    attribute :from, :string
    attribute :to, :string
    attribute :index, :integer, default: 0

    validates :from, :to, :index, presence: true
  end

  class Graph
    include StoreModel::Model

    attribute :steps, Step.to_hash_type, default: -> { {} }
    attribute :links, Link.to_hash_type, default: -> { {} }
    attribute :published_at, :datetime

    validates :steps, :links, exclusion: { in: [nil] }
  end
end
