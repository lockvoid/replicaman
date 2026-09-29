module JobPayloads
  class ScalarArrayType < ActiveModel::Type::Value
    attr_reader :subtype

    def initialize(subtype)
      super()
      @subtype = ActiveModel::Type.lookup(subtype)
    end

    def type
      :array
    end

    def cast(value)
      Array(value).map { subtype.cast(it) } unless value.nil?
    end
  end

  class Metrics
    include StoreModel::Model

    attribute :sample_count, :integer
    attribute :mean_confidence, :float
  end

  class Ready
    include StoreModel::Model

    attribute :display_name, :string
    attribute :metrics, Metrics.to_type
    attribute :history, Metrics.to_array_type, default: -> { [] }

    validates :display_name, presence: true
    validates :history, exclusion: { in: [nil] }
    validates :metrics, presence: true, if: -> { false }
  end

  class Empty
    include StoreModel::Model

    attribute :reason_code, :string
  end

  class Summary
    include StoreModel::Model

    attribute :state, :string, default: 'queued'
    attribute :active, :boolean, default: false
    attribute :metrics_by_key, Metrics.to_hash_type, default: -> { {} }
    attribute :labels, ScalarArrayType.new(:string), default: -> { [] }
    attribute :token, :string, default: -> { SecureRandom.hex(4) }

    validates :state, inclusion: { in: %w[queued running done] }
    validates :active, :metrics_by_key, exclusion: { in: [nil] }
    validates :labels, inclusion: { in: %w[system custom] }
  end

  VARIANTS = {
    'empty' => Empty,
    'ready' => Ready
  }.freeze

  Polymorphic = ReplicaMan.union(by: :kind, variants: VARIANTS)
end
