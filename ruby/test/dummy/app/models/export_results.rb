module ExportResults
  class Image
    include StoreModel::Model

    attribute :width, :integer
    attribute :height, :integer
    attribute :histogram, JobPayloads::ScalarArrayType.new(:integer)
    attribute :color_space, :string

    validates :width, :height, presence: true
    validates :color_space, inclusion: { in: %w[sdr hdr] }, allow_nil: true
  end

  class Video
    include StoreModel::Model

    attribute :duration, :float
    attribute :frame_rate, :float
    attribute :keyframes, JobPayloads::ScalarArrayType.new(:float), default: -> { [] }
    attribute :muted_tracks, JobPayloads::ScalarArrayType.new(:boolean)
    attribute :color_space, :string

    validates :duration, presence: true
    validates :color_space, inclusion: { in: %w[sdr hdr] }, allow_nil: true
  end

  VARIANTS = {
    'image' => Image,
    'video' => Video
  }.freeze

  Polymorphic = ReplicaMan.union(by: :kind, variants: VARIANTS)
end
