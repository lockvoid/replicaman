module ThemeShapes
  class Color
    include StoreModel::Model

    attribute :id, :string
    attribute :hex, :string

    validates :id, :hex, presence: true
  end

  class Font
    include StoreModel::Model

    attribute :family, :string
    attribute :weight, :integer, default: 400
    attribute :slant, :string, default: 'upright'

    validates :family, presence: true
    validates :slant, inclusion: { in: %w[upright italic] }
  end

  class Heading
    include StoreModel::Model

    attribute :font, Font.to_type
    attribute :uppercase, :boolean, default: false

    validates :uppercase, exclusion: { in: [nil] }
  end
end
