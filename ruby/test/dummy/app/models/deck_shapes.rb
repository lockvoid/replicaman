module DeckShapes
  class HexColor < ActiveModel::Type::String
    def type
      :hex_color
    end
  end

  class Offset
    include StoreModel::Model

    attribute :x, :float, default: 0.0
    attribute :y, :float, default: 0.0

    validates :x, :y, presence: true
  end

  class Style
    include StoreModel::Model

    attribute :fill_color, HexColor.new
    attribute :text_decoration, JobPayloads::ScalarArrayType.new(:string)
    attribute :shadow_offset, Offset.to_type

    validates :text_decoration, inclusion: { in: %w[underline strikethrough] }
  end

  class Slide
    include StoreModel::Model

    attribute :key, :string
    attribute :layout_key, :string
    attribute :start, :float
    attribute :duration, :float
    attribute :opacity, :float, default: 1.0
    attribute :fit_mode, :string, default: 'auto'
    attribute :style, Style.to_type

    validates :key, :layout_key, :start, :duration, :opacity, :fit_mode, presence: true
    validates :fit_mode, inclusion: { in: %w[auto contain cover] }
  end

  class Layout
    include StoreModel::Model

    attribute :key, :string
    attribute :name, :string
    attribute :columns, :integer, default: 1
    attribute :visible, :boolean, default: true
    attribute :background_color, HexColor.new

    validates :key, :name, :columns, presence: true
    validates :visible, exclusion: { in: [nil] }
  end

  class Settings
    include StoreModel::Model

    attribute :aspect_ratio, :string, default: '16:9'
    attribute :autoplay, :boolean, default: false
    attribute :loop_count, :integer, default: 0
    attribute :accent_color, HexColor.new

    validates :aspect_ratio, :loop_count, presence: true
    validates :aspect_ratio, inclusion: { in: %w[4:3 16:9] }
    validates :autoplay, exclusion: { in: [nil] }
  end
end
