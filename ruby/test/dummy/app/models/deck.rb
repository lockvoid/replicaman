class Deck < ApplicationRecord
  store_attribute :content, :slides, DeckShapes::Slide.to_array_type, default: -> { [] }
  store_attribute :content, :layouts, DeckShapes::Layout.to_array_type,
                  default: -> { [DeckShapes::Layout.new(key: 'title', name: 'Title')] }
  store_attribute :content, :settings, DeckShapes::Settings.to_type, default: -> { DeckShapes::Settings.new }
end
