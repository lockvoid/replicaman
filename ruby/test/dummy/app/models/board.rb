class Board < ApplicationRecord
  belongs_to :user
  has_many :items, dependent: :destroy

  attribute :name, default: 'Untitled'

  validates :name, presence: true
end
