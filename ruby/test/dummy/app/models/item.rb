class Item < ApplicationRecord
  belongs_to :board

  store_attribute :metadata, :label, :string

  store_attribute :metadata, :internal_note, :string

  validates :rank, presence: true
  validates :label, inclusion: { in: %w[idea task] }, allow_nil: true
end
