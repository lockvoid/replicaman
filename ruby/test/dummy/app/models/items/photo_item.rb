class Items::PhotoItem < Item
  store_attribute :metadata, :width, :integer
  store_attribute :metadata, :caption, :string

  validates :caption, inclusion: { in: %w[wide square] }, allow_nil: true
  validates :width, inclusion: { in: (1..10).to_a }, allow_nil: true
end
