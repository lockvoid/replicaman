class Items::TextItem < Item
  store_attribute :metadata, :body, :string

  validates :body, presence: true
end
