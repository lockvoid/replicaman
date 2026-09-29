class ItemTemplates::PhotoItem < ItemTemplate
  store_attribute :metadata, :tone, :string
  store_attribute :metadata, :aspect, :string

  validates :tone, inclusion: { in: %w[light dark] }, allow_nil: true
end
