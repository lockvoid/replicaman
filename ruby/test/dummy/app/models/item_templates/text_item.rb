class ItemTemplates::TextItem < ItemTemplate
  store_attribute :metadata, :tone, :string
  store_attribute :metadata, :body, :string

  validates :tone, inclusion: { in: %w[light dark] }, allow_nil: true
end
