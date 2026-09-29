module ReplicaMan
  class Union
    attr_reader :discriminator, :variants

    def initialize(by:, variants:)
      @discriminator = by.to_s
      @variants = variants.freeze
      discriminator = @discriminator
      resolved = @variants
      @one_of = StoreModel.one_of do |json|
        # Unknown/missing variants remain a StoreModel decoding error.
        # StoreModel accepts Hash and other objects implementing to_h (for
        # example OpenStruct). Normalize before strict discriminator lookup.
        attributes = json.to_h
        kind = attributes.fetch(discriminator.to_sym) { attributes.fetch(discriminator, nil) }
        resolved.fetch(kind.to_s, nil)
      end
    end

    def to_type(...)
      @one_of.to_type(...)
    end
  end

  def self.union(by:, variants:)
    Union.new(by: by, variants: variants)
  end
end
