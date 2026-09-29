require 'base64'

module ReplicaMan
  # A shard cursor is opaque to clients: the position reached in each bucket
  # the principal reads for that shard.
  module Cursor
    def self.encode(positions)
      Base64.urlsafe_encode64(JSON.generate(positions.map { |bucket, position| [bucket, position.to_s] }), padding: false)
    end

    def self.decode(value, buckets)
      return buckets.to_h { [it, 0] } if value.nil?
      raise InvalidRequest, 'cursor must be a string' unless value.is_a?(String)

      positions = parse(value)
      raise Protocol::Error.new('CursorInvalid') unless positions && positions.keys.sort == buckets.sort

      positions
    end

    def self.parse(value)
      return if value.bytesize > 4096

      pairs = JSON.parse(Base64.urlsafe_decode64(value))
      return unless pairs.is_a?(Array) && pairs.all? { it.is_a?(Array) && it.size == 2 && it.all?(String) }

      positions = pairs.to_h { |bucket, position| [bucket, Protocol.counter!(position, 'cursor position')] }
      positions if positions.size == pairs.size
    rescue ArgumentError, JSON::ParserError, InvalidRequest
      nil
    end
    private_class_method :parse
  end
end
