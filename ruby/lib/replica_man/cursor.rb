require 'base64'

module ReplicaMan
  # A shard cursor is opaque to clients: the position reached in each bucket
  # the principal reads for that shard. A bootstrap that spans pages stays a
  # bootstrap and remembers the heads it started from: a deletion committed past
  # those heads happened while the round was in flight, and a later page of the
  # round delivers it, where the deletions that preceded the round are omitted.
  module Cursor
    STARTED = '@bootstrap:'.freeze

    def self.encode(positions, started: nil)
      pairs = positions.map { |bucket, position| [bucket, position.to_s] }
      started&.each { |bucket, head| pairs << ["#{STARTED}#{bucket}", head.to_s] }
      Base64.urlsafe_encode64(JSON.generate(pairs), padding: false)
    end

    def self.decode(value, buckets)
      return buckets.to_h { [it, 0] } if value.nil?
      raise InvalidRequest, 'cursor must be a string' unless value.is_a?(String)

      positions, = parse(value)
      raise Protocol::Error.new('CursorInvalid') unless positions && positions.keys.sort == buckets.sort

      positions
    end

    # The heads a bootstrap round started from, by bucket: empty for a round
    # that starts with this request, nil for an incremental round.
    def self.started(value, buckets)
      return {} if value.nil?

      positions, started = parse(value)
      raise Protocol::Error.new('CursorInvalid') unless positions
      return if started.empty?
      raise Protocol::Error.new('CursorInvalid') unless started.keys.sort == buckets.sort

      started
    end

    def self.parse(value)
      return if value.bytesize > 4096

      pairs = JSON.parse(Base64.urlsafe_decode64(value))
      return unless pairs.is_a?(Array) && pairs.all? { it.is_a?(Array) && it.size == 2 && it.all?(String) }

      marks, rest = pairs.partition { |bucket, _| bucket.start_with?(STARTED) }
      positions = rest.to_h { |bucket, position| [bucket, Protocol.counter!(position, 'cursor position')] }
      started = marks.to_h { |mark, head| [mark.delete_prefix(STARTED), Protocol.counter!(head, 'cursor head')] }
      [positions, started] if positions.size == rest.size && started.size == marks.size
    rescue ArgumentError, JSON::ParserError, InvalidRequest
      nil
    end
    private_class_method :parse
  end
end
