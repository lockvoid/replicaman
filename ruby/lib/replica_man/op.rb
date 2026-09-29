require 'base64'
require 'digest'

module ReplicaMan
  class Op
    def self.validate_batch!(operations, limit:)
      raise InvalidRequest, 'operations must be an array' unless operations.is_a?(Array)
      raise InvalidRequest, "at most #{limit} operations are allowed" if operations.size > limit

      ids = Set.new

      operations.each do |raw|
        RequestBody.require_fields!(raw, %w[id op stream row_id incarnation])

        %w[id op stream incarnation].each do |field|
          Protocol.identifier!(raw.fetch(field), "operation #{field}")
        end

        Protocol.row_id!(raw.fetch('row_id'), 'operation row_id')

        unless ids.add?(raw.fetch('id'))
          raise InvalidRequest, 'operation IDs must be unique within a submission'
        end

        validate_values!(raw)
      end
    end

    def self.validate_values!(raw)
      if raw.key?('replaces')
        Protocol.identifier!(raw.fetch('replaces'), 'operation replaces')
        raise InvalidRequest, 'only a birth may replace an incarnation' unless raw.fetch('op') == 'row.create'
      end

      if raw.key?('data') && !raw.fetch('data').is_a?(Hash)
        raise InvalidRequest, 'operation data must be an object'
      end

      %w[type codec seed payload group].each do |field|
        next unless raw.key?(field)

        unless raw.fetch(field).is_a?(String)
          raise InvalidRequest, "operation #{field} must be a string"
        end
      end

      if raw.fetch('op') == 'row.patch'
        RequestBody.require_fields!(raw, %w[data])
      end

      if raw.fetch('op') == 'doc.delta'
        RequestBody.require_fields!(raw, %w[codec payload])
      end

      if raw.key?('expected')
        expected = raw.fetch('expected')
        raise InvalidRequest, 'expected must be an object' unless expected.is_a?(Hash)

        Protocol.counter!(expected.fetch('revision'), 'expected revision') if expected.key?('revision')

        if expected.key?('fields') && !expected.fetch('fields').is_a?(Hash)
          raise InvalidRequest, 'expected fields must be an object'
        end
      end

      references = raw.fetch('references', [])
      raise InvalidRequest, 'references must be an array' unless references.is_a?(Array)

      raise InvalidRequest, 'at most 64 references are allowed' if references.size > 64
      names = Set.new
      references.each do |reference|
        RequestBody.require_fields!(reference, %w[stream id incarnation])
        if reference.key?('name')
          Protocol.identifier!(reference.fetch('name'), 'reference name')
          raise InvalidRequest, 'duplicate reference name' unless names.add?(reference.fetch('name'))
        end
        %w[stream incarnation].each { Protocol.identifier!(reference.fetch(it), "reference #{it}") }
        Protocol.row_id!(reference.fetch('id'), 'reference id')
      end
    end

    attr_reader :user

    def initialize(raw, user:)
      @raw = raw.deep_stringify_keys
      @user = user
    end

    def id
      @raw.fetch('id')
    end

    def verb
      @raw.fetch('op')
    end

    def stream_name
      @raw.fetch('stream')
    end

    def row_id
      @raw.fetch('row_id')
    end

    def incarnation
      @raw.fetch('incarnation')
    end

    def replaces
      @raw.fetch('replaces', nil)
    end

    def group
      @raw.fetch('group', nil)
    end

    def type
      @raw.fetch('type', nil)
    end

    def data
      @raw.fetch('data', {})
    end

    def codec
      required_document_field('codec')
    end

    def expected
      @raw.fetch('expected', nil)
    end

    def references
      @raw.fetch('references', [])
    end

    def seed
      @seed ||= binary('seed')
    end

    def payload
      @payload ||= binary('payload')
    end

    def to_h
      @raw.deep_dup
    end

    def digest
      @digest ||= Digest::SHA256.digest(JSON.generate(Protocol.canonical(@raw)))
    end

    private

    # Requiredness depends on the declared stream lane. A row birth has no
    # codec/seed; a document birth missing either is a semantic refusal.
    def required_document_field(field)
      @raw.fetch(field) { raise Refused, "document operation requires #{field}" }
    end

    def binary(field)
      Base64.strict_decode64(required_document_field(field))
    rescue ArgumentError
      raise Refused, "malformed base64 in #{field}"
    end
  end
end
