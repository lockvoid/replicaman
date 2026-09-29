require 'digest'
require 'securerandom'

module ReplicaMan
  module Protocol
    VERSION = 2
    MAX_SEQUENCE = (1 << 63) - 1
    MAX_OPERATIONS = 100
    PAGE_BYTES = 256 * 1024
    ENTITY_BYTES = 32 * 1024 * 1024

    class Error < InvalidRequest
      attr_reader :code, :details

      def initialize(code, message = code, status: 409, **details)
        @code = code
        @details = details

        super(message, status: status)
      end
    end

    def self.identifier!(value, name)
      unless value.is_a?(String) && value.match?(/\A[a-zA-Z0-9_.:-]{1,128}\z/)
        raise InvalidRequest, "#{name} must be an identifier of 1 to 128 bytes"
      end

      value
    end

    # Business keys are opaque UTF-8, not protocol tokens. Hosts legitimately
    # use paths, spaces and non-ASCII keys; PostgreSQL text cannot store NUL.
    def self.row_id!(value, name)
      unless value.is_a?(String) && value.valid_encoding? &&
             (1..1024).cover?(value.bytesize) && !value.include?("\0")
        raise InvalidRequest, "#{name} must be UTF-8 text of 1 to 1024 bytes without NUL"
      end

      value
    end

    def self.counter!(value, name)
      unless value.is_a?(String) && value.match?(/\A(0|[1-9][0-9]{0,18})\z/) && value.to_i <= MAX_SEQUENCE
        raise InvalidRequest, "#{name} must be a decimal int64 string"
      end

      value.to_i
    end

    def self.principal(user)
      JSON.generate([user.class.base_class.name, user.id.to_s])
    end

    def self.validate!(replica, body, initial: false)
      RequestBody.require_fields!(body, %w[protocol namespace schema])

      unless body.fetch('protocol').to_s == VERSION.to_s
        raise Error.new('UpgradeRequired', supported: [VERSION])
      end

      unless body.fetch('namespace') == replica.namespace
        raise Error.new('NamespaceChanged')
      end

      unless body.fetch('schema').to_s == replica.schema_version.to_s
        raise Error.new('UpgradeRequired', schema: replica.schema_version)
      end

      return if initial && body.fetch('dataset', nil).nil?

      RequestBody.require_fields!(body, %w[dataset])

      unless body.fetch('dataset') == replica.dataset_epoch
        raise Error.new('DatasetChanged', dataset: replica.dataset_epoch)
      end
    end

    def self.canonical(value)
      case value
      when Hash then value.keys.sort.to_h { |key| [key, canonical(value.fetch(key))] }
      when Array then value.map { canonical(it) }
      when Float
        raise InvalidRequest, 'non-finite number' unless value.finite?
        value
      else value
      end
    end

    def self.digest(bytes)
      Digest::SHA256.hexdigest(bytes)
    end

    def self.header(replica)
      {
        protocol: VERSION,
        namespace: replica.namespace,
        dataset: replica.dataset_epoch,
        schema: replica.schema_version,
      }
    end
  end
end
