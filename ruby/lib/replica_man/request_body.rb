require 'stringio'
require 'zlib'

module ReplicaMan
  class InvalidRequest < StandardError
    attr_reader :status

    def initialize(message, status: 400)
      @status = status
      super(message)
    end
  end

  # Compression is part of the replica transport contract. Hosts should not
  # need a separate application middleware just to accept native client pushes.
  module RequestBody
    DEFAULT_LIMIT = 32 * 1024 * 1024

    def self.require_fields!(body, fields)
      raise InvalidRequest, 'request must be an object' unless body.is_a?(Hash)

      fields.each { body.fetch(it) }
    rescue KeyError => error
      raise InvalidRequest, "missing request field: #{error.key}"
    end

    def self.read(request, limit: DEFAULT_LIMIT)
      body = request.body.read(limit + 1)
      raise InvalidRequest.new('request body exceeds the limit', status: 413) if body.bytesize > limit

      encoding = request.get_header('HTTP_CONTENT_ENCODING').to_s.downcase.strip
      case encoding
      when '', 'identity' then body
      when 'gzip'
        Zlib::GzipReader.wrap(StringIO.new(body)) do |reader|
          inflated = reader.read(limit + 1)
          raise InvalidRequest.new('inflated request exceeds the limit', status: 413) if inflated.bytesize > limit
          inflated
        end
      else
        raise InvalidRequest.new('unsupported content encoding', status: 415)
      end
    rescue Zlib::Error, EOFError
      raise InvalidRequest, 'malformed gzip body'
    end
  end
end
