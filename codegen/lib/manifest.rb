# frozen_string_literal: true

module ReplicaCodegen
  class InvalidManifest < StandardError; end

  module Naming
    def camelize(string)
      string.split('_').map { |part| part[0].upcase + part[1..] }.join
    end

    def lower_camel(string)
      string[0].downcase + string[1..]
    end

    def singularize(name)
      return 'bus' if name == 'buses'
      return name.sub(/ies\z/, 'y') if name.end_with?('ies')
      return name.sub(/es\z/, '') if name.match?(/(ch|sh|x|ss)es\z/)
      name.sub(/s\z/, '')
    end

    def model_name(stream)
      stream.fetch('modelName') { camelize(singularize(stream.fetch('name'))) }
    end
  end

  # One semantic model precedes every language emitter. Native emitters own
  # syntax and native type construction; identity, wire capability, validation,
  # and enum vocabulary sharing are decided here exactly once.
  class Manifest
    include Naming
    IDENTIFIER = /\A[A-Za-z_][A-Za-z0-9_]*\z/
    SCALARS = %w[string text hex_color integer bigint float decimal boolean datetime date time].freeze
    TYPES = (SCALARS + %w[array json jsonb]).freeze

    def initialize(raw, config = {})
      @raw = Marshal.load(Marshal.dump(raw))
      @config = config
    end

    def compile
      fail!('expected a manifest object with version 1') unless @raw.is_a?(Hash) && @raw.fetch('version', nil) == 1
      namespace = @raw.fetch('namespace', nil)
      unless namespace.is_a?(String) && /\A[a-zA-Z0-9_.:-]{1,128}\z/.match?(namespace)
        fail!('namespace must be a stable identifier of 1 to 128 bytes')
      end
      version = @raw.fetch('schemaVersion', nil)
      unless version.is_a?(Integer) && (1..2_147_483_647).cover?(version)
        fail!('schemaVersion must be a positive int32')
      end
      streams = @raw.fetch('streams', nil)
      fail!('streams must be an array') unless streams.is_a?(Array)
      unique!(streams.map { it.fetch('name', nil) }, 'stream names')
      declared = []
      streams.each do |stream|
        name = identifier!(stream.fetch('name', nil), 'stream name')
        fail!("#{name}: unsupported lane") unless %w[row document].include?(stream.fetch('lane', nil))
        identifier!(stream.fetch('shard', 'user'), "#{name} shard")
        if stream.fetch('lane', nil) == 'document'
          fail!("#{name}: document codec is required") unless stream.fetch('codec', nil).is_a?(String) && !stream.fetch('codec', nil).empty?
        end
        stream['modelName'] = identifier!(@config.dig('models', name) || model_name(stream), "#{name} model")
        declared << stream.fetch('modelName', nil)
        columns!(stream, stream.fetch('columns', nil), name)
        variants = stream.fetch('variants', [])
        fail!("#{name}: variants must be an array") unless variants.is_a?(Array)
        unique!(variants.map { it.fetch('type', nil) }, "#{name} variant types")
        variants.each do |variant|
          wire_type = identifier!(variant.fetch('type', nil), "#{name} variant")
          emitted = identifier!(@config.dig('variants', name, wire_type) || wire_type, "#{name} variant model")
          %w[swift kotlin rust].each { variant["#{it}_type"] = emitted }
          declared << emitted
          columns!(stream, variant.fetch('columns', nil), "#{name}.#{wire_type}")
        end
        vocabularies = variants.flat_map { it.fetch('columns', nil) }.select { it.fetch('enum', nil) }.group_by { it.fetch('name', nil) }
        forked = vocabularies.select { |_, columns| columns.map { it.fetch('enum', nil) }.uniq.size > 1 }.keys
        variants.each do |variant|
          variant.fetch('columns', nil).each do |column|
            next unless column.fetch('enum', nil)
            owner = forked.include?(column.fetch('name', nil)) ? variant.fetch('swift_type', nil) : stream.fetch('modelName', nil)
            column['enumName'] = "#{owner}#{camelize(column.fetch('name', nil))}"
          end
        end
        (stream.fetch('shapes', nil) || {}).each { |name, shape| shape!(shape, "#{stream.fetch('name', nil)}##{name}") }
      end
      references!(streams)
      unique!(declared, 'generated type names (use --config models/variants to resolve collisions)')
      @raw
    end

    private

    def references!(streams)
      names = streams.map { it.fetch('name') }
      streams.each do |stream|
        references = stream.fetch('references', [])
        fail!('references must be an array of at most 64 entries') unless references.is_a?(Array) && references.size <= 64
        unique!(references.map { it.fetch('name') }, "#{stream.fetch('name')} references")
        references.each do |reference|
          identifier!(reference.fetch('name'), 'reference name')
          fail!('unknown reference stream') unless names.include?(reference.fetch('stream'))
          field = reference.fetch('field', nil)
          segment = reference.fetch('keySegment', nil)
          fail!('reference requires exactly one of field or keySegment') unless field.nil? != segment.nil?
          identifier!(field, 'reference field') if field
          if segment && !(segment.is_a?(Integer) && segment >= 0)
            fail!('reference keySegment must be a nonnegative integer')
          end
          if reference.key?('keyPrefix') && (!segment || !reference.fetch('keyPrefix').is_a?(String))
            fail!('keyPrefix requires keySegment and must be a string')
          end
          fail!('reference optional must be boolean') unless [true, false].include?(reference.fetch('optional', false))
        end
        next unless stream.key?('lifetimeFrom')
        lifetime = references.find { it.fetch('name') == stream.fetch('lifetimeFrom') }
        fail!('lifetimeFrom must name a required reference') unless lifetime && !lifetime.fetch('optional', false)
      end
      by_name = streams.to_h { [it.fetch('name'), it] }
      streams.each do |start|
        seen = []
        stream = start
        while stream.key?('lifetimeFrom')
          name = stream.fetch('name')
          fail!('cyclic derived entity lifetimes') if seen.include?(name)
          seen << name
          reference = stream.fetch('references').find { it.fetch('name') == stream.fetch('lifetimeFrom') }
          stream = by_name.fetch(reference.fetch('stream'))
        end
      end
    end

    def fail!(message)
      raise InvalidManifest, message
    end

    def identifier!(value, where)
      fail!("#{where}: invalid identifier #{value.inspect}") unless value.is_a?(String) && IDENTIFIER.match?(value)
      value
    end

    def unique!(values, where)
      duplicates = values.tally.select { |_, count| count > 1 }.keys
      fail!("#{where}: duplicate #{duplicates.join(', ')}") if duplicates.any?
    end

    def columns!(stream, columns, where)
      fail!("#{where}: columns must be an array") unless columns.is_a?(Array)
      unique!(columns.map { it.fetch('name', nil) }, "#{where} columns")
      columns.each do |column|
        name = identifier!(column.fetch('name', nil), "#{where} column")
        fail!("#{where}.#{name}: unsupported column type") unless TYPES.include?(column.fetch('type', nil))
        # Unknown virtual-attribute nullability is nullable; only false is required.
        column['null'] = true if column.fetch('null', nil).nil?
        %w[push pull null].each do |flag|
          fail!("#{where}.#{name}: #{flag} must be boolean") unless [true, false].include?(column[flag])
        end
        if column.fetch('pull', nil) == false && column.fetch('null', nil) == false
          fail!("#{where}.#{name} is a required intake — a pulled row cannot decode it")
        end
        creatable = stream.fetch('columns', nil).any? { it.fetch('name', nil) == 'createdAt' && it.fetch('push', nil) }
        if creatable && column.fetch('push', nil) == false && column.fetch('null', nil) == false && !column.fetch('reflects', nil)
          fail!("#{where}.#{name} is a required pull-only column — a created row cannot decode it")
        end
        fail!("#{where}.#{name}: blob: true requires an explicit blob integration") if column.fetch('blob', nil) == true
        if column.fetch('items', nil) && !SCALARS.include?(column.dig('items', 'type'))
          fail!("#{where}.#{name}: only scalar array item types are supported")
        end
        if column.fetch('enum', nil)
          fail!("#{where}.#{name}: enum must contain strings") unless column.fetch('enum', nil).is_a?(Array) && column.fetch('enum', nil).all? { it.is_a?(String) }
          unique!(column.fetch('enum', nil), "#{where}.#{name} enum")
        end
        shape!(column.fetch('shapes', nil), "#{where}.#{name}") if column.fetch('shapes', nil)
        union_variants!(stream, column.fetch('shapes'), "#{where}.#{name}") if column.dig('shapes', 'discriminator')
      end
    end

    def union_variants!(stream, shape, where)
      variants = shape.fetch('variants', nil)
      fail!("#{where}: a discriminated shape needs a variants array") unless variants.is_a?(Array)
      variants.each do |variant|
        wire_type = variant.fetch('type', nil).to_s
        renamed = @config.dig('variants', stream.fetch('name'), wire_type)
        variant['variantName'] = identifier!(renamed || camelize(wire_type.split('::').last.to_s), "#{where} variant")
      end
      unique!(variants.map { it.fetch('variantName') }, "#{where} variant names")
    end

    def shape!(node, where)
      fail!("#{where}: shape must be an object") unless node.is_a?(Hash)
      Array(node.fetch('fields', nil)).each do |field|
        identifier!(field.fetch('name', nil), "#{where} field")
        shape!(field, "#{where}.#{field.fetch('name', nil)}")
      end
      shape!(node.fetch('items', nil), "#{where}[]") if node.fetch('items', nil)
      Array(node.fetch('variants', nil)).each { shape!(it, where) }
    end
  end
end
