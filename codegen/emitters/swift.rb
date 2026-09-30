#!/usr/bin/env ruby
# frozen_string_literal: true

# Native swift syntax emitter. Manifest semantics and output installation
# belong to codegen/lib; execute through codegen/bin/replica-codegen.

require 'erb'
require 'json'
require 'net/http'
require 'optparse'
require 'fileutils'
require 'uri'

# --- naming ------------------------------------------------------------------



SWIFT_KEYWORDS = %w[
  associatedtype class deinit enum extension fileprivate func import init inout
  internal let open operator private precedencegroup protocol public rethrows
  static struct subscript typealias var break case catch continue default defer
  do else fallthrough for guard if in repeat return throw switch where while as
  Any false is nil self Self super throws true try
].freeze

def swift_case(value)
  identifier = lower_camel(camelize(value.to_s.gsub(/[^0-9A-Za-z]+/, '_')))
  identifier = "_#{identifier}" if identifier.match?(/\A\d/)
  SWIFT_KEYWORDS.include?(identifier) ? "`#{identifier}`" : identifier
end

def swift_string(value)
  JSON.generate(value.to_s)
end

# Streams are table-plural by convention; the model is the singular. Naive on
# purpose (stdlib only): -ies → -y, -ches/-shes/-xes/-ses → drop -es, then
# drop a trailing -s.

# Two STI streams can flatten the same demodulized subclass into top-level
# Swift (Shop::Assets::ImageAsset vs Gallery::Assets::ImageAsset). Only the
# emitted Swift names move — the wire type stays the manifest's.

# Stream names are honest (stream == table, boot-enforced server-side), so
# the Swift model is always the singularized stream name — no rename map.

# --- type mapping ------------------------------------------------------------

SWIFT_TYPES = {
  'string' => 'String', 'text' => 'String',
  'hex_color' => 'String',
  'integer' => 'Int', 'bigint' => 'Int',
  'float' => 'Double', 'decimal' => 'Double',
  'boolean' => 'Bool',
  'datetime' => 'String', 'date' => 'String', 'time' => 'String',
}.freeze

def swift_type(wire_type)
  SWIFT_TYPES.fetch(wire_type, 'ReplicaValue')
end

def wire_accessor(wire_type)
  case swift_type(wire_type)
  when 'String' then '?.string'
  when 'Int' then '?.int'
  when 'Double' then '?.number'
  when 'Bool' then '?.bool'
  else ''
  end
end

def wire_encode_expression(wire_type, variable)
  case swift_type(wire_type)
  when 'String' then ".string(#{variable})"
  when 'Int' then ".signedInteger(Int64(#{variable}))"
  when 'Double' then ".number(#{variable})"
  when 'Bool' then ".bool(#{variable})"
  else variable
  end
end

# --- column shaping ----------------------------------------------------------

Column = Struct.new(
  :name, :wire_type, :items, :optional, :push, :pull, :enum_name, :enum_values, :shape_name, :shape,
  keyword_init: true
) do
  def storage_name
    materialized_projection? ? "#{name}JSON" : name
  end

  # A list column (Postgres array) reads its element type from the manifest's
  # `items` node — the same array/items grammar shapes speak. A shape-backed
  # array column carries its structure under `shapes` instead.
  def list?
    wire_type == 'array' && !items.nil?
  end

  def item_type
    items.fetch('type')
  end

  def base_type
    return enum_name if enum_name
    return shape_swift_type({ 'type' => 'array', 'items' => items }, nil, typed_enums: false) if list?
    return array_shape? ? "[#{shape_name}]" : shape_name if typed_shape?

    swift_type(wire_type)
  end

  def declared_type
    optional ? "#{base_type}?" : base_type
  end

  def parameter
    optional ? "#{storage_name}: #{declared_type} = nil" : "#{storage_name}: #{declared_type}"
  end

  def decode_expression
    value = "fields[\"#{name}\"]"
    return "(#{value}?.string.flatMap { #{enum_name}(rawValue: $0) })" if enum_name
    return "(#{value}?.items?.compactMap { $0#{wire_accessor(item_type).delete_prefix('?')} })" if list?
    return "(#{value}.flatMap { try? ReplicaValueCoding.decode(#{base_type}.self, from: $0) })" if typed_shape?

    "#{value}#{wire_accessor(wire_type)}"
  end

  def encode_expression(variable)
    return ".string(#{variable}.rawValue)" if enum_name
    return ".array(#{variable}.map { #{wire_encode_expression(item_type, '$0')} })" if list?
    return "((try? ReplicaValueCoding.encode(#{variable})) ?? .null)" if typed_shape?

    wire_encode_expression(wire_type, variable)
  end

  # A discriminated union keeps its raw value beside the materialized one: a
  # row whose kind this build does not know still carries it.
  def projection_source(indent)
    return unless materialized_projection?

    setter = optional ? "#{storage_name} = #{name}?.replicaValue" : "#{storage_name} = #{name}?.replicaValue ?? .object([:])"
    <<~SWIFT.chomp
      #{indent}public var #{name}: #{shape_name}? {
      #{indent}    didSet { #{setter} }
      #{indent}}
    SWIFT
  end

  def materialized_projection?
    shape_name && shape&.key?('discriminator')
  end

  # Every other shape IS the column's type: decoded where the row decodes, so
  # a row whose required shape does not decode is not a row.
  def typed_shape?
    shape_name && !materialized_projection?
  end

  # An array-of-object column (`colors`). The doc lane already emits
  # typed `[Slide]` from exactly this manifest node; this is the row lane
  # reaching the same generator instead of falling through to `ReplicaValue`.
  def array_shape?
    shape && shape.fetch('type', nil) == 'array' && shape.dig('items', 'type') == 'object'
  end

  def materialization_assignment(indent, source)
    return unless materialized_projection?

    "#{indent}self.#{name} = #{shape_name}(replicaValue: #{source})"
  end
end

def check_column!(stream, column)
  if column.fetch('items', nil) && !SWIFT_TYPES.key?(column.dig('items', 'type'))
    raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is an array of " \
          "#{column.dig('items', 'type').inspect} — only scalar item types are generated"
  end

  if column.fetch('pull', nil) == false && column.fetch('null', nil) == false
    raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is a required intake — " \
          'a pulled row can never satisfy its decode guard'
  end

  # The mirror, on streams the device can CREATE in: a required column it
  # cannot write is one it has no authored value for. A stream whose own
  # `createdAt` is pull-only is not creatable at all (a derived id,
  # server-seeded, one patch door), so the rule has nothing to say about it.
  # A column its document reflects is written by the engine from the seed.
  creatable = stream.fetch('columns').any? { |c| c.fetch('name') == 'createdAt' && c.fetch('push', nil) }
  return unless creatable && column.fetch('push', nil) == false && column.fetch('null', nil) == false && !column.fetch('reflects', nil)

  raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is a required pull-only column — " \
        'a created row can never satisfy its own decode guard'
end

def base_columns(stream)
  owner = model_name(stream)
  stream.fetch('columns').map do |column|
    check_column!(stream, column)

    Column.new(
      name: column.fetch('name'),
      wire_type: column.fetch('type'),
      items: column.fetch('items', nil),
      optional: column.fetch('null', nil) != false,
      push: column.fetch('push'),
      pull: column.fetch('pull'),
      enum_name: column.fetch('enum', nil) ? "#{owner}#{camelize(column.fetch('name'))}" : nil,
      enum_values: column.fetch('enum', nil),
      shape_name: shape_type_name(owner, column),
      shape: column.fetch('shapes', nil)
    )
  end
end

# `colors` shaped as an array of objects names its struct after ONE of
# them — `ThemeColor`, not `…Colors`.
def shape_type_name(owner, column)
  return nil unless column.fetch('shapes', nil)

  name = column.fetch('name')
  name = singularize(name) if column.dig('shapes', 'type') == 'array'
  "#{owner}#{camelize(name)}"
end

# Enum identity keys on (stream, column), not on the declaring variant: the
# same wire key declared by several kinds is ONE vocabulary. A genuine fork —
# declarers disagreeing on the values — falls back to per-variant names for
# EVERY declarer of that column, never a half-shared state.

def variant_columns(stream, variant)
  variant.fetch('columns').map do |column|
    check_column!(stream, column)

    Column.new(
      name: column.fetch('name'),
      wire_type: column.fetch('type'),
      items: column.fetch('items', nil),
      optional: column.fetch('null', nil) != false,
      push: column.fetch('push'),
      pull: column.fetch('pull'),
      enum_name: column.fetch('enum', nil) ? column.fetch('enumName') : nil,
      enum_values: column.fetch('enum', nil),
      shape_name: shape_type_name(variant.fetch('swift_type'), column),
      shape: column.fetch('shapes', nil)
    )
  end
end

def sti_kind_value(model, variant_type)
  stem = variant_type.end_with?(model) ? variant_type.delete_suffix(model) : variant_type
  lower_camel(stem)
end

def enum_definitions(stream)
  model = model_name(stream)
  definitions = []
  if stream.fetch('sti', nil)
    definitions << {
      name: "#{model}Kind",
      values: stream.fetch('variants', nil).map { sti_kind_value(model, it.fetch('type')) }
    }
  end
  base_columns(stream).filter(&:enum_name).each do |column|
    definitions << { name: column.enum_name, values: column.enum_values }
  end
  stream.fetch('variants', []).to_a.each do |variant|
    variant_columns(stream, variant).filter(&:enum_name).each do |column|
      definitions << { name: column.enum_name, values: column.enum_values }
    end
  end
  definitions.uniq { it[:name] }
end

def enum_source(stream)
  enum_definitions(stream).map do |definition|
    cases = definition[:values].map do |value|
      "    case #{swift_case(value)} = #{swift_string(value)}"
    end.join("\n")
    <<~SWIFT
      public enum #{definition[:name]}: String, Codable, Sendable, CaseIterable {
      #{cases}
      }
    SWIFT
  end.join("\n")
end

def shape_nested_name(field, singular_collections: false)
  name = field.fetch('name')
  name = singularize(name) if singular_collections && %w[array map].include?(field.fetch('type'))
  camelize(name)
end

def shape_swift_type(node, nested_name, typed_enums: true)
  return nested_name if typed_enums && node.fetch('enum', nil)

  case node.fetch('type')
  when 'array'
    items = node.fetch('items')
    "[#{shape_swift_type(items, nested_name, typed_enums: typed_enums)}#{'?' if items.fetch('null', nil)}]"
  when 'map'
    "[String: #{shape_swift_type(node.fetch('values'), nested_name, typed_enums: typed_enums)}]"
  when 'object'
    nested_name
  else
    swift_type(node.fetch('type'))
  end
end

def shape_field_type(field, singular_collections: false, typed_enums: true)
  shape_swift_type(
    field,
    shape_nested_name(field, singular_collections: singular_collections),
    typed_enums: typed_enums
  )
end

def shape_nested_object(field)
  case field.fetch('type')
  when 'object'
    field
  when 'array'
    field.fetch('items') if field.dig('items', 'type') == 'object'
  when 'map'
    field.fetch('values') if field.dig('values', 'type') == 'object'
  end
end

def shape_enum_node(field)
  return field if field.fetch('enum', nil)
  return field.fetch('items', nil) if field.fetch('type') == 'array' && field.dig('items', 'enum')
  return field.fetch('values', nil) if field.fetch('type') == 'map' && field.dig('values', 'enum')
end

def shape_enum_source(name, values, indent)
  pad = ' ' * indent
  cases = values.map { |value| "#{pad}    case #{swift_case(value)} = #{swift_string(value)}" }
  ([
    "#{pad}public enum #{name}: String, Codable, Sendable, CaseIterable {",
  ] + cases + ["#{pad}}"]).join("\n")
end

def replica_value_literal(value)
  case value
  when nil
    '.null'
  when true, false
    ".bool(#{value})"
  when Integer
    ".signedInteger(#{value})"
  when Numeric
    ".number(#{value})"
  when String
    ".string(#{swift_string(value)})"
  when Array
    ".array([#{value.map { replica_value_literal(it) }.join(', ')}])"
  when Hash
    pairs = value.map { |key, item| "#{swift_string(key)}: #{replica_value_literal(item)}" }
    pairs.empty? ? '.object([:])' : ".object([#{pairs.join(', ')}])"
  else
    raise "replica-codegen: unsupported JSON default #{value.inspect}"
  end
end

def shape_value_source(
  node,
  value,
  type_reference: nil,
  singular_collections: false,
  typed_enums: true
)
  return 'nil' if value.nil?
  return typed_enums ? ".#{swift_case(value)}" : swift_string(value) if node.fetch('enum', nil)

  case node.fetch('type')
  when 'array'
    item = node.fetch('items')
    children = value.map do
      shape_value_source(
        item,
        it,
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
    end
    "[#{children.join(', ')}]"
  when 'map'
    item = node.fetch('values')
    pairs = value.map do |key, child|
      child_source = shape_value_source(
        item,
        child,
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
      "#{swift_string(key)}: #{child_source}"
    end
    pairs.empty? ? '[:]' : "[#{pairs.join(', ')}]"
  when 'object'
    constructor = type_reference || '.init'
    fields = node.fetch('fields').filter_map do |field|
      next unless value.key?(field.fetch('name'))

      expression = shape_value_source(
        field,
        value.fetch(field.fetch('name')),
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
      "#{field.fetch('name')}: #{expression}"
    end
    "#{constructor}(#{fields.join(', ')})"
  when 'json', 'jsonb'
    replica_value_literal(value)
  when 'string', 'text', 'hex_color', 'datetime', 'date', 'time'
    swift_string(value)
  when 'boolean', 'integer', 'bigint', 'float', 'decimal'
    value.to_s
  else
    replica_value_literal(value)
  end
end

def shape_parameter(field, singular_collections: false, typed_enums: true)
  name = field.fetch('name')
  type = shape_field_type(
    field,
    singular_collections: singular_collections,
    typed_enums: typed_enums
  )
  default =
    if field.key?('default')
      shape_value_source(
        field,
        field.fetch('default', nil),
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
    elsif field.fetch('null', nil) != false
      'nil'
    end
  "#{name}: #{type}#{field.fetch('null', nil) == false ? '' : '?'}#{default ? " = #{default}" : ''}"
end

def shape_struct_source(
  name,
  fields,
  discriminator: nil,
  indent: 0,
  singular_collections: false,
  typed_enums: true
)
  pad = ' ' * indent
  property_pad = ' ' * (indent + 4)
  lines = ["#{pad}public struct #{name}: Sendable, Hashable, Codable {"]

  enums = fields.filter_map do |field|
    enum_node = shape_enum_node(field)
    next unless enum_node

    shape_enum_source(
      shape_nested_name(field, singular_collections: singular_collections),
      enum_node.fetch('enum'),
      indent + 4
    )
  end
  unless enums.empty?
    lines << enums.join("\n\n")
    lines << ''
  end

  lines << "#{property_pad}public let #{discriminator}: String" if discriminator
  fields.each do |field|
    type = shape_field_type(
      field,
      singular_collections: singular_collections,
      typed_enums: typed_enums
    )
    if field.fetch('null', nil) == false
      lines << "#{property_pad}public var #{field.fetch('name')}: #{type}"
    else
      lines << "#{property_pad}public var #{field.fetch('name')}: #{type}? = nil"
    end
  end

  parameters = []
  assignments = []
  if discriminator
    parameters << "#{discriminator}: String"
    assignments << "#{property_pad}    self.#{discriminator} = #{discriminator}"
  end
  fields.each do |field|
    field_name = field.fetch('name')
    parameters << shape_parameter(
      field,
      singular_collections: singular_collections,
      typed_enums: typed_enums
    )
    assignments << "#{property_pad}    self.#{field_name} = #{field_name}"
  end
  lines << ''
  lines << "#{property_pad}public init(#{parameters.join(', ')}) {"
  lines.concat(assignments)
  lines << "#{property_pad}}"

  # Schema-evolution contract, read side (mirrors the server's StoreModel):
  # a missing key with a declared default reads AS the default, a missing
  # nullable key reads nil, unknown keys are skipped by the keyed container,
  # and an enum VALUE this build does not know collapses to the declared
  # default (nil when nullable). Only a key that is required — non-null with
  # no default — still fails the decode. Without this, every field added to
  # a document shape made every pre-existing document's entries undecodable.
  coding_keys = []
  coding_keys << discriminator if discriminator
  coding_keys.concat(fields.map { it.fetch('name') })
  lines << ''
  lines << "#{property_pad}private enum CodingKeys: String, CodingKey {"
  lines << "#{property_pad}    case #{coding_keys.join(', ')}"
  lines << "#{property_pad}}"
  lines << ''
  lines << "#{property_pad}public init(from decoder: Decoder) throws {"
  lines << "#{property_pad}    let container = try decoder.container(keyedBy: CodingKeys.self)"
  if discriminator
    lines << "#{property_pad}    self.#{discriminator} = try container.decode(String.self, forKey: .#{discriminator})"
  end
  fields.each do |field|
    field_name = field.fetch('name')
    type = shape_field_type(field, singular_collections: singular_collections, typed_enums: typed_enums)
    plain_enum = typed_enums && field.fetch('enum', nil) && field.fetch('type') != 'array' && field.fetch('type') != 'map'
    default =
      if field.key?('default')
        shape_value_source(field, field.fetch('default', nil), singular_collections: singular_collections, typed_enums: typed_enums)
      end
    lines <<
      if plain_enum && field.fetch('null', nil) == false && default
        "#{property_pad}    self.#{field_name} = (try container.decodeIfPresent(String.self, forKey: .#{field_name})).flatMap(#{type}.init(rawValue:)) ?? #{default}"
      elsif plain_enum && field.fetch('null', nil) != false
        "#{property_pad}    self.#{field_name} = (try container.decodeIfPresent(String.self, forKey: .#{field_name})).flatMap(#{type}.init(rawValue:))"
      elsif field.fetch('null', nil) == false && default
        "#{property_pad}    self.#{field_name} = try container.decodeIfPresent(#{type}.self, forKey: .#{field_name}) ?? #{default}"
      elsif field.fetch('null', nil) == false
        "#{property_pad}    self.#{field_name} = try container.decode(#{type}.self, forKey: .#{field_name})"
      else
        "#{property_pad}    self.#{field_name} = try container.decodeIfPresent(#{type}.self, forKey: .#{field_name})"
      end
  end
  lines << "#{property_pad}}"

  nested = fields.filter_map do |field|
    node = shape_nested_object(field)
    next unless node

    shape_struct_source(
      shape_nested_name(field, singular_collections: singular_collections),
      node.fetch('fields'),
      indent: indent + 4,
      singular_collections: singular_collections,
      typed_enums: typed_enums
    )
  end
  unless nested.empty?
    lines << ''
    lines << nested.join("\n\n")
  end

  unless typed_enums
    accessors = fields.filter_map do |field|
      enum_node = shape_enum_node(field)
      next unless enum_node

      field_name = field.fetch('name')
      enum_name = shape_nested_name(field, singular_collections: singular_collections)
      optional = field.fetch('null', nil) != false
      if field.fetch('type') == 'array'
        <<~SWIFT.chomp
          #{property_pad}public var #{field_name}Value: [#{enum_name}]#{optional ? '?' : ''} {
          #{property_pad}    get { #{field_name}#{optional ? '?' : ''}.compactMap(#{enum_name}.init(rawValue:)) }
          #{property_pad}    set { #{field_name} = newValue#{optional ? '?' : ''}.map(\\.rawValue)#{optional ? '' : ' ?? []'} }
          #{property_pad}}
        SWIFT
      else
        <<~SWIFT.chomp
          #{property_pad}public var #{field_name}Value: #{enum_name}? {
          #{property_pad}    get { #{optional ? "#{field_name}.flatMap" : ''}#{optional ? " { #{enum_name}(rawValue: $0) }" : "#{enum_name}(rawValue: #{field_name})"} }
          #{property_pad}    set {
          #{property_pad}        if let newValue { #{field_name} = newValue.rawValue }
          #{property_pad}        #{optional ? "else { #{field_name} = nil }" : ''}
          #{property_pad}    }
          #{property_pad}}
        SWIFT
      end
    end
    unless accessors.empty?
      lines << ''
      lines << accessors.join("\n\n")
    end
  end

  lines << "#{pad}}"
  lines.join("\n")
end

def shape_source(name, shape)
  unless shape.fetch('discriminator', nil)
    if shape.fetch('type', nil) == 'array'
      # The element type. The column decodes and encodes the whole array, so
      # the element needs no `replicaValue` of its own.
      return shape_struct_source(name, shape.fetch('items').fetch('fields'))
    end
    raise "replica-codegen: shape #{name} must be an object or an array of objects" unless shape.fetch('type', nil) == 'object'

    return shape_struct_source(name, shape.fetch('fields'))
  end

  discriminator = shape.fetch('discriminator')
  variants = shape.fetch('variants').map do |variant|
    variant.merge(
      'swift_name' => variant.fetch('variantName'),
      'swift_case' => lower_camel(variant.fetch('variantName'))
    )
  end

  cases = variants.map { |variant| "    case #{variant.fetch('swift_case')}(#{variant.fetch('swift_name')})" }
  decode_cases = variants.map do |variant|
    "        case #{swift_string(variant.fetch('value'))}: self = .#{variant.fetch('swift_case')}(try #{variant.fetch('swift_name')}(from: decoder))"
  end
  encode_cases = variants.map do |variant|
    "        case .#{variant.fetch('swift_case')}(let value): try value.encode(to: encoder)"
  end
  accessors = variants.map do |variant|
    swift_name = variant.fetch('swift_name')
    swift_case_name = variant.fetch('swift_case')
    "    public var as#{swift_name}: #{swift_name}? { if case .#{swift_case_name}(let value) = self { return value }; return nil }"
  end
  structs = variants.map do |variant|
    shape_struct_source(
      variant.fetch('swift_name'),
      variant.fetch('fields'),
      discriminator: discriminator,
      indent: 4,
      typed_enums: false
    )
  end

  <<~SWIFT
    public enum #{name}: Sendable, Hashable, Codable {
    #{cases.join("\n")}
        case unknown(Shared)

        public struct Shared: Sendable, Hashable, Codable {
            public let #{discriminator}: String

            public init(#{discriminator}: String) {
                self.#{discriminator} = #{discriminator}
            }
        }

    #{structs.join("\n\n")}

        private enum Probe: String, CodingKey {
            case discriminator = #{swift_string(discriminator)}
        }

        public init(from decoder: Decoder) throws {
            let discriminator = try decoder.container(keyedBy: Probe.self)
                .decode(String.self, forKey: .discriminator)
            switch discriminator {
    #{decode_cases.join("\n")}
            default: self = .unknown(try Shared(from: decoder))
            }
        }

        public func encode(to encoder: Encoder) throws {
            switch self {
    #{encode_cases.join("\n")}
            case .unknown(let value): try value.encode(to: encoder)
            }
        }

        public init?(replicaValue: ReplicaValue?) {
            guard let replicaValue,
                  let decoded = try? ReplicaValueCoding.decode(Self.self, from: replicaValue)
            else { return nil }
            self = decoded
        }

        public var replicaValue: ReplicaValue {
            (try? ReplicaValueCoding.encode(self)) ?? .null
        }
    }

    extension #{name} {
    #{accessors.join("\n")}
    }
  SWIFT
end

def shape_definitions(stream)
  columns = base_columns(stream)
  stream.fetch('variants', []).each { |variant| columns.concat(variant_columns(stream, variant)) }
  columns.filter(&:shape_name).uniq(&:shape_name).map do |column|
    shape_source(column.shape_name, column.shape)
  end.join("\n")
end

def support_definitions(stream)
  [enum_source(stream), shape_definitions(stream)].reject(&:empty?).join("\n")
end

def header(stream)
  parts = []
  parts << if stream.fetch('lane') == 'document'
             "document lane · codec #{stream.fetch('codec', nil)}"
           else
             'row lane'
           end
  parts << 'STI' if stream.fetch('sti', nil)
  parts << 'readonly' if stream.fetch('readonly', nil)
  parts << "shard #{stream.fetch('shard', nil)}"
  "// Stream `#{stream.fetch('name')}` · #{parts.join(' · ')}"
end

def guard_lines(columns, indent)
  columns.reject(&:optional).map do |column|
    "#{indent}guard let #{column.storage_name} = #{column.decode_expression} else { return nil }"
  end
end

# The declared push set IS the outbound wire: a `push: false` column
# (server-derived pulls like blobUrl, server-stamped state) NEVER enters
# `encode()` — save() diffs must not echo it back up the wire. Writable
# optionals are full value semantics: nil is an explicit `.null` clear so
# saveRow can distinguish it from a column the model does not author.
# A separate birth snapshot preserves every required local decode field;
# the runtime must never journal that snapshot as the outbound payload.
def encode_lines(columns, indent, prefix: '', snapshot: false)
  (snapshot ? columns : columns.select(&:push)).map do |column|
    reference = "#{prefix}#{column.storage_name}"
    if column.optional
      "#{indent}encoded[\"#{column.name}\"] = #{reference}.map { value in #{column.encode_expression('value')} } ?? .null"
    else
      "#{indent}encoded[\"#{column.name}\"] = #{column.encode_expression(reference)}"
    end
  end
end

# The engine owns ordinary row provenance (`ReplicaStamp.standard` fills
# the ownership/clock columns from the authenticated session at create, and
# the clock again whenever the document moves on the device). A document
# stream with those standard columns therefore exposes only the
# caller-authored slice of its declared push set; no manifest naming hint or
# platform override is involved.
STANDARD_STAMP = {
  'userId' => %w[integer bigint],
  'createdAt' => %w[datetime],
  'updatedAt' => %w[datetime]
}.freeze

def standard_stamp?(stream)
  by_name = stream.fetch('columns').to_h { [it.fetch('name'), it.fetch('type')] }
  stream.fetch('lane') == 'document' && STANDARD_STAMP.all? { |name, types| types.include?(by_name[name]) }
end

def document_create_extension(stream, model, columns)
  return '' unless standard_stamp?(stream)

  authored = columns.select(&:push).reject { |column| STANDARD_STAMP.key?(column.name) }
  parameters = authored.map(&:parameter)
  signature = (["id: String"] + parameters + ["snapshot: Data", "documentPeer: UInt64"]).join(', ')
  entries = authored.map do |column|
    encoded =
      if column.optional
        "#{column.storage_name}.map { value in #{column.encode_expression('value')} } ?? .null"
      else
        column.encode_expression(column.storage_name)
      end
    "                \"#{column.name}\": #{encoded},"
  end

  <<~SWIFT

    extension DocumentStream where Model == #{model} {
        @discardableResult
        public func create(#{signature}) async throws -> Bool {
            try await engine.createDoc(
                stream: Model.streamName,
                id: id,
                seed: snapshot,
                peer: documentPeer,
                data: [
    #{entries.join("\n")}
                ]
            )
        }
    }
  SWIFT
end

# --- templates ---------------------------------------------------------------

DOC_MODEL = ERB.new(<<~'SWIFT', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  <%= header(stream) %>

  import Foundation
  import ReplicaMan

  <%= support_definitions(stream) %>
  public struct <%= model %>: ReplicaDocModel, ReplicaColumns, Equatable {
      public static let streamName = "<%= stream['name'] %>"
  <%= field_enum_source(stream, '    ') %>
  <%= column_enum_source(columns, '    ') %>

      public var id: String
  <% columns.each do |column| -%>
      public var <%= column.storage_name %>: <%= column.declared_type %>
  <% end -%>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.projection_source('    ') %>
  <% end -%>

      public init(id: String<% columns.each do |column| %>, <%= column.parameter %><% end %>) {
          self.id = id
  <% columns.each do |column| -%>
          self.<%= column.storage_name %> = <%= column.storage_name %>
  <% end -%>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.materialization_assignment('        ', column.storage_name) %>
  <% end -%>
      }

      public init?(id: String, data fields: [String: ReplicaValue]) {
  <% guard_lines(columns, '        ').each do |line| -%>
  <%= line %>
  <% end -%>
          self.id = id
  <% columns.each do |column| -%>
          self.<%= column.storage_name %> = <%= column.optional ? column.decode_expression : column.storage_name %>
  <% end -%>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.materialization_assignment('        ', column.optional ? column.decode_expression : column.storage_name) %>
  <% end -%>
      }
  }

  <%= document_create_extension(stream, model, columns) %>
SWIFT

ROW_MODEL = ERB.new(<<~'SWIFT', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  <%= header(stream) %>

  import Foundation
  import ReplicaMan

  <%= support_definitions(stream) %>
  public struct <%= model %>: <%= protocol_name %>, ReplicaColumns, Equatable {
      public static let streamName = "<%= stream['name'] %>"
  <%= field_enum_source(stream, '    ') %>
  <%= column_enum_source(columns, '    ') %>

      public var id: String
  <% columns.each do |column| -%>
      public var <%= column.storage_name %>: <%= column.declared_type %>
  <% end -%>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.projection_source('    ') %>
  <% end -%>

      public init(id: String<% columns.each do |column| %>, <%= column.parameter %><% end %>) {
          self.id = id
  <% columns.each do |column| -%>
          self.<%= column.storage_name %> = <%= column.storage_name %>
  <% end -%>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.materialization_assignment('        ', column.storage_name) %>
  <% end -%>
      }

      public init?(id: String, type: String?, data fields: [String: ReplicaValue]) {
  <% guard_lines(columns, '        ').each do |line| -%>
  <%= line %>
  <% end -%>
          self.id = id
  <% columns.each do |column| -%>
          self.<%= column.storage_name %> = <%= column.optional ? column.decode_expression : column.storage_name %>
  <% end -%>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.materialization_assignment('        ', column.optional ? column.decode_expression : column.storage_name) %>
  <% end -%>
      }

      public var typeName: String? { nil }

      public func encode() -> [String: ReplicaValue] {
  <% lines = encode_lines(columns, '        ') -%>
  <% if lines.empty? -%>
          [:]
  <% else -%>
          var encoded: [String: ReplicaValue] = [:]
  <% lines.each do |line| -%>
  <%= line %>
  <% end -%>
          return encoded
  <% end -%>
      }
  <% unless stream['readonly'] -%>

      public func encodeSnapshot() -> [String: ReplicaValue] {
          var encoded: [String: ReplicaValue] = [:]
  <% encode_lines(columns, '        ', snapshot: true).each do |line| -%>
  <%= line %>
  <% end -%>
          return encoded
      }
  <% end -%>
  }
SWIFT

STI_MODEL = ERB.new(<<~'SWIFT', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  <%= header(stream) %>

  import Foundation
  import ReplicaMan

  <%= support_definitions(stream) %>
  <% variants.each do |variant| -%>
  public struct <%= variant['swift_type'] %>: Identifiable, ReplicaVariant, ReplicaColumns, Sendable, Equatable {
      public static let streamName = "<%= stream['name'] %>"
      public static let wireType = "<%= variant['type'] %>"
  <%= column_enum_source(base + variant_columns(stream, variant), '    ') %>

      public var id: String
  <% (base + variant_columns(stream, variant)).each do |column| -%>
      public var <%= column.storage_name %>: <%= column.declared_type %>
  <% end -%>
  <% (base + variant_columns(stream, variant)).filter(&:materialized_projection?).each do |column| -%>
  <%= column.projection_source('    ') %>
  <% end -%>

      public init(id: String<% (base + variant_columns(stream, variant)).each do |column| %>, <%= column.parameter %><% end %>) {
          self.id = id
  <% (base + variant_columns(stream, variant)).each do |column| -%>
          self.<%= column.storage_name %> = <%= column.storage_name %>
  <% end -%>
  <% (base + variant_columns(stream, variant)).filter(&:materialized_projection?).each do |column| -%>
  <%= column.materialization_assignment('        ', column.storage_name) %>
  <% end -%>
      }
  }

  <% end -%>
  public enum <%= model %>: <%= protocol_name %>, ReplicaColumns, Equatable {
  <% variants.each do |variant| -%>
      case <%= lower_camel(variant['swift_type']) %>(<%= variant['swift_type'] %>)
  <% end -%>

      public static let streamName = "<%= stream['name'] %>"
  <%= field_enum_source(stream, '    ') %>
  <%= column_enum_source(base, '    ') %>

      public init?(id: String, type: String?, data fields: [String: ReplicaValue]) {
  <% guard_lines(base, '        ').each do |line| -%>
  <%= line %>
  <% end -%>
          switch type {
  <% variants.each do |variant| -%>
          case "<%= variant['type'] %>":
  <% guard_lines(variant_columns(stream, variant), '            ').each do |line| -%>
  <%= line %>
  <% end -%>
              self = .<%= lower_camel(variant['swift_type']) %>(<%= variant['swift_type'] %>(
                  id: id<% base.each do |column| %>, <%= column.storage_name %>: <%= column.optional ? column.decode_expression : column.storage_name %><% end %><% if variant_columns(stream, variant).empty? %>
  <% end -%>
  <% variant_columns(stream, variant).each_with_index do |column, index| -%>
  <%= index.zero? ? ",\n" : '' %>                <%= column.storage_name %>: <%= column.optional ? column.decode_expression : column.storage_name %><%= index == variant_columns(stream, variant).size - 1 ? '' : ',' %>
  <% end -%>
              ))
  <% end -%>
          default:
              return nil
          }
      }

      public var id: String {
          switch self {
  <% variants.each do |variant| -%>
          case .<%= lower_camel(variant['swift_type']) %>(let model): return model.id
  <% end -%>
          }
      }

      public var typeName: String? {
          switch self {
  <% variants.each do |variant| -%>
          case .<%= lower_camel(variant['swift_type']) %>: return "<%= variant['type'] %>"
  <% end -%>
          }
      }

      public func encode() -> [String: ReplicaValue] {
  <% encodings = variants.map { |variant| [variant, encode_lines(base + variant_columns(stream, variant), '            ', prefix: 'model.')] } -%>
  <% if encodings.all? { |_, lines| lines.empty? } -%>
          [:]
  <% else -%>
          var encoded: [String: ReplicaValue] = [:]
          switch self {
  <% encodings.each do |variant, lines| -%>
  <% if lines.empty? -%>
          case .<%= lower_camel(variant['swift_type']) %>:
              break
  <% else -%>
          case .<%= lower_camel(variant['swift_type']) %>(let model):
  <% lines.each do |line| -%>
  <%= line %>
  <% end -%>
  <% end -%>
  <% end -%>
          }
          return encoded
  <% end -%>
      }
  <% unless stream['readonly'] -%>

      public func encodeSnapshot() -> [String: ReplicaValue] {
          var encoded: [String: ReplicaValue] = [:]
          switch self {
  <% variants.each do |variant| -%>
  <% lines = encode_lines(base + variant_columns(stream, variant), '            ', prefix: 'model.', snapshot: true) -%>
  <% if lines.empty? -%>
          case .<%= lower_camel(variant['swift_type']) %>:
              break
  <% else -%>
          case .<%= lower_camel(variant['swift_type']) %>(let model):
  <% lines.each do |line| -%>
  <%= line %>
  <% end -%>
  <% end -%>
  <% end -%>
          }
          return encoded
      }
  <% end -%>
  }
SWIFT

CONTAINER = ERB.new(<<~'SWIFT', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  // Manifest version <%= manifest['version'] %> · streams: <%= streams.map { it['name'] }.join(', ') %>

  import Foundation
  import ReplicaMan

  public struct <%= container %>: Sendable {
      public static let schema = ReplicaSchema(streams: [
  <% streams.each do |stream| -%>
          ReplicaStreamSpec(name: "<%= stream['name'] %>", lane: .<%= stream['lane'] %>, readonly: <%= stream['readonly'] %>, shard: "<%= stream['shard'] %>"<% if stream['lane'] == 'document' %>, codec: "<%= stream['codec'] %>"<%= reflections_argument(stream) %><% end %><% if standard_stamp?(stream) %>, stamp: .standard<% end %><%= preconditions_argument(stream) %><%= pushed_argument(stream) %><%= references_argument(stream) %>),
  <% end -%>
      ]<%= indexes_argument(streams) %>, namespace: <%= manifest.fetch('namespace').to_json %>, version: <%= manifest.fetch('schemaVersion') %>)

      public let engine: ReplicaEngine

      public init(engine: ReplicaEngine) {
          self.engine = engine
      }

  <% streams.each do |stream| -%>
      public var <%= lower_camel(camelize(stream['name'])) %>: <%= handle_type(stream) %><<%= model_name(stream) %>> { <%= handle_type(stream) %>(engine: engine) }
  <% end -%>

      /// One local transaction: read, decide and write under the writer.
      @discardableResult
      public func write<T>(_ body: (ReplicaTransaction) throws -> T) throws -> T {
          try engine.write(body)
      }

      @discardableResult
      public func write<T: Sendable>(_ body: @escaping @Sendable (ReplicaTransaction) throws -> T) async throws -> T {
          try await engine.write(body)
      }
  }

  extension ReplicaTransaction {
  <% streams.select { it['lane'] == 'row' }.each do |stream| -%>
      public var <%= lower_camel(camelize(stream['name'])) %>: <%= transaction_handle_type(stream) %><<%= model_name(stream) %>> { <%= stream['readonly'] ? 'readonlyRows' : 'rows' %>(<%= model_name(stream) %>.self) }
  <% end -%>
  }
SWIFT

# One `Field` enum per model — the stream's INDEXED fields (manifest
# `indexes:`), raw value = wire name; every predicate operator takes it. No
# indexes ⇒ `ReplicaNoField`, so no predicate over the model can be formed.
def field_enum_source(stream, indent)
  fields = (stream.fetch('indexes', nil) || []).map { it.fetch('field') }.uniq.sort
  return "#{indent}public typealias Field = ReplicaNoField" if fields.empty?

  cases = fields.map { "#{indent}    case #{swift_case(it)} = #{swift_string(it)}" }
  ["#{indent}public enum Field: String, ReplicaIndexedField {", *cases, "#{indent}}"].join("\n")
end

# One `Column` enum per model — EVERY wire column of the row (a variant's:
# the base's and its own), raw value = wire name — so a host names a column
# by type, never by string (`BlobGate(Theme.self, .logoRef)`).
def column_enum_source(columns, indent)
  cases = ['id', *columns.map(&:name)].uniq.map { "#{indent}    case #{swift_case(it)} = #{swift_string(it)}" }
  ["#{indent}public enum Column: String, Sendable {", *cases, "#{indent}}"].join("\n")
end

def indexes_argument(streams)
  specs = streams.flat_map do |stream|
    (stream.fetch('indexes', nil) || []).map do |index|
      "        ReplicaIndexSpec(stream: #{swift_string(stream.fetch('name'))}, field: #{swift_string(index.fetch('field', nil))}, kind: .#{index.fetch('kind', nil)}),"
    end
  end
  specs.empty? ? '' : ", indexes: [\n#{specs.join("\n")}\n    ]"
end

def reflections_argument(stream)
  reflections = stream.fetch('columns').select { it.fetch('reflects', nil) }.map do |column|
    path = column.fetch('reflects', nil).map { swift_string(it) }.join(', ')
    "ReplicaReflection(field: #{swift_string(column.fetch('name'))}, path: [#{path}])"
  end
  reflections.empty? ? '' : ", reflections: [#{reflections.join(', ')}]"
end

def preconditions_argument(stream)
  names = stream.fetch('columns').select { it.fetch('precondition', nil) }.map { swift_string(it.fetch('name')) }
  names.empty? ? '' : ", preconditions: [#{names.join(', ')}]"
end

# The columns a device may send, every variant's included — a row a sync gate
# held leaves as these fields of its state, and the push door refuses any
# other. One set per stream, so a column two variants flag differently fails
# the codegen rather than the wire.
def pushed_argument(stream)
  return '' if stream.fetch('readonly', nil) || stream.fetch('lane') == 'document'

  columns = stream.fetch('columns') + (stream.fetch('variants', nil) || []).flat_map { it.fetch('columns') }
  flags = columns.group_by { it.fetch('name') }.transform_values { |named| named.map { it.fetch('push', nil) }.uniq }
  split = flags.select { |_, values| values.size > 1 }.keys
  raise "#{stream.fetch('name')}: variants disagree on push for #{split.join(', ')}" unless split.empty?

  names = flags.select { |_, values| values == [true] }.keys.sort.map { swift_string(it) }
  ", pushed: [#{names.join(', ')}]"
end

def transaction_handle_type(stream)
  stream.fetch('readonly', nil) ? 'TransactionReadonlyRows' : 'TransactionRows'
end

def handle_type(stream)
  if stream.fetch('lane') == 'document'
    stream.fetch('readonly', nil) ? 'ReadonlyDocumentStream' : 'DocumentStream'
  else
    'RowStream'
  end
end

def snake_case(name)
  name
    .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
    .gsub(/([a-z\d])([A-Z])/, '\1_\2')
    .downcase
end

def document_root_field(name, shape)
  shape.merge('name' => name, 'null' => false)
end

def document_registry_shape?(shape)
  shape.fetch('type', nil) == 'array' && shape.dig('items', 'type') == 'object'
end

def document_shape_descriptor(shape)
  return '.string' if shape.fetch('enum', nil)

  case shape.fetch('type')
  when 'string', 'text', 'hex_color', 'datetime', 'date', 'time'
    '.string'
  when 'integer', 'bigint'
    '.integer'
  when 'float', 'decimal'
    '.number'
  when 'boolean'
    '.boolean'
  when 'json', 'jsonb'
    '.json'
  when 'array'
    ".array(#{document_shape_descriptor(shape.fetch('items'))})"
  when 'map'
    ".map(#{document_shape_descriptor(shape.fetch('values'))})"
  when 'object'
    fields = shape.fetch('fields').map do |field|
      "#{swift_string(snake_case(field.fetch('name')))}: #{document_shape_descriptor(field)}"
    end
    ".object([#{fields.join(', ')}])"
  else
    '.json'
  end
end

def document_projection_extensions(model, shapes)
  extensions = shapes.filter_map do |root_name, shape|
    root_field = document_root_field(root_name, shape)
    nested_name = shape_nested_name(root_field, singular_collections: true)
    qualified_name = "#{model}Document.#{nested_name}"

    if document_registry_shape?(shape)
      descriptor = document_shape_descriptor(shape.fetch('items'))
      <<~SWIFT
        extension #{qualified_name} {
            /// An entry of the document's `#{root_name}` registry: its key and its fields.
            public init?(key: String, documentFields: [String: ReplicaValue]) {
                var fields = documentFields
                fields["key"] = .string(key)
                guard let decoded = #{model}DocumentCoding.decode(fields, as: Self.self) else { return nil }
                self = decoded
            }

            /// The entry's fields as the document holds them — the key is its
            /// slot's, never a field.
            public var documentFields: [String: ReplicaValue] {
                var fields = #{model}DocumentCoding.encode(self, shape: #{descriptor})
                fields.removeValue(forKey: "key")
                return fields
            }
        }
      SWIFT
    elsif shape.fetch('type', nil) == 'object'
      descriptor = document_shape_descriptor(shape)
      <<~SWIFT
        extension #{qualified_name} {
            public init?(documentFields: [String: ReplicaValue]) {
                guard let decoded = #{model}DocumentCoding.decode(documentFields, as: Self.self) else { return nil }
                self = decoded
            }

            public var documentFields: [String: ReplicaValue] {
                #{model}DocumentCoding.encode(self, shape: #{descriptor})
            }
        }
      SWIFT
    end
  end
  extensions.join("\n")
end

def document_projection_source(model, stream)
  shapes = stream.fetch('shapes')
  defaults = stream.fetch('default', {})
  registries = []
  readers = []
  guards = []
  typed = []
  roots = []
  shapes.each do |root_name, shape|
    root_field = document_root_field(root_name, shape)
    nested_name = shape_nested_name(root_field, singular_collections: true)
    name = swift_string(root_name)

    if document_registry_shape?(shape)
      registries << name
      order = Array(defaults.fetch(root_name, [])).filter_map { it.fetch('key', nil) }.map { swift_string(it) }
      readers << <<~SWIFT
        /// The `#{root_name}` registry off a materialized document — a default
        /// entry's place first, then by key. This display read omits invalid
        /// entries. Use the complete document initializer before round-tripping.
        public static func #{root_name}(in document: ReplicaValue) -> [#{nested_name}] {
            registry(document[#{name}], order: [#{order.join(', ')}])
                .compactMap { #{nested_name}(key: $0.key, documentFields: $0.fields) }
        }
      SWIFT
      guards << "let #{root_name} = Self.strictRegistry(document[#{name}], order: [#{order.join(', ')}], decode: #{nested_name}.init(key:documentFields:))"
      typed << "#{root_name}: #{root_name}"
      roots << "#{name}: .object(Dictionary(#{root_name}.map { ($0.key, .object($0.documentFields)) }, uniquingKeysWith: { _, last in last }))"
    elsif shape.fetch('type', nil) == 'object'
      readers << <<~SWIFT
        public static func #{root_name}(in document: ReplicaValue) -> #{nested_name}? {
            guard document[#{name}] == nil || document[#{name}]?.object != nil else { return nil }
            return #{nested_name}(documentFields: document[#{name}]?.object ?? [:])
        }
      SWIFT
      guards << "let #{root_name} = Self.#{root_name}(in: document)"
      typed << "#{root_name}: #{root_name}"
      roots << "#{name}: .object(#{root_name}.documentFields)"
    else
      raise "replica-codegen: document root #{root_name} must be an object or keyed object array"
    end
  end

  guard_source = guards.empty? ? '' : "        guard #{guards.join(",\n              ")} else { return nil }\n"
  registry_reader =
    if registries.empty?
      ''
    else
      <<~SWIFT

        private static func strictRegistry<Value>(
            _ value: ReplicaValue?, order: [String], decode: (String, [String: ReplicaValue]) -> Value?
        ) -> [Value]? {
            guard let value else { return [] }
            guard let fields = value.object, fields.values.allSatisfy({ $0.object != nil }) else { return nil }
            var result: [Value] = []
            for entry in registry(value, order: order) {
                guard let decoded = decode(entry.key, entry.fields) else { return nil }
                result.append(decoded)
            }
            return result
        }

        private static func registry(_ value: ReplicaValue?, order: [String]) -> [(key: String, fields: [String: ReplicaValue])] {
            (value?.object ?? [:])
                .map { (key: $0.key, fields: $0.value.object ?? [:]) }
                .sorted { lhs, rhs in
                    let left = order.firstIndex(of: lhs.key) ?? order.count
                    let right = order.firstIndex(of: rhs.key) ?? order.count
                    return left == right ? lhs.key < rhs.key : left < right
                }
        }
      SWIFT
    end
  indent = ->(text) { text.lines.map { it.strip.empty? ? "\n" : "    #{it}" }.join }

  <<~SWIFT
    extension #{model}Document {
        /// The roots the manifest declares as keyed registries — `key → entry`,
        /// each entry merging field by field.
        public static let registries: Set<String> = [#{registries.join(', ')}]

    #{readers.map { indent.call(it) }.join("\n")}
        /// The typed document off its materialized value (the adapter's
        /// `LoroDocument.value`).
        public init?(document: ReplicaValue) {
            guard document.object != nil else { return nil }
    #{guard_source}        self.init(
                #{typed.join(",\n            ")}
            )
        }

        /// The document as the roots its birth seeds.
        public var documentRoots: [String: ReplicaValue] {
            [
                #{roots.join(",\n            ")},
            ]
        }
    #{indent.call(registry_reader)}}
  SWIFT
end

def document_coding_source(model)
  <<~SWIFT
    private enum #{model}DocumentCoding {
        indirect enum Shape {
            case string
            case integer
            case number
            case boolean
            case json
            case object([String: Shape])
            case array(Shape)
            case map(Shape)
        }

        // Direct tree decode (ReplicaValueCoding) — the JSON round-trip this
        // replaces ground on main in hang profiles (TextStyle(from:)
        // through JSONEncoder+JSONDecoder per typed read). Encode below stays
        // on the shaped JSON path: the manifest Shape drives .integer vs
        // .number on the way out.
        static func decode<Value: Decodable>(
            _ fields: [String: ReplicaValue],
            as type: Value.Type
        ) -> Value? {
            ReplicaDiagnostics.attemptOnce("decode a document \\(Value.self)") {
                try ReplicaValueCoding.decode(type, from: .object(fields))
            }
        }

        static func encode<Value: Encodable>(
            _ value: Value,
            shape: Shape
        ) -> [String: ReplicaValue] {
            let encoder = JSONEncoder()
            encoder.keyEncodingStrategy = .convertToSnakeCase
            let object: Any
            do {
                object = try JSONSerialization.jsonObject(with: encoder.encode(value))
            } catch {
                preconditionFailure("generated document value did not encode: \\(error)")
            }
            guard case let .object(fields) = documentValue(object, shape: shape) else {
                preconditionFailure("generated document value did not match its manifest shape")
            }
            return fields
        }

        private static func documentValue(_ value: Any, shape: Shape) -> ReplicaValue {
            if value is NSNull { return .null }

            switch shape {
            case .string:
                guard let value = value as? String else { return .null }
                return .string(value)
            case .integer:
                guard let value = value as? NSNumber else { return .null }
                return .integer(value.int64Value)
            case .number:
                guard let value = value as? NSNumber else { return .null }
                return .number(value.doubleValue)
            case .boolean:
                guard let value = value as? NSNumber else { return .null }
                return .bool(value.boolValue)
            case let .object(fields):
                guard let value = value as? [String: Any] else { return .null }
                return .object(Dictionary(uniqueKeysWithValues: fields.map { key, shape in
                    let child = value[key].map { documentValue($0, shape: shape) } ?? .null
                    return (key, child)
                }))
            case let .array(item):
                guard let value = value as? [Any] else { return .null }
                return .array(value.map { documentValue($0, shape: item) })
            case let .map(item):
                guard let value = value as? [String: Any] else { return .null }
                return .object(value.mapValues { documentValue($0, shape: item) })
            case .json:
                return jsonValue(value)
            }
        }

        private static func jsonValue(_ value: Any) -> ReplicaValue {
            switch value {
            case is NSNull:
                return .null
            case let value as String:
                return .string(value)
            case let value as [Any]:
                return .array(value.map(jsonValue))
            case let value as [String: Any]:
                return .object(value.mapValues(jsonValue))
            case let value as NSNumber:
                return String(cString: value.objCType) == "c"
                    ? .bool(value.boolValue)
                    : .number(value.doubleValue)
            default:
                return .null
            }
        }
    }
  SWIFT
end

def document_source(stream)
  shapes = stream.fetch('shapes')
  model = model_name(stream)
  root_fields = shapes.map { |name, shape| document_root_field(name, shape) }
  body = shape_struct_source(
    "#{model}Document",
    root_fields,
    singular_collections: true
  )

  <<~SWIFT
    // Generated by replica-codegen — DO NOT EDIT.
    // Document value for stream `#{stream.fetch('name')}` · codec #{stream.fetch('codec', nil)}
    // #{model}Document owns generated values; #{model}Doc edits the document through the Loro adapter.

    import Foundation
    import ReplicaMan

    #{body}

    #{document_coding_source(model)}
    #{document_projection_extensions(model, shapes)}
    #{document_projection_source(model, stream)}
  SWIFT
end

def document_defaults_source(stream)
  defaults = stream.fetch('default', {})
  return if defaults.empty?

  model = model_name(stream)
  lines = defaults.map do |root_name, value|
    shape = stream.fetch('shapes').fetch(root_name)
    root_field = document_root_field(root_name, shape)
    nested_name = shape_nested_name(root_field, singular_collections: true)
    type = shape_field_type(root_field, singular_collections: true)
    qualified_type =
      if shape.fetch('type', nil) == 'object'
        "#{model}Document.#{nested_name}"
      elsif shape.fetch('type', nil) == 'array'
        "[#{model}Document.#{nested_name}]"
      else
        type
      end
    expression = shape_value_source(
      shape,
      value,
      type_reference: shape.fetch('type', nil) == 'object' ? "#{model}Document.#{nested_name}" : nil,
      singular_collections: true
    )
    annotation = shape.fetch('type', nil) == 'array' ? ": #{qualified_type}" : ''
    "    public static let #{root_name}#{annotation} = #{expression}"
  end

  <<~SWIFT
    // Generated by replica-codegen — DO NOT EDIT.
    // Defaults for document stream `#{stream.fetch('name')}`.

    import Foundation

    public enum #{model}Defaults {
    #{lines.join("\n")}
    }
  SWIFT
end


def references_argument(stream)
  references = stream.fetch('references', []).map do |reference|
    fields = ['name: ' + reference.fetch('name').inspect, 'stream: ' + reference.fetch('stream').inspect]
    fields << 'field: ' + reference.fetch('field').inspect if reference.key?('field')
    fields << 'keySegment: ' + reference.fetch('keySegment').to_s if reference.key?('keySegment')
    fields << 'keyPrefix: ' + reference.fetch('keyPrefix').inspect if reference.key?('keyPrefix')
    fields << 'optional: ' + reference.fetch('optional', false).to_s
    'ReplicaReferenceSpec(' + fields.join(', ') + ')'
  end
  result = references.empty? ? '' : ", references: [#{references.join(', ')}]"
  result += ', lifetimeFrom: ' + stream.fetch('lifetimeFrom').inspect if stream.key?('lifetimeFrom')
  result
end

# --- emit --------------------------------------------------------------------

streams = manifest.fetch('streams')
out = options[:out]
FileUtils.mkdir_p(out)

streams.each do |stream|
  model = model_name(stream)
  source =
    if stream.fetch('lane') == 'document'
      DOC_MODEL.result_with_hash(stream: stream, model: model, columns: base_columns(stream))
    elsif stream.fetch('sti', nil)
      STI_MODEL.result_with_hash(
        stream: stream, model: model, base: base_columns(stream), variants: stream.fetch('variants', nil),
        protocol_name: stream.fetch('readonly', nil) ? 'ReplicaRowModel' : 'ReplicaWritableRowModel'
      )
    else
      ROW_MODEL.result_with_hash(
        stream: stream, model: model, columns: base_columns(stream),
        protocol_name: stream.fetch('readonly', nil) ? 'ReplicaRowModel' : 'ReplicaWritableRowModel'
      )
    end
  File.write(File.join(out, "#{model}.swift"), source)
end

File.write(
  File.join(out, "#{options[:name]}.swift"),
  CONTAINER.result_with_hash(manifest: manifest, streams: streams, container: options[:name])
)

if options[:document_out]
  document_out = options[:document_out]
  FileUtils.mkdir_p(document_out)
  streams.filter { it.fetch('lane') == 'document' && it.fetch('shapes', nil) }.each do |stream|
    model = model_name(stream)
    File.write(File.join(document_out, "#{model}Document.swift"), document_source(stream))
    defaults = document_defaults_source(stream)
    File.write(File.join(document_out, "#{model}+Defaults.swift"), defaults) if defaults
  end
end
