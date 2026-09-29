#!/usr/bin/env ruby
# frozen_string_literal: true

# Native kotlin syntax emitter. Manifest semantics and output installation
# belong to codegen/lib; execute through codegen/bin/replica-codegen.

require 'erb'
require 'json'
require 'net/http'
require 'optparse'
require 'fileutils'
require 'uri'

# --- naming ------------------------------------------------------------------



# An enum ENTRY cannot be `name`/`ordinal`/`entries`/`values`/`valueOf` (they
# collide with what Kotlin synthesizes on every enum) and cannot be a keyword
# either. SCREAMING_SNAKE is collision-free by construction and is what Kotlin
# enums read like; the wire value stays in `rawValue`.
def kotlin_enum_case(value)
  identifier = value.to_s.gsub(/[-_\s]+/, '_').gsub(/[^0-9A-Za-z_]+/, '')
  identifier = identifier.gsub(/([a-z])([A-Z])/, '\1_\2').gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2').upcase
  identifier = "_#{identifier}" if identifier.match?(/\A\d/)
  identifier
end

def kotlin_string(value)
  JSON.generate(value.to_s)
end

# Streams are table-plural by convention; the model is the singular. Naive on
# purpose (stdlib only): -ies → -y, -ches/-shes/-xes/-ses → drop -es, then
# drop a trailing -s.

# Two STI streams can flatten the same demodulized subclass into one
# top-level Kotlin type (Shop::Assets::ImageAsset vs Gallery::Assets::ImageAsset).
# Only the emitted Kotlin names move — the wire type stays the manifest's.

# Stream names are honest (stream == table, boot-enforced server-side), so
# the Kotlin model is always the singularized stream name — no rename map.

# --- type mapping ------------------------------------------------------------

KOTLIN_TYPES = {
  'string' => 'String', 'text' => 'String',
  'hex_color' => 'String',
  'integer' => 'Long', 'bigint' => 'Long',
  'float' => 'Double', 'decimal' => 'Double',
  'boolean' => 'Boolean',
  'datetime' => 'String', 'date' => 'String', 'time' => 'String',
}.freeze

def kotlin_type(wire_type)
  KOTLIN_TYPES.fetch(wire_type, 'ReplicaValue')
end

def wire_accessor(wire_type)
  case kotlin_type(wire_type)
  when 'String' then '?.string'
  when 'Long' then '?.long'
  when 'Double' then '?.number'
  when 'Boolean' then '?.bool'
  else ''
  end
end

def wire_encode_expression(wire_type, variable)
  case kotlin_type(wire_type)
  when 'String' then "ReplicaValue.Str(#{variable})"
  when 'Long' then "ReplicaValue.signedInteger(#{variable})"
  when 'Double' then "ReplicaValue.Num(#{variable})"
  when 'Boolean' then "ReplicaValue.Bool(#{variable})"
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
  # array column carries its structure under `shapes` instead and keeps the
  # raw-JSON + typed-projection treatment.
  def list?
    wire_type == 'array' && !items.nil?
  end

  def item_type
    items.fetch('type')
  end

  def base_type
    return enum_name if enum_name
    return shape_kotlin_type({ 'type' => 'array', 'items' => items }, nil, typed_enums: false) if list?
    return array_shape? ? "List<#{shape_name}>" : shape_name if typed_shape?

    kotlin_type(wire_type)
  end

  def declared_type
    optional ? "#{base_type}?" : base_type
  end

  def parameter
    optional ? "#{storage_name}: #{declared_type} = null" : "#{storage_name}: #{declared_type}"
  end

  def decode_expression
    value = "data[\"#{name}\"]"
    return "(#{value}?.string?.let { #{enum_name}.fromRaw(it) })" if enum_name
    return "(#{value}?.items?.mapNotNull { it#{wire_accessor(item_type).delete_prefix('?')} })" if list?

    return "(#{value}?.let { raw -> runCatching { ReplicaValueCoding.decode(#{shape_serializer}, raw) }.getOrNull() })" if typed_shape?

    "#{value}#{wire_accessor(wire_type)}"
  end

  def encode_expression(variable)
    return "ReplicaValue.Str(#{variable}.rawValue)" if enum_name
    return "ReplicaValue.Arr(#{variable}.map { #{wire_encode_expression(item_type, 'it')} })" if list?

    return "ReplicaValueCoding.encode(#{shape_serializer}, #{variable})" if typed_shape?

    wire_encode_expression(wire_type, variable)
  end

  def projection_source(indent)
    return unless materialized_projection?

    setter = optional ? "#{storage_name} = newValue?.replicaValue" : "#{storage_name} = newValue?.replicaValue ?: ReplicaValue.Obj(emptyMap())"
    <<~KOTLIN.chomp
      #{indent}public var #{name}: #{shape_name}? = #{shape_name}.fromReplicaValue(#{storage_name})
      #{indent}    set(newValue) {
      #{indent}        field = newValue
      #{indent}        #{setter}
      #{indent}    }
    KOTLIN
  end

  def typed_shape?
    shape_name && !materialized_projection?
  end

  def shape_serializer
    array_shape? ? "ListSerializer(#{shape_name}.serializer())" : "#{shape_name}.serializer()"
  end

  def materialized_projection?
    shape_name && shape&.key?('discriminator')
  end

  # An array-of-object column (`colors`). The doc lane already emits
  # typed `List<Slide>` from exactly this manifest node; this is the row lane
  # reaching the same generator instead of falling through to `ReplicaValue`.
  def array_shape?
    shape && shape.fetch('type', nil) == 'array' && shape.dig('items', 'type') == 'object'
  end
end

def check_column!(stream, column)
    if column.fetch('items', nil) && !KOTLIN_TYPES.key?(column.dig('items', 'type'))
      raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is an array of " \
            "#{column.dig('items', 'type').inspect} — only scalar item types are generated"
    end

    if column.fetch('pull', nil) == false && column.fetch('null', nil) == false
      raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is a required intake — " \
            'a pulled row can never satisfy its decode guard'
    end

    creatable = stream.fetch('columns').any? { |c| c.fetch('name') == 'createdAt' && c.fetch('push', nil) }
    if creatable && column.fetch('push', nil) == false && column.fetch('null', nil) == false && !column.fetch('reflects', nil)
      raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is a required pull-only column — " \
            'a created row can never satisfy its own decode guard'
    end
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

# Variant (typed-store) columns are ALWAYS optional: store keys may simply be
# absent from a row.
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
      shape_name: shape_type_name(variant.fetch('kotlin_type'), column),
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
      "    #{kotlin_enum_case(value)}(#{kotlin_string(value)}),"
    end.join("\n")
    <<~KOTLIN
      public enum class #{definition[:name]}(public val rawValue: String) {
      #{cases}
          ;

          public companion object {
              public fun fromRaw(rawValue: String?): #{definition[:name]}? =
                  entries.firstOrNull { it.rawValue == rawValue }
          }
      }
    KOTLIN
  end.join("\n")
end

def shape_nested_name(field, singular_collections: false)
  name = field.fetch('name')
  name = singularize(name) if singular_collections && %w[array map].include?(field.fetch('type'))
  camelize(name)
end

def shape_kotlin_type(node, nested_name, typed_enums: true)
  return nested_name if typed_enums && node.fetch('enum', nil)

  case node.fetch('type')
  when 'array'
    items = node.fetch('items')
    "List<#{shape_kotlin_type(items, nested_name, typed_enums: typed_enums)}#{'?' if items.fetch('null', nil)}>"
  when 'map'
    "Map<String, #{shape_kotlin_type(node.fetch('values'), nested_name, typed_enums: typed_enums)}>"
  when 'object'
    nested_name
  else
    kotlin_type(node.fetch('type'))
  end
end

def shape_field_type(field, singular_collections: false, typed_enums: true)
  shape_kotlin_type(
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

# The entry name is SCREAMING_SNAKE; the WIRE value is the serial name. Swift
# gets that from `enum X: String, Codable` for free — kotlinx serializes an
# enum by its entry name unless told otherwise, so a shape enum reached
# through a List/Map (no per-field codec exists for those) would otherwise
# read and write "UNDERLINE" where the wire says "underline".
def shape_enum_source(name, values, indent)
  pad = ' ' * indent
  cases = values.map do |value|
    "#{pad}    @SerialName(#{kotlin_string(value)}) #{kotlin_enum_case(value)}(#{kotlin_string(value)}),"
  end
  ([
    "#{pad}@Serializable",
    "#{pad}public enum class #{name}(public val rawValue: String) {",
  ] + cases + [
    "#{pad}    ;",
    '',
    "#{pad}    public companion object {",
    "#{pad}        public fun fromRaw(rawValue: String): #{name}? = entries.find { it.rawValue == rawValue }",
    "#{pad}    }",
    "#{pad}}",
  ]).join("\n")
end

# The tolerant enum codec Swift gets for free from `flatMap(init(rawValue:))`:
# a VALUE this build does not know reads as the declared default (null when
# the field is nullable), never as a thrown decode.
def shape_enum_codec_source(name, enum_name, fallback, indent)
  pad = ' ' * indent
  nullable = fallback == 'null'
  type = nullable ? "#{enum_name}?" : enum_name
  <<~KOTLIN.chomp
    #{pad}public object #{name} : KSerializer<#{type}> {
    #{pad}    override val descriptor: SerialDescriptor =
    #{pad}        PrimitiveSerialDescriptor(#{kotlin_string(name)}, PrimitiveKind.STRING)#{nullable ? '.nullable' : ''}

    #{pad}    override fun serialize(encoder: Encoder, value: #{type}) {
    #{pad}        #{nullable ? 'if (value == null) encoder.encodeNull() else encoder.encodeString(value.rawValue)' : 'encoder.encodeString(value.rawValue)'}
    #{pad}    }

    #{pad}    override fun deserialize(decoder: Decoder): #{type} =
    #{pad}        #{nullable ? "if (decoder.decodeNotNullMark()) #{enum_name}.fromRaw(decoder.decodeString()) else decoder.decodeNull()" : "#{enum_name}.fromRaw(decoder.decodeString()) ?: #{fallback}"}
    #{pad}}
  KOTLIN
end

def replica_value_literal(value)
  case value
  when nil
    'ReplicaValue.Null'
  when true, false
    "ReplicaValue.Bool(#{value})"
  when Integer
    "ReplicaValue.signedInteger(#{value == -9223372036854775808 ? 'Long.MIN_VALUE' : "#{value}L"})"
  when Numeric
    "ReplicaValue.Num(#{value.to_f})"
  when String
    "ReplicaValue.Str(#{kotlin_string(value)})"
  when Array
    "ReplicaValue.Arr(listOf(#{value.map { replica_value_literal(it) }.join(', ')}))"
  when Hash
    pairs = value.map { |key, item| "#{kotlin_string(key)} to #{replica_value_literal(item)}" }
    pairs.empty? ? 'ReplicaValue.Obj(emptyMap())' : "ReplicaValue.Obj(mapOf(#{pairs.join(', ')}))"
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
  return 'null' if value.nil?
  # Swift infers a leading-dot enum case; Kotlin needs the type, and a nested
  # enum read from OUTSIDE its struct needs the whole path — so every literal
  # carries the qualified reference its parent computed.
  return typed_enums ? "#{type_reference}.#{kotlin_enum_case(value)}" : kotlin_string(value) if node.fetch('enum', nil)

  case node.fetch('type')
  when 'array'
    item = node.fetch('items')
    children = value.map do
      shape_value_source(
        item,
        it,
        type_reference: type_reference,
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
    end
    children.empty? ? 'emptyList()' : "listOf(#{children.join(', ')})"
  when 'map'
    item = node.fetch('values')
    pairs = value.map do |key, child|
      child_source = shape_value_source(
        item,
        child,
        type_reference: type_reference,
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
      "#{kotlin_string(key)} to #{child_source}"
    end
    pairs.empty? ? 'emptyMap()' : "mapOf(#{pairs.join(', ')})"
  when 'object'
    constructor = type_reference || shape_nested_name(node, singular_collections: singular_collections)
    fields = node.fetch('fields').filter_map do |field|
      next unless value.key?(field.fetch('name'))

      child_reference = "#{constructor}.#{shape_nested_name(field, singular_collections: singular_collections)}"
      expression = shape_value_source(
        field,
        value.fetch(field.fetch('name')),
        type_reference: child_reference,
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
      "#{field.fetch('name')} = #{expression}"
    end
    "#{constructor}(#{fields.join(', ')})"
  when 'json', 'jsonb'
    replica_value_literal(value)
  when 'string', 'text', 'hex_color', 'datetime', 'date', 'time'
    kotlin_string(value)
  when 'boolean'
    value.to_s
  when 'integer', 'bigint'
    "#{value.to_i}L"
  when 'float', 'decimal'
    value.to_f.to_s
  else
    replica_value_literal(value)
  end
end

# A shape enum default names its own nested enum, so the literal has to be
# qualified by the type the field declares — `State.queued`, not `.queued`.
def shape_enum_default(field, singular_collections: false)
  return nil unless field.key?('default')
  return 'null' if field.fetch('default', nil).nil?

  "#{shape_nested_name(field, singular_collections: singular_collections)}.#{kotlin_enum_case(field.fetch('default', nil))}"
end

def shape_property(field, singular_collections: false, typed_enums: true)
  name = field.fetch('name')
  type = shape_field_type(
    field,
    singular_collections: singular_collections,
    typed_enums: typed_enums
  )
  optional = field.fetch('null', nil) != false
  # Swift's `init(from:)` reads a NULLABLE field with `decodeIfPresent` and
  # ignores its declared default; only a non-null field falls back to one.
  # Kotlin has ONE default per property and decode fidelity wins.
  default =
    if optional
      'null'
    elsif field.key?('default')
      shape_value_source(
        field,
        field.fetch('default', nil),
        type_reference: shape_nested_name(field, singular_collections: singular_collections),
        singular_collections: singular_collections,
        typed_enums: typed_enums
      )
    end
  "#{name}: #{type}#{optional ? '?' : ''}#{default ? " = #{default}" : ''}"
end

def shape_struct_source(
  name,
  fields,
  discriminator: nil,
  indent: 0,
  singular_collections: false,
  replica_bridge: false,
  typed_enums: true,
  variant_of: nil,
  companion: false,
  companion_nested: false,
  enum_codecs: true
)
  pad = ' ' * indent
  member_pad = ' ' * (indent + 4)
  lines = ["#{pad}@Serializable"]
  lines << "#{pad}public data class #{name}("
  lines << "#{member_pad}public val #{discriminator}: String," if discriminator

  codecs = []
  fields.each do |field|
    annotation = nil
    if enum_codecs && typed_enums && field.fetch('enum', nil) && !%w[array map].include?(field.fetch('type'))
      enum_name = shape_nested_name(field, singular_collections: singular_collections)
      fallback = field.fetch('null', nil) != false ? 'null' : shape_enum_default(field, singular_collections: singular_collections)
      if fallback
        codec_name = "#{enum_name}Codec"
        codecs << { name: codec_name, enum_name: enum_name, fallback: fallback }
        annotation = "@Serializable(with = #{codec_name}::class)"
      end
    end
    lines << "#{member_pad}#{annotation}" if annotation
    lines << "#{member_pad}public val #{shape_property(field, singular_collections: singular_collections, typed_enums: typed_enums)},"
  end
  body = []

  enums = fields.filter_map do |field|
    enum_node = shape_enum_node(field)
    next unless enum_node

    shape_enum_source(
      shape_nested_name(field, singular_collections: singular_collections),
      enum_node.fetch('enum'),
      indent + 4
    )
  end
  body << enums.join("\n\n") unless enums.empty?

  codecs.uniq { it[:name] }.each do |codec|
    body << shape_enum_codec_source(codec[:name], codec[:enum_name], codec[:fallback], indent + 4)
  end

  nested = fields.filter_map do |field|
    node = shape_nested_object(field)
    next unless node

    shape_struct_source(
      shape_nested_name(field, singular_collections: singular_collections),
      node.fetch('fields'),
      indent: indent + 4,
      singular_collections: singular_collections,
      typed_enums: typed_enums,
      companion: companion_nested,
      enum_codecs: enum_codecs
    )
  end
  body << nested.join("\n\n") unless nested.empty?

  unless typed_enums
    accessors = fields.filter_map do |field|
      enum_node = shape_enum_node(field)
      next unless enum_node

      field_name = field.fetch('name')
      enum_name = shape_nested_name(field, singular_collections: singular_collections)
      optional = field.fetch('null', nil) != false
      if field.fetch('type') == 'array'
        item = field.dig('items', 'null') ? "it?.let { #{enum_name}.fromRaw(it) }" : "#{enum_name}.fromRaw(it)"
        <<~KOTLIN.chomp
          #{member_pad}public val #{field_name}Value: List<#{enum_name}>#{optional ? '?' : ''}
          #{member_pad}    get() = #{field_name}#{optional ? '?' : ''}.mapNotNull { #{item} }
        KOTLIN
      else
        read = optional ? "#{field_name}?.let { #{enum_name}.fromRaw(it) }" : "#{enum_name}.fromRaw(#{field_name})"
        <<~KOTLIN.chomp
          #{member_pad}public val #{field_name}Value: #{enum_name}?
          #{member_pad}    get() = #{read}
        KOTLIN
      end
    end
    body << accessors.join("\n\n") unless accessors.empty?
  end

  if replica_bridge
    body << <<~KOTLIN.chomp
      #{member_pad}public val replicaValue: ReplicaValue
      #{member_pad}    get() = runCatching {
      #{member_pad}        ReplicaValueCoding.encode(serializer(), this)
      #{member_pad}    }.getOrDefault(ReplicaValue.Null)

      #{member_pad}public companion object {
      #{member_pad}    public fun fromReplicaValue(value: ReplicaValue?): #{name}? {
      #{member_pad}        if (value == null) return null
      #{member_pad}        return runCatching { ReplicaValueCoding.decode(serializer(), value) }.getOrNull()
      #{member_pad}    }
      #{member_pad}}
    KOTLIN
  end

  # Kotlin extensions hang off a companion that must exist: the document
  # half declares `X.Companion.fromDocumentFields(...)`.
  body << "#{member_pad}public companion object" if companion

  suffix = variant_of ? " : #{variant_of}" : ''
  if body.empty?
    lines << "#{pad})#{suffix}"
  else
    lines << "#{pad})#{suffix} {"
    lines << body.join("\n\n")
    lines << "#{pad}}"
  end
  lines.join("\n")
end

def shape_source(name, shape)
  unless shape.fetch('discriminator', nil)
    if shape.fetch('type', nil) == 'array'
      # The element type. Arrays bridge at the ARRAY level in
      # `projection_source`, so the element needs no `replicaValue` of its own.
      return shape_struct_source(name, shape.fetch('items').fetch('fields'))
    end
    raise "replica-codegen: shape #{name} must be an object or an array of objects" unless shape.fetch('type', nil) == 'object'

    return shape_struct_source(name, shape.fetch('fields'), replica_bridge: true)
  end

  discriminator = shape.fetch('discriminator')
  variants = shape.fetch('variants').map do |variant|
    variant.merge(
      'kotlin_name' => variant.fetch('variantName'),
      'kotlin_case' => lower_camel(variant.fetch('variantName'))
    )
  end

  decode_cases = variants.map do |variant|
    "            #{kotlin_string(variant.fetch('value'))} -> ReplicaValueCoding.decode(#{name}.#{variant.fetch('kotlin_name')}.serializer(), value)"
  end
  encode_cases = variants.map do |variant|
    "            is #{name}.#{variant.fetch('kotlin_name')} -> encoder.encodeSerializableValue(#{name}.#{variant.fetch('kotlin_name')}.serializer(), value)"
  end
  accessors = variants.map do |variant|
    kotlin_name = variant.fetch('kotlin_name')
    kotlin_case_name = variant.fetch('kotlin_case')
    "    public val as#{kotlin_name}: #{kotlin_name}?\n        get() = this as? #{kotlin_name}"
  end
  structs = variants.map do |variant|
    shape_struct_source(
      variant.fetch('kotlin_name'),
      variant.fetch('fields'),
      discriminator: discriminator,
      indent: 4,
      typed_enums: false,
      variant_of: name
    )
  end

  <<~KOTLIN
    @Serializable(with = #{name}Serializer::class)
    public sealed interface #{name} {
        /** The variant this build does not know — its discriminator, verbatim. */
        @Serializable
        public data class Shared(
            public val #{discriminator}: String,
        ) : #{name}

    #{structs.join("\n\n")}

    #{accessors.join("\n\n")}

        public val replicaValue: ReplicaValue
            get() = runCatching {
                ReplicaValueCoding.encode(#{name}Serializer, this)
            }.getOrDefault(ReplicaValue.Null)

        public companion object {
            public fun fromReplicaValue(value: ReplicaValue?): #{name}? {
                if (value == null) return null
                return runCatching { ReplicaValueCoding.decode(#{name}Serializer, value) }.getOrNull()
            }
        }
    }

    public object #{name}Serializer : KSerializer<#{name}> {
        override val descriptor: SerialDescriptor =
            buildClassSerialDescriptor(#{kotlin_string(name)})

        override fun deserialize(decoder: Decoder): #{name} {
            val value = (decoder as? ReplicaValueDecoder)?.replicaValue
                ?: throw SerializationException("#{name} decodes only from a ReplicaValue tree")
            return when (value[#{kotlin_string(discriminator)}]?.string) {
    #{decode_cases.join("\n")}
                else -> ReplicaValueCoding.decode(#{name}.Shared.serializer(), value)
            }
        }

        override fun serialize(encoder: Encoder, value: #{name}) {
            when (value) {
    #{encode_cases.join("\n")}
                is #{name}.Shared -> encoder.encodeSerializableValue(#{name}.Shared.serializer(), value)
            }
        }
    }
  KOTLIN
end

def shape_definitions(stream)
  columns = base_columns(stream)
  stream.fetch('variants', []).each { |variant| columns.concat(variant_columns(stream, variant)) }
  columns.filter(&:shape_name).uniq(&:shape_name).map do |column|
    shape_source(column.shape_name, column.shape)
  end.join("\n")
end

def support_definitions(stream)
  body = [enum_source(stream), shape_definitions(stream)].reject(&:empty?).join("\n")
  body.empty? ? '' : "#{body.chomp}\n"
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
    "#{indent}val decoded#{camelize(column.storage_name)} = #{column.decode_expression} ?: return null"
  end
end

# The declared push set IS the outbound wire: a `push: false` column
# (server-derived pulls like blobUrl, server-stamped state) NEVER enters
# `encode()` — save() diffs must not echo it back up the wire. Writable
# optionals are full value semantics: null is an explicit `Null` clear so
# saveRow can distinguish it from a column the model does not author.
# A separate birth snapshot preserves every required local decode field;
# the runtime must never journal that snapshot as the outbound payload.
def encode_lines(columns, indent, prefix: '', snapshot: false)
  (snapshot ? columns : columns.select(&:push)).map do |column|
    reference = "#{prefix}#{column.storage_name}"
    if column.optional
      "#{indent}encoded[\"#{column.name}\"] = #{reference}?.let { value -> #{column.encode_expression('value')} } ?: ReplicaValue.Null"
    else
      "#{indent}encoded[\"#{column.name}\"] = #{column.encode_expression(reference)}"
    end
  end
end

def field_type_reference(stream, model)
  fields = (stream.fetch('indexes', nil) || []).map { it.fetch('field') }.uniq.sort
  fields.empty? ? 'ReplicaNoField' : "#{model}.Field"
end

# One `Field` enum per model — the stream's INDEXED fields (manifest
# `indexes:`), raw value = wire name; every predicate operator takes it. No
# indexes ⇒ `ReplicaNoField`, so no predicate over the model can be formed.
def field_enum_source(stream, indent)
  fields = (stream.fetch('indexes', nil) || []).map { it.fetch('field') }.uniq.sort
  return '' if fields.empty?

  cases = fields.map { "#{indent}    #{kotlin_enum_case(it)}(#{kotlin_string(it)})," }
  [
    "#{indent}public enum class Field(override val rawValue: String) : ReplicaIndexedField {",
    *cases,
    "#{indent}}",
    '',
    '',
  ].join("\n")
end

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
  signature = (['id: String'] + parameters + ['snapshot: ByteArray', 'documentPeer: ULong']).join(', ')
  entries = authored.map do |column|
    encoded =
      if column.optional
        "#{column.storage_name}?.let { value -> #{column.encode_expression('value')} } ?: ReplicaValue.Null"
      else
        column.encode_expression(column.storage_name)
      end
    "            #{kotlin_string(column.name)} to #{encoded},"
  end

  <<~KOTLIN

    public suspend fun DocumentStream<#{model}, #{field_type_reference(stream, model)}>.create(#{signature}): Boolean =
        engine.createDoc(
            stream = #{model}.streamName,
            id = id,
            seed = snapshot,
            peer = documentPeer,
            data = mapOf(
    #{entries.join("\n")}
            ),
        )
  KOTLIN
end

def model_imports(stream)
  columns = base_columns(stream) +
            stream.fetch('variants', []).to_a.flat_map { |variant| variant_columns(stream, variant) }
  needs_serialization = columns.any?(&:shape_name)
  needs_exception = columns.any?(&:materialized_projection?)
  imports = ['import kotlin.reflect.KClass']
  if needs_serialization
    imports += [
      'import kotlinx.serialization.KSerializer',
      'import kotlinx.serialization.SerialName',
      'import kotlinx.serialization.Serializable',
      'import kotlinx.serialization.builtins.ListSerializer',
      'import kotlinx.serialization.descriptors.PrimitiveKind',
      'import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor',
      'import kotlinx.serialization.descriptors.SerialDescriptor',
      'import kotlinx.serialization.descriptors.buildClassSerialDescriptor',
      'import kotlinx.serialization.descriptors.nullable',
      'import kotlinx.serialization.encoding.Decoder',
      'import kotlinx.serialization.encoding.Encoder',
    ]
    imports << 'import kotlinx.serialization.SerializationException' if needs_exception
    imports = imports.sort
  end
  imports << 'import io.replicaman.*'
  imports.join("\n")
end

# --- templates ---------------------------------------------------------------

DOC_MODEL = ERB.new(<<~'KOTLIN', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  <%= header(stream) %>

  package <%= package %>

  <%= model_imports(stream) %>

  <%= support_definitions(stream) %>
  public data class <%= model %>(
      override val id: String,
  <% columns.each do |column| -%>
      public var <%= column.parameter %>,
  <% end -%>
  ) : ReplicaDocModel {
  <%= field_enum_source(stream, '    ') -%>
  <%= column_enum_source(defined?(columns) && columns ? columns : base, '    ') %>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.projection_source('    ') %>

  <% end -%>
      public companion object : ReplicaDocModelType<<%= model %>, <%= field_type_reference(stream, model) %>> {
          override val streamName: String = "<%= stream['name'] %>"

          override val modelKey: KClass<*> = <%= model %>::class

          override fun from(id: String, data: Map<String, ReplicaValue>): <%= model %>? {
  <% guard_lines(columns, '            ').each do |line| -%>
  <%= line %>
  <% end -%>
              return <%= model %>(
                  id = id,
  <% columns.each do |column| -%>
                  <%= column.storage_name %> = <%= column.optional ? column.decode_expression : "decoded#{camelize(column.storage_name)}" %>,
  <% end -%>
              )
          }
      }
  }
  <%= document_create_extension(stream, model, columns) %>
KOTLIN

ROW_MODEL = ERB.new(<<~'KOTLIN', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  <%= header(stream) %>

  package <%= package %>

  <%= model_imports(stream) %>

  <%= support_definitions(stream) %>
  public data class <%= model %>(
      override val id: String,
  <% columns.each do |column| -%>
      public var <%= column.parameter %>,
  <% end -%>
  ) : <%= protocol_name %> {
  <%= field_enum_source(stream, '    ') -%>
  <%= column_enum_source(defined?(columns) && columns ? columns : base, '    ') %>
  <% columns.filter(&:materialized_projection?).each do |column| -%>
  <%= column.projection_source('    ') %>

  <% end -%>
      override val typeName: String? get() = null

      override fun encode(): Map<String, ReplicaValue> {
          val encoded = LinkedHashMap<String, ReplicaValue>()
  <% encode_lines(columns, '        ').each do |line| -%>
  <%= line %>
  <% end -%>
          return encoded
      }
  <% unless stream['readonly'] -%>

      override fun encodeSnapshot(): Map<String, ReplicaValue> {
          val encoded = LinkedHashMap<String, ReplicaValue>()
  <% encode_lines(columns, '        ', snapshot: true).each do |line| -%>
  <%= line %>
  <% end -%>
          return encoded
      }
  <% end -%>

      public companion object : <%= type_protocol_name %><<%= model %>, <%= field_type_reference(stream, model) %>> {
          override val streamName: String = "<%= stream['name'] %>"

          override val modelKey: KClass<*> = <%= model %>::class

          override fun from(id: String, type: String?, data: Map<String, ReplicaValue>): <%= model %>? {
  <% guard_lines(columns, '            ').each do |line| -%>
  <%= line %>
  <% end -%>
              return <%= model %>(
                  id = id,
  <% columns.each do |column| -%>
                  <%= column.storage_name %> = <%= column.optional ? column.decode_expression : "decoded#{camelize(column.storage_name)}" %>,
  <% end -%>
              )
          }
      }
  }
KOTLIN

STI_MODEL = ERB.new(<<~'KOTLIN', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  <%= header(stream) %>

  package <%= package %>

  <%= model_imports(stream) %>

  <%= support_definitions(stream) %>
  <% variants.each do |variant| -%>
  public data class <%= variant['kotlin_type'] %>(
      override val id: String,
  <% (base + variant_columns(stream, variant)).each do |column| -%>
      public var <%= column.parameter %>,
  <% end -%>
  ) : <%= model %> {
  <%= column_enum_source(base + variant_columns(stream, variant), '    ') %>
      public companion object : ReplicaVariant {
          override val wireType: String = "<%= variant['type'] %>"
          public const val streamName: String = "<%= stream['name'] %>"
      }
  <% (base + variant_columns(stream, variant)).filter(&:materialized_projection?).each do |column| -%>
  <%= column.projection_source('    ') %>

  <% end -%>
      override val typeName: String? get() = "<%= variant['type'] %>"

      override fun encode(): Map<String, ReplicaValue> {
          val encoded = LinkedHashMap<String, ReplicaValue>()
  <% encode_lines(base + variant_columns(stream, variant), '        ').each do |line| -%>
  <%= line %>
  <% end -%>
          return encoded
      }
  <% unless stream['readonly'] -%>

      override fun encodeSnapshot(): Map<String, ReplicaValue> {
          val encoded = LinkedHashMap<String, ReplicaValue>()
  <% encode_lines(base + variant_columns(stream, variant), '        ', snapshot: true).each do |line| -%>
  <%= line %>
  <% end -%>
          return encoded
      }
  <% end -%>
  }

  <% end -%>
  public sealed interface <%= model %> : <%= protocol_name %> {
  <%= field_enum_source(stream, '    ') -%>
  <%= column_enum_source(defined?(columns) && columns ? columns : base, '    ') %>
      public companion object : <%= type_protocol_name %><<%= model %>, <%= field_type_reference(stream, model) %>> {
          override val streamName: String = "<%= stream['name'] %>"

          override val modelKey: KClass<*> = <%= model %>::class

          override fun from(id: String, type: String?, data: Map<String, ReplicaValue>): <%= model %>? {
  <% guard_lines(base, '            ').each do |line| -%>
  <%= line %>
  <% end -%>
              return when (type) {
  <% variants.each do |variant| -%>
                  "<%= variant['type'] %>" -> {
  <% guard_lines(variant_columns(stream, variant), '                    ').each do |line| -%>
  <%= line %>
  <% end -%>
                      <%= variant['kotlin_type'] %>(
                      id = id,
  <% base.each do |column| -%>
                      <%= column.storage_name %> = <%= column.optional ? column.decode_expression : "decoded#{camelize(column.storage_name)}" %>,
  <% end -%>
  <% variant_columns(stream, variant).each do |column| -%>
                      <%= column.storage_name %> = <%= column.optional ? column.decode_expression : "decoded#{camelize(column.storage_name)}" %>,
  <% end -%>
                  )
                  }
  <% end -%>
                  else -> null
              }
          }
      }
  }
KOTLIN

CONTAINER = ERB.new(<<~'KOTLIN', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  // Manifest version <%= manifest['version'] %> · streams: <%= streams.map { it['name'] }.join(', ') %>

  package <%= package %>

  import io.replicaman.*

  public class <%= container %>(
      public val engine: ReplicaEngine,
  ) {
  <% streams.each do |stream| -%>
      public val <%= lower_camel(camelize(stream['name'])) %>: <%= handle_type(stream) %><<%= model_name(stream) %>, <%= field_type_reference(stream, model_name(stream)) %>>
          get() = <%= handle_type(stream) %>(engine, <%= model_name(stream) %>)

  <% end -%>
      public fun <T> write(body: (ReplicaTransaction) -> T): T = engine.write(body)

      public suspend fun <T> writeAsync(body: (ReplicaTransaction) -> T): T = engine.writeAsync(body)

      public companion object {
          public val schema: ReplicaSchema = ReplicaSchema(
              streams = listOf(
  <% streams.each do |stream| -%>
                  ReplicaStreamSpec(name = "<%= stream['name'] %>", lane = ReplicaStreamSpec.Lane.<%= stream['lane'].upcase %>, readonly = <%= stream['readonly'] %>, shard = "<%= stream['shard'] %>"<% if stream['lane'] == 'document' %>, codec = "<%= stream['codec'] %>"<%= reflections_argument(stream) %><% end %><% if standard_stamp?(stream) %>, stamp = ReplicaStamp.standard<% end %><%= preconditions_argument(stream) %><%= pushed_argument(stream) %><%= references_argument(stream) %>),
  <% end -%>
              ),
              indexes = listOf(<%= indexes_argument(streams) %>),
              namespace = <%= manifest.fetch('namespace').to_json %>,
              version = <%= manifest.fetch('schemaVersion') %>,
          )

      }
  }

  <% streams.select { it['lane'] == 'row' }.each do |stream| -%>
  public val ReplicaTransaction.<%= lower_camel(camelize(stream['name'])) %>: <%= stream['readonly'] ? 'TransactionReadonlyRows' : 'TransactionRows' %><<%= model_name(stream) %>, <%= field_type_reference(stream, model_name(stream)) %>>
      get() = <%= stream['readonly'] ? 'readonlyRows' : 'rows' %>(<%= model_name(stream) %>)

  <% end -%>
KOTLIN

def column_enum_source(columns, indent)
  cases = ['id', *columns.map(&:name)].uniq.map { "#{indent}    #{kotlin_enum_case(it)}(#{kotlin_string(it)})," }
  ["#{indent}public enum class Column(override val rawValue: String) : ReplicaColumn {", *cases, "#{indent}}"].join("\n")
end

def preconditions_argument(stream)
  names = stream.fetch('columns').select { it.fetch('precondition', nil) }.map { kotlin_string(it.fetch('name')) }
  names.empty? ? '' : ", preconditions = listOf(#{names.join(', ')})"
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

  names = flags.select { |_, values| values == [true] }.keys.sort.map { kotlin_string(it) }
  ", pushed = setOf(#{names.join(', ')})"
end

def reflections_argument(stream)
  reflections = stream.fetch('columns').select { it.fetch('reflects', nil) }.map do |column|
    path = column.fetch('reflects', nil).map { kotlin_string(it) }.join(', ')
    "ReplicaReflection(field = #{kotlin_string(column.fetch('name'))}, path = listOf(#{path}))"
  end
  reflections.empty? ? '' : ", reflections = listOf(#{reflections.join(', ')})"
end

def indexes_argument(streams)
  specs = streams.flat_map do |stream|
    (stream.fetch('indexes', nil) || []).map do |index|
      "                ReplicaIndexSpec(stream = #{kotlin_string(stream.fetch('name'))}, field = #{kotlin_string(index.fetch('field', nil))}, kind = ReplicaIndexKind.#{index.fetch('kind', nil).upcase}),"
    end
  end
  specs.empty? ? '' : "\n#{specs.join("\n")}\n            "
end

def handle_type(stream)
  if stream.fetch('lane') == 'document'
    stream.fetch('readonly', nil) ? 'ReadonlyDocumentStream' : 'DocumentStream'
  else
    stream.fetch('readonly', nil) ? 'ReadonlyRowStream' : 'RowStream'
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

def document_shape_descriptor(model, shape)
  coding = "#{model}DocumentCoding.Shape"
  return "#{coding}.String" if shape.fetch('enum', nil)

  case shape.fetch('type')
  when 'string', 'text', 'hex_color', 'datetime', 'date', 'time'
    "#{coding}.String"
  when 'integer', 'bigint'
    "#{coding}.Integer"
  when 'float', 'decimal'
    "#{coding}.Number"
  when 'boolean'
    "#{coding}.Boolean"
  when 'json', 'jsonb'
    "#{coding}.Json"
  when 'array'
    "#{coding}.Array(#{document_shape_descriptor(model, shape.fetch('items'))})"
  when 'map'
    "#{coding}.Map(#{document_shape_descriptor(model, shape.fetch('values'))})"
  when 'object'
    fields = shape.fetch('fields').map do |field|
      "#{kotlin_string(snake_case(field.fetch('name')))} to #{document_shape_descriptor(model, field)}"
    end
    "#{coding}.Object(mapOf(#{fields.join(', ')}))"
  else
    "#{coding}.Json"
  end
end

def document_projection_extensions(model, shapes)
  extensions = shapes.filter_map do |root_name, shape|
    root_field = document_root_field(root_name, shape)
    nested_name = shape_nested_name(root_field, singular_collections: true)
    qualified_name = "#{model}Document.#{nested_name}"
    shape_val = "#{lower_camel(nested_name)}Shape"

    if document_registry_shape?(shape)
      descriptor = document_shape_descriptor(model, shape.fetch('items'))
      <<~KOTLIN
        private val #{shape_val} = #{descriptor}

        public fun #{qualified_name}.Companion.from(documentEntry: DocumentEntry): #{qualified_name}? {
            val fields = documentEntry.fields + ("key" to DocumentValue.String(documentEntry.key))
            return #{model}DocumentCoding.decode(fields, serializer<#{qualified_name}>())
        }

        public val #{qualified_name}.documentEntry: DocumentEntry
            get() {
                val fields = #{model}DocumentCoding.encode(this, serializer<#{qualified_name}>(), #{shape_val})
                return DocumentEntry(key = key, fields = fields - "key")
            }
      KOTLIN
    elsif shape.fetch('type', nil) == 'object'
      descriptor = document_shape_descriptor(model, shape)
      <<~KOTLIN
        private val #{shape_val} = #{descriptor}

        public fun #{qualified_name}.Companion.from(
            documentFields: Map<String, DocumentValue>,
        ): #{qualified_name}? = #{model}DocumentCoding.decode(documentFields, serializer<#{qualified_name}>())

        public val #{qualified_name}.documentFields: Map<String, DocumentValue>
            get() = #{model}DocumentCoding.encode(this, serializer<#{qualified_name}>(), #{shape_val})
      KOTLIN
    end
  end
  extensions.join("\n")
end

def document_projection_source(model, shapes)
  decoded = []
  assignments = []
  shapes.each do |root_name, shape|
    root_field = document_root_field(root_name, shape)
    nested_name = shape_nested_name(root_field, singular_collections: true)
    qualified = "#{model}Document.#{nested_name}"

    if document_registry_shape?(shape)
      decoded << "if (projection.#{root_name}.map { it.key }.toSet().size != projection.#{root_name}.size) return null"
      decoded << "val #{root_name} = projection.#{root_name}.map { #{qualified}.from(it) ?: return null }"
      assignments << "#{root_name} = #{root_name}"
    elsif shape.fetch('type', nil) == 'object'
      decoded << "val #{root_name} = #{qualified}.from(projection.#{root_name}) ?: return null"
      assignments << "#{root_name} = #{root_name}"
    else
      raise "replica-codegen: document root #{root_name} must be an object or keyed object array"
    end
  end

  projection_arguments = shapes.map do |root_name, shape|
    value =
      if document_registry_shape?(shape)
        "#{root_name}.map { it.documentEntry }"
      elsif shape.fetch('type', nil) == 'object'
        "#{root_name}.documentFields"
      end
    "        #{root_name} = #{value},"
  end

  guard_source = decoded.empty? ? '' : "#{decoded.map { |line| "    #{line}" }.join("\n")}\n"

  <<~KOTLIN
    /** Refuse a partial typed document: round-tripping it would discard undecodable entries. */
    public fun #{model}Document.Companion.from(projection: #{model}Projection): #{model}Document? {
    #{guard_source}    return #{model}Document(
            #{assignments.join(",\n        ")},
        )
    }

    public val #{model}Document.projection: #{model}Projection
        get() = #{model}Projection(
    #{projection_arguments.join("\n")}
        )
  KOTLIN
end

def document_coding_source(model)
  <<~KOTLIN
    private object #{model}DocumentCoding {
        public sealed interface Shape {
            public data object String : Shape
            public data object Integer : Shape
            public data object Number : Shape
            public data object Boolean : Shape
            public data object Json : Shape
            public data class Object(val fields: kotlin.collections.Map<kotlin.String, Shape>) : Shape
            public data class Array(val item: Shape) : Shape
            public data class Map(val item: Shape) : Shape
        }

        // Direct tree decode (io.replicaman.DocumentValueDecoding) — the JSON
        // round-trip this replaces ground on main in hang profiles
        // (TextStyle decode through Json encode+decode per typed read).
        // Encode below stays on the shaped JSON path: the manifest Shape
        // drives .Int vs .Double on the way out.
        @OptIn(ExperimentalSerializationApi::class)
        fun <Value> decode(
            fields: kotlin.collections.Map<kotlin.String, DocumentValue>,
            deserializer: DeserializationStrategy<Value>,
        ): Value? = try {
            DocumentValueDecoding.decode(deserializer, fields)
        } catch (error: Exception) {
            io.replicaman.ReplicaDiagnostics.report("decode a document ${deserializer.descriptor.serialName}", error)
            null
        }

        @OptIn(ExperimentalSerializationApi::class)
        private val json = Json {
            namingStrategy = JsonNamingStrategy.SnakeCase
            encodeDefaults = true
            explicitNulls = true
        }

        fun <Value> encode(
            value: Value,
            serializer: SerializationStrategy<Value>,
            shape: Shape,
        ): kotlin.collections.Map<kotlin.String, DocumentValue> {
            val encoded = documentValue(json.encodeToJsonElement(serializer, value), shape)
            val fields = (encoded as? DocumentValue.Map)?.value
            checkNotNull(fields) { "generated #{stream_word(model)} document value did not match its manifest shape" }
            return fields
        }

        private fun documentValue(value: JsonElement, shape: Shape): DocumentValue {
            if (value is JsonNull) return DocumentValue.Null

            return when (shape) {
                Shape.String -> (value as? JsonPrimitive)?.takeIf { it.isString }
                    ?.let { DocumentValue.String(it.content) } ?: DocumentValue.Null
                Shape.Integer -> (value as? JsonPrimitive)
                    ?.let { it.longOrNull ?: it.doubleOrNull?.toLong() }
                    ?.let { DocumentValue.Int(it) } ?: DocumentValue.Null
                Shape.Number -> (value as? JsonPrimitive)?.doubleOrNull
                    ?.let { DocumentValue.Double(it) } ?: DocumentValue.Null
                Shape.Boolean -> (value as? JsonPrimitive)?.booleanOrNull
                    ?.let { DocumentValue.Bool(it) } ?: DocumentValue.Null
                is Shape.Object -> {
                    val bag = value as? JsonObject ?: return DocumentValue.Null
                    DocumentValue.Map(
                        shape.fields.mapValues { (key, child) ->
                            bag[key]?.let { documentValue(it, child) } ?: DocumentValue.Null
                        },
                    )
                }
                is Shape.Array -> {
                    val items = value as? JsonArray ?: return DocumentValue.Null
                    DocumentValue.List(items.map { documentValue(it, shape.item) })
                }
                is Shape.Map -> {
                    val bag = value as? JsonObject ?: return DocumentValue.Null
                    DocumentValue.Map(bag.mapValues { documentValue(it.value, shape.item) })
                }
                Shape.Json -> jsonValue(value)
            }
        }

        private fun jsonValue(value: JsonElement): DocumentValue = when (value) {
            is JsonNull -> DocumentValue.Null
            is JsonArray -> DocumentValue.List(value.map(::jsonValue))
            is JsonObject -> DocumentValue.Map(value.mapValues { jsonValue(it.value) })
            is JsonPrimitive ->
                if (value.isString) {
                    DocumentValue.String(value.content)
                } else {
                    value.booleanOrNull?.let(DocumentValue::Bool)
                        ?: value.doubleOrNull?.let(DocumentValue::Double)
                        ?: DocumentValue.Null
                }
        }
    }
  KOTLIN
end

# `DeckDocument` → "deck", the word the coding object's one message uses.
def stream_word(model)
  model.gsub(/([a-z\d])([A-Z])/, '\1 \2').downcase
end

# `SerialName` only appears when a shape declares an enum.
def document_imports(shapes, model, options)
  imports = [
    'import kotlinx.serialization.DeserializationStrategy',
    'import kotlinx.serialization.ExperimentalSerializationApi',
  ]
  imports << 'import kotlinx.serialization.SerialName' if JSON.generate(shapes).include?('"enum"')
  imports += [
    'import kotlinx.serialization.SerializationStrategy',
    'import kotlinx.serialization.Serializable',
    'import kotlinx.serialization.json.Json',
    'import kotlinx.serialization.json.JsonArray',
    'import kotlinx.serialization.json.JsonElement',
    'import kotlinx.serialization.json.JsonNamingStrategy',
    'import kotlinx.serialization.json.JsonNull',
    'import kotlinx.serialization.json.JsonObject',
    'import kotlinx.serialization.json.JsonPrimitive',
    'import kotlinx.serialization.json.booleanOrNull',
    'import kotlinx.serialization.json.doubleOrNull',
    'import kotlinx.serialization.json.longOrNull',
    'import kotlinx.serialization.serializer',
    'import io.replicaman.DocumentEntry',
    'import io.replicaman.DocumentValue',
    'import io.replicaman.DocumentValueDecoding',
    *(options[:document_projection] ? ["import #{options[:document_projection]} as #{model}Projection"] : []),
  ]
  imports.join("\n")
end

def document_source(stream, package_name, options)
  shapes = stream.fetch('shapes')
  model = model_name(stream)
  root_fields = shapes.map { |name, shape| document_root_field(name, shape) }
  body = shape_struct_source(
    "#{model}Document",
    root_fields,
    singular_collections: true,
    companion: true,
    companion_nested: true,
    enum_codecs: false
  )

  <<~KOTLIN
    // Generated by replica-codegen — DO NOT EDIT.
    // Document value for stream `#{stream.fetch('name')}` · codec #{stream.fetch('codec', nil)}
    // #{model}Document owns generated values; #{model}Doc owns the Loro-backed editing engine.

    package #{package_name}

    #{document_imports(shapes, model, options)}

    #{body}

    #{document_coding_source(model)}
    #{document_projection_extensions(model, shapes)}
    #{unless options[:document_projection]
      fields = shapes.map { |name, shape| "    val #{name}: #{document_registry_shape?(shape) ? 'List<DocumentEntry> = emptyList()' : 'Map<String, DocumentValue> = emptyMap()'}," }
      "public data class #{model}Projection(\n#{fields.join("\n")}\n)"
    end}
    #{document_projection_source(model, shapes).chomp}
  KOTLIN
end

def document_defaults_source(stream, package_name)
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
        "List<#{model}Document.#{nested_name}>"
      else
        type
      end
    expression = shape_value_source(
      shape,
      value,
      type_reference: "#{model}Document.#{nested_name}",
      singular_collections: true
    )
    "    public val #{root_name}: #{qualified_type} = #{expression}"
  end

  <<~KOTLIN
    // Generated by replica-codegen — DO NOT EDIT.
    // Defaults for document stream `#{stream.fetch('name')}`.

    package #{package_name}

    public object #{model}Defaults {
    #{lines.join("\n")}
    }
  KOTLIN
end

# --- validation --------------------------------------------------------------

# Column and shape-field names are emitted VERBATIM as Kotlin property names.
# Only the HARD keywords are illegal there — a soft or modifier keyword
# (`data`, `open`, `value`) is a perfectly good property name.
KOTLIN_HARD_KEYWORDS = %w[
  as break class continue do else false for fun if in interface is null object
  package return super this throw true try typealias typeof val var when while
].freeze

# A COLUMN additionally shares the model's namespace with `ReplicaRowModel`.
# A nested shape struct does not — `ThemeColor.id` is fine.
KOTLIN_RESERVED_COLUMN_NAMES = (KOTLIN_HARD_KEYWORDS + %w[id typeName]).freeze

# The three ways a manifest silently flattens into ONE Kotlin identifier.
# `kotlin_enum_case` is many-to-one (`foo-bar` and `foo_bar`, `open` and
# `OPEN`), `model_name` is many-to-one (`boxes` and `box`), and the second
# `File.write` of a colliding model simply overwrites the first — the handle
# then reads the wrong stream. Every one of these is caught HERE, at
# generation, or not at all.
def enum_case_collisions(values)
  values.group_by { kotlin_enum_case(it) }.select { |_, declared| declared.uniq.size > 1 }
end

# Shape fields are emitted verbatim too, at every depth.
def shape_field_problems(node, where, problems)
  return unless node.is_a?(Hash)

  (node.fetch('fields', nil) || []).each do |field|
    if KOTLIN_HARD_KEYWORDS.include?(field.fetch('name'))
      problems << "#{where}.#{field.fetch('name')} is a reserved Kotlin property name"
    end
    shape_field_problems(field, "#{where}.#{field.fetch('name')}", problems)
  end
  %w[items values].each { |key| shape_field_problems(node[key], where, problems) }
  (node.fetch('variants', nil) || []).each { shape_field_problems(it, "#{where}:#{it.fetch('value', nil)}", problems) }
end

def validate!(streams)
  problems = []
  streams.each do |stream|
    columns = (stream.fetch('columns') || []) +
              stream.fetch('variants', []).to_a.flat_map { it.fetch('columns') }
    columns.each do |column|
      if KOTLIN_RESERVED_COLUMN_NAMES.include?(column.fetch('name'))
        problems << "#{stream.fetch('name')}.#{column.fetch('name')} is a reserved Kotlin property name"
      end
      if column.fetch('blob', nil)
        problems << "#{stream.fetch('name')}.#{column.fetch('name')} declares blob: true — " \
                    'ReplicaBlobRef does not exist in :replicaman, so the emitted model cannot compile'
      end
      next unless column.fetch('enum', nil)

      enum_case_collisions(column.fetch('enum', nil)).each do |entry, declared|
        problems << "#{stream.fetch('name')}.#{column.fetch('name')} enum values #{declared.inspect} " \
                    "all spell the entry #{entry.inspect}"
      end
      column.fetch('enum', nil).reject { kotlin_enum_case(it).match?(/\A[A-Z_][0-9A-Z_]*\z/) }.each do |value|
        problems << "#{stream.fetch('name')}.#{column.fetch('name')} enum value #{value.inspect} spells " \
                    "#{kotlin_enum_case(value).inspect}, which is not a Kotlin identifier"
      end
    end
    columns.each { |column| shape_field_problems(column.fetch('shapes', nil), "#{stream.fetch('name')}.#{column.fetch('name')}", problems) }
    (stream.fetch('shapes', nil) || {}).each { |root, shape| shape_field_problems(shape, "#{stream.fetch('name')}##{root}", problems) }
    fields = (stream.fetch('indexes', nil) || []).map { it.fetch('field') }.uniq
    enum_case_collisions(fields).each do |entry, declared|
      problems << "#{stream.fetch('name')} index fields #{declared.inspect} all spell Field.#{entry}"
    end
  end
  streams.group_by { model_name(it) }.each do |model, declared|
    next unless declared.size > 1

    problems << "streams #{declared.map { it.fetch('name') }.inspect} all singularize to the model #{model}"
  end
  return if problems.empty?

  abort "replica-codegen:\n  #{problems.join("\n  ")}"
end


def references_argument(stream)
  references = stream.fetch('references', []).map do |reference|
    fields = ['name = ' + reference.fetch('name').inspect, 'stream = ' + reference.fetch('stream').inspect]
    fields << 'field = ' + reference.fetch('field').inspect if reference.key?('field')
    fields << 'keySegment = ' + reference.fetch('keySegment').to_s if reference.key?('keySegment')
    fields << 'keyPrefix = ' + reference.fetch('keyPrefix').inspect if reference.key?('keyPrefix')
    fields << 'optional = ' + reference.fetch('optional', false).to_s
    'ReplicaReferenceSpec(' + fields.join(', ') + ')'
  end
  result = references.empty? ? '' : ", references = listOf(#{references.join(', ')})"
  result += ', lifetimeFrom = ' + stream.fetch('lifetimeFrom').inspect if stream.key?('lifetimeFrom')
  result
end

# --- emit --------------------------------------------------------------------

streams = manifest.fetch('streams')
validate!(streams)
out = options[:out]
FileUtils.mkdir_p(out)

streams.each do |stream|
  model = model_name(stream)
  source =
    if stream.fetch('lane') == 'document'
      DOC_MODEL.result_with_hash(
        stream: stream, model: model, columns: base_columns(stream), package: options[:package]
      )
    elsif stream.fetch('sti', nil)
      STI_MODEL.result_with_hash(
        stream: stream, model: model, base: base_columns(stream), variants: stream.fetch('variants', nil),
        package: options[:package],
        protocol_name: stream.fetch('readonly', nil) ? 'ReplicaRowModel' : 'ReplicaWritableRowModel',
        type_protocol_name: stream.fetch('readonly', nil) ? 'ReplicaRowModelType' : 'ReplicaWritableRowModelType'
      )
    else
      ROW_MODEL.result_with_hash(
        stream: stream, model: model, columns: base_columns(stream),
        package: options[:package],
        protocol_name: stream.fetch('readonly', nil) ? 'ReplicaRowModel' : 'ReplicaWritableRowModel',
        type_protocol_name: stream.fetch('readonly', nil) ? 'ReplicaRowModelType' : 'ReplicaWritableRowModelType'
      )
    end
  File.write(File.join(out, "#{model}.kt"), source)
end

File.write(
  File.join(out, "#{options[:name]}.kt"),
  CONTAINER.result_with_hash(
    manifest: manifest, streams: streams, container: options[:name], package: options[:package]
  ).rstrip + "\n"
)

if options[:document_out]
  document_out = options[:document_out]
  FileUtils.mkdir_p(document_out)
  streams.filter { it.fetch('lane') == 'document' && it.fetch('shapes', nil) }.each do |stream|
    model = model_name(stream)
    File.write(
      File.join(document_out, "#{model}Document.kt"),
      document_source(stream, options[:document_package], options)
    )
    defaults = document_defaults_source(stream, options[:document_package])
    File.write(File.join(document_out, "#{model}Defaults.kt"), defaults) if defaults
  end
end
