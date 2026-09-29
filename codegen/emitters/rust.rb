#!/usr/bin/env ruby
# frozen_string_literal: true

# Native rust syntax emitter. Manifest semantics and output installation
# belong to codegen/lib; execute through codegen/bin/replica-codegen.

require 'erb'
require 'json'
require 'net/http'
require 'optparse'
require 'fileutils'
require 'uri'

# The formatting contract for emitted Rust. Passed explicitly rather than
# discovered, so a regeneration into a scratch directory (the byte-identity
# test) formats exactly like a regeneration in place. Keep in step with the
# repo's rustfmt.toml.
RUSTFMT_EDITION = '2024'
RUSTFMT_MAX_WIDTH = '100'

# --- naming ------------------------------------------------------------------



# Rust's reserved words, 2024 edition, plus the reserved-for-future set. The
# Swift emitter backticks its keywords; Rust spells the same escape `r#`,
# which is legal for fields and functions but NOT for a type or a variant —
# those are UpperCamel here, and no Rust keyword is, so the collision cannot
# arise (see `rust_variant`).
RUST_KEYWORDS = %w[
  as break const continue crate dyn else enum extern false fn for if impl in
  let loop match mod move mut pub ref return self static struct super trait
  true type unsafe use where while async await gen try
  abstract become box do final macro override priv typeof unsized virtual yield
].freeze

# `r#` cannot spell these three even though they are keywords.
RUST_UNRAWABLE_KEYWORDS = %w[crate self super].freeze

def snake_case(name)
  name
    .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
    .gsub(/([a-z\d])([A-Z])/, '\1_\2')
    .downcase
end

# A wire field name as a Rust binding/field identifier. Wire names are
# camelCase on the row lane and snake_case on the document lane; both land on
# snake_case here, and the WIRE spelling is kept in the encode/decode maps.
def rust_field(value)
  identifier = snake_case(value.to_s.gsub(/[^0-9A-Za-z]+/, '_'))
  identifier = "_#{identifier}" if identifier.match?(/\A\d/)
  if RUST_UNRAWABLE_KEYWORDS.include?(identifier)
    raise "replica-codegen: wire field #{value.inspect} maps to the Rust keyword " \
          "`#{identifier}`, which a raw identifier cannot spell"
  end

  RUST_KEYWORDS.include?(identifier) ? "r##{identifier}" : identifier
end

# An enum case, as the Swift emitter names it, then upper-cased for Rust.
# A leading underscore (a raw value that starts with a digit) survives —
# rustc's non_camel_case_types lint trims leading underscores before it
# checks, so `_169` and `_43` pass while Swift's `sdr` becomes `Sdr`.
def rust_variant(value)
  identifier = lower_camel(camelize(value.to_s.gsub(/[^0-9A-Za-z]+/, '_')))
  identifier = "_#{identifier}" if identifier.match?(/\A\d/)
  identifier.sub(/\A(_?)([a-z])/) { "#{::Regexp.last_match(1)}#{::Regexp.last_match(2).upcase}" }
end

def rust_string(value)
  JSON.generate(value.to_s)
end

# Rust items need a blank line between them to read; rustfmt never ADDS one
# (it only collapses runs), so the emitter separates every top-level chunk.
# clippy caps a function at 7 arguments; a wide table simply has more
# columns than that, and a generated memberwise constructor is not the place
# to hide them behind a builder.
CLIPPY_ARGUMENT_LIMIT = 7

def too_many_arguments(parameters)
  parameters.size > CLIPPY_ARGUMENT_LIMIT ? ['    #[allow(clippy::too_many_arguments)]'] : []
end

def join_items(parts)
  parts.reject { it.to_s.strip.empty? }.map { it.sub(/\n+\z/, '') }.join("\n\n")
end

# Streams are table-plural by convention; the model is the singular. Naive on
# purpose (stdlib only): -ies → -y, -ches/-shes/-xes/-ses → drop -es, then
# drop a trailing -s.
# Plurals the rules below get WRONG, spelled out. A document root's name is
# a TYPE name on three platforms, so `buses` becoming `Buse` is not a
# cosmetic slip — it is the name every consumer writes by hand. One table,
# not a cleverer rule: English has no rule here that does not also break
# something else (`-ses` → `-s` would turn `poses` into `pos`).


# Two STI streams can flatten the same demodulized subclass into top-level
# Rust (Shop::Assets::ImageAsset vs Gallery::Assets::ImageAsset). Only the
# emitted Rust names move — the wire type stays the manifest's.

# Stream names are honest (stream == table, boot-enforced server-side), so
# the Rust model is always the singularized stream name — no rename map.

# --- type mapping ------------------------------------------------------------

RUST_TYPES = {
  'string' => 'String', 'text' => 'String',
  'hex_color' => 'String',
  'integer' => 'i64', 'bigint' => 'i64',
  'float' => 'f64', 'decimal' => 'f64',
  'boolean' => 'bool',
  'datetime' => 'String', 'date' => 'String', 'time' => 'String',
}.freeze

def rust_type(wire_type)
  RUST_TYPES.fetch(wire_type, 'ReplicaValue')
end

# `ReplicaValue` → typed scalar. Swift spells these as optional-chained
# properties (`?.string`); Rust spells them as `and_then` over the accessor,
# and `String` needs the extra `to_owned` because the accessor lends.
def wire_accessor(wire_type)
  case rust_type(wire_type)
  when 'String' then '.and_then(ReplicaValue::as_string).map(str::to_owned)'
  when 'i64' then '.and_then(ReplicaValue::as_int)'
  when 'f64' then '.and_then(ReplicaValue::as_number)'
  when 'bool' then '.and_then(ReplicaValue::as_bool)'
  else '.cloned()'
  end
end

# The element half of the accessor above, applied inside a list's
# `filter_map` where the item is already a `&ReplicaValue`.
def item_accessor(wire_type)
  case rust_type(wire_type)
  when 'String' then 'filter_map(ReplicaValue::as_string).map(str::to_owned)'
  when 'i64' then 'filter_map(ReplicaValue::as_int)'
  when 'f64' then 'filter_map(ReplicaValue::as_number)'
  when 'bool' then 'filter_map(ReplicaValue::as_bool)'
  else 'cloned()'
  end
end

# Typed scalar → `ReplicaValue`. `by_ref` is the difference between a place
# of type `T` (`self.volume`) and a borrow of one (`|value|` out of
# `Option::as_ref`), which Rust needs spelled and Swift does not.
def wire_encode_expression(wire_type, variable, by_ref: false)
  star = by_ref ? '*' : ''
  case rust_type(wire_type)
  when 'String' then "ReplicaValue::String(#{variable}.clone())"
  when 'i64' then "ReplicaValue::signed_integer(#{star}#{variable})"
  when 'f64' then "ReplicaValue::Number(#{star}#{variable})"
  when 'bool' then "ReplicaValue::Bool(#{star}#{variable})"
  else "#{variable}.clone()"
  end
end

# --- column shaping ----------------------------------------------------------

Column = Struct.new(
  :name, :wire_type, :items, :optional, :push, :pull, :enum_name, :enum_values, :shape_name, :shape,
  :blob,
  keyword_init: true
) do
  # The Rust field that HOLDS the column. A shape-backed column keeps the
  # lossless raw value here and hangs the typed projection off an accessor.
  def storage_name
    shape_name ? "#{rust_field(name)}_json" : rust_field(name)
  end

  # The typed projection's accessor name (Swift spells it as a computed
  # property with the column's own name; Rust has no computed properties).
  def projection_name
    rust_field(name)
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
    return "Vec<#{rust_type(item_type)}>" if list?

    rust_type(wire_type)
  end

  def declared_type
    optional ? "Option<#{base_type}>" : base_type
  end

  def parameter
    "#{storage_name}: #{declared_type}"
  end

  def decode_expression
    value = "fields.get(#{rust_string(name)})"
    return "#{value}.and_then(ReplicaValue::as_string).and_then(#{enum_name}::from_wire)" if enum_name
    if list?
      return "#{value}.and_then(ReplicaValue::items).map(|items| items.iter().#{item_accessor(item_type)}.collect())"
    end

    "#{value}#{wire_accessor(wire_type)}"
  end

  def encode_expression(variable, by_ref: false)
    if enum_name
      return "ReplicaValue::String(#{variable}.as_wire().to_owned())"
    end
    if list?
      return "ReplicaValue::Array(#{variable}.iter().map(|item| " \
             "#{wire_encode_expression(item_type, 'item', by_ref: true)}).collect())"
    end

    wire_encode_expression(wire_type, variable, by_ref: by_ref)
  end

  # Swift bridges a shape-backed column through a computed property; Rust
  # spells the same pair as methods over the raw storage. A discriminated
  # shape MATERIALIZES (Swift's `didSet` mirrors the typed value back into
  # the raw column) — so the typed value is stored and the setter is the
  # mirror.
  def raw_reference
    optional ? "self.#{storage_name}.as_ref()" : "Some(&self.#{storage_name})"
  end

  # What a non-optional raw column falls back to when the typed value is
  # cleared — Swift's `?? .object([:])` / `?? .array([])`.
  def raw_empty
    array_shape? ? 'ReplicaValue::Array(Vec::new())' : 'ReplicaValue::Object(ReplicaFields::new())'
  end

  def raw_assignment(expression)
    return "self.#{storage_name} = #{expression};" if optional

    "self.#{storage_name} = #{expression}.unwrap_or_else(|| #{raw_empty});"
  end

  def projection_source(indent)
    return unless shape_name

    if array_shape?
      return <<~RUST.chomp
        #{indent}pub fn #{projection_name}(&self) -> Option<Vec<#{shape_name}>> {
        #{indent}    #{raw_reference}.and_then(|raw| value_coding::decode(raw).ok())
        #{indent}}
        #{indent}
        #{indent}pub fn set_#{projection_name}(&mut self, value: Option<Vec<#{shape_name}>>) {
        #{indent}    #{raw_assignment('value.and_then(|value| value_coding::encode(&value).ok())')}
        #{indent}}
      RUST
    end

    if materialized_projection?
      <<~RUST.chomp
        #{indent}pub fn #{projection_name}(&self) -> Option<&#{shape_name}> {
        #{indent}    self.#{projection_name}.as_ref()
        #{indent}}
        #{indent}
        #{indent}/// Swift's `didSet` mirror: the typed value is the truth and the raw
        #{indent}/// column follows it.
        #{indent}pub fn set_#{projection_name}(&mut self, value: Option<#{shape_name}>) {
        #{indent}    self.#{projection_name} = value;
        #{indent}    #{raw_assignment("self.#{projection_name}.as_ref().map(#{shape_name}::replica_value)")}
        #{indent}}
      RUST
    else
      <<~RUST.chomp
        #{indent}pub fn #{projection_name}(&self) -> Option<#{shape_name}> {
        #{indent}    #{shape_name}::from_replica_value(#{raw_reference})
        #{indent}}
        #{indent}
        #{indent}pub fn set_#{projection_name}(&mut self, value: Option<#{shape_name}>) {
        #{indent}    #{raw_assignment("value.as_ref().map(#{shape_name}::replica_value)")}
        #{indent}}
      RUST
    end
  end

  def materialized_projection?
    shape_name && shape&.key?('discriminator')
  end

  # An array-of-object column (`colors`). The doc lane already emits
  # typed `Vec<Slide>` from exactly this manifest node; this is the row lane
  # reaching the same generator instead of falling through to `ReplicaValue`.
  def array_shape?
    shape && shape.fetch('type', nil) == 'array' && shape.dig('items', 'type') == 'object'
  end

  # The stored, materialized typed field a discriminated shape column keeps
  # beside its raw JSON.
  def materialized_field_source(indent)
    return unless materialized_projection?

    "#{indent}#{projection_name}: Option<#{shape_name}>,"
  end

  def materialization_assignment(indent, source)
    return unless materialized_projection?

    "#{indent}#{projection_name}: #{shape_name}::from_replica_value(#{source}),"
  end
end

def base_columns(stream)
  owner = model_name(stream)
  stream.fetch('columns').map do |column|
    if column.fetch('items', nil) && !RUST_TYPES.key?(column.dig('items', 'type'))
      raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is an array of " \
            "#{column.dig('items', 'type').inspect} — only scalar item types are generated"
    end

    if column.fetch('pull', nil) == false && column.fetch('null', nil) == false
      raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is a required intake — " \
            'a pulled row can never satisfy its decode guard'
    end

    if column.fetch('blob', nil) == true
      raise "replica-codegen: #{stream.fetch('name')}.#{column.fetch('name')} is a blob column — " \
            'the Rust runtime has no ReplicaBlobRef yet, and emitting a bare String would ' \
            'let a caller pour an arbitrary string into a byte-plane field. Add ' \
            'ReplicaBlobRef to replicaman (and the blob branch to Column) first'
    end

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
      shape: column.fetch('shapes', nil),
      blob: column.fetch('blob', nil) == true
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
def variant_columns(variant)
  variant.fetch('columns').map do |column|
    if column.fetch('blob', nil) == true
      raise "replica-codegen: #{variant.fetch('type')}.#{column.fetch('name')} is a blob column — " \
            'the Rust runtime has no ReplicaBlobRef yet'
    end

    Column.new(
      name: column.fetch('name'),
      wire_type: column.fetch('type'),
      optional: true,
      push: column.fetch('push'),
      pull: column.fetch('pull'),
      enum_name: column.fetch('enum', nil) ? column.fetch('enumName') : nil,
      enum_values: column.fetch('enum', nil),
      shape_name: column.fetch('shapes', nil) ? "#{variant.fetch('rust_type')}#{camelize(column.fetch('name'))}" : nil,
      shape: column.fetch('shapes', nil),
      blob: column.fetch('blob', nil) == true
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
    variant_columns(variant).filter(&:enum_name).each do |column|
      definitions << { name: column.enum_name, values: column.enum_values }
    end
  end
  definitions.uniq { it[:name] }
end

# The one enum shape, shared by every lane: a `#[serde(rename)]` on every
# variant (the wire spelling is never the identifier) plus the exhaustive
# `from_wire`/`as_wire` pair. `from_wire` is what makes a LENIENT decode
# possible (an unknown value collapses to the declared default); the derived
# `Deserialize` is what keeps a `Vec<Enum>` STRICT. Both are needed.
def enum_source(name, values)
  cases = values.map do |value|
    "    #[serde(rename = #{rust_string(value)})]\n    #{rust_variant(value)},"
  end.join("\n")
  from_wire = values.map do |value|
    "            #{rust_string(value)} => Some(#{name}::#{rust_variant(value)}),"
  end.join("\n")
  as_wire = values.map do |value|
    "            #{name}::#{rust_variant(value)} => #{rust_string(value)},"
  end.join("\n")

  <<~RUST
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
    pub enum #{name} {
    #{cases}
    }

    impl #{name} {
        pub fn from_wire(wire: &str) -> Option<Self> {
            match wire {
    #{from_wire}
                _ => None,
            }
        }

        pub fn as_wire(&self) -> &'static str {
            match self {
    #{as_wire}
            }
        }
    }
  RUST
end

def stream_enum_source(stream)
  join_items(enum_definitions(stream).map { enum_source(it[:name], it[:values]) })
end

# --- shapes ------------------------------------------------------------------

def shape_nested_name(field, singular_collections: false)
  return field.fetch('rust_type') if field.key?('rust_type')

  name = field.fetch('name')
  name = singularize(name) if singular_collections && %w[array map].include?(field.fetch('type'))
  camelize(name.gsub(/[^0-9A-Za-z]+/, '_'))
end

def shape_rust_type(node, nested_name, typed_enums: true)
  return nested_name if typed_enums && node.fetch('enum', nil)

  case node.fetch('type')
  when 'array'
    items = node.fetch('items')
    inner = shape_rust_type(items, nested_name, typed_enums: typed_enums)
    inner = "Option<#{inner}>" if items.fetch('null', nil)
    "Vec<#{inner}>"
  when 'map'
    "BTreeMap<String, #{shape_rust_type(node.fetch('values'), nested_name, typed_enums: typed_enums)}>"
  when 'object'
    nested_name
  else
    rust_type(node.fetch('type'))
  end
end

def shape_field_type(field, prefix:, singular_collections: false, typed_enums: true)
  shape_rust_type(
    field,
    prefix + shape_nested_name(field, singular_collections: singular_collections),
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

def format_float(value)
  number = Float(value)
  raise "replica-codegen: #{value.inspect} is not a finite float default" unless number.finite?

  number.to_s
end

def replica_value_literal(value)
  case value
  when nil
    'ReplicaValue::Null'
  when true, false
    "ReplicaValue::Bool(#{value})"
  when Integer
    "ReplicaValue::signed_integer(#{value})"
  when Numeric
    "ReplicaValue::Number(#{format_float(value)})"
  when String
    "ReplicaValue::String(#{rust_string(value)}.to_owned())"
  when Array
    return 'ReplicaValue::Array(Vec::new())' if value.empty?

    "ReplicaValue::Array(vec![#{value.map { replica_value_literal(it) }.join(', ')}])"
  when Hash
    return 'ReplicaValue::Object(ReplicaFields::new())' if value.empty?

    pairs = value.map { |key, item| "(#{rust_string(key)}.to_owned(), #{replica_value_literal(item)})" }
    "ReplicaValue::Object(ReplicaFields::from([#{pairs.join(', ')}]))"
  else
    raise "replica-codegen: unsupported JSON default #{value.inspect}"
  end
end

# A manifest default, as a Rust expression. `type_name` is the FLATTENED name
# of this node's own nested type — Swift leans on leading-dot inference for
# enums and on `.init` for objects; Rust needs the path spelled.
def shape_value_source(node, value, type_name:, singular_collections: false, typed_enums: true)
  return 'None' if value.nil?

  if node.fetch('enum', nil)
    return typed_enums ? "#{type_name}::#{rust_variant(value)}" : "#{rust_string(value)}.to_owned()"
  end

  case node.fetch('type')
  when 'array'
    item = node.fetch('items')
    children = value.map do
      shape_value_source(item, it, type_name: type_name, singular_collections: singular_collections, typed_enums: typed_enums)
    end
    children.empty? ? 'Vec::new()' : "vec![#{children.join(', ')}]"
  when 'map'
    item = node.fetch('values')
    pairs = value.map do |key, child|
      child_source = shape_value_source(
        item, child,
        type_name: type_name, singular_collections: singular_collections, typed_enums: typed_enums
      )
      "(#{rust_string(key)}.to_owned(), #{child_source})"
    end
    pairs.empty? ? 'BTreeMap::new()' : "BTreeMap::from([#{pairs.join(', ')}])"
  when 'object'
    fields = node.fetch('fields').map do |field|
      child_type = type_name + shape_nested_name(field, singular_collections: singular_collections)
      expression =
        if value.key?(field.fetch('name'))
          shape_value_source(
            field, value.fetch(field.fetch('name')),
            type_name: child_type, singular_collections: singular_collections, typed_enums: typed_enums
          )
        else
          shape_field_default(field, child_type, singular_collections: singular_collections, typed_enums: typed_enums)
        end
      "#{rust_field(field.fetch('name'))}: #{expression}"
    end
    "#{type_name} { #{fields.join(', ')} }"
  when 'json', 'jsonb'
    replica_value_literal(value)
  when 'string', 'text', 'hex_color', 'datetime', 'date', 'time'
    "#{rust_string(value)}.to_owned()"
  when 'boolean'
    value.to_s
  when 'integer', 'bigint'
    Integer(value).to_s
  when 'float', 'decimal'
    format_float(value)
  else
    replica_value_literal(value)
  end
end

# What a field holds when nobody supplies it: its declared default, or `None`.
# A nullable field wraps its default in `Some` — Swift's `T? = .queued`.
def shape_field_default(field, type_name, singular_collections: false, typed_enums: true)
  unless field.key?('default')
    if field.fetch('null', nil) == false
      raise "replica-codegen: #{field.fetch('name')} is required and has no default — " \
            'it cannot be filled in'
    end

    return 'None'
  end

  value = field.fetch('default', nil)
  return 'None' if value.nil?

  expression = shape_value_source(
    field, value,
    type_name: type_name, singular_collections: singular_collections, typed_enums: typed_enums
  )
  field.fetch('null', nil) == false ? expression : "Some(#{expression})"
end

# The Swift emitter nests types inside their owner; Rust cannot, so the tree
# is walked once and flattened. Enums come out PRE-order and structs
# POST-order, which is what puts every declaration ahead of its use.
def collect_shape_types(name, fields, prefix:, enums:, structs:, discriminator: nil,
                        singular_collections: false, typed_enums: true, replica_bridge: false,
                        derive_deserialize: false, snake_wire: false)
  fields.each do |field|
    node = shape_enum_node(field)
    next unless node

    enums << {
      name: prefix + shape_nested_name(field, singular_collections: singular_collections),
      values: node.fetch('enum')
    }
  end

  fields.each do |field|
    node = shape_nested_object(field)
    next unless node

    child = prefix + shape_nested_name(field, singular_collections: singular_collections)
    collect_shape_types(
      child, node.fetch('fields'),
      prefix: child, enums: enums, structs: structs,
      singular_collections: singular_collections, typed_enums: typed_enums,
      snake_wire: snake_wire
    )
  end

  structs << {
    name: name, fields: fields, prefix: prefix, discriminator: discriminator,
    singular_collections: singular_collections, typed_enums: typed_enums,
    replica_bridge: replica_bridge, derive_deserialize: derive_deserialize,
    snake_wire: snake_wire
  }
end

# A field's `Raw` mirror type: every field reads as `Option<…>`, and an enum
# reads as `Option<String>` so an unknown VALUE can collapse to the declared
# default instead of failing the whole entry.
def raw_field_type(field, prefix:, singular_collections:, typed_enums:)
  return 'Option<String>' if typed_enums && field.fetch('enum', nil) && !%w[array map].include?(field.fetch('type'))

  "Option<#{shape_field_type(field, prefix: prefix, singular_collections: singular_collections, typed_enums: typed_enums)}>"
end

# The wire spelling of a shape field. The ROW lane speaks the server's
# camelized names verbatim; the DOCUMENT lane speaks snake_case (Swift gets
# there with `keyEncodingStrategy = .convertToSnakeCase`), which is already
# what a Rust field is called — so only the row lane needs a rename.
def wire_field_name(name, snake_wire:)
  snake_wire ? snake_case(name) : name
end

def serde_rename(name, snake_wire:)
  wire = wire_field_name(name, snake_wire: snake_wire)
  rust_field(name) == wire ? nil : "    #[serde(rename = #{rust_string(wire)})]"
end

def shape_struct_source(entry)
  name = entry.fetch(:name)
  fields = entry.fetch(:fields)
  prefix = entry.fetch(:prefix)
  discriminator = entry[:discriminator]
  singular_collections = entry.fetch(:singular_collections)
  typed_enums = entry.fetch(:typed_enums)
  snake_wire = entry.fetch(:snake_wire)

  lines = []
  derives = ['Clone', 'Debug', 'PartialEq', 'Serialize']
  derives << 'Deserialize' if entry[:derive_deserialize]
  lines << "#[derive(#{derives.join(', ')})]"
  lines << "pub struct #{name} {"
  if discriminator
    rename = serde_rename(discriminator, snake_wire: snake_wire)
    lines << rename if rename
    lines << "    pub #{rust_field(discriminator)}: String,"
  end
  fields.each do |field|
    rename = serde_rename(field.fetch('name'), snake_wire: snake_wire)
    lines << rename if rename
    type = shape_field_type(field, prefix: prefix, singular_collections: singular_collections, typed_enums: typed_enums)
    type = "Option<#{type}>" if field.fetch('null', nil) != false
    lines << "    pub #{rust_field(field.fetch('name'))}: #{type},"
  end
  lines << '}'
  lines << ''

  # `new(..)` takes exactly the fields with no declared default, in
  # declaration order; everything else is filled with what it declares.
  parameters = []
  assignments = []
  if discriminator
    parameters << "#{rust_field(discriminator)}: impl Into<String>"
    assignments << "            #{rust_field(discriminator)}: #{rust_field(discriminator)}.into(),"
  end
  fields.each do |field|
    field_name = rust_field(field.fetch('name'))
    child_type = prefix + shape_nested_name(field, singular_collections: singular_collections)
    type = shape_field_type(field, prefix: prefix, singular_collections: singular_collections, typed_enums: typed_enums)
    if field.fetch('null', nil) == false && !field.key?('default')
      if type == 'String'
        parameters << "#{field_name}: impl Into<String>"
        assignments << "            #{field_name}: #{field_name}.into(),"
      else
        parameters << "#{field_name}: #{type}"
        assignments << "            #{field_name},"
      end
    else
      default = shape_field_default(field, child_type, singular_collections: singular_collections, typed_enums: typed_enums)
      assignments << "            #{field_name}: #{default},"
    end
  end

  body = ["impl #{name} {"] + too_many_arguments(parameters) +
         ["    pub fn new(#{parameters.join(', ')}) -> Self {", '        Self {']
  body.concat(assignments)
  body << '        }'
  body << '    }'

  unless typed_enums
    fields.each do |field|
      node = shape_enum_node(field)
      next unless node

      body.concat(untyped_enum_accessor(field, prefix: prefix, singular_collections: singular_collections))
    end
  end

  body.concat(entry[:extra_impl]) if entry[:extra_impl]

  if entry[:replica_bridge]
    body << ''
    body << "    pub fn from_replica_value(value: Option<&ReplicaValue>) -> Option<Self> {"
    body << '        value.and_then(|value| value_coding::decode(value).ok())'
    body << '    }'
    body << ''
    body << '    pub fn replica_value(&self) -> ReplicaValue {'
    body << '        value_coding::encode(self).unwrap_or(ReplicaValue::Null)'
    body << '    }'
  end
  body << '}'
  lines.concat(body)

  if parameters.empty?
    lines << ''
    lines << "impl Default for #{name} {"
    lines << '    fn default() -> Self {'
    lines << '        Self::new()'
    lines << '    }'
    lines << '}'
  end

  unless entry[:derive_deserialize]
    lines << ''
    lines.concat(shape_deserialize_source(entry))
  end

  lines.join("\n")
end

# The accessors a DISCRIMINATED variant gets: its enum-typed columns keep the
# established raw-value surface and the vocabulary rides alongside.
def untyped_enum_accessor(field, prefix:, singular_collections:)
  field_name = rust_field(field.fetch('name'))
  enum_name = prefix + shape_nested_name(field, singular_collections: singular_collections)
  optional = field.fetch('null', nil) != false
  lines = ['']
  if field.fetch('type') == 'array'
    if optional
      lines << "    pub fn #{field_name}_value(&self) -> Option<Vec<#{enum_name}>> {"
      lines << "        self.#{field_name}"
      lines << '            .as_ref()'
      lines << "            .map(|values| values.iter().filter_map(|value| #{enum_name}::from_wire(value)).collect())"
      lines << '    }'
      lines << ''
      lines << "    pub fn set_#{field_name}_value(&mut self, value: Option<Vec<#{enum_name}>>) {"
      lines << "        self.#{field_name} ="
      lines << "            value.map(|values| values.iter().map(|value| value.as_wire().to_owned()).collect());"
      lines << '    }'
    else
      lines << "    pub fn #{field_name}_value(&self) -> Vec<#{enum_name}> {"
      lines << "        self.#{field_name}"
      lines << '            .iter()'
      lines << "            .filter_map(|value| #{enum_name}::from_wire(value))"
      lines << '            .collect()'
      lines << '    }'
      lines << ''
      lines << "    pub fn set_#{field_name}_value(&mut self, value: Vec<#{enum_name}>) {"
      lines << "        self.#{field_name} = value.iter().map(|value| value.as_wire().to_owned()).collect();"
      lines << '    }'
    end
  elsif optional
    lines << "    pub fn #{field_name}_value(&self) -> Option<#{enum_name}> {"
    lines << "        self.#{field_name}.as_deref().and_then(#{enum_name}::from_wire)"
    lines << '    }'
    lines << ''
    lines << "    pub fn set_#{field_name}_value(&mut self, value: Option<#{enum_name}>) {"
    lines << "        self.#{field_name} = value.map(|value| value.as_wire().to_owned());"
    lines << '    }'
  else
    lines << "    pub fn #{field_name}_value(&self) -> Option<#{enum_name}> {"
    lines << "        #{enum_name}::from_wire(&self.#{field_name})"
    lines << '    }'
    lines << ''
    lines << "    pub fn set_#{field_name}_value(&mut self, value: #{enum_name}) {"
    lines << "        self.#{field_name} = value.as_wire().to_owned();"
    lines << '    }'
  end
  lines
end

# Schema-evolution contract, read side (mirrors the server's StoreModel):
# a missing key with a declared default reads AS the default, a missing
# nullable key reads None, unknown keys are skipped, and an enum VALUE this
# build does not know collapses to the declared default (None when nullable).
# Only a key that is required — non-null with no default — still fails the
# decode. Without this, every field added to a document shape made every
# pre-existing document's entries undecodable.
#
# Rust spells it as a private all-`Option` `Raw` mirror declared inside the
# `deserialize` body — the positional analogue of Swift's nested `CodingKeys`
# plus `init(from:)`.
# A default cheap enough to build unconditionally. Anything that allocates
# rides `unwrap_or_else` so clippy's `or_fun_call` stays quiet — and so the
# allocation only happens when the key really is missing.
# An empty collection IS the type's `Default`, so it spells as
# `unwrap_or_default()` — `unwrap_or_else(|| Vec::new())` is a closure clippy
# (rightly) calls redundant. A `ReplicaValue::Object(…)` is NOT in this set:
# `ReplicaValue::default()` is `Null`, not an empty object.
EMPTY_IS_DEFAULT = ['Vec::new()', 'BTreeMap::new()', 'String::new()'].freeze

def simple_default?(field, typed_enums:)
  return true if typed_enums && field.fetch('enum', nil) && !%w[array map].include?(field.fetch('type'))

  %w[boolean integer bigint float decimal].include?(field.fetch('type'))
end

def shape_deserialize_source(entry)
  name = entry.fetch(:name)
  fields = entry.fetch(:fields)
  prefix = entry.fetch(:prefix)
  discriminator = entry[:discriminator]
  singular_collections = entry.fetch(:singular_collections)
  typed_enums = entry.fetch(:typed_enums)
  snake_wire = entry.fetch(:snake_wire)

  raw = []
  if discriminator
    rename = serde_rename(discriminator, snake_wire: snake_wire)
    raw << "            #{rename.strip}" if rename
    raw << "            #{rust_field(discriminator)}: Option<String>,"
  end
  fields.each do |field|
    rename = serde_rename(field.fetch('name'), snake_wire: snake_wire)
    raw << "            #{rename.strip}" if rename
    type = raw_field_type(field, prefix: prefix, singular_collections: singular_collections, typed_enums: typed_enums)
    raw << "            #{rust_field(field.fetch('name'))}: #{type},"
  end

  assignments = []
  if discriminator
    field_name = rust_field(discriminator)
    assignments << "            #{field_name}: raw.#{field_name}.ok_or_else(|| de::Error::missing_field(#{rust_string(wire_field_name(discriminator, snake_wire: snake_wire))}))?,"
  end
  fields.each do |field|
    wire_name = wire_field_name(field.fetch('name'), snake_wire: snake_wire)
    field_name = rust_field(field.fetch('name'))
    child_type = prefix + shape_nested_name(field, singular_collections: singular_collections)
    plain_enum = typed_enums && field.fetch('enum', nil) && field.fetch('type') != 'array' && field.fetch('type') != 'map'
    default =
      if field.key?('default') && !field.fetch('default', nil).nil?
        shape_value_source(field, field.fetch('default', nil), type_name: child_type, singular_collections: singular_collections, typed_enums: typed_enums)
      end
    expression =
      if plain_enum && field.fetch('null', nil) == false && default
        "raw.#{field_name}.as_deref().and_then(#{child_type}::from_wire).unwrap_or(#{default})"
      elsif plain_enum && field.fetch('null', nil) == false
        # A REQUIRED enum with no declared default: there is nothing to
        # collapse an unknown value to, so a missing key and a value this
        # build does not know are both a decode refusal — which is what the
        # evolution contract says a required field does.
        "raw.#{field_name}\n                .as_deref()\n                .and_then(#{child_type}::from_wire)\n                .ok_or_else(|| de::Error::missing_field(#{rust_string(wire_name)}))?"
      elsif plain_enum && field.fetch('null', nil) != false
        "raw.#{field_name}.as_deref().and_then(#{child_type}::from_wire)"
      elsif field.fetch('null', nil) == false && default
        if simple_default?(field, typed_enums: typed_enums)
          "raw.#{field_name}.unwrap_or(#{default})"
        elsif EMPTY_IS_DEFAULT.include?(default)
          "raw.#{field_name}.unwrap_or_default()"
        else
          "raw.#{field_name}.unwrap_or_else(|| #{default})"
        end
      elsif field.fetch('null', nil) == false
        "raw.#{field_name}.ok_or_else(|| de::Error::missing_field(#{rust_string(wire_name)}))?"
      else
        "raw.#{field_name}"
      end
    assignments << "            #{field_name}: #{expression},"
  end

  [
    "impl<'de> Deserialize<'de> for #{name} {",
    "    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {",
    '        #[derive(Deserialize)]',
    '        struct Raw {',
    *raw,
    '        }',
    '',
    '        let raw = Raw::deserialize(deserializer)?;',
    '',
    "        Ok(#{name} {",
    *assignments,
    '        })',
    '    }',
    '}'
  ]
end

# One shape-backed column's whole type surface: an object shape is a struct
# with the ReplicaValue bridge, an array-of-objects shape is the ELEMENT
# struct (the column bridges at the array level), and a discriminated shape
# is a closed union with an `Unknown` arm that keeps a future kind readable.
def shape_source(name, shape)
  enums = []
  structs = []

  unless shape.fetch('discriminator', nil)
    if shape.fetch('type', nil) == 'array'
      # The element type. Arrays bridge at the ARRAY level in
      # `projection_source`, so the element needs no `replica_value` of its own.
      collect_shape_types(name, shape.fetch('items').fetch('fields'), prefix: name, enums: enums, structs: structs)
      return render_shape_types(enums, structs)
    end
    raise "replica-codegen: shape #{name} must be an object or an array of objects" unless shape.fetch('type', nil) == 'object'

    collect_shape_types(name, shape.fetch('fields'), prefix: name, enums: enums, structs: structs, replica_bridge: true)
    return render_shape_types(enums, structs)
  end

  discriminator = shape.fetch('discriminator')
  variants = shape.fetch('variants').map do |variant|
    variant.merge('rust_name' => "#{name}#{variant.fetch('variantName')}", 'case_name' => variant.fetch('variantName'))
  end

  variants.each do |variant|
    collect_shape_types(
      variant.fetch('rust_name'), variant.fetch('fields'),
      prefix: variant.fetch('rust_name'), enums: enums, structs: structs,
      discriminator: discriminator, typed_enums: false
    )
  end

  shared = "#{name}Shared"
  lines = [render_shape_types(enums, structs)]
  lines << <<~RUST
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct #{shared} {
    #{[serde_rename(discriminator, snake_wire: false), "    pub #{rust_field(discriminator)}: String,"].compact.join("\n")}
    }

    impl #{shared} {
        pub fn new(#{rust_field(discriminator)}: impl Into<String>) -> Self {
            Self {
                #{rust_field(discriminator)}: #{rust_field(discriminator)}.into(),
            }
        }
    }
  RUST

  cases = variants.map { "    #{it.fetch('case_name')}(#{it.fetch('rust_name')})," }
  serialize_arms = variants.map { "            #{name}::#{it.fetch('case_name')}(value) => value.serialize(serializer)," }
  decode_arms = variants.map do
    "            #{rust_string(it.fetch('value'))} => value_coding::decode(&value)\n" \
      "                .map(#{name}::#{it.fetch('case_name')})\n" \
      '                .map_err(de::Error::custom),'
  end
  accessors = variants.map do
    <<~RUST.chomp
          pub fn as_#{snake_case(it.fetch('case_name'))}(&self) -> Option<&#{it.fetch('rust_name')}> {
              if let #{name}::#{it.fetch('case_name')}(value) = self {
                  Some(value)
              } else {
                  None
              }
          }
    RUST
  end

  lines << <<~RUST
    #[derive(Clone, Debug, PartialEq)]
    pub enum #{name} {
    #{cases.join("\n")}
        Unknown(#{shared}),
    }

    impl #{name} {
    #{accessors.join("\n\n")}

        pub fn from_replica_value(value: Option<&ReplicaValue>) -> Option<Self> {
            value.and_then(|value| value_coding::decode(value).ok())
        }

        pub fn replica_value(&self) -> ReplicaValue {
            value_coding::encode(self).unwrap_or(ReplicaValue::Null)
        }
    }

    impl Serialize for #{name} {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match self {
    #{serialize_arms.join("\n")}
                #{name}::Unknown(value) => value.serialize(serializer),
            }
        }
    }

    impl<'de> Deserialize<'de> for #{name} {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            let value = ReplicaValue::deserialize(deserializer)?;
            let discriminator = value
                .get(#{rust_string(discriminator)})
                .and_then(ReplicaValue::as_string)
                .ok_or_else(|| de::Error::missing_field(#{rust_string(discriminator)}))?
                .to_owned();

            match discriminator.as_str() {
    #{decode_arms.join("\n")}
                _ => value_coding::decode(&value)
                    .map(#{name}::Unknown)
                    .map_err(de::Error::custom),
            }
        }
    }
  RUST
  join_items(lines)
end

def render_shape_types(enums, structs)
  join_items(enums.map { enum_source(it[:name], it[:values]) } + structs.map { shape_struct_source(it) })
end

def shape_definitions(stream)
  columns = base_columns(stream)
  stream.fetch('variants', []).each { |variant| columns.concat(variant_columns(variant)) }
  join_items(columns.filter(&:shape_name).uniq(&:shape_name).map do |column|
    shape_source(column.shape_name, column.shape)
  end)
end

def support_definitions(stream)
  join_items([stream_enum_source(stream), shape_definitions(stream)])
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

# Swift's `guard let x = … else { return nil }`. The decode initializers all
# answer `Option<Self>`, so Rust spells the same early-out as `?` — which is
# also what clippy insists on.
def guard_lines(columns, indent)
  columns.reject(&:optional).map do |column|
    "#{indent}let #{column.storage_name} = #{column.decode_expression}?;"
  end
end

# The declared push set IS the outbound wire: a `push: false` column
# (server-derived pulls like previewUrl, server-stamped state) NEVER enters
# `encode()` — save() diffs must not echo it back up the wire. Writable
# optionals are full value semantics: None is an explicit `.null` clear so
# save_row can distinguish it from a column the model does not author.
def encode_lines(columns, indent, prefix: 'self.')
  columns.select(&:push).map do |column|
    reference = "#{prefix}#{column.storage_name}"
    if column.optional
      "#{indent}encoded.insert(#{rust_string(column.name)}.to_owned(), #{reference}.as_ref()" \
        ".map_or(ReplicaValue::Null, |value| #{column.encode_expression('value', by_ref: true)}));"
    else
      "#{indent}encoded.insert(#{rust_string(column.name)}.to_owned(), #{column.encode_expression(reference)});"
    end
  end
end

# The engine owns ordinary row provenance (`ReplicaCreateStamp::standard()`
# fills the ownership/clock columns from the authenticated session at
# create). A document stream with those standard columns therefore exposes
# only the caller-authored slice of its declared push set; no manifest
# naming hint or platform override is involved.
#
# Swift hangs this off `extension DocumentStream where Model == Deck`.
# Rust forbids an inherent impl on a foreign generic type AND would collide
# with the runtime's own `create`, so it lands as an extension TRAIT with a
# model-qualified verb. The trait is declared with an explicit RPITIT
# signature so `-D warnings` does not trip `async_fn_in_trait`.
def document_create_extension(stream, model, columns)
  standard = {
    'userId' => %w[integer bigint],
    'createdAt' => %w[datetime],
    'updatedAt' => %w[datetime]
  }
  by_name = columns.to_h { |column| [column.name, column] }
  return '' unless standard.all? { |name, types| by_name[name] && types.include?(by_name[name].wire_type) }

  authored = columns.select(&:push).reject { |column| standard.key?(column.name) }
  verb = "create_#{snake_case(model)}"
  parameters = authored.map { "#{it.storage_name}: #{it.declared_type}" }
  signature = (['id: &str'] + parameters + ['snapshot: &[u8]', 'document_peer: u64']).join(', ')
  entries = authored.map do |column|
    encoded =
      if column.optional
        "#{column.storage_name}.as_ref().map_or(ReplicaValue::Null, |value| #{column.encode_expression('value', by_ref: true)})"
      else
        column.encode_expression(column.storage_name)
      end
    "            data.insert(#{rust_string(column.name)}.to_owned(), #{encoded});"
  end

  <<~RUST

    pub trait #{model}Create {
        fn #{verb}(
            &self,
            #{signature},
        ) -> impl std::future::Future<Output = ReplicaResult<bool>> + Send;
    }

    impl #{model}Create for DocumentStream<#{model}> {
        async fn #{verb}(
            &self,
            #{signature},
        ) -> ReplicaResult<bool> {
            let mut data = ReplicaFields::new();
    #{entries.join("\n")}
            self.engine
                .create_doc(
                    #{model}::stream_name(),
                    id,
                    snapshot,
                    document_peer,
                    &data,
                    Some(&ReplicaCreateStamp::standard()),
                )
                .await
        }
    }
  RUST
end

# --- imports -----------------------------------------------------------------

# Rust has no `import Foundation` catch-all, so the use block is derived from
# the body that was just rendered. Scanning the text rather than re-deriving
# the conditions keeps the two from drifting apart.
def rust_use_block(body, replicaman: [])
  groups = []

  std = []
  std << 'use std::collections::BTreeMap;' if body.include?('BTreeMap')
  groups << std unless std.empty?

  serde = []
  de = []
  de << 'self' if body.include?('de::Error')
  de << 'Deserializer' if body.include?('Deserializer')
  serde << "use serde::de::{#{de.join(', ')}};" unless de.empty?
  top = []
  top << 'Deserialize' if body.include?('Deserialize')
  top << 'Serialize' if body.include?('Serialize')
  top << 'Serializer' if body.include?('Serializer')
  serde << "use serde::{#{top.join(', ')}};" unless top.empty?
  groups << serde unless serde.empty?

  crate = []
  crate << 'use replicaman::value_coding;' if body.include?('value_coding::')
  symbols = replicaman.uniq.sort
  crate << "use replicaman::{#{symbols.join(', ')}};" unless symbols.empty?
  groups << crate unless crate.empty?

  groups.map { it.join("\n") }.join("\n\n")
end

def row_model_symbols(stream, body)
  symbols = ['ReplicaFields', 'ReplicaValue']
  if stream.fetch('lane') == 'document'
    symbols << 'ReplicaDocModel'
    if body.include?('DocumentStream<')
      symbols.concat(%w[DocumentStream ReplicaCreateStamp ReplicaResult])
    end
  else
    symbols << 'ReplicaRowModel'
    symbols << 'ReplicaWritableRowModel' unless stream.fetch('readonly', nil)
  end
  symbols
end

# --- templates ---------------------------------------------------------------

MODEL_PRELUDE = ERB.new(<<~'RUST', trim_mode: '-')
  // Generated by replica-codegen — DO NOT EDIT.
  <%= header(stream) %>
RUST

# The stored fields of one model shape (a plain model, or one STI variant).
def model_fields_source(columns)
  lines = ['    pub id: String,']
  columns.each do |column|
    lines << "    pub #{column.storage_name}: #{column.declared_type},"
  end
  columns.filter(&:materialized_projection?).each do |column|
    lines << column.materialized_field_source('    ')
  end
  lines.join("\n")
end

# `new(..)` takes `id` plus every column that cannot be absent; an optional
# column starts as `None`, exactly like Swift's `= nil` memberwise default.
def model_new_source(columns)
  parameters = ['id: impl Into<String>']
  bindings = []
  assignments = ['            id: id.into(),']
  columns.each do |column|
    if column.optional
      assignments << "            #{column.storage_name}: None,"
    else
      type = column.declared_type
      parameters << (type == 'String' ? "#{column.storage_name}: impl Into<String>" : "#{column.storage_name}: #{type}")
      assignments << (type == 'String' ? "            #{column.storage_name}: #{column.storage_name}.into()," : "            #{column.storage_name},")
    end
  end
  columns.filter(&:materialized_projection?).each do |column|
    if column.optional
      assignments << "            #{column.projection_name}: None,"
    else
      bindings << "        let #{column.projection_name} = #{column.shape_name}::from_replica_value(Some(&#{column.storage_name}));"
      assignments << "            #{column.projection_name},"
    end
  end

  (too_many_arguments(parameters) + [
    "    pub fn new(#{parameters.join(', ')}) -> Self {",
  ] + bindings + ['        Self {'] + assignments + ['        }', '    }']).join("\n")
end

# A struct-literal field init. A required column was already bound by its
# guard under the same name, so it takes Rust's shorthand — clippy refuses
# the `name: name` spelling.
def model_field_init(column)
  return column.storage_name unless column.optional

  "#{column.storage_name}: #{column.decode_expression}"
end

# The decode half: a guard per required column, then the literal. Optional
# columns read inline, exactly as the Swift initializer does.
def model_decode_body(columns, indent, constructor, id_expression: 'id.to_owned()')
  lines = guard_lines(columns, indent)
  lines << "#{indent}Some(#{constructor} {"
  lines << "#{indent}    id: #{id_expression},"
  columns.each do |column|
    lines << "#{indent}    #{model_field_init(column)},"
  end
  columns.filter(&:materialized_projection?).each do |column|
    lines << column.materialization_assignment("#{indent}    ", "fields.get(#{rust_string(column.name)})")
  end
  lines << "#{indent}})"
  lines.join("\n")
end

def model_encode_body(columns, indent)
  lines = encode_lines(columns, "#{indent}    ")
  return "#{indent}    ReplicaFields::new()" if lines.empty?

  ([
    "#{indent}    let mut encoded = ReplicaFields::new();"
  ] + lines + ["#{indent}    encoded"]).join("\n")
end

def model_projection_source(columns)
  columns.filter(&:shape_name).map { it.projection_source('    ') }
end

def doc_model_source(stream, model, columns)
  body = []
  support = support_definitions(stream)
  body << support unless support.empty?
  body << <<~RUST
    #[derive(Clone, Debug, PartialEq)]
    pub struct #{model} {
    #{model_fields_source(columns)}
    }
  RUST

  impl_body = [model_new_source(columns)]
  impl_body.concat(model_projection_source(columns).map { "\n#{it}" })
  body << "impl #{model} {\n#{impl_body.join("\n")}\n}\n"

  body << <<~RUST
    impl ReplicaDocModel for #{model} {
        fn stream_name() -> &'static str {
            #{rust_string(stream.fetch('name'))}
        }

        fn decode(id: &str, fields: &ReplicaFields) -> Option<Self> {
    #{model_decode_body(columns, '        ', 'Self')}
        }

        fn id(&self) -> &str {
            &self.id
        }
    }
  RUST

  extension = document_create_extension(stream, model, columns)
  body << extension unless extension.empty?
  rendered = join_items(body)
  join_items([MODEL_PRELUDE.result_with_hash(stream: stream),
              rust_use_block(rendered, replicaman: row_model_symbols(stream, rendered)), rendered]) + "\n"
end

def row_model_source(stream, model, columns)
  body = []
  support = support_definitions(stream)
  body << support unless support.empty?
  body << <<~RUST
    #[derive(Clone, Debug, PartialEq)]
    pub struct #{model} {
    #{model_fields_source(columns)}
    }
  RUST

  impl_body = [model_new_source(columns)]
  impl_body.concat(model_projection_source(columns).map { "\n#{it}" })
  body << "impl #{model} {\n#{impl_body.join("\n")}\n}\n"

  body << <<~RUST
    impl ReplicaRowModel for #{model} {
        fn stream_name() -> &'static str {
            #{rust_string(stream.fetch('name'))}
        }

        fn decode(id: &str, _row_type: Option<&str>, fields: &ReplicaFields) -> Option<Self> {
    #{model_decode_body(columns, '        ', 'Self')}
        }

        fn id(&self) -> &str {
            &self.id
        }

        fn type_name(&self) -> Option<&str> {
            None
        }

        fn encode(&self) -> ReplicaFields {
    #{model_encode_body(columns, '    ')}
        }
    }
  RUST
  body << "impl ReplicaWritableRowModel for #{model} {}\n" unless stream.fetch('readonly', nil)

  rendered = join_items(body)
  join_items([MODEL_PRELUDE.result_with_hash(stream: stream),
              rust_use_block(rendered, replicaman: row_model_symbols(stream, rendered)), rendered]) + "\n"
end

def sti_model_source(stream, model, base, variants)
  body = []
  support = support_definitions(stream)
  body << support unless support.empty?

  variants.each do |variant|
    columns = base + variant_columns(variant)
    name = variant.fetch('rust_type')
    body << <<~RUST
      #[derive(Clone, Debug, PartialEq)]
      pub struct #{name} {
      #{model_fields_source(columns)}
      }
    RUST
    impl_body = [model_new_source(columns)]
    impl_body.concat(model_projection_source(columns).map { "\n#{it}" })
    body << "impl #{name} {\n#{impl_body.join("\n")}\n}\n"
  end

  cases = variants.map { "    #{it.fetch('rust_type')}(#{it.fetch('rust_type')})," }
  body << <<~RUST
    #[derive(Clone, Debug, PartialEq)]
    pub enum #{model} {
    #{cases.join("\n")}
    }
  RUST

  decode_arms = variants.map do |variant|
    columns = base + variant_columns(variant)
    literal = ["            #{rust_string(variant.fetch('type'))} => Some(#{model}::#{variant.fetch('rust_type')}(#{variant.fetch('rust_type')} {"]
    literal << '                id: id.to_owned(),'
    columns.each do |column|
      literal << "                #{model_field_init(column)},"
    end
    columns.filter(&:materialized_projection?).each do |column|
      literal << column.materialization_assignment('                ', "fields.get(#{rust_string(column.name)})")
    end
    literal << '            })),'
    literal.join("\n")
  end

  id_arms = variants.map { "            #{model}::#{it.fetch('rust_type')}(model) => &model.id," }
  type_arms = variants.map { "            #{model}::#{it.fetch('rust_type')}(_) => Some(#{rust_string(it.fetch('type'))})," }

  encode_arms = variants.map do |variant|
    lines = encode_lines(base + variant_columns(variant), '                ', prefix: 'model.')
    if lines.empty?
      "            #{model}::#{variant.fetch('rust_type')}(_) => {}"
    else
      (["            #{model}::#{variant.fetch('rust_type')}(model) => {"] + lines + ['            }']).join("\n")
    end
  end
  encode_body =
    if variants.all? { encode_lines(base + variant_columns(it), '', prefix: 'model.').empty? }
      '        ReplicaFields::new()'
    else
      ([
        '        let mut encoded = ReplicaFields::new();',
        '        match self {'
      ] + encode_arms + ['        }', '        encoded']).join("\n")
    end

  body << <<~RUST
    impl ReplicaRowModel for #{model} {
        fn stream_name() -> &'static str {
            #{rust_string(stream.fetch('name'))}
        }

        fn decode(id: &str, row_type: Option<&str>, fields: &ReplicaFields) -> Option<Self> {
    #{guard_lines(base, '        ').join("\n")}
            match row_type? {
    #{decode_arms.join("\n")}
                _ => None,
            }
        }

        fn id(&self) -> &str {
            match self {
    #{id_arms.join("\n")}
            }
        }

        fn type_name(&self) -> Option<&str> {
            match self {
    #{type_arms.join("\n")}
            }
        }

        fn encode(&self) -> ReplicaFields {
    #{encode_body}
        }
    }
  RUST
  body << "impl ReplicaWritableRowModel for #{model} {}\n" unless stream.fetch('readonly', nil)

  rendered = join_items(body)
  join_items([MODEL_PRELUDE.result_with_hash(stream: stream),
              rust_use_block(rendered, replicaman: row_model_symbols(stream, rendered)), rendered]) + "\n"
end

# --- container ---------------------------------------------------------------

def handle_type(stream)
  if stream.fetch('lane') == 'document'
    stream.fetch('readonly', nil) ? 'ReadonlyDocumentStream' : 'DocumentStream'
  else
    stream.fetch('readonly', nil) ? 'ReadonlyRowStream' : 'RowStream'
  end
end

def blob_fields_literal(streams)
  entries = streams.filter_map do |stream|
    names = (stream.fetch('columns') || []).select { it.fetch('blob', nil) }.map { rust_string(it.fetch('name')) }
    next if names.empty?

    "            (#{rust_string(stream.fetch('name'))}, vec![#{names.join(', ')}]),"
  end
  return '        BTreeMap::new()' if entries.empty?

  (['        BTreeMap::from(['] + entries + ['        ])']).join("\n")
end

def container_source(manifest, streams, container)
  specs = streams.map do |stream|
    codec = stream.fetch('lane') == 'document' ? "Some(#{rust_string(stream.fetch('codec', nil))}.to_owned())" : 'None'
    lane = stream.fetch('lane') == 'document' ? 'StreamLane::Document' : 'StreamLane::Row'
    reflections = stream.fetch('columns').filter_map do |column|
      next unless column.fetch('reflects', nil)
      path = column.fetch('reflects', nil).map { rust_string(it) }.join(', ')
      "ReplicaReflection::new(#{rust_string(column.fetch('name'))}, [#{path}])"
    end
    references = stream.fetch('references', []).map do |reference|
      name = rust_string(reference.fetch('name'))
      target = rust_string(reference.fetch('stream'))
      field = reference.key?('field') ? "Some(#{rust_string(reference.fetch('field'))}.into())" : 'None'
      segment = reference.key?('keySegment') ? "Some(#{reference.fetch('keySegment')})" : 'None'
      prefix = reference.key?('keyPrefix') ? "Some(#{rust_string(reference.fetch('keyPrefix'))}.into())" : 'None'
      "ReplicaReferenceSpec { name: #{name}.into(), stream: #{target}.into(), field: #{field}, key_segment: #{segment}, key_prefix: #{prefix}, optional: #{reference.fetch('optional', false)} }"
    end
    lifetime = stream.key?('lifetimeFrom') ? "Some(#{rust_string(stream.fetch('lifetimeFrom'))}.into())" : 'None'
    preconditions = stream.fetch('columns').select { it.fetch('precondition', nil) }.map { "#{rust_string(it.fetch('name'))}.into()" }
    columns = stream.fetch('columns') + (stream.fetch('variants', nil) || []).flat_map { it.fetch('columns') }
    pushed = if stream.fetch('readonly', nil) || stream.fetch('lane') == 'document'
      'None'
    else
      names = columns.select { it.fetch('push', nil) }.map { it.fetch('name') }.uniq.sort.map { "#{rust_string(it)}.into()" }
      "Some([#{names.join(', ')}].into_iter().collect())"
    end
    by_name = stream.fetch('columns').to_h { [it.fetch('name'), it.fetch('type')] }
    standard = %w[integer bigint].include?(by_name.fetch('userId', nil)) && by_name.fetch('createdAt', nil) == 'datetime' && by_name.fetch('updatedAt', nil) == 'datetime'
    stamp = standard ? 'Some(ReplicaCreateStamp::standard())' : 'None'
    <<~RUST.chomp
              ReplicaStreamSpec {
                  name: #{rust_string(stream.fetch('name'))}.to_owned(),
                  lane: #{lane},
                  readonly: #{stream.fetch('readonly', nil)},
                  shard: #{rust_string(stream.fetch('shard', nil))}.to_owned(),
                  codec: #{codec},
                  reflections: vec![#{reflections.join(', ')}],
                  stamp: #{stamp},
                  preconditions: vec![#{preconditions.join(', ')}],
                  pushed: #{pushed},
                  references: vec![#{references.join(', ')}],
                  lifetime_from: #{lifetime},
              },
    RUST
  end

  handles = streams.map do |stream|
    handle = handle_type(stream)
    <<~RUST.chomp
          pub fn #{rust_field(stream.fetch('name'))}(&self) -> #{handle}<#{model_name(stream)}> {
              #{handle}::new(self.engine.clone())
          }
    RUST
  end

  models = streams.map { model_name(it) }.sort.uniq
  symbols = streams.map { handle_type(it) } + %w[ReplicaEngine ReplicaSchema ReplicaStreamSpec StreamLane]
  symbols << 'ReplicaCreateStamp' if specs.any? { it.include?('ReplicaCreateStamp::') }
  symbols << 'ReplicaReferenceSpec' if specs.any? { it.include?('ReplicaReferenceSpec {') }
  symbols << 'ReplicaReflection' if specs.any? { it.include?('ReplicaReflection::') }
  symbols = symbols.uniq.sort

  body = <<~RUST
    pub struct #{container} {
        pub engine: Arc<ReplicaEngine>,
    }

    impl #{container} {
        pub fn schema() -> ReplicaSchema {
            ReplicaSchema::new(vec![
    #{specs.join("\n")}
            ]).with_identity(#{manifest.fetch("namespace").to_json}, #{manifest.fetch("schemaVersion")})
        }

        /// Blob-reference fields per stream (wire names) — the byte plane's
        /// map: which row fields carry device-minted references, for the
        /// engine's staged-blob reconcile and reference-watch GC.
        pub fn blob_fields() -> BTreeMap<&'static str, Vec<&'static str>> {
    #{blob_fields_literal(streams)}
        }

        pub fn new(engine: Arc<ReplicaEngine>) -> Self {
            Self { engine }
        }

    #{handles.join("\n\n")}
    }
  RUST

  <<~RUST
    // Generated by replica-codegen — DO NOT EDIT.
    // Manifest version #{manifest.fetch('version')} · streams: #{streams.map { it.fetch('name') }.join(', ')}

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use replicaman::{#{symbols.join(', ')}};

    use super::{#{models.join(', ')}};

    #{body}
  RUST
end

def module_source(manifest, streams, modules, banner)
  <<~RUST
    // Generated by replica-codegen — DO NOT EDIT.
    // #{banner} · manifest version #{manifest.fetch('version')} · streams: #{streams.map { it.fetch('name') }.join(', ')}

    #{modules.sort.map { "mod #{it};" }.join("\n")}

    #{modules.sort.map { "pub use #{it}::*;" }.join("\n")}
  RUST
end

# --- document ----------------------------------------------------------------

def document_root_field(name, shape)
  shape.merge('name' => name, 'null' => false)
end

def document_shapes(stream)
  shapes = stream.fetch('shapes')
  return shapes unless shapes.key?('meta')

  shapes.merge('meta' => shapes.fetch('meta').merge('rust_type' => "#{model_name(stream)}DocumentMeta"))
end

def document_registry_shape?(shape)
  shape.fetch('type', nil) == 'array' && shape.dig('items', 'type') == 'object'
end

def document_shape_descriptor(shape)
  return 'Shape::String' if shape.fetch('enum', nil)

  case shape.fetch('type')
  when 'string', 'text', 'hex_color', 'datetime', 'date', 'time'
    'Shape::String'
  when 'integer', 'bigint'
    'Shape::Integer'
  when 'float', 'decimal'
    'Shape::Number'
  when 'boolean'
    'Shape::Boolean'
  when 'json', 'jsonb'
    'Shape::Json'
  when 'array'
    "Shape::array(#{document_shape_descriptor(shape.fetch('items'))})"
  when 'map'
    "Shape::Map(Box::new(#{document_shape_descriptor(shape.fetch('values'))}))"
  when 'object'
    fields = shape.fetch('fields').map do |field|
      "(#{rust_string(snake_case(field.fetch('name')))}, #{document_shape_descriptor(field)})"
    end
    "Shape::object(vec![#{fields.join(', ')}])"
  else
    'Shape::Json'
  end
end

# The two bridges into the doc plane. A registry root moves through
# `DocumentEntry` and injects/strips `key` so the key is never ALSO a field
# (the two would drift); a plain root is a bare field bag.
def document_projection_extensions(shapes)
  registry = shapes.select { |_, shape| document_registry_shape?(shape) }
  plain = shapes.reject { |_, shape| document_registry_shape?(shape) }

  extensions = registry.map do |root_name, shape|
    name = shape_nested_name(document_root_field(root_name, shape), singular_collections: true)
    <<~RUST
      impl #{name} {
          pub fn from_document_entry(entry: &DocumentEntry) -> Option<Self> {
              let mut fields = entry.fields.clone();
              fields.insert("key".to_owned(), DocumentValue::String(entry.key.clone()));

              coding::decode(&fields)
          }

          pub fn document_entry(&self) -> DocumentEntry {
              let mut fields = coding::encode(self, &#{document_shape_descriptor(shape.fetch('items'))});
              fields.remove("key");

              DocumentEntry::new(self.key.clone(), fields)
          }
      }
    RUST
  end

  extensions += plain.map do |root_name, shape|
    unless shape.fetch('type', nil) == 'object'
      raise "replica-codegen: document root #{root_name} must be an object or keyed object array"
    end

    name = shape_nested_name(document_root_field(root_name, shape), singular_collections: true)
    <<~RUST
      impl #{name} {
          pub fn from_document_fields(fields: &DocumentFields) -> Option<Self> {
              coding::decode(fields)
          }

          pub fn document_fields(&self) -> DocumentFields {
              coding::encode(self, &#{document_shape_descriptor(shape)})
          }
      }
    RUST
  end

  join_items(extensions)
end

def document_projection_methods(shapes, projection)
  guards = []
  arguments = []
  shapes.each do |root_name, shape|
    field = rust_field(root_name)
    name = shape_nested_name(document_root_field(root_name, shape), singular_collections: true)
    if document_registry_shape?(shape)
      guards << <<~RUST.chomp.lines.map { |line| "        #{line}" }.join
        let #{field}_keys: std::collections::HashSet<_> =
            projection.#{field}.iter().map(|entry| &entry.key).collect();
        if #{field}_keys.len() != projection.#{field}.len() {
            return None;
        }
      RUST
      arguments << "            projection\n                .#{field}\n                .iter()\n                .map(#{name}::from_document_entry)\n                .collect::<Option<Vec<_>>>()?,"

    else
      guards << "        let #{field} = #{name}::from_document_fields(&projection.#{field})?;"
      arguments << "            #{field},"
    end
  end

  registry = shapes.select { |_, shape| document_registry_shape?(shape) }
  plain = shapes.reject { |_, shape| document_registry_shape?(shape) }
  assignments = registry.map do |root_name, shape|
    name = shape_nested_name(document_root_field(root_name, shape), singular_collections: true)
    "            #{rust_field(root_name)}: self.#{rust_field(root_name)}.iter().map(#{name}::document_entry).collect(),"
  end
  assignments += plain.map do |root_name, _|
    "            #{rust_field(root_name)}: self.#{rust_field(root_name)}.document_fields(),"
  end

  <<~RUST.chomp.lines.map(&:chomp)
    #{''}
        /// Refuse a partial typed document: round-tripping it would discard
        /// undecodable registry entries. The caller retains the raw projection.
        pub fn from_projection(projection: &#{projection}) -> Option<Self> {
    #{guards.join("\n")}

            Some(Self::new(
    #{arguments.join("\n")}
            ))
        }

        pub fn projection(&self) -> #{projection} {
            #{projection} {
    #{assignments.join("\n")}
            }
        }
  RUST
end

# The `Shape` descriptor and the two directions across it, byte-fixed like
# Swift's `private enum DeckDocumentCoding`.
DOCUMENT_CODING_SOURCE = <<~'RUST'
  /// The manifest `Shape` descriptor and the two directions across it.
  ///
  /// Decode is the direct tree walk (`document_value_decoding`) — the JSON
  /// round-trip it replaces ground on the main thread in hang profiles.
  /// Encode stays on the shaped path: the manifest `Shape` is what drives
  /// `Int` vs `Double` on the way OUT, which a blind serializer cannot know.
  mod coding {
      use serde::Serialize;
      use serde::de::DeserializeOwned;

      use replicaman::document_value::{DocumentFields, DocumentValue};
      use replicaman::document_value_decoding;

      #[allow(dead_code)]
      pub enum Shape {
          String,
          Integer,
          Number,
          Boolean,
          Json,
          Object(Vec<(&'static str, Shape)>),
          Array(Box<Shape>),
          Map(Box<Shape>),
      }

      impl Shape {
          pub fn object(fields: Vec<(&'static str, Shape)>) -> Self {
              Shape::Object(fields)
          }

          #[allow(dead_code)]
          pub fn array(item: Shape) -> Self {
              Shape::Array(Box::new(item))
          }
      }

      pub fn decode<Value: DeserializeOwned>(fields: &DocumentFields) -> Option<Value> {
          document_value_decoding::decode(fields).ok()
      }

      pub fn encode<Value: Serialize>(value: &Value, shape: &Shape) -> DocumentFields {
          let json = serde_json::to_value(value)
              .expect("generated document value did not match its manifest shape");

          match document_value(&json, shape) {
              DocumentValue::Map(fields) => fields,
              _ => panic!("generated document value did not match its manifest shape"),
          }
      }

      fn document_value(value: &serde_json::Value, shape: &Shape) -> DocumentValue {
          if value.is_null() {
              return DocumentValue::Null;
          }

          match shape {
              Shape::String => match value.as_str() {
                  Some(value) => DocumentValue::String(value.to_owned()),
                  None => DocumentValue::Null,
              },
              Shape::Integer => match value.as_i64().or_else(|| value.as_f64().map(|v| v as i64)) {
                  Some(value) => DocumentValue::Int(value),
                  None => DocumentValue::Null,
              },
              Shape::Number => match value.as_f64() {
                  Some(value) => DocumentValue::Double(value),
                  None => DocumentValue::Null,
              },
              Shape::Boolean => match value.as_bool() {
                  Some(value) => DocumentValue::Bool(value),
                  None => DocumentValue::Null,
              },
              Shape::Object(fields) => {
                  let Some(object) = value.as_object() else {
                      return DocumentValue::Null;
                  };
                  DocumentValue::Map(
                      fields
                          .iter()
                          .map(|(key, shape)| {
                              let child = object
                                  .get(*key)
                                  .map(|value| document_value(value, shape))
                                  .unwrap_or(DocumentValue::Null);
                              ((*key).to_owned(), child)
                          })
                          .collect(),
                  )
              }
              Shape::Array(item) => match value.as_array() {
                  Some(values) => {
                      DocumentValue::List(values.iter().map(|v| document_value(v, item)).collect())
                  }
                  None => DocumentValue::Null,
              },
              Shape::Map(item) => match value.as_object() {
                  Some(object) => DocumentValue::Map(
                      object
                          .iter()
                          .map(|(key, v)| (key.clone(), document_value(v, item)))
                          .collect(),
                  ),
                  None => DocumentValue::Null,
              },
              Shape::Json => json_value(value),
          }
      }

      /// Under a `json` shape every numeric is a double — the manifest has said
      /// nothing about its kind, so there is nothing to preserve.
      fn json_value(value: &serde_json::Value) -> DocumentValue {
          match value {
              serde_json::Value::Null => DocumentValue::Null,
              serde_json::Value::Bool(value) => DocumentValue::Bool(*value),
              serde_json::Value::Number(value) => {
                  DocumentValue::Double(value.as_f64().unwrap_or_default())
              }
              serde_json::Value::String(value) => DocumentValue::String(value.clone()),
              serde_json::Value::Array(values) => {
                  DocumentValue::List(values.iter().map(json_value).collect())
              }
              serde_json::Value::Object(object) => DocumentValue::Map(
                  object
                      .iter()
                      .map(|(key, value)| (key.clone(), json_value(value)))
                      .collect(),
              ),
          }
      }
  }

  use coding::Shape;
RUST

DOCUMENT_HEADER = <<~'RUST'
  // Generated by replica-codegen — DO NOT EDIT.
  // Document value for stream `%<stream>s` · codec %<codec>s
  //
  // Rust cannot nest a type inside a struct, so the Swift emitter's nesting
  // path is flattened into one identifier with the ROOT stripped
  // (`DeckDocument.Slide.Style.ShadowOffset` → `SlideStyleShadowOffset`),
  // except `meta`, which every document may carry (`DeckDocument.Meta` →
  // `DeckDocumentMeta`).
  // Enum cases keep the emitter's spelling with the first character
  // upper-cased; a leading underscore (a raw value that starts with a digit)
  // survives (`_169`, `_43`, `sdr` → `Sdr`). Struct fields are snake_case,
  // which is already the document lane's wire spelling, so no serde rename is
  // needed here. `new(..)` takes the fields with NO declared default, in
  // declaration order; a struct whose fields all have defaults also gets
  // `Default`. Decoding runs through a private all-`Option` `Raw` mirror: a
  // declared default fills a missing key, an enum VALUE this build does not
  // know collapses to the field's declared default, and a required field still
  // refuses — while a LIST of enums stays strict, because the derived
  // `Deserialize` is what decodes it.
  //
  // %<model>sDocument owns generated values; ReplicaMan owns the live document.
RUST

def document_source(stream, options)
  shapes = document_shapes(stream)
  model = model_name(stream)
  root = "#{model}Document"
  root_fields = shapes.map { |name, shape| document_root_field(name, shape) }

  root_fields.each do |field|
    next unless %w[json jsonb].include?(field.fetch('type'))

    raise "replica-codegen: document root #{field.fetch('name')} is raw json — the document " \
          'plane has no ReplicaValue; declare a shape'
  end

  enums = []
  structs = []
  collect_shape_types(
    root, root_fields, prefix: '', enums: enums, structs: structs,
    singular_collections: true, derive_deserialize: true, snake_wire: true
  )
  root_struct = structs.pop
  projection = options[:document_projection] ? "Projection" : "#{model}Projection"
  root_struct[:extra_impl] = document_projection_methods(shapes, projection)

  registry = shapes.any? { |_, shape| document_registry_shape?(shape) }
  plain = shapes.any? { |_, shape| !document_registry_shape?(shape) }
  body_needs_btreemap = render_shape_types(enums, structs).include?('BTreeMap')
  document_symbols = []
  document_symbols << 'DocumentFields' if plain
  document_symbols << 'DocumentValue' if registry
  projection_symbols = []
  projection_symbols << 'DocumentEntry' if registry


  uses = []
  # A typed scalar map (a plugin's knob bag) is the one document shape that
  # needs a std import; the row lane derives its use block from the rendered
  # body, and this is the document lane's half of that.
  uses << "use std::collections::BTreeMap;\n" if body_needs_btreemap
  body = render_shape_types(enums, structs) + document_projection_extensions(shapes).to_s + shape_struct_source(root_struct)
  uses += [document_de_use(body), 'use serde::{Deserialize, Serialize};', '']
  uses << "use replicaman::document_value::{#{document_symbols.join(', ')}};" if document_symbols.size > 1
  uses << "use replicaman::document_value::#{document_symbols.first};" if document_symbols.size == 1
  uses << "use replicaman::document_value::{#{projection_symbols.join(', ')}};" if projection_symbols.size > 1
  uses << "use replicaman::document_value::#{projection_symbols.first};" if projection_symbols.size == 1

  projection_source = if options[:document_projection]
    "use #{options[:document_projection]} as Projection;"
  else
    fields = shapes.map { |name, shape| "    pub #{rust_field(name)}: #{document_registry_shape?(shape) ? 'Vec<DocumentEntry>' : 'DocumentFields'}," }.join("\n")
    "#[derive(Clone, Debug, Default, PartialEq)]\npub struct #{projection} {\n#{fields}\n}"
  end

  join_items([
    format(DOCUMENT_HEADER, stream: stream.fetch('name'), codec: stream.fetch('codec', nil), model: model),
    uses.join("\n"),
    projection_source,
    render_shape_types(enums, structs),
    document_projection_extensions(shapes),
    shape_struct_source(root_struct),
    DOCUMENT_CODING_SOURCE
  ]) + "\n"
end

def document_de_use(body)
  body.include?('de::Error') ? 'use serde::de::{self, Deserializer};' : 'use serde::de::Deserializer;'
end

def document_defaults_source(stream, document)
  defaults = stream.fetch('default', {})
  return if defaults.empty?

  model = model_name(stream)
  declared = document.scan(/^pub (?:enum|struct) (\w+)/).flatten
  lines = defaults.map do |root_name, value|
    shape = document_shapes(stream).fetch(root_name)
    name = shape_nested_name(document_root_field(root_name, shape), singular_collections: true)
    type = shape_rust_type(document_root_field(root_name, shape), name)
    expression = shape_value_source(shape, value, type_name: name, singular_collections: true)
    <<~RUST.chomp
          pub fn #{rust_field(root_name)}() -> #{type} {
              #{expression}
          }
    RUST
  end

  body = "pub struct #{model}Defaults;\n\nimpl #{model}Defaults {\n#{lines.join("\n\n")}\n}\n"
  imports = (body.scan(/\b[A-Z][A-Za-z0-9]*\b/).uniq & declared).sort

  <<~RUST
    // Generated by replica-codegen — DO NOT EDIT.
    // Defaults for document stream `#{stream.fetch('name')}`.
    //
    // Swift spells these as `static let` constants; a Rust value that owns a
    // `String` is not const-constructible, so they are associated functions.
    // Every field is written out, exactly as the Swift literal does — nothing
    // is filled in by `Default`.

    use super::#{snake_case(model)}_document::{#{imports.join(', ')}};

    #{body}
  RUST
end

# --- typescript --------------------------------------------------------------
# The TYPESCRIPT emitter half — the same manifest read, the same run, a third
# destination. It exists here rather than in a sibling package precisely so it
# CANNOT regenerate against a different manifest state than the Rust halves.
#
# What it emits is the wire, in the wire's own language: a `ReplicaRow`'s
# `data` map. `id` and `type` ride the row ENVELOPE, not the field map, so the
# emitted interface is named `<Model>Fields` — which is what Rust calls the
# same thing (`decode(id, row_type, fields: &ReplicaFields)`).
#
# Two things the Rust half gets for free and this one does not:
#
#   * FORMATTING. Rust delegates every line break to rustfmt, which is what
#     makes a scratch regeneration byte-identical to the tree. There is no
#     TypeScript formatter in this repo and there must not be one (Prettier is
#     banned), so this emitter owns its own line breaking: one member per line,
#     unions wrapped at TS_MAX_WIDTH, no trailing whitespace, one trailing
#     newline. Every construct below is written to land on exactly one shape.
#   * TYPE NESTING. Rust flattens because it must; TypeScript could nest under
#     a namespace but deliberately does NOT, so a name here is the same name
#     there and `SlideStyle` means one thing across both languages.
#
# Numbers are `number`: the wire is byte-stable JSON and a whole double is an
# int, so Int vs Double is a JSDoc note, never a type.

TS_TYPES = {
  'string' => 'string', 'text' => 'string',
  'hex_color' => 'string',
  'integer' => 'number', 'bigint' => 'number',
  'float' => 'number', 'decimal' => 'number',
  'boolean' => 'boolean',
  'datetime' => 'string', 'date' => 'string', 'time' => 'string',
}.freeze

# Matches the Rust half's rustfmt width, so the two outputs wrap alike.
TS_MAX_WIDTH = 100

# Untyped json. `unknown` would be honest but unusable; this is the same thing
# with the JSON grammar spelled out, and it is what `ReplicaValue` means.
TS_VALUE_TYPE = 'ReplicaJsonValue'

TS_NUMBER_NOTES = {
  'integer' => 'Int on the wire.', 'bigint' => 'Int on the wire.',
  'float' => 'Double on the wire.', 'decimal' => 'Double on the wire.',
}.freeze

def ts_type(wire_type)
  TS_TYPES.fetch(wire_type, TS_VALUE_TYPE)
end

# Single-quoted, matching the app's TypeScript style.
def ts_string(value)
  "'#{value.to_s.gsub('\\') { '\\\\' }.gsub("'") { "\\'" }}'"
end

# A wire name as an interface member. Wire names are identifiers in practice;
# anything else is quoted rather than mangled — the KEY must stay the wire's.
def ts_property(name)
  name.match?(/\A[A-Za-z_$][A-Za-z0-9_$]*\z/) ? name : ts_string(name)
end

def ts_number_note(node)
  TS_NUMBER_NOTES[node.fetch('type', nil)]
end

# `export type X = a | b;` on one line, or one arm per line when that would
# run past the width. The two forms are chosen by length alone, so the choice
# is reproducible.
def ts_type_alias(name, parts)
  single = "export type #{name} = #{parts.join(' | ')};"
  return single if single.length <= TS_MAX_WIDTH

  lines = ["export type #{name} ="]
  parts.each_with_index do |part, index|
    lines << "  | #{part}#{index == parts.size - 1 ? ';' : ''}"
  end
  lines.join("\n")
end

def ts_enum_source(name, values)
  ts_type_alias(name, values.map { ts_string(it) })
end

def shape_ts_type(node, nested_name, typed_enums: true)
  return nested_name if typed_enums && node.fetch('enum', nil)

  case node.fetch('type')
  when 'array'
    items = node.fetch('items')
    inner = shape_ts_type(items, nested_name, typed_enums: typed_enums)
    items.fetch('null', nil) ? "(#{inner} | null)[]" : "#{inner}[]"
  when 'map'
    "Record<string, #{shape_ts_type(node.fetch('values'), nested_name, typed_enums: typed_enums)}>"
  when 'object'
    nested_name
  else
    ts_type(node.fetch('type'))
  end
end

def shape_ts_field_type(field, prefix:, singular_collections: false, typed_enums: true)
  shape_ts_type(
    field,
    prefix + shape_nested_name(field, singular_collections: singular_collections),
    typed_enums: typed_enums
  )
end

# One `collect_shape_types` entry as an interface. The collector is shared with
# the Rust half — that is what guarantees the two emissions agree on names.
def ts_interface_source(entry)
  name = entry.fetch(:name)
  fields = entry.fetch(:fields)
  prefix = entry.fetch(:prefix)
  discriminator = entry[:discriminator]
  singular_collections = entry.fetch(:singular_collections)
  typed_enums = entry.fetch(:typed_enums)
  snake_wire = entry.fetch(:snake_wire)

  lines = ["export interface #{name} {"]
  if discriminator
    # Rust stores the discriminator as a plain `String` in every variant.
    # TypeScript can hold the LITERAL, which is what makes the union below
    # narrow on `kind`, so it does — same wire, a sharper type.
    lines << "  #{ts_property(wire_field_name(discriminator, snake_wire: snake_wire))}: " \
             "#{ts_string(entry.fetch(:discriminator_value))};"
  end
  fields.each do |field|
    note = ts_number_note(field)
    lines << "  /** #{note} */" if note
    type = shape_ts_field_type(
      field, prefix: prefix, singular_collections: singular_collections, typed_enums: typed_enums
    )
    type = "#{type} | null" if field.fetch('null', nil) != false
    lines << "  #{ts_property(wire_field_name(field.fetch('name'), snake_wire: snake_wire))}: #{type};"
  end
  lines << '}'
  lines.join("\n")
end

def ts_render_shape_types(enums, structs)
  join_items(enums.map { ts_enum_source(it[:name], it[:values]) } + structs.map { ts_interface_source(it) })
end

# Mirrors `shape_source` arm for arm, including the discriminated case.
def ts_shape_source(name, shape)
  enums = []
  structs = []

  unless shape.fetch('discriminator', nil)
    if shape.fetch('type', nil) == 'array'
      collect_shape_types(name, shape.fetch('items').fetch('fields'), prefix: name, enums: enums, structs: structs)
      return ts_render_shape_types(enums, structs)
    end

    collect_shape_types(name, shape.fetch('fields'), prefix: name, enums: enums, structs: structs)
    return ts_render_shape_types(enums, structs)
  end

  discriminator = shape.fetch('discriminator')
  variants = shape.fetch('variants').map do |variant|
    variant.merge('ts_name' => "#{name}#{variant.fetch('variantName')}")
  end

  variants.each do |variant|
    collect_shape_types(
      variant.fetch('ts_name'), variant.fetch('fields'),
      prefix: variant.fetch('ts_name'), enums: enums, structs: structs,
      discriminator: discriminator, typed_enums: false
    )
    structs.last[:discriminator_value] = variant.fetch('value')
  end

  shared = "#{name}Shared"
  lines = [ts_render_shape_types(enums, structs)]
  lines << <<~TS.chomp
    /**
     * The arm a payload lands in when its `#{discriminator}` is a value this
     * build does not know — Rust's `#{name}::Unknown`. In the union on purpose:
     * an older client must still carry a newer server's payload.
     */
    export interface #{shared} {
      #{ts_property(discriminator)}: string;
    }
  TS
  lines << ts_type_alias(name, variants.map { it.fetch('ts_name') } + [shared])
  join_items(lines)
end

def ts_shape_definitions(stream)
  columns = base_columns(stream)
  stream.fetch('variants', []).to_a.each { |variant| columns.concat(variant_columns(variant)) }
  join_items(columns.filter(&:shape_name).uniq(&:shape_name).map do |column|
    ts_shape_source(column.shape_name, column.shape)
  end)
end

def ts_support_definitions(stream)
  join_items([
    join_items(enum_definitions(stream).map { ts_enum_source(it[:name], it[:values]) }),
    ts_shape_definitions(stream)
  ])
end

# A column as it reads on the wire. Rust keeps a shape-backed column's raw
# JSON in `<field>_json` and hangs the typed value off an accessor, because it
# needs a lossless store; TypeScript reads the JSON natively, so the typed
# shape IS the field.
def ts_column_type(column)
  base =
    if column.enum_name
      column.enum_name
    elsif column.shape_name
      column.array_shape? ? "#{column.shape_name}[]" : column.shape_name
    elsif column.list?
      "#{ts_type(column.item_type)}[]"
    else
      ts_type(column.wire_type)
    end
  column.optional ? "#{base} | null" : base
end

def ts_fields_members(columns)
  columns.map do |column|
    lines = []
    note = TS_NUMBER_NOTES[column.list? ? column.item_type : column.wire_type]
    lines << "  /** #{note} */" if note
    lines << "  #{ts_property(column.name)}: #{ts_column_type(column)};"
    lines.join("\n")
  end
end

def ts_fields_interface(name, columns, doc)
  ([doc, "export interface #{name} {"] + ts_fields_members(columns) + ['}']).join("\n")
end

def ts_prelude(stream)
  "// Generated by replica-codegen — DO NOT EDIT.\n#{header(stream)}"
end

# The `ReplicaJsonValue` import, emitted only when the file actually names it.
def ts_value_import(body)
  return nil unless body.match?(/\b#{TS_VALUE_TYPE}\b/)

  "import type { #{TS_VALUE_TYPE} } from './replica-value';"
end

def ts_file_source(stream, chunks)
  body = join_items(chunks)
  import = ts_value_import(body)
  "#{join_items([ts_prelude(stream), import, body].compact)}\n"
end

def ts_row_model_source(stream, model, columns)
  doc = <<~TS.chomp
    /**
     * The `data` map of a `#{stream.fetch('name')}` row. `id` and `type` ride the row
     * envelope, not this map.
     */
  TS
  ts_file_source(stream, [ts_support_definitions(stream), ts_fields_interface("#{model}Fields", columns, doc)])
end

def ts_sti_model_source(stream, model, base, variants)
  chunks = [ts_support_definitions(stream)]
  variants.each do |variant|
    name = variant.fetch('rust_type')
    doc = <<~TS.chomp
      /**
       * The `data` map of one `#{stream.fetch('name')}` row whose envelope `type` is
       * `#{variant.fetch('type')}`. A kind-specific key may simply be absent from
       * a row, so every one of them is nullable.
       */
    TS
    chunks << ts_fields_interface("#{name}Fields", base + variant_columns(variant), doc)
  end
  union_doc = <<~TS.chomp
    /**
     * Every kind this build knows. Not discriminated by a member: the STI
     * discriminator is the row envelope's `type`, which is not in `data` —
     * narrow with `#{lower_camel(model)}Kind(row.type)`, then index this union.
     */
  TS
  chunks << "#{union_doc}\n#{ts_type_alias("#{model}Fields", variants.map { "#{it.fetch('rust_type')}Fields" })}"
  ts_file_source(stream, chunks)
end

# The document lane's field types, from the same shapes that feed
# `replica-models/src/documents`. The document wire is snake_case (Swift reaches
# it with `.convertToSnakeCase`); the row lane is camelCase. Both are spelled
# here exactly as they cross.
def ts_document_source(stream)
  shapes = document_shapes(stream)
  model = model_name(stream)
  root = "#{model}Document"
  root_fields = shapes.map { |name, shape| document_root_field(name, shape) }

  enums = []
  structs = []
  collect_shape_types(
    root, root_fields, prefix: '', enums: enums, structs: structs,
    singular_collections: true, snake_wire: true
  )

  prelude = <<~TS.chomp
    // Generated by replica-codegen — DO NOT EDIT.
    // Document `#{stream.fetch('name')}` · codec #{stream.fetch('codec', nil)} · wire keys are snake_case
  TS
  body = ts_render_shape_types(enums, structs)
  "#{join_items([prelude, ts_value_import(body), body].compact)}\n"
end

# The JSON value the wire actually carries, for every column the manifest
# leaves untyped. Its own file so each stream file can import one symbol.
def ts_value_source(manifest)
  <<~TS
    // Generated by replica-codegen — DO NOT EDIT.
    // Manifest version #{manifest.fetch('version')}

    /** Any value the wire's JSON can carry — what Rust spells `ReplicaValue`. */
    export type #{TS_VALUE_TYPE} =
      | string
      | number
      | boolean
      | null
      | #{TS_VALUE_TYPE}[]
      | { [key: string]: #{TS_VALUE_TYPE} };
  TS
end

def ts_index_source(manifest, streams, modules)
  <<~TS
    // Generated by replica-codegen — DO NOT EDIT.
    // Manifest version #{manifest.fetch('version')} · streams: #{streams.map { it.fetch('name') }.join(', ')}

    #{modules.sort.map { "export type * from './#{it}';" }.join("\n")}
  TS
end

# --- emit --------------------------------------------------------------------

streams = manifest.fetch('streams')
out = options[:out]
FileUtils.mkdir_p(out)
written = []
modules = []

streams.each do |stream|
  model = model_name(stream)
  source =
    if stream.fetch('lane') == 'document'
      doc_model_source(stream, model, base_columns(stream))
    elsif stream.fetch('sti', nil)
      sti_model_source(stream, model, base_columns(stream), stream.fetch('variants', nil))
    else
      row_model_source(stream, model, base_columns(stream))
    end
  path = File.join(out, "#{snake_case(model)}.rs")
  File.write(path, source)
  written << path
  modules << snake_case(model)
end

container_module = snake_case(options[:name])
if modules.include?(container_module)
  abort "replica-codegen: the container name #{options[:name]} collides with a stream model"
end
container_path = File.join(out, "#{container_module}.rs")
File.write(container_path, container_source(manifest, streams, options[:name]))
written << container_path
modules << container_module

mod_path = File.join(out, 'mod.rs')
File.write(mod_path, module_source(manifest, streams, modules, 'Row models'))
written << mod_path

# Every emitted type lands in ONE flat namespace (Rust cannot nest a type in a
# struct), so a collision is a build refusal rather than a silent shadow.
declared = Hash.new { |hash, key| hash[key] = [] }
written.each do |path|
  File.read(path).scan(/^pub (?:enum|struct|trait) (\w+)/).flatten.each { declared[it] << File.basename(path) }
end
collisions = declared.select { |_, files| files.size > 1 }
unless collisions.empty?
  abort "replica-codegen: emitted type name collision — #{collisions.map { |name, files| "#{name} in #{files.join(', ')}" }.join('; ')}"
end

if options[:document_out]
  document_out = options[:document_out]
  FileUtils.mkdir_p(document_out)
  document_modules = []
  streams.filter { it.fetch('lane') == 'document' && it.fetch('shapes', nil) }.each do |stream|
    model = model_name(stream)
    document = document_source(stream, options)
    document_path = File.join(document_out, "#{snake_case(model)}_document.rs")
    File.write(document_path, document)
    written << document_path
    document_modules << "#{snake_case(model)}_document"

    defaults = document_defaults_source(stream, document)
    next unless defaults

    defaults_path = File.join(document_out, "#{snake_case(model)}_defaults.rs")
    File.write(defaults_path, defaults)
    written << defaults_path
    document_modules << "#{snake_case(model)}_defaults"
  end

  unless document_modules.empty?
    document_streams = streams.filter { it.fetch('lane') == 'document' && it.fetch('shapes', nil) }
    document_mod = File.join(document_out, 'mod.rs')
    File.write(document_mod, module_source(manifest, document_streams, document_modules, 'Document values'))
    written << document_mod
  end
end

# TypeScript is the THIRD destination of this same run — one manifest read, one
# refresh, all outputs regenerated together. Its paths are kept out of
# `written`: rustfmt must never see them, and nothing else formats them.
if options[:ts_out]
  ts_out = options[:ts_out]
  FileUtils.mkdir_p(ts_out)
  ts_written = []
  ts_modules = []

  value_path = File.join(ts_out, 'replica-value.ts')
  File.write(value_path, ts_value_source(manifest))
  ts_written << value_path
  ts_modules << 'replica-value'

  streams.each do |stream|
    model = model_name(stream)
    source =
      if stream.fetch('sti', nil)
        ts_sti_model_source(stream, model, base_columns(stream), stream.fetch('variants', nil))
      else
        ts_row_model_source(stream, model, base_columns(stream))
      end
    path = File.join(ts_out, "#{snake_case(model)}.ts")
    File.write(path, source)
    ts_written << path
    ts_modules << snake_case(model)
  end

  streams.filter { it.fetch('lane') == 'document' && it.fetch('shapes', nil) }.each do |stream|
    model = model_name(stream)
    path = File.join(ts_out, "#{snake_case(model)}_document.ts")
    File.write(path, ts_document_source(stream))
    ts_written << path
    ts_modules << "#{snake_case(model)}_document"
  end

  index_path = File.join(ts_out, 'index.ts')
  File.write(index_path, ts_index_source(manifest, streams, ts_modules))
  ts_written << index_path

  # The same refusal the Rust half makes: `index.ts` re-exports every file into
  # ONE namespace, so a duplicate name is a generator abort, never a silent
  # shadow at the import site.
  ts_declared = Hash.new { |hash, key| hash[key] = [] }
  ts_written.each do |path|
    File.read(path).scan(/^export (?:interface|type) (\w+)/).flatten.each { ts_declared[it] << File.basename(path) }
  end
  ts_collisions = ts_declared.select { |_, files| files.size > 1 }
  unless ts_collisions.empty?
    abort 'replica-codegen: emitted TypeScript type name collision — ' \
          "#{ts_collisions.map { |name, files| "#{name} in #{files.join(', ')}" }.join('; ')}"
  end
end

# rustfmt owns the line breaking: the emitter would otherwise have to
# reproduce it by hand for `cargo fmt --check` to stay green, and the pinned
# edition/width make the result the same in the tree and in a scratch
# directory (the byte-identity test).
unless system('rustfmt', '--version', out: File::NULL, err: File::NULL)
  abort 'replica-codegen: rustfmt is required to emit deterministic Rust (rustup component add rustfmt)'
end
system(
  'rustfmt', '--edition', RUSTFMT_EDITION, '--config', "max_width=#{RUSTFMT_MAX_WIDTH}",
  *written.uniq, exception: true
)
