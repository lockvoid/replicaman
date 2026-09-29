# frozen_string_literal: true

require_relative "test_helper"

class ValueConversionTest < Minitest::Test
  def setup
    @map = Loro::Doc.new(peer_id: 1).get_map("values")
  end

  def round_trip(value)
    @map.set("value", value)
    @map.get("value")
  end

  def test_scalar_values
    assert_nil round_trip(nil)
    assert_equal true, round_trip(true)
    assert_equal false, round_trip(false)
    assert_equal 123, round_trip(123)
    assert_equal(-45.5, round_trip(-45.5))
    assert_equal "hello", round_trip("hello")
    assert_equal "symbol", round_trip(:symbol)
  end

  def test_special_float_values
    assert round_trip(Float::NAN).nan?
    assert_equal Float::INFINITY, round_trip(Float::INFINITY)
    assert_equal(-Float::INFINITY, round_trip(-Float::INFINITY))
  end

  def test_i64_boundaries
    assert_equal 2**63 - 1, round_trip(2**63 - 1)
    assert_equal(-2**63, round_trip(-2**63))
    assert_raises(RangeError) { round_trip(2**63) }
    assert_raises(RangeError) { round_trip(-2**63 - 1) }
  end

  def test_binary_and_text_strings_remain_distinct
    binary = "\xFF\x00".b
    text = "café"
    latin1 = "caf\xE9".dup.force_encoding(Encoding::ISO_8859_1)

    assert_equal binary, round_trip(binary)
    assert_equal Encoding::BINARY, round_trip(binary).encoding
    assert_equal text, round_trip(text)
    assert_equal Encoding::UTF_8, round_trip(text).encoding
    assert_equal text, round_trip(latin1)
  end

  def test_every_binary_byte_and_utf16_text
    binary = (0..255).to_a.pack("C*").b
    utf16 = "hello Ω".encode(Encoding::UTF_16LE)

    assert_equal binary, round_trip(binary)
    assert_equal Encoding::BINARY, round_trip(binary).encoding
    assert_equal "hello Ω", round_trip(utf16)
    assert_equal Encoding::UTF_8, round_trip(utf16).encoding
  end

  def test_invalid_text_encoding_is_rejected
    invalid = "\xFF".dup.force_encoding(Encoding::UTF_8)

    assert_raises(EncodingError) { round_trip(invalid) }
  end

  def test_nested_array_and_hash_values
    value = {
      title: "example",
      "items" => [1, true, nil, {nested: [:symbol, "bytes".b]}]
    }

    assert_equal(
      {"title" => "example", "items" => [1, true, nil, {"nested" => ["symbol", "bytes".b]}]},
      round_trip(value)
    )
  end

  def test_empty_and_large_nested_values
    assert_equal({"array" => [], "hash" => {}, "binary" => "".b, "text" => ""},
      round_trip({array: [], hash: {}, binary: "".b, text: ""}))

    flat = (0...20_000).to_a
    deep = "leaf"
    500.times { deep = [deep] }
    value = round_trip({flat: flat, deep: deep})

    assert_equal 20_000, value.fetch("flat").length
    cursor = value.fetch("deep")
    500.times { cursor = cursor.fetch(0) }
    assert_equal "leaf", cursor
  end

  def test_rejects_unknown_values_and_hash_keys
    assert_raises(Loro::TypeError) { round_trip(Object.new) }
    assert_raises(Loro::TypeError) { round_trip({1 => "bad key"}) }
  end

  def test_rejects_cyclic_arrays_and_hashes
    array = []
    array << array
    hash = {}
    hash["self"] = hash

    assert_raises(Loro::TypeError) { round_trip(array) }
    assert_raises(Loro::TypeError) { round_trip(hash) }

    shared = ["value"]
    assert_equal [shared, shared], round_trip([shared, shared])
  end
end
