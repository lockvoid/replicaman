# frozen_string_literal: true

require_relative "test_helper"

class PropertyConversionTest < Minitest::Test
  SEED = 12_071
  CASES = 100

  def test_seeded_supported_values_round_trip
    random = Random.new(SEED)
    map = Loro::Doc.new(peer_id: 1).get_map("values")

    CASES.times do |index|
      value = random_value(random, 4)
      map.set(index.to_s, value)
      expected = normalize(value)
      actual = map.get(index.to_s)
      if expected.nil?
        assert_nil actual, "seed=#{SEED} case=#{index}"
      else
        assert_equal expected, actual, "seed=#{SEED} case=#{index}"
      end
    end
  end

  private

  def random_value(random, depth)
    return random_scalar(random) if depth.zero?

    case random.rand(10)
    when 0..5
      random_scalar(random)
    when 6, 7
      Array.new(random.rand(5)) { random_value(random, depth - 1) }
    else
      Array.new(random.rand(5)) do |index|
        key = index.even? ? "key_#{index}" : :"symbol_#{index}"
        [key, random_value(random, depth - 1)]
      end.to_h
    end
  end

  def random_scalar(random)
    case random.rand(8)
    when 0
      nil
    when 1
      random.rand(2).zero?
    when 2
      random.rand(-(2**63)..(2**63 - 1))
    when 3
      random.rand * 2_000 - 1_000
    when 4
      "text_#{random.rand(1_000_000)}"
    when 5
      :"symbol_#{random.rand(1_000)}"
    else
      random.bytes(random.rand(16)).b
    end
  end

  def normalize(value)
    case value
    when Symbol
      value.to_s
    when Array
      value.map { normalize(it) }
    when Hash
      value.to_h { |key, item| [key.to_s, normalize(item)] }
    else
      value
    end
  end
end
