# frozen_string_literal: true

require_relative "test_helper"

class MapTest < Minitest::Test
  def setup
    @doc = Loro::Doc.new(peer_id: 1)
    @map = @doc.get_map(:root)
  end

  def test_set_get_delete_and_queries
    assert_nil @map.set(:one, 1)
    assert_nil @map.set("two", 2)

    assert_equal 1, @map.get("one")
    assert @map.key?(:two)
    assert_equal %w[one two], @map.keys.sort
    assert_equal 2, @map.size
    assert_nil @map.delete(:one)
    assert_nil @map.get("one")
    refute @map.key?("one")
  end

  def test_nested_maps_and_deep_materialization
    child = @map.ensure_mergeable_map(:child)
    child.set(:answer, 42)

    assert_instance_of Loro::Map, @map.get_map("child")
    assert_equal({"child" => {"answer" => 42}}, @map.to_h)
    assert_equal({"root" => {"child" => {"answer" => 42}}}, @doc.to_h)
  end

  def test_a_mergeable_child_refuses_a_key_holding_a_plain_value
    @map.set("item", {kind: "value"})

    error = assert_raises(Loro::Error) { @map.ensure_mergeable_map("item") }
    assert_match(/non-mergeable value/, error.message)
    assert_equal({kind: "value"}.transform_keys(&:to_s), @map.get("item"))
  end

  def test_a_mergeable_child_is_idempotent_and_survives_delete_then_recreate
    @map.ensure_mergeable_map("child").set("answer", 42)
    assert_equal({"answer" => 42}, @map.ensure_mergeable_map("child").to_h)

    @map.delete("child")
    assert_nil @map.get("child")

    @map.ensure_mergeable_map("child").set("answer", 7)
    assert_equal({"answer" => 7}, @map.get("child"))
  end

  def test_two_peers_creating_the_same_key_converge_instead_of_forking
    left = Loro::Doc.new(peer_id: 11)
    right = Loro::Doc.new(peer_id: 22)

    left.get_map("root").ensure_mergeable_map("shared").set("from_left", true)
    right.get_map("root").ensure_mergeable_map("shared").set("from_right", true)

    left.import(right.export_updates)
    right.import(left.export_updates)

    both = {"from_left" => true, "from_right" => true}
    assert_equal({"root" => {"shared" => both}}, left.to_h)
    assert_equal(left.to_h, right.to_h)
  end

  def test_empty_names_and_keys_are_valid
    empty = @doc.get_map("")
    empty.set("", "value")

    assert_equal "value", empty.get("")
    assert_equal({"" => {"" => "value"}, "root" => {}}, @doc.to_h)
  end

  def test_nil_keys_are_rejected_by_every_keyed_method
    calls = [
      -> { @map.set(nil, 1) },
      -> { @map.get(nil) },
      -> { @map.get_map(nil) },
      -> { @map.ensure_mergeable_map(nil) },
      -> { @map.delete(nil) },
      -> { @map.key?(nil) }
    ]

    calls.each { assert_raises(Loro::TypeError, &it) }
    assert_equal({}, @map.to_h)
  end

  def test_public_api_is_the_curated_surface
    assert_equal %i[delete get get_map key? keys set ensure_mergeable_map size to_h].sort,
      Loro::Map.instance_methods(false).sort
  end
end
