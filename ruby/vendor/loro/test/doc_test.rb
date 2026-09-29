# frozen_string_literal: true

require_relative "test_helper"

class DocTest < Minitest::Test
  def test_construction_and_peer_id
    doc = Loro::Doc.new(peer_id: 42)

    assert_equal 42, doc.peer_id
    assert_equal 99, doc.peer_id = 99
    assert_equal 99, doc.peer_id
  end

  def test_peer_id_validation
    assert_raises(RangeError) { Loro::Doc.new(peer_id: -1) }
    assert_raises(RangeError) { Loro::Doc.new(peer_id: 2**64) }

    assert_equal 0, Loro::Doc.new(peer_id: 0).peer_id
    assert_equal 2**64 - 2, Loro::Doc.new(peer_id: 2**64 - 2).peer_id
    assert_raises(Loro::Error) { Loro::Doc.new(peer_id: 2**64 - 1) }

    doc = Loro::Doc.new
    assert_raises(RangeError) { doc.peer_id = -1 }
    assert_raises(RangeError) { doc.peer_id = 2**64 }
  end

  def test_peer_change_auto_commits_under_pinned_loro
    doc = Loro::Doc.new(peer_id: 1)
    doc.get_map("m").set("from_one", true)

    assert_equal 2, doc.peer_id = 2
    doc.get_map("m").set("from_two", true)

    restored = hydrate(doc.export_updates)
    assert_equal({"m" => {"from_one" => true, "from_two" => true}}, restored.to_h)
  end

  def test_snapshot_round_trip_and_peer_override
    source = Loro::Doc.new(peer_id: 1)
    source.get_map("settings").set("enabled", true)

    restored = Loro::Doc.from_snapshot(source.export_snapshot, peer_id: 77)

    assert_equal source.to_h, restored.to_h
    assert_equal 77, restored.peer_id
  end

  def test_updates_since_version_vector
    doc = Loro::Doc.new(peer_id: 1)
    empty_vector = doc.version_vector
    doc.get_map("root").set("a", 1)

    first = doc.export_updates(since: empty_vector)
    current_vector = doc.version_vector
    caught_up = doc.export_updates(since: current_vector)

    assert_equal({"root" => {"a" => 1}}, hydrate(first).to_h)
    assert_equal({}, hydrate(caught_up).to_h)

    doc.get_map("root").set("b", 2)
    refute_equal current_vector, doc.version_vector
  end

  def test_import_is_idempotent_and_batch_is_order_insensitive
    first = Loro::Doc.new(peer_id: 1)
    second = Loro::Doc.new(peer_id: 2)
    first.get_map("m").set("a", 1)
    second.get_map("m").set("b", 2)
    blobs = [first.export_updates, second.export_updates]

    doc = Loro::Doc.new
    assert_equal({pending: false}, doc.import_batch(blobs.reverse))
    assert_equal({pending: false}, doc.import(blobs.first))
    assert_equal({"m" => {"a" => 1, "b" => 2}}, doc.to_h)
  end

  def test_empty_import_batch_and_explicit_nil_keywords
    doc = Loro::Doc.new(peer_id: nil)

    assert_equal({pending: false}, doc.import_batch([]))
    assert_equal({}, doc.to_h)
    assert_equal Encoding::BINARY, doc.export_updates(since: nil).encoding
  end

  def test_commit_controls_change_grouping
    source = Loro::Doc.new(peer_id: 1)
    source.get_map("m").set("a", 1)
    assert_nil source.commit
    first_vector = source.version_vector
    first = source.export_updates
    source.get_map("m").set("b", 2)
    second = source.export_updates(since: first_vector)

    target = hydrate(first)
    assert_equal({"m" => {"a" => 1}}, target.to_h)
    target.import(second)
    assert_equal source.to_h, target.to_h
  end

  def test_binary_metadata
    doc = Loro::Doc.new(peer_id: 1)
    doc.get_map("m").set("a", 1)

    [
      doc.export_updates,
      doc.export_snapshot,
      doc.version_vector,
      doc.frontiers
    ].each { assert_equal Encoding::BINARY, it.encoding }
  end

  def test_public_api_is_the_curated_surface
    assert_equal %i[
      checkout checkout_to_latest commit detached? export_snapshot export_updates fork_at frontiers get_map
      import import_batch peer_id peer_id= revert_to to_h version_vector
    ].sort, Loro::Doc.instance_methods(false).sort
    assert_equal %i[from_snapshot new].sort, Loro::Doc.singleton_methods(false).sort
  end
end
