# frozen_string_literal: true

require_relative "test_helper"

class ConvergenceTest < Minitest::Test
  def test_concurrent_set_same_key_has_pinned_winner
    one = Loro::Doc.new(peer_id: 1)
    two = Loro::Doc.new(peer_id: 2)
    one.get_map("m").set("key", "one")
    two.get_map("m").set("key", "two")
    one_blob = one.export_updates
    two_blob = two.export_updates

    one.import(two_blob)
    two.import(one_blob)

    assert_equal one.to_h, two.to_h
    assert_equal({"m" => {"key" => "two"}}, one.to_h)
  end

  def test_concurrent_nested_map_edits_both_survive
    base = Loro::Doc.new(peer_id: 1)
    base.get_map("root").ensure_mergeable_map("child")
    snapshot = base.export_snapshot
    one = Loro::Doc.from_snapshot(snapshot, peer_id: 1)
    two = Loro::Doc.from_snapshot(snapshot, peer_id: 2)
    common = base.version_vector
    one.get_map("root").get_map("child").set("a", 1)
    two.get_map("root").get_map("child").set("b", 2)
    one_blob = one.export_updates(since: common)
    two_blob = two.export_updates(since: common)

    one.import(two_blob)
    two.import(one_blob)

    assert_equal one.to_h, two.to_h
    assert_equal({"root" => {"child" => {"a" => 1, "b" => 2}}}, one.to_h)
  end

  def test_concurrent_delete_and_set_has_pinned_winner
    base = Loro::Doc.new(peer_id: 1)
    base.get_map("m").set("key", "base")
    snapshot = base.export_snapshot
    common = base.version_vector
    deleting = Loro::Doc.from_snapshot(snapshot, peer_id: 1)
    setting = Loro::Doc.from_snapshot(snapshot, peer_id: 2)
    deleting.get_map("m").delete("key")
    setting.get_map("m").set("key", "new")
    delete_blob = deleting.export_updates(since: common)
    set_blob = setting.export_updates(since: common)

    deleting.import(set_blob)
    setting.import(delete_blob)

    assert_equal deleting.to_h, setting.to_h
    assert_equal({"m" => {"key" => "new"}}, deleting.to_h)
  end

  def test_three_actors_converge_across_shuffled_duplicate_deliveries
    blobs = (1..3).map do |peer|
      Loro::Doc.new(peer_id: peer).tap do |doc|
        doc.get_map("m").set("peer_#{peer}", peer)
        doc.get_map("m").set("winner", peer)
      end.export_updates
    end
    deliveries = [
      [0, 1, 2, 1],
      [2, 0, 2, 1],
      [1, 2, 0, 0]
    ]
    docs = deliveries.map do |order|
      Loro::Doc.new.tap { |doc| order.each { doc.import(blobs[it]) } }
    end

    docs.each do |doc|
      assert_equal docs.first.to_h, doc.to_h
      assert_equal docs.first.version_vector, doc.version_vector
    end
    assert_equal({"m" => {"peer_1" => 1, "peer_2" => 2, "peer_3" => 3, "winner" => 3}}, docs.first.to_h)
  end

  def test_snapshot_and_all_updates_hydrate_equivalently
    source = Loro::Doc.new(peer_id: 1)
    source.get_map("m").set("nested", {"a" => [1, 2, 3]})

    snapshot = Loro::Doc.from_snapshot(source.export_snapshot)
    updates = hydrate(source.export_updates)

    assert_equal source.to_h, snapshot.to_h
    assert_equal source.to_h, updates.to_h
    assert_equal snapshot.version_vector, updates.version_vector
  end

  def test_update_with_missing_dependency_stays_pending_then_auto_applies
    source = Loro::Doc.new(peer_id: 1)
    source.get_map("m").set("a", 1)
    update_a = source.export_updates
    after_a = source.version_vector
    source.get_map("m").set("b", 2)
    update_b = source.export_updates(since: after_a)

    target = Loro::Doc.new
    assert_equal({pending: true}, target.import(update_b))
    refute target.get_map("m").key?("b")
    assert_equal({pending: false}, target.import(update_a))
    assert_equal source.to_h, target.to_h
    assert_equal source.version_vector, target.version_vector
  end

  def test_committed_golden_fixtures
    Dir[File.join(__dir__, "fixtures/convergence/*")].sort.each do |directory|
      next unless File.directory?(directory)

      manifest = JSON.parse(File.read(File.join(directory, "manifest.json")))
      expected = JSON.parse(File.read(File.join(directory, "expected.json")))
      paths = Dir[File.join(directory, "*.update.bin")].sort
      blobs = paths.map { File.binread(it) }
      assert_kind_of Array, manifest.fetch("peers")
      refute_empty manifest.fetch("description")
      refute_empty blobs, "fixture #{File.basename(directory)} has no update blobs"
      paths.each.with_index(1) do |path, index|
        assert_match(/^#{format('%02d', index)}_.+\.update\.bin$/, File.basename(path))
      end

      orders = [blobs, blobs.reverse, blobs.rotate(1), blobs.reverse + [blobs.first]]
      docs = orders.map do |order|
        Loro::Doc.new.tap { |doc| order.each { doc.import(it) } }
      end
      docs.each { assert_equal expected, it.to_h }
      docs.each { assert_equal docs.first.version_vector, it.version_vector }
    end
  end
end
