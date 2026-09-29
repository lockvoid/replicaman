# frozen_string_literal: true

require_relative "test_helper"

class HistoryTest < Minitest::Test
  def versioned_doc
    doc = Loro::Doc.new(peer_id: 7)
    doc.get_map("m").set("a", 1)
    doc.commit
    v1 = doc.frontiers
    doc.get_map("m").set("a", 2)
    doc.get_map("m").set("b", true)
    doc.commit
    [doc, v1]
  end

  def test_checkout_is_a_detached_read_of_a_past_state
    doc, v1 = versioned_doc

    doc.checkout(v1)

    assert doc.detached?
    assert_equal({ "m" => { "a" => 1 } }, doc.to_h)

    doc.checkout_to_latest

    refute doc.detached?
    assert_equal({ "m" => { "a" => 2, "b" => true } }, doc.to_h)
  end

  def test_history_survives_a_snapshot_round_trip
    doc, v1 = versioned_doc
    reloaded = Loro::Doc.from_snapshot(doc.export_snapshot, peer_id: 8)

    reloaded.checkout(v1)

    assert_equal({ "m" => { "a" => 1 } }, reloaded.to_h)
  end

  def test_revert_to_appends_new_operations_and_keeps_every_version
    doc, v1 = versioned_doc
    v2 = doc.frontiers

    doc.revert_to(v1)

    refute doc.detached?
    assert_equal({ "m" => { "a" => 1 } }, doc.to_h)
    refute_equal v1, doc.frontiers, "a restore is a new version, not a rewind"

    doc.checkout(v2)
    assert_equal({ "m" => { "a" => 2, "b" => true } }, doc.to_h, "the reverted-away version is still there")
  end

  def test_fork_at_is_an_independent_branch
    doc, v1 = versioned_doc

    branch = doc.fork_at(v1, peer_id: 9)
    branch.get_map("m").set("c", "branch")
    branch.commit

    assert_equal({ "m" => { "a" => 1, "c" => "branch" } }, branch.to_h)
    assert_equal({ "m" => { "a" => 2, "b" => true } }, doc.to_h, "the origin is untouched")
    assert_equal 9, branch.peer_id
  end

  def test_garbage_frontiers_are_refused
    doc, = versioned_doc

    assert_raises(Loro::Error) { doc.checkout("not frontiers") }
  end
end
