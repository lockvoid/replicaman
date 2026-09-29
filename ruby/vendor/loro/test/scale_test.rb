# frozen_string_literal: true

require_relative "test_helper"

class ScaleTest < Minitest::Test
  def test_thousands_of_operations_export_import_and_materialize
    source = Loro::Doc.new(peer_id: 1)
    map = source.get_map("items")
    5_000.times { map.set(it.to_s, it) }

    restored = hydrate(source.export_updates)
    values = restored.get_map("items")

    assert_equal 5_000, values.size
    assert_equal 0, values.get("0")
    assert_equal 2_499, values.get("2499")
    assert_equal 4_999, values.get("4999")
    assert_equal source.version_vector, restored.version_vector
  end

  def test_many_peers_converge_with_reordering_and_duplicates
    blobs = (1..32).map do |peer|
      doc = Loro::Doc.new(peer_id: peer)
      doc.get_map("m").set("peer_#{peer}", peer)
      doc.get_map("m").set("winner", peer)
      doc.export_updates
    end

    forward = Loro::Doc.new
    reverse = Loro::Doc.new
    (blobs + blobs.first(8)).each { forward.import(it) }
    (blobs.reverse + blobs.last(8)).each { reverse.import(it) }

    assert_equal forward.to_h, reverse.to_h
    assert_equal forward.version_vector, reverse.version_vector
    assert_equal 33, forward.get_map("m").size
    assert_equal 32, forward.to_h.dig("m", "winner")
  end

  def test_independent_documents_are_safe_across_ruby_threads
    blobs = 8.times.map do |offset|
      Thread.new do
        peer = offset + 1
        doc = Loro::Doc.new(peer_id: peer)
        map = doc.get_map("m")
        250.times { map.set("#{peer}:#{it}", it) }
        doc.export_updates
      end
    end.map(&:value)

    merged = Loro::Doc.new
    assert_equal({pending: false}, merged.import_batch(blobs.reverse))
    assert_equal 2_000, merged.get_map("m").size
    assert_equal 249, merged.get_map("m").get("8:249")
  end
end
