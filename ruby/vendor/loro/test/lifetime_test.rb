# frozen_string_literal: true

require_relative "test_helper"

class LifetimeTest < Minitest::Test
  def test_map_handle_keeps_document_alive_across_gc
    map = begin
      doc = Loro::Doc.new(peer_id: 1)
      doc.get_map("retained")
    end

    10.times { GC.start(full_mark: true, immediate_sweep: true) }
    map.set("alive", true)

    assert_equal true, map.get("alive")
    assert_equal({"alive" => true}, map.to_h)
  end
end
