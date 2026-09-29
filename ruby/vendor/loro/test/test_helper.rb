# frozen_string_literal: true

require "minitest/autorun"
require "json"
require "loro"

module LoroTestHelpers
  def update(doc, since: nil)
    doc.export_updates(since: since)
  end

  def hydrate(blob, peer_id: nil)
    Loro::Doc.new(peer_id: peer_id).tap { it.import(blob) }
  end
end

class Minitest::Test
  include LoroTestHelpers
end
