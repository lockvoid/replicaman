# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require "rbconfig"

class ProcessDeterminismTest < Minitest::Test
  ROOT = File.expand_path("..", __dir__)
  SCRIPT = <<~RUBY
    $LOAD_PATH.unshift(File.expand_path("lib", #{ROOT.dump}))
    require "loro"
    STDOUT.binmode
    doc = Loro::Doc.new(peer_id: 42)
    map = doc.get_map("document")
    map.set("title", "deterministic")
    map.set("nested", {"values" => [1, true, nil, "bytes".b]})
    doc.commit
    STDOUT.write(doc.export_updates)
  RUBY

  def test_timestamp_free_export_is_identical_across_processes
    first = run_export_process
    sleep 1.1
    second = run_export_process

    assert_equal Encoding::BINARY, first.encoding
    assert_equal first, second
    assert_equal({"document" => {
      "title" => "deterministic",
      "nested" => {"values" => [1, true, nil, "bytes".b]}
    }}, hydrate(first).to_h)
  end

  private

  def run_export_process
    stdout, stderr, status = Open3.capture3(RbConfig.ruby, "-e", SCRIPT, chdir: ROOT)
    assert status.success?, "export subprocess failed: #{stderr}"
    stdout.b
  end
end
