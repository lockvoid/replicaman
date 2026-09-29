# frozen_string_literal: true

require_relative "test_helper"

class ImportEdgeTest < Minitest::Test
  def setup
    @source = Loro::Doc.new(peer_id: 1)
    @source.get_map("m").set("ok", true)
    @updates = @source.export_updates
    @snapshot = @source.export_snapshot
  end

  def test_malformed_updates_raise_import_error
    malformed_blobs = [
      "".b,
      "not-loro".b,
      Random.new(12_071).bytes(64),
      @updates.byteslice(0, 1),
      @updates.byteslice(0, 8),
      @updates.byteslice(0, @updates.bytesize / 2),
      @updates.byteslice(0, @updates.bytesize - 1)
    ]

    malformed_blobs.each do |blob|
      assert_raises(Loro::ImportError, "accepted malformed blob with #{blob.bytesize} bytes") do
        Loro::Doc.new.import(blob)
      end
    end
  end

  def test_failed_batch_is_atomic
    target = Loro::Doc.new

    assert_raises(Loro::ImportError) { target.import_batch([@updates, "corrupt".b]) }
    assert_equal({}, target.to_h)
    assert_equal Loro::Doc.new.version_vector, target.version_vector
  end

  def test_batch_rejects_non_string_entries_before_importing
    target = Loro::Doc.new

    assert_raises(::TypeError) { target.import_batch([@updates, Object.new]) }
    assert_equal({}, target.to_h)
  end

  def test_snapshot_rejects_updates_and_corruption
    assert_raises(Loro::ImportError) { Loro::Doc.from_snapshot(@updates) }

    prefixes = [0, 1, 8, @snapshot.bytesize / 2, @snapshot.bytesize - 1]
    prefixes.each do |length|
      assert_raises(Loro::ImportError, "accepted snapshot prefix of #{length} bytes") do
        Loro::Doc.from_snapshot(@snapshot.byteslice(0, length))
      end
    end
  end

  def test_malformed_version_vector_raises_loro_error
    assert_raises(Loro::Error) { @source.export_updates(since: "not-a-version-vector".b) }
  end
end
