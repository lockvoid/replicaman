# frozen_string_literal: true

require_relative "test_helper"

class ErrorTest < Minitest::Test
  def test_error_hierarchy
    assert_operator Loro::Error, :<, StandardError
    assert_operator Loro::ImportError, :<, Loro::Error
    assert_operator Loro::TypeError, :<, Loro::Error
  end

  def test_garbage_import_raises_import_error
    error = assert_raises(Loro::ImportError) { Loro::Doc.new.import("not loro".b) }
    refute_empty error.message
  end

  def test_garbage_snapshot_raises_import_error
    assert_raises(Loro::ImportError) { Loro::Doc.from_snapshot("not loro".b) }
  end
end
