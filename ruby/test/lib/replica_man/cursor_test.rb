require 'test_helper'

class CursorTest < ActiveSupport::TestCase
  BUCKETS = %w[user:1 user:*].freeze

  test 'a bootstrap in flight carries the heads it started from' do
    cursor = ReplicaMan::Cursor.encode({ 'user:1' => 7, 'user:*' => 0 }, started: { 'user:1' => 12, 'user:*' => 3 })

    assert_equal({ 'user:1' => 7, 'user:*' => 0 }, ReplicaMan::Cursor.decode(cursor, BUCKETS))
    assert_equal({ 'user:1' => 12, 'user:*' => 3 }, ReplicaMan::Cursor.started(cursor, BUCKETS))
  end

  test 'no cursor starts a bootstrap now; a published cursor is incremental' do
    assert_equal({}, ReplicaMan::Cursor.started(nil, BUCKETS))
    assert_nil ReplicaMan::Cursor.started(ReplicaMan::Cursor.encode({ 'user:1' => 7, 'user:*' => 0 }), BUCKETS)
  end

  test 'a bootstrap cursor missing a bucket head, or naming another principal, is invalid' do
    partial = ReplicaMan::Cursor.encode({ 'user:1' => 7, 'user:*' => 0 }, started: { 'user:1' => 12 })
    error = assert_raises(ReplicaMan::Protocol::Error) { ReplicaMan::Cursor.started(partial, BUCKETS) }
    assert_equal 'CursorInvalid', error.code

    foreign = ReplicaMan::Cursor.encode({ 'user:2' => 7, 'user:*' => 0 }, started: { 'user:2' => 12, 'user:*' => 3 })
    assert_raises(ReplicaMan::Protocol::Error) { ReplicaMan::Cursor.decode(foreign, BUCKETS) }
    assert_raises(ReplicaMan::Protocol::Error) { ReplicaMan::Cursor.started('not a cursor', BUCKETS) }
  end
end
