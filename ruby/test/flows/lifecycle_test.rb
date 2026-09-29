require 'test_helper'

class LifecycleTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
  end

  def push(*ops)
    push_ops(DummyReplica, user: @user, ops: ops)[:verdicts]
  end

  def create_op(row_id, name, id:)
    doc = Loro::Doc.new(peer_id: 11)
    doc.get_map('meta').set('name', name)
    doc.commit
    { id: id, op: 'row.create', stream: 'boards', row_id: row_id,
      codec: 'loro@1', seed: Base64.strict_encode64(doc.export_snapshot) }
  end

  test 'the quota refusal parks the third board, but a replayed accepted create stays accepted' do
    first = push(create_op('b1', 'One', id: 'c1'), create_op('b2', 'Two', id: 'c2'))
    assert_equal %w[accepted accepted], first.map { it[:outcome] }

    third = push(create_op('b3', 'Three', id: 'c3'))
    assert_equal [{ id: 'c3', outcome: 'rejected', reason: 'board quota reached' }], third
    assert_equal 2, Board.count

    replay = push(create_op('b2', 'Two', id: 'c2'))
    assert_equal [{ id: 'c2', outcome: 'accepted' }], replay,
                 'idempotent replay of an accepted create must not trip the quota it filled'
  end

  test 'a delta for a document row that was never created is rejected — push is not row-shaped storage' do
    doc = Loro::Doc.new(peer_id: 11)
    doc.get_map('meta').set('name', 'Ghost')

    verdicts = push({ id: 'd1', op: 'doc.delta', stream: 'boards', row_id: 'ghost', codec: 'loro@1',
                      payload: Base64.strict_encode64(doc.export_updates) })

    assert_equal [{ id: 'd1', outcome: 'rejected', reason: 'entity incarnation is no longer current' }], verdicts
    assert_equal 0, ReplicaMan::Delta.count
  end

  test 'a client deletes its own board: tombstone + row.delete frame, deltas stop, history stays for undelete' do
    push(create_op('b1', 'Mine to end', id: 'c1'))
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'inside')
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'Edited once') }
    cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

    verdicts = push({ id: 'del1', op: 'row.delete', stream: 'boards', row_id: 'b1' })
    assert_equal [{ id: 'del1', outcome: 'accepted' }], verdicts
    assert_nil Board.find_by(id: 'b1')
    assert ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').deleted_at, 'the fold row tombstones'

    frames = pull_checkpoint(DummyReplica, user: @user, cursor: cursor)[:frames]
    assert_equal [%w[boards b1], %w[items i1]], frames.map { [it.fetch(:stream), it.fetch(:id)] }.sort
    assert frames.all? { it.fetch(:frame) == 'row.delete' && it.fetch(:incarnation).present? }
    assert_empty frames.select { it[:frame] == 'doc.delta' },
                 'a dead row serves no deltas even though its history still exists pre-GC'
    assert ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').exists?,
           'history survives until GC — that is the undelete window'

    replay = push({ id: 'del1', op: 'row.delete', stream: 'boards', row_id: 'b1' })
    assert_equal [{ id: 'del1', outcome: 'accepted' }], replay, 'a replayed delete reports accepted again'

    fresh = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:frames]
    assert_empty fresh, 'a fresh bootstrap no longer knows the dead board'
  end

  test 'deleting a foreign or unknown board is a rejected verdict' do
    rival = User.create!(id: 'u2', name: 'Rival')
    DummyReplica.document(:boards, 'b9').create(user_id: rival.id) { it.get_map('meta').set('name', 'Theirs') }

    verdicts = push_ops(DummyReplica, user: @user, ops: [
      { id: 'del1', op: 'row.delete', stream: 'boards', row_id: 'b9' },
      { id: 'del2', op: 'row.delete', stream: 'boards', row_id: 'nope' }
    ])[:verdicts]

    assert_equal [
      { id: 'del1', outcome: 'rejected', reason: 'row is outside your replica' },
      { id: 'del2', outcome: 'rejected', reason: 'entity incarnation is no longer current' }
    ], verdicts
    assert Board.find_by(id: 'b9'), 'the foreign board is untouched'
  end

  test 'a holder still receives a deletion after its tombstone payload has been collected' do
    push(create_op('b1', 'Home', id: 'c1'))
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'doomed')
    cursor = pull_checkpoint(DummyReplica, user: @user).fetch(:cursor)
    incarnation = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').incarnation
    Item.find('i1').destroy!
    DummyReplica.gc(window: 0.seconds)
    tombstone = ReplicaMan::Snapshot.find_by!(row_id: 'i1')
    assert_equal incarnation, tombstone.incarnation
    assert_empty tombstone.data

    result = pull_checkpoint(DummyReplica, user: @user, cursor: cursor)
    refute result.fetch(:reset)
    assert_equal [['row.delete', 'items', 'i1', incarnation]],
                 result.fetch(:frames).map { it.values_at(:frame, :stream, :id, :incarnation) }
  end

  test 'GC releases old payloads and deltas while retaining lifetime fences' do
    push(create_op('b1', 'Old', id: 'c1'), create_op('b2', 'Fresh', id: 'c2'))
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'x')
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'Old edited') }

    Board.find('b1').destroy!
    ReplicaMan::Snapshot.where(row_id: %w[b1 i1]).update_all(deleted_at: 3.days.ago)

    DummyReplica.gc(window: 7.days)
    assert_equal 2, ReplicaMan::Snapshot.where(row_id: %w[b1 i1]).count, 'inside the window nothing is reaped'

    DummyReplica.gc(window: 1.day)
    tombstones = ReplicaMan::Snapshot.where(row_id: %w[b1 i1])
    assert_equal 2, tombstones.count
    assert tombstones.all? { it.data.empty? && it.document.nil? && it.deleted_at }
    assert_equal 0, DummyReplica.gc(window: 1.day), 'already compacted fences need no further work'
    assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count,
                 'a dead row takes its history with it'
    assert ReplicaMan::Snapshot.find_by(stream: 'boards', row_id: 'b2'), 'live rows are untouchable'
  end
end
