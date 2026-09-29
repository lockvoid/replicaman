require 'test_helper'

class SnapshotTimestampsTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @board = create_board!(id: 'b1', user: @user, name: 'Plans')
  end

  def snapshot(stream, row_id)
    ReplicaMan::Snapshot.find_by!(stream: stream, row_id: row_id)
  end

  test 'a captured create stamps both timestamps in one moment' do
    Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'hello')

    row = snapshot('items', 'i1')
    assert row.created_at.present?
    assert_equal row.created_at, row.updated_at, 'a fresh row was created and updated in the same write'
  end

  test 'a captured update advances updated_at and never touches created_at' do
    item = Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'hello')
    born = snapshot('items', 'i1')

    item.update!(rank: 'b')

    row = snapshot('items', 'i1')
    assert_equal born.created_at, row.created_at, 'created_at is the row\'s birth, frozen'
    assert_operator row.updated_at, :>, born.updated_at, 'every capture upsert moves the wall clock'
  end

  test 'the reconcile tombstone is a physical write and moves updated_at' do
    Job.create!(id: 'j1', user: @user, state: 'queued')
    born = snapshot('jobs', 'j1')

    uncaptured_fixture(Job) { Job.where(id: 'j1').delete_all }
    ReplicaMan::Reconcile.call(DummyReplica)

    row = snapshot('jobs', 'j1')
    assert row.deleted_at.present?, 'precondition: the sweep tombstoned the orphan'
    assert_equal born.created_at, row.created_at
    assert_operator row.updated_at, :>, born.updated_at, 'capture-free writes must stamp time by hand'
  end

  test 'compaction folds the tail and moves updated_at' do
    base = Loro::Doc.new(peer_id: 11)
    base.get_map('meta').set('name', 'Long-lived')
    base.commit
    push_ops(DummyReplica, user: @user, ops: [{ id: 'c1', op: 'row.create', stream: 'boards', row_id: 'bd1',
                                           codec: 'loro@1', seed: Base64.strict_encode64(base.export_snapshot) }])
    before = base.version_vector
    base.get_map('meta').set('color', 'red')
    push_ops(DummyReplica, user: @user, ops: [{ id: 'd1', op: 'doc.delta', stream: 'boards', row_id: 'bd1', codec: 'loro@1',
                                           payload: Base64.strict_encode64(base.export_updates(since: before)) }])
    folded = snapshot('boards', 'bd1')

    DummyReplica.document(:boards, 'bd1').compact

    row = snapshot('boards', 'bd1')
    assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'bd1').count, 'the tail was folded'
    assert_equal folded.created_at, row.created_at
    assert_operator row.updated_at, :>, folded.updated_at, 'fold_tail runs with no capture around it'
  end
end
