require 'test_helper'

class ReconcileTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
  end

  test 'a bypassed insert is captured, with virtuals computed' do
    uncaptured_fixture(Job) { Job.insert_all([{ id: 'j1', user_id: 'u1', state: 'queued' }]) }
    assert_nil ReplicaMan::Snapshot.find_by(stream: 'jobs', row_id: 'j1'), 'precondition: insert_all left no capture'

    counts = ReplicaMan::Reconcile.call(DummyReplica)

    assert_equal({ recaptured: 1, tombstoned: 0, missing_fold: 0 }, counts[:jobs])
    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1')
    assert_equal 'queued', snapshot.data['state']
    assert_equal({ 'state' => 'queued', 'active' => true, 'metricsByKey' => {} }, snapshot.data['summary'])
  end

  test 'a bypassed update is re-captured with a fresh position; a current row is left alone' do
    job = Job.create!(id: 'j1', user: @user, state: 'queued')
    captured_position = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').position

    uncaptured_fixture(Job) { Job.where(id: 'j1').update_all(state: 'done') }
    assert_equal 'queued', ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').data['state'],
                 'precondition: the snapshot is stale'

    counts = ReplicaMan::Reconcile.call(DummyReplica)

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1')
    assert_equal 'done', snapshot.data['state']
    refute_equal captured_position, snapshot.position, 'tails must hear the true-up'
    assert_equal 1, counts[:jobs][:recaptured]

    again = ReplicaMan::Reconcile.call(DummyReplica)
    assert_equal({ recaptured: 0, tombstoned: 0, missing_fold: 0 }, again[:jobs],
                 'idempotent — a clean world sweeps to zero (datetime round-trips must not read as drift)')
    assert_equal snapshot.position, ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').position
  end

  test 'a bypassed delete is tombstoned, keeping last data, moving the position' do
    Job.create!(id: 'j1', user: @user, state: 'queued')
    live_position = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').position
    uncaptured_fixture(Job) { Job.where(id: 'j1').delete_all }

    counts = ReplicaMan::Reconcile.call(DummyReplica)

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1')
    assert snapshot.deleted_at.present?, 'the orphaned snapshot becomes a tombstone — pull emits row.delete'
    assert_equal 'u1', snapshot.data['userId'], 'last data stays — membership stays decidable'
    refute_equal live_position, snapshot.position
    assert_equal 1, counts[:jobs][:tombstoned]
  end

  test 'document lane: projection drift is re-captured without moving the fold position; a doorless birth is counted, not healed' do
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Plans') }
    fold_position = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').document_position

    uncaptured_fixture(Board) { Board.where(id: 'b1').update_all(name: 'Renamed behind the door') }
    uncaptured_fixture(Board) { Board.insert_all([{ id: 'b2', user_id: 'u1', name: 'No fold' }]) }

    counts = ReplicaMan::Reconcile.call(DummyReplica)

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1')
    assert_equal 'Renamed behind the door', snapshot.data['name']
    assert_equal fold_position, snapshot.document_position, 'a projection true-up is not a doc reship'
    assert_equal({ recaptured: 1, tombstoned: 0, missing_fold: 1 }, counts[:boards])
    assert_nil ReplicaMan::Snapshot.find_by(stream: 'boards', row_id: 'b2'),
               'a doc row with no fold cannot be conjured — it is reported for a human'
  end
end
