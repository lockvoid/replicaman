require 'test_helper'

class PullLambdaTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Plans') }
  end

  def push(*ops)
    push_ops(DummyReplica, user: @user, ops: ops)[:verdicts]
  end

  test 'capture computes a pull lambda on every save and stores it in the snapshot data' do
    job = Job.create!(id: 'j1', user: @user, state: 'queued')

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1')
    assert_equal({ 'state' => 'queued', 'active' => true, 'metricsByKey' => {} }, snapshot.data['summary'],
                 'a shaped computed value stores its structure, not a string')

    job.update!(state: 'done')
    assert_equal({ 'state' => 'done', 'active' => false, 'metricsByKey' => {} },
                 ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').data['summary'],
                 'recomputed at every save — a computed value can never lag its own row')
  end

  test 'pull relays the captured value verbatim' do
    Job.create!(id: 'j1', user: @user, state: 'queued')

    frames = pull_checkpoint(DummyReplica, user: @user)[:frames]
    job_frame = frames.find { it[:stream] == 'jobs' && it[:id] == 'j1' }

    assert_equal({ 'state' => 'queued', 'active' => true, 'metricsByKey' => {} }, job_frame[:data]['summary'],
                 'shipped from storage — pull computes nothing')
  end

  test 'row ops have computed keys stripped before decode — derived data can never be pushed' do
    verdicts = push({ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
                      data: { boardId: 'b1', rank: 'a', body: 'hello', rankBadge: 'forged' } })

    assert_equal [{ id: 'op1', outcome: 'accepted' }], verdicts, 'a pushed computed key neither errors nor writes'
    assert_equal '#a', ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').data['rankBadge'],
                 'the stored value is the computed one, not the client claim'

    push({ id: 'op2', op: 'row.patch', stream: 'items', row_id: 'i1',
           data: { rank: 'z', rankBadge: 'still forged' } })
    assert_equal '#z', ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').data['rankBadge']
  end

  test 'backfill serializes computed values like any column' do
    uncaptured_fixture(Job) { Job.insert_all([{ id: 'j1', user_id: 'u1', state: 'queued' }]) }

    ReplicaMan::Backfill.call(DummyReplica)

    assert_equal({ 'state' => 'queued', 'active' => true, 'metricsByKey' => {} },
                 ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').data['summary'])
  end
end
