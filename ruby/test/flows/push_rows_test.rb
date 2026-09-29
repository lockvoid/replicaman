require 'test_helper'

class PushRowsTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @rival = User.create!(id: 'u2', name: 'Rival')
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Mine') }
    DummyReplica.document(:boards, 'b9').create(user_id: 'u2') { it.get_map('meta').set('name', 'Theirs') }
  end

  def push(*ops)
    push_ops(DummyReplica, user: @user, ops: ops)[:verdicts]
  end

  test 'a mixed batch answers per-op verdicts and applies each op independently' do
    verdicts = push(
      { id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
        data: { boardId: 'b1', rank: 'a', body: 'hello' } },
      { id: 'op2', op: 'row.create', stream: 'items', row_id: 'i2', type: 'TextItem',
        data: { boardId: 'b1', rank: 'b' } },
      { id: 'op3', op: 'row.create', stream: 'items', row_id: 'i3', type: 'PhotoItem',
        data: { boardId: 'b1', rank: 'c', width: 5 } },
    )

    assert_equal [
      { id: 'op1', outcome: 'accepted' },
      { id: 'op2', outcome: 'rejected', reason: "Body can't be blank" },
      { id: 'op3', outcome: 'accepted' }
    ], verdicts, 'the STI subclass validation speaks in the verdict; one refusal does not dam the batch'

    assert_equal 'hello', Item.find('i1').body
    assert_nil Item.find_by(id: 'i2')
    assert_equal 5, Item.find('i3').width
    assert_equal 'TextItem', ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').row_type,
                 'an accepted push captures like any other write'
  end

  test 'opaque business keys survive capture, replay and a checkpoint unchanged' do
    id = 'pmck/Element/é-owned key/original'
    operation = { id: 'path-key', op: 'row.create', stream: 'items', row_id: id,
                  type: 'TextItem', data: { boardId: 'b1', rank: 'a', body: 'opaque' } }
    assert_equal 'accepted', push(operation).sole.fetch(:outcome)
    assert_equal 'accepted', push(operation).sole.fetch(:outcome)
    assert_equal 'opaque', Item.find(id).body
    frames = pull_checkpoint(DummyReplica, user: @user).fetch(:frames)
    assert frames.any? { it.fetch(:stream) == 'items' && it.fetch(:id) == id }
  end

  test 'invalid business keys refuse the request before an operation is recorded' do
    client = ProtocolClient.new(@user)
    [nil, '', "bad\0key", 'é' * 513].each do |id|
      operation = { 'id' => DomainClient.uuid('bad-key'), 'op' => 'row.create', 'stream' => 'items', 'row_id' => id,
                    'incarnation' => 'life', 'type' => 'TextItem', 'data' => { 'boardId' => 'b1', 'body' => 'bad' } }
      assert_raises(ReplicaMan::InvalidRequest) { client.push(operation) }
      assert_equal 0, ReplicaMan::Operation.count
    end
  end

  test 'row.patch applies only the changed fields' do
    push({ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
           data: { boardId: 'b1', rank: 'a', body: 'original' } })

    verdicts = push({ id: 'op2', op: 'row.patch', stream: 'items', row_id: 'i1', data: { rank: 'z' } })

    assert_equal [{ id: 'op2', outcome: 'accepted' }], verdicts
    item = Item.find('i1')
    assert_equal 'z', item.rank
    assert_equal 'original', item.body, 'untouched fields survive — cross-device edits to different fields never clobber'

    push({ id: 'op3', op: 'row.patch', stream: 'items', row_id: 'i1', data: { body: 'edited' } })
    assert_equal 'edited', Item.find('i1').body, 'store keys patch through the same flat wire shape'

    missing = push({ id: 'op4', op: 'row.patch', stream: 'items', row_id: 'i404', data: { rank: 'q' } })
    assert_equal [{ id: 'op4', outcome: 'rejected', reason: 'entity incarnation is no longer current' }], missing
  end

  test 'row.delete tombstones the row and retries idempotently' do
    push({ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
           data: { boardId: 'b1', rank: 'a', body: 'bye' } })

    first = push({ id: 'op2', op: 'row.delete', stream: 'items', row_id: 'i1' })
    again = push({ id: 'op2', op: 'row.delete', stream: 'items', row_id: 'i1' })

    assert_equal [{ id: 'op2', outcome: 'accepted' }], first
    assert_equal [{ id: 'op2', outcome: 'accepted' }], again, 'a retried delete reports accepted again'
    assert_nil Item.find_by(id: 'i1')
    assert ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').deleted_at
  end

  test 'a retried batch answers identical verdicts without double-applying' do
    ops = [
      { id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
        data: { boardId: 'b1', rank: 'a', body: 'once' } },
      { id: 'op2', op: 'row.create', stream: 'items', row_id: 'i2', type: 'TextItem',
        data: { boardId: 'b1', rank: 'b' } }
    ]

    first = push(*ops)
    retried = push(*ops)

    assert_equal first, retried, 'a re-pushed batch reports once — same ids, same verdicts'
    assert_equal 1, Item.where(id: 'i1').count
    assert_equal 'once', Item.find('i1').body
  end

  test 'receipt replay succeeds but new creates cannot overwrite an existing identity' do
    push({ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
           data: { boardId: 'b1', rank: 'a', body: 'first' } })
    push({ id: 'op2', op: 'row.patch', stream: 'items', row_id: 'i1', data: { rank: 'b' } })

    stale = push({ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
                   data: { boardId: 'b1', rank: 'a', body: 'first' } })
    assert_equal [{ id: 'op1', outcome: 'accepted' }], stale,
                 'a lost-ack replay acks; the payload is NOT applied'
    item = Item.find('i1')
    assert_equal 'b', item.rank, 'the patched (fresher) state survives the replay'
    assert_equal 'first', item.body

    retyped = push({ id: 'op3', op: 'row.create', stream: 'items', row_id: 'i1', type: 'PhotoItem',
                     data: { boardId: 'b1', rank: 'b', width: 1 } })
    assert_equal [{ id: 'op3', outcome: 'rejected', reason: 'entity already exists with another incarnation' }], retyped
    assert_equal 'TextItem', Item.find('i1').class.name.demodulize,
                 'a new birth cannot change the existing row type'

    matching = push({ id: 'op4', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
                      data: { boardId: 'b1', rank: 'b', body: 'first' } })
    assert_equal [{ id: 'op4', outcome: 'rejected', reason: 'entity already exists with another incarnation' }], matching,
                 'matching values do not turn a new operation into the original receipt'
  end

  test 'readonly streams refuse every write — server-authored rows have no client door' do
    verdicts = push({ id: 'op1', op: 'row.create', stream: 'jobs', row_id: 'j1',
                      data: { userId: 'u1', state: 'queued' } })

    assert_equal [{ id: 'op1', outcome: 'rejected', reason: 'stream jobs is readonly' }], verdicts
    assert_equal 0, Job.count
  end

  test 'unknown streams, verbs, columns and types are verdicts, not errors' do
    verdicts = push(
      { id: 'op1', op: 'row.create', stream: 'gizmos', row_id: 'g1', data: {} },
      { id: 'op2', op: 'row.zap', stream: 'items', row_id: 'i1' },
      { id: 'op3', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
        data: { boardId: 'b1', rank: 'a', body: 'x', nope: 1 } },
      { id: 'op4', op: 'row.create', stream: 'items', row_id: 'i1', type: 'VideoItem',
        data: { boardId: 'b1', rank: 'a' } },
    )

    assert_equal [
      { id: 'op1', outcome: 'rejected', reason: 'unknown stream: gizmos' },
      { id: 'op2', outcome: 'rejected', reason: 'unknown op: row.zap' },
      { id: 'op3', outcome: 'rejected', reason: 'unknown column: nope' },
      { id: 'op4', outcome: 'rejected', reason: 'unknown type: VideoItem' }
    ], verdicts
  end

  test 'membership gates every write with the same rule as pull' do
    into_theirs = push({ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
                         data: { boardId: 'b9', rank: 'a', body: 'trespass' } })
    assert_equal 'rejected', into_theirs.first[:outcome]
    assert_equal 'row is outside your replica', into_theirs.first[:reason]
    assert_nil Item.find_by(id: 'i1')

    theirs = Items::TextItem.create!(id: 'i9', board_id: 'b9', rank: 'a', body: 'theirs')
    steal = push(
      { id: 'op2', op: 'row.patch', stream: 'items', row_id: 'i9', data: { body: 'mine now' } },
      { id: 'op3', op: 'row.delete', stream: 'items', row_id: 'i9' },
      { id: 'op4', op: 'row.patch', stream: 'items', row_id: 'i9', data: { boardId: 'b1' } },
    )
    assert_equal %w[rejected rejected rejected], steal.map { it[:outcome] }
    assert_equal 'theirs', theirs.reload.body
  end

  test 'row.patch on a document stream is refused — the projection is not writable' do
    verdicts = push({ id: 'op1', op: 'row.patch', stream: 'boards', row_id: 'b1',
                      data: { name: 'sneaky' } })

    assert_equal [{ id: 'op1', outcome: 'rejected',
                    reason: 'stream boards is a document stream — push deltas, not row ops' }], verdicts
    assert_equal 'Mine', Board.find('b1').name
  end
end
