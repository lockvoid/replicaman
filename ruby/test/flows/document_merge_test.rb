require 'test_helper'

class DocumentMergeTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
  end

  def push(*ops)
    push_ops(DummyReplica, user: @user, ops: ops)[:verdicts]
  end

  def create_op(row_id, doc, id: 'create-1')
    { id: id, op: 'row.create', stream: 'boards', row_id: row_id,
      codec: 'loro@1', seed: Base64.strict_encode64(doc.export_snapshot) }
  end

  def delta_op(row_id, payload, id:)
    { id: id, op: 'doc.delta', stream: 'boards', row_id: row_id, codec: 'loro@1',
      payload: Base64.strict_encode64(payload) }
  end

  def server_fold(row_id)
    Loro::Doc.from_snapshot(ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: row_id).document)
  end

  test 'row.create births the document row: fold + projection + verdict, idempotent on retry' do
    doc = Loro::Doc.new(peer_id: 11)
    doc.get_map('meta').set('name', 'Pushed board')
    doc.commit

    verdicts = push(create_op('b1', doc))
    assert_equal [{ id: 'create-1', outcome: 'accepted' }], verdicts

    board = Board.find('b1')
    assert_equal 'Pushed board', board.name, 'the AR row is PROJECTED from the doc'
    assert_equal 'u1', board.user_id, 'ownership comes from the pushing user'
    assert_equal 'Pushed board', server_fold('b1').get_map('meta').get('name')

    retried = push(create_op('b1', doc))
    assert_equal [{ id: 'create-1', outcome: 'accepted' }], retried, 'a replayed create reports accepted again'
    assert_equal 1, Board.where(id: 'b1').count
  end

  test 'a document create and its delta land in ONE batch — the offline first-sync shape' do
    doc = Loro::Doc.new(peer_id: 11)
    doc.get_map('meta').set('name', 'Offline board')
    doc.commit
    seed = doc.export_snapshot
    fork_version = doc.version_vector
    doc.get_map('meta').set('name', 'Renamed offline')
    doc.commit

    verdicts = push_ops(DummyReplica, user: @user, ops: [
      { id: 'create-1', op: 'row.create', stream: 'boards', row_id: 'b1',
        codec: 'loro@1', seed: Base64.strict_encode64(seed) },
      delta_op('b1', doc.export_updates(since: fork_version), id: 'd-1')
    ])[:verdicts]

    assert_equal %w[accepted accepted], verdicts.map { it[:outcome] },
                 "the same-batch delta was refused: #{verdicts.inspect}"
    assert_equal 'Renamed offline', server_fold('b1').get_map('meta').get('name')
    assert_equal 'Renamed offline', Board.find('b1').name, 'the projection reads the delta'
  end

  test 'two divergent clients push deltas and every replica converges to one fold' do
    base = Loro::Doc.new(peer_id: 11)
    base.get_map('meta').set('name', 'Shared')
    base.commit
    push(create_op('b1', base))
    cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

    alice = Loro::Doc.from_snapshot(base.export_snapshot, peer_id: 11)
    bella = Loro::Doc.from_snapshot(base.export_snapshot, peer_id: 22)
    fork_version = alice.version_vector

    alice.get_map('meta').set('color', 'red')
    bella.get_map('meta').set('size', 'large')

    verdicts = push(
      delta_op('b1', alice.export_updates(since: fork_version), id: 'd-a'),
      delta_op('b1', bella.export_updates(since: fork_version), id: 'd-b'),
    )
    assert_equal %w[accepted accepted], verdicts.map { it[:outcome] }

    fold = server_fold('b1').get_map('meta')
    assert_equal 'Shared', fold.get('name')
    assert_equal 'red', fold.get('color')
    assert_equal 'large', fold.get('size')

    frames = pull_checkpoint(DummyReplica, user: @user, cursor: cursor).fetch(:frames)
    assert_equal %w[doc.delta doc.delta row.set], frames.pluck(:frame)
    frames.select { it[:frame] == 'doc.delta' }.each { alice.import(Base64.strict_decode64(it.fetch(:payload))) }
    assert_equal server_fold('b1').to_h, alice.to_h, 'a pull carries both concurrent edits'
  end

  test 'a delta depending on changes the server has not seen is a verdict, not a parked blob' do
    base = Loro::Doc.new(peer_id: 11)
    base.get_map('meta').set('name', 'Gappy')
    base.commit
    push(create_op('b1', base))

    seen = base.version_vector
    base.get_map('meta').set('color', 'red')
    base.commit
    middle = base.version_vector
    base.get_map('meta').set('size', 'small')

    orphan = push(delta_op('b1', base.export_updates(since: middle), id: 'd-2'))
    assert_equal [{ id: 'd-2', outcome: 'rejected', reason: 'delta depends on changes the server has not seen' }],
                 orphan
    assert_nil server_fold('b1').get_map('meta').get('size'), 'the fold must not absorb an orphan'

    healed = push(delta_op('b1', base.export_updates(since: seen), id: 'd-3'))
    assert_equal 'accepted', healed.first[:outcome]
    assert_equal 'small', server_fold('b1').get_map('meta').get('size')
  end

  test 'a retried delta answers accepted again without double-applying' do
    base = Loro::Doc.new(peer_id: 11)
    base.get_map('meta').set('name', 'Once')
    base.commit
    push(create_op('b1', base))

    before = base.version_vector
    base.get_map('meta').set('color', 'blue')
    op = delta_op('b1', base.export_updates(since: before), id: 'd-1')

    assert_equal 'accepted', push(op).first[:outcome]
    assert_equal 'accepted', push(op).first[:outcome], 'identical verdict on retry'
    assert_equal 1, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count,
                 'no second delta row — ids make a retried batch report once'
  end

  test 'a delta updates the complete fold and projection without compacting its history' do
    base = Loro::Doc.new(peer_id: 11)
    base.get_map('meta').set('name', 'Quiet')
    base.commit
    push(create_op('b1', base))
    cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]
    birth_fold = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').document_position

    before = base.version_vector
    base.get_map('meta').set('name', 'Renamed by delta')
    push(delta_op('b1', base.export_updates(since: before), id: 'd-1'))

    assert_equal 'Renamed by delta', Board.find('b1').name, 'the projection still follows the doc'
    fold = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1')
    assert_equal 'Renamed by delta', fold.data['name'], 'and so does the captured data'
    assert_equal birth_fold, fold.document_position, 'but the fold axis must not move — or every delta reships the whole doc'

    second = pull_checkpoint(DummyReplica, user: @user, cursor: cursor)
    frames = second[:frames]
    assert_equal %w[doc.delta row.set], frames.pluck(:frame)
    assert_equal 'Renamed by delta', frames.last.fetch(:data).fetch('name')
    assert_equal 1, ReplicaMan::Delta.where(row_id: 'b1').count

    push({ id: 'del-1', op: 'row.delete', stream: 'boards', row_id: 'b1' })
    assert_equal ['row.delete'], pull_checkpoint(DummyReplica, user: @user, cursor: second[:cursor])[:frames].pluck(:frame),
                 'the tombstone reaches the holder without reshipping the doc'
  end

  test 'server authoring lands like a client push: delta + fold + reprojection' do
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Before') }
    cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'After') }

    assert_equal 'After', Board.find('b1').name, 'the projection follows the doc'
    assert_equal 'After', server_fold('b1').get_map('meta').get('name')

    frames = pull_checkpoint(DummyReplica, user: @user, cursor: cursor)[:frames]
    assert_equal %w[doc.delta row.set], frames.pluck(:frame)
    assert_equal 'After', frames.last.fetch(:data).fetch('name')

    client = Loro::Doc.new(peer_id: 33)
    client.import(ReplicaMan::Snapshot.find_by!(row_id: 'b1').document)
    assert_equal 'After', client.get_map('meta').get('name')
  end
end
