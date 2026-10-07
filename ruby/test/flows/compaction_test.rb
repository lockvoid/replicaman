require 'test_helper'

class CompactionTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')

    @base = Loro::Doc.new(peer_id: 11)
    @base.get_map('meta').set('name', 'Long-lived')
    @base.commit
    push({ id: 'c1', op: 'row.create', stream: 'boards', row_id: 'b1',
           codec: 'loro@1', seed: Base64.strict_encode64(@base.export_snapshot) })
  end

  def push(*ops)
    push_ops(DummyReplica, user: @user, ops: ops)[:verdicts]
  end

  def push_edit(doc, id:)
    before = doc.version_vector
    yield doc
    push({ id: id, op: 'doc.delta', stream: 'boards', row_id: 'b1', codec: 'loro@1',
           payload: Base64.strict_encode64(doc.export_updates(since: before)) })
  end

  def server_fold
    Loro::Doc.from_snapshot(ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').document)
  end

  test 'the tail is folded and deleted; a cursor older than the fold gets a doc.snapshot' do
    stale_cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

    push_edit(@base, id: 'd1') { it.get_map('meta').set('color', 'red') }
    push_edit(@base, id: 'd2') { it.get_map('meta').set('size', 'large') }
    assert_equal 2, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count

    DummyReplica.document(:boards, 'b1').compact

    assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count, 'the folded tail is gone'

    late = pull_checkpoint(DummyReplica, user: @user, cursor: stale_cursor)
    assert_empty late[:frames].select { it[:frame] == 'doc.delta' }
    docs = late[:frames].select { it[:frame] == 'doc.snapshot' }
    assert_equal ['b1'], docs.map { it[:id] }, 'past-the-fold cursors are handed the doc instead'
    assert_equal 'Long-lived', docs.first[:data]['name'], 'with its projection riding along'

    replica = Loro::Doc.from_snapshot(Base64.strict_decode64(docs.first[:snapshot]))
    assert_equal({ 'name' => 'Long-lived', 'color' => 'red', 'size' => 'large' },
                 replica.get_map('meta').to_h, 'the doc frame carries everything the deltas said')
  end

  test 'the 64th accepted delta folds the tail inline and bumps the fold axis' do
    fold_position = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').document_position

    63.times { |i| push_edit(@base, id: "d#{i}") { it.get_map('meta').set("k#{i}", i) } }
    assert_equal 63, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count
    assert_equal fold_position, ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').document_position,
                 'below the budget nothing moves — plain accepts never bump the fold'

    push_edit(@base, id: 'd63') { it.get_map('meta').set('k63', 63) }

    assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count,
                 'the accept that reaches the budget folds the tail in the same request'
    refute_equal fold_position, ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').document_position,
                 'the fold moved — stale cursors must be handed the doc, not the vanished rows'
    assert_equal 63, server_fold.get_map('meta').get('k63'), 'nothing folded is lost'
  end

  test 'a cursor mid-tail at the auto-fold gets a doc.snapshot, never a gap' do
    with_compact_every(4) do
      push_edit(@base, id: 'd1') { it.get_map('meta').set('a', 1) }
      push_edit(@base, id: 'd2') { it.get_map('meta').set('b', 2) }
      mid_tail = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

      push_edit(@base, id: 'd3') { it.get_map('meta').set('c', 3) }
      push_edit(@base, id: 'd4') { it.get_map('meta').set('d', 4) }
      assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count, 'the 4th accept auto-folded'

      late = pull_checkpoint(DummyReplica, user: @user, cursor: mid_tail)
      assert_empty late[:frames].select { it[:frame] == 'doc.delta' }
      docs = late[:frames].select { it[:frame] == 'doc.snapshot' }
      assert_equal ['b1'], docs.map { it[:id] }, 'past-the-fold cursors are handed the doc instead'

      replica = Loro::Doc.from_snapshot(Base64.strict_decode64(docs.first[:snapshot]))
      assert_equal 4, replica.get_map('meta').get('d'), 'carrying everything the vanished deltas said'
    end
  end

  test 'a cursor caught up to the delta before the auto-fold is handed the document' do
    with_compact_every(4) do
      %w[d1 d2 d3].each_with_index { |name, index| push_edit(@base, id: name) { it.get_map('meta').set("k#{index}", index) } }
      caught_up = pull_checkpoint(DummyReplica, user: @user, cursor: nil)
      baseline = caught_up[:frames].find { it[:frame] == 'doc.snapshot' }
      client = Loro::Doc.from_snapshot(Base64.strict_decode64(baseline[:snapshot]), peer_id: 99)

      push_edit(@base, id: 'd4') { it.get_map('meta').set('k3', 3) }
      assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count, 'the 4th accept auto-folded'

      late = pull_checkpoint(DummyReplica, user: @user, cursor: caught_up[:cursor])
      assert_equal %w[doc.snapshot], late[:frames].map { it[:frame] }, 'the fold is a new baseline for every cursor behind it'
      client.import(Base64.strict_decode64(late[:frames].first[:snapshot]))
      assert_equal 3, client.get_map('meta').get('k3'), 'the folding delta reaches the caught-up device'

      push_edit(@base, id: 'd5') { it.get_map('meta').set('k4', 4) }
      after = pull_checkpoint(DummyReplica, user: @user, cursor: late[:cursor])
      assert_equal %w[doc.delta row.set], after[:frames].map { it[:frame] }
      refute client.import(Base64.strict_decode64(after[:frames].first[:payload])).fetch(:pending), 'the next delta merges into that baseline'
      assert_equal 4, client.get_map('meta').get('k4')
    end
  end

  test 'a server edit that auto-folds without moving the projection still moves the fold axis' do
    with_compact_every(4) do
      3.times { |index| DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set("s#{index}", index) } }
      caught_up = pull_checkpoint(DummyReplica, user: @user, cursor: nil)
      position = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1').position

      DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('s3', 3) }

      assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count
      snapshot = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1')
      assert_operator snapshot.position, :>, position, 'the fold takes a position though the projection is unchanged'
      assert_equal snapshot.position, snapshot.document_position, 'and that position is the fold axis'
      late = pull_checkpoint(DummyReplica, user: @user, cursor: caught_up[:cursor])
      assert_equal %w[doc.snapshot], late[:frames].map { it[:frame] }
      assert_equal 3, Loro::Doc.from_snapshot(Base64.strict_decode64(late[:frames].first[:snapshot])).get_map('meta').get('s3')
    end
  end

  test 'an explicit compaction takes a position: every cursor behind it is handed the document' do
    push_edit(@base, id: 'd1') { it.get_map('meta').set('a', 1) }
    caught_up = pull_checkpoint(DummyReplica, user: @user, cursor: nil)
    before = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1')

    DummyReplica.document(:boards, 'b1').compact

    after = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'b1')
    assert_operator after.position, :>, before.position
    assert_equal after.position, after.document_position
    late = pull_checkpoint(DummyReplica, user: @user, cursor: caught_up[:cursor])
    assert_equal %w[doc.snapshot], late[:frames].map { it[:frame] }
    assert_equal after.revision.to_s, late[:frames].first[:revision], 'the frame carries the revision the compaction took'
  end

  test 'a late pusher after the auto-fold still merges — the fold keeps FULL history' do
    offline = Loro::Doc.from_snapshot(@base.export_snapshot, peer_id: 99)
    ancient_version = offline.version_vector

    with_compact_every(2) do
      push_edit(@base, id: 'd1') { it.get_map('meta').set('color', 'red') }
      push_edit(@base, id: 'd2') { it.get_map('meta').set('size', 'large') }
      assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count, 'auto-folded at the budget'

      offline.get_map('meta').set('note', 'from the past')
      verdicts = push({ id: 'late-1', op: 'doc.delta', stream: 'boards', row_id: 'b1', codec: 'loro@1',
                        payload: Base64.strict_encode64(offline.export_updates(since: ancient_version)) })

      assert_equal [{ id: 'late-1', outcome: 'accepted' }], verdicts,
                   'auto-compaction must never change merge semantics — full history stays in the fold'
      assert_equal 'from the past', server_fold.get_map('meta').get('note')
      assert_equal 'large', server_fold.get_map('meta').get('size')
    end
  end

  test 'server-authored edits fold on the same budget' do
    with_compact_every(2) do
      DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('a', 1) }
      DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('b', 2) }

      assert_equal 0, ReplicaMan::Delta.where(stream: 'boards', row_id: 'b1').count,
                   'a server-only editor must not grow an unbounded tail'
      assert_equal 2, server_fold.get_map('meta').get('b')
    end
  end

  def with_compact_every(budget)
    normalizer = DummyReplica.streams[:boards].normalizer
    normalizer.define_singleton_method(:compact_every) { budget }
    yield
  ensure
    normalizer.singleton_class.send(:remove_method, :compact_every)
  end

  test 'a late pusher with pre-compaction deps still merges — the fold keeps FULL history' do
    offline = Loro::Doc.from_snapshot(@base.export_snapshot, peer_id: 99)
    ancient_version = offline.version_vector

    push_edit(@base, id: 'd1') { it.get_map('meta').set('color', 'red') }
    DummyReplica.document(:boards, 'b1').compact
    push_edit(@base, id: 'd2') { it.get_map('meta').set('size', 'large') }
    DummyReplica.document(:boards, 'b1').compact

    offline.get_map('meta').set('note', 'from the past')
    verdicts = push({ id: 'late-1', op: 'doc.delta', stream: 'boards', row_id: 'b1', codec: 'loro@1',
                      payload: Base64.strict_encode64(offline.export_updates(since: ancient_version)) })

    assert_equal [{ id: 'late-1', outcome: 'accepted' }], verdicts,
                 'months-offline deltas must always merge — the alternative is the one-star review'
    assert_equal 'from the past', server_fold.get_map('meta').get('note')
    assert_equal 'large', server_fold.get_map('meta').get('size'), 'nothing already folded is lost'
  end
end
