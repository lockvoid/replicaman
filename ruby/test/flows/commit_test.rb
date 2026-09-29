require 'test_helper'

class CommitTest < ActiveSupport::TestCase
  setup { @user = User.create!(id: 'u1', name: 'Ada') }

  test 'a command returns its value and one authenticated refresh hint per affected shard' do
    other = User.create!(id: 'u2', name: 'Other')
    commit = DummyReplica.capture(user: @user) do
      Job.create!(id: 'mine', user: @user, state: 'queued')
      Job.create!(id: 'foreign', user: other, state: 'queued')
      Ticket.create!(code: 'ticket', user: @user, note: 'also changed')
      :answer
    end
    assert_equal :answer, commit.value
    assert_equal ReplicaMan::Protocol.header(DummyReplica).deep_stringify_keys.merge('shards' => ['user']), decode(commit)
    client = ProtocolClient.new(@user)
    frames = client.frames(client.pull)
    assert_equal %w[mine ticket], frames.map { it.fetch('id') }.sort
    assert_equal 'queued', frames.find { it.fetch('id') == 'mine' }.fetch('data').fetch('state')
  end

  test 'a command keeps its own outermost transaction — capture opens none' do
    commit = DummyReplica.capture(user: @user) { ActiveRecord::Base.connection.transaction_open? }

    refute commit.value, 'a host ledger that must own the outermost transaction runs inside commands'
  end

  test 'rollback is excluded and collection does not leak to later commands' do
    commit = DummyReplica.capture(user: @user) do
      ActiveRecord::Base.transaction(requires_new: true) do
        Job.create!(id: 'gone', user: @user, state: 'queued')
        raise ActiveRecord::Rollback
      end
    end
    Job.create!(id: 'outside', user: @user, state: 'queued')
    assert_empty decode(commit).fetch('shards')
  end

  test 'an envelope cannot escape an uncommitted outer transaction' do
    ActiveRecord::Base.transaction do
      commit = DummyReplica.capture(user: @user) { Job.create!(id: 'pending', user: @user, state: 'queued') }
      assert_raises(ReplicaMan::Commit::Uncommitted) { commit.encode }
      raise ActiveRecord::Rollback
    end
  end

  test 'capture inside an outer transaction includes its final committed rows only' do
    commit = nil
    ActiveRecord::Base.transaction do
      commit = DummyReplica.capture(user: @user) { Job.create!(id: 'inside', user: @user, state: 'queued') }
      Job.find('inside').update!(state: 'running')
      Job.create!(id: 'outside', user: @user, state: 'queued')
    end
    assert_equal ['user'], decode(commit).fetch('shards')
    client = ProtocolClient.new(@user)
    frames = client.frames(client.pull)
    assert_equal %w[inside outside], frames.pluck('id').sort
    assert_equal 'running', frames.find { it.fetch('id') == 'inside' }.fetch('data').fetch('state')
  end

  test 'nested capture honors savepoint rollback and keeps both collectors' do
    inner = nil
    outer = DummyReplica.capture(user: @user) do
      ActiveRecord::Base.transaction do
        inner = DummyReplica.capture(user: @user) do
          Job.create!(id: 'kept', user: @user, state: 'queued')
          ActiveRecord::Base.transaction(requires_new: true) do
            Job.create!(id: 'rolled-back', user: @user, state: 'queued')
            raise ActiveRecord::Rollback
          end
        end
      end
    end
    assert_equal ['user'], decode(inner).fetch('shards')
    assert_equal ['kept'], Job.pluck(:id)
    assert_equal decode(inner), decode(outer)
  end

  test 'delete and recreation advance revisions even after tombstone collection' do
    job = Job.create!(id: 'j1', user: @user, state: 'queued')
    first = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: job.id).revision
    commit = DummyReplica.capture(user: @user) { job.destroy! }
    assert_equal ['user'], decode(commit).fetch('shards')
    dead = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: job.id)
    assert dead.deleted_at
    assert_operator dead.revision, :>, first
    DummyReplica.gc(window: 0.seconds)
    Job.create!(id: 'j1', user: @user, state: 'running')
    assert_operator ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').revision, :>, dead.revision
  end

  test 'an empty command has no refresh hint' do
    commit = DummyReplica.capture(user: @user) { :nothing }
    assert_equal :nothing, commit.value
    assert_empty decode(commit).fetch('shards')
  end

  test 'encoding once gives a stable response even if the row changes again' do
    commit = DummyReplica.capture(user: @user) { Job.create!(id: 'j1', user: @user, state: 'queued') }
    encoded = commit.encode
    Job.find('j1').update!(state: 'running')
    assert_equal encoded, commit.encode
    assert_equal ['user'], decode(commit).fetch('shards')
  end

  test 'same-row positions and revisions follow commits even when transactions began in reverse order' do
    job = Job.create!(id: 'j1', user: @user, state: 'queued')
    ready, proceed = Queue.new, Queue.new
    older = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do |connection|
        connection.transaction do
          ready << connection.select_value('SELECT pg_current_xact_id()::text').to_i
          proceed.pop
          Job.find(job.id).update!(state: 'done')
        end
      end
    end
    ready.pop
    job.update!(state: 'running')
    middle = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: job.id)
    proceed << true
    older.value
    last = ReplicaMan::Snapshot.uncached { ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: job.id) }
    assert_operator last.position, :>, middle.position
    assert_operator last.revision, :>, middle.revision
    assert_equal 'done', last.data['state']
  ensure
    proceed << true
    older&.join
  end

  test 'a document command refreshes a checkpoint with a full fold covering its deltas' do
    seed = Loro::Doc.new(peer_id: 42)
    seed.get_map('meta').set('name', 'Before')
    push_ops(DummyReplica, user: @user, ops: [{ id: 'create', op: 'row.create', stream: 'boards', row_id: 'b1', codec: 'loro@1', seed: Base64.strict_encode64(seed.export_snapshot) }])
    commit = DummyReplica.capture(user: @user) do
      DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'After') }
    end
    assert_equal ['user'], decode(commit).fetch('shards')
    client = ProtocolClient.new(@user)
    frame = client.frames(client.pull).find { it.fetch('id') == 'b1' }
    assert_equal 'doc.snapshot', frame.fetch('frame')
    doc = Loro::Doc.from_snapshot(Base64.strict_decode64(frame.fetch('snapshot')), peer_id: 99)
    assert_equal 'After', doc.get_map('meta').get('name')
  end

  private

  def decode(commit)
    JSON.parse(Base64.strict_decode64(commit.encode))
  end
end
