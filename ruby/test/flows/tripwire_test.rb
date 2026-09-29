require 'test_helper'

class TripwireTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @io = StringIO.new
    @previous = Rails.logger
    Rails.logger = ActiveSupport::Logger.new(@io)
  end

  teardown do
    Rails.logger = @previous
  end

  test 'pull logs shard, frame count, latency and whether it was a bootstrap' do
    Job.create!(id: 'j1', user: @user, state: 'queued')

    result = pull_checkpoint(DummyReplica, user: @user, cursor: nil)
    assert_match(/\[replica_man\] pull shard=user frames=1 ms=\d+ bootstrap=true/, @io.string)

    pull_checkpoint(DummyReplica, user: @user, cursor: result[:cursor])
    assert_match(/\[replica_man\] pull shard=user frames=0 ms=\d+ bootstrap=false/, @io.string)
  end

  test 'push logs the batch size and the rejected-verdict count' do
    board = Loro::Doc.new(peer_id: 11)
    board.get_map('meta').set('name', 'Plans')
    board.commit

    push_ops(DummyReplica, user: @user, ops: [
      { id: 'c1', op: 'row.create', stream: 'boards', row_id: 'b1',
        codec: 'loro@1', seed: Base64.strict_encode64(board.export_snapshot) },
      { id: 'x1', op: 'row.zap', stream: 'boards', row_id: 'b1' }
    ])

    assert_match(/\[replica_man\] push ops=2 rejected=1 ms=\d+/, @io.string)
  end

  test 'every compact logs the folded tail size and the fold bytes' do
    board = Loro::Doc.new(peer_id: 11)
    board.get_map('meta').set('name', 'Plans')
    board.commit
    push_ops(DummyReplica, user: @user, ops: [{ id: 'c1', op: 'row.create', stream: 'boards', row_id: 'b1',
                                           codec: 'loro@1', seed: Base64.strict_encode64(board.export_snapshot) }])
    before = board.version_vector
    board.get_map('meta').set('color', 'red')
    board.commit
    push_ops(DummyReplica, user: @user, ops: [{ id: 'd1', op: 'doc.delta', stream: 'boards', row_id: 'b1',
                                           codec: 'loro@1', payload: Base64.strict_encode64(board.export_updates(since: before)) }])

    DummyReplica.document(:boards, 'b1').compact

    assert_match(/\[replica_man\] compact stream=boards row=b1 folded=1 fold_bytes=\d+/, @io.string)
  end
end
