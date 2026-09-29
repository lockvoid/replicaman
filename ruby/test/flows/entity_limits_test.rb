require 'test_helper'
require 'active_support/testing/constant_stubbing'

class EntityLimitsTest < ActiveSupport::TestCase
  include ActiveSupport::Testing::ConstantStubbing

  setup do
    @user = User.create!(id: 'owner', name: 'Owner')
    @board = create_board!(id: 'board', user: @user, name: 'Small')
  end

  test 'an entity measures exactly as its baseline frame' do
    Items::TextItem.create!(id: 'row', board: @board, rank: 'a', body: 'é' * 10)
    Items::TextItem.create!(id: 'gone', board: @board, rank: 'b', body: 'x').destroy!
    DummyReplica.document(:boards, @board.id).edit { it.get_map('meta').set('name', 'Grown') }

    [%w[boards board], %w[items row], %w[items gone]].each do |stream, row_id|
      snapshot = ReplicaMan::Snapshot.find_by!(stream: stream, row_id: row_id)
      assert_equal JSON.generate(ReplicaMan::Frames.baseline(DummyReplica, snapshot)).bytesize,
                   ReplicaMan::Frames.size(DummyReplica, stream, row_id), "#{stream}/#{row_id}"
    end
  end

  test 'oversized host writes roll back both domain and capture' do
    stub_const(ReplicaMan::Protocol, :ENTITY_BYTES, 1024) do
      assert_raises(ReplicaMan::Refused) do
        Items::TextItem.create!(id: 'large', board: @board, rank: 'a', body: 'x' * 2048)
      end
    end

    assert_nil Item.find_by(id: 'large')
    assert_nil ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'large')
    assert_equal 0, ReplicaMan::CaptureHooks.changes(ActiveRecord::Base.connection).to_a.size
  end

  test 'an oversized member refuses the entire group and retries the same result' do
    client = ProtocolClient.new(@user)
    operations = %w[small large].map do |id|
      { 'id' => DomainClient.uuid(id), 'group' => DomainClient.uuid('oversized'), 'op' => 'row.create', 'stream' => 'items', 'row_id' => id,
        'incarnation' => SecureRandom.uuid, 'type' => 'TextItem',
        'data' => { 'boardId' => @board.id, 'rank' => 'a', 'body' => id == 'large' ? 'x' * 2048 : 'keep' } }
    end

    stub_const(ReplicaMan::Protocol, :ENTITY_BYTES, 1024) do
      result = client.push(*operations)
      verdicts = result.fetch(:verdicts)
      assert_equal %w[rejected rejected], verdicts.map { it.fetch(:outcome) }
      assert_match(/synchronization limit/, verdicts.first.fetch(:reason))
      assert_equal result, client.push(*operations)
    end
    assert_empty Item.where(id: %w[small large])
    assert_empty ReplicaMan::Snapshot.where(stream: 'items', row_id: %w[small large])
  end

  test 'document growth rolls back its fold tail and projection' do
    before = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: @board.id).attributes
    tail = ReplicaMan::Delta.count
    stub_const(ReplicaMan::Protocol, :ENTITY_BYTES, 1024) do
      assert_raises(ReplicaMan::Refused) do
        DummyReplica.document(:boards, @board.id).edit do |doc|
          doc.get_map('meta').set('name', 'x' * 2048)
        end
      end
    end

    assert_equal 'Small', @board.reload.name
    assert_equal before, ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: @board.id).attributes
    assert_equal tail, ReplicaMan::Delta.count
  end
end
