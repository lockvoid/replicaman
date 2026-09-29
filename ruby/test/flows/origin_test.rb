require 'test_helper'

class OriginTest < ActiveSupport::TestCase
  include Rack::Test::Methods

  def app
    DummyReplica
  end

  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @board = create_board!(id: 'b1', user: @user, name: 'Plans')
    @rings = []
    DummyReplica.doorbell { |shard:, captures:| @rings << { shard: shard, captures: captures } }
  end

  teardown do
    DummyReplica.doorbell { }
  end

  def captured_origin(row_id)
    capture = @rings.flat_map { it[:captures] }.find { it[:row_id] == row_id }
    assert capture, "no ring captured row #{row_id}"
    capture[:origin]
  end

  def item_op(id:, row_id:, verb: 'row.create', data: { rank: 'a', body: 'x' })
    op = { id: id, op: verb, stream: 'items', row_id: row_id, type: 'TextItem' }
    op[:data] = data.merge(boardId: 'b1') unless verb == 'row.delete'
    op
  end

  test 'a push-applied row carries the pushing device identity into its ring' do
    push_ops(DummyReplica, user: @user, ops: [item_op(id: 'op1', row_id: 'i1')], origin: 'device-1')

    assert_equal 'device-1', captured_origin('i1')
  end

  test 'a push without origin leaves every capture unattributed' do
    push_ops(DummyReplica, user: @user, ops: [item_op(id: 'op1', row_id: 'i1')])

    assert_nil captured_origin('i1')
  end

  test 'a same-transaction cascade onto ANOTHER row stays unattributed inside the op window' do
    ReplicaMan::Capture.with_op_origin('device-1', stream: 'items', row_id: 'i1') do
      ActiveRecord::Base.transaction do
        Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'mine')
        Job.create!(id: 'j1', user: @user, state: 'queued')
      end
    end

    assert_equal 'device-1', captured_origin('i1'), 'the op row is the device\'s verbatim write'
    assert_nil captured_origin('j1'), 'a cascade is CAUSED by the device but not WRITTEN by it — it must ring'
  end

  test 'a later re-save of the op row outside the window overwrites to unattributed' do
    ActiveRecord::Base.transaction do
      ReplicaMan::Capture.with_op_origin('device-1', stream: 'items', row_id: 'i1') do
        Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'mine')
      end
      Item.find('i1').update!(body: 'server rewrote this')
    end

    assert_nil captured_origin('i1'),
               'the server touched the row after the verbatim apply — the device no longer holds its state'
  end

  test 'server-side writes are always unattributed — plain saves and document authoring alike' do
    Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'server')
    DummyReplica.document(:boards, 'b2').create(user_id: 'u1') { it.get_map('meta').set('name', 'Server made') }
    DummyReplica.document(:boards, 'b2').edit { |doc| doc.get_map('meta').set('name', 'Server pass') }

    assert_nil captured_origin('i1')
    assert_nil captured_origin('b2')
  end

  test 'an accepted row.delete carries the device identity on its tombstone capture' do
    Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'bye')
    @rings.clear

    push_ops(DummyReplica, user: @user, ops: [item_op(id: 'op1', row_id: 'i1', verb: 'row.delete')], origin: 'device-1')

    assert_equal 'device-1', captured_origin('i1')
  end

  test 'a doc.delta push stamps the document row capture with the device identity' do
    doc = Loro::Doc.new(peer_id: 11)
    doc.get_map('meta').set('name', 'Pushed board')
    doc.commit
    push_ops(DummyReplica, user: @user, ops: [
                        { id: 'op1', op: 'row.create', stream: 'boards', row_id: 'bd1',
                          codec: 'loro@1', seed: Base64.strict_encode64(doc.export_snapshot) }
                      ], origin: 'device-1')

    assert_equal 'device-1', captured_origin('bd1')

    @rings.clear
    fork_version = doc.version_vector
    doc.get_map('meta').set('name', 'Edited on device')
    doc.commit
    push_ops(DummyReplica, user: @user, ops: [
                        { id: 'op2', op: 'doc.delta', stream: 'boards', row_id: 'bd1',
                          codec: 'loro@1',
                          payload: Base64.strict_encode64(doc.export_updates(since: fork_version)) }
                      ], origin: 'device-1')

    assert_equal 'device-1', captured_origin('bd1')
  end

  test 'the rack door reads X-Device-Id as the push origin' do
    client = ProtocolClient.new(@user)
    operation = item_op(id: DomainClient.uuid('op1'), row_id: 'i1').merge(incarnation: SecureRandom.uuid)
    body = client.headers.merge('ops' => [operation])
    post '/push', JSON.generate(body),
         { 'HTTP_X_USER_ID' => 'u1', 'HTTP_X_DEVICE_ID' => 'device-9', 'CONTENT_TYPE' => 'application/json' }

    assert_equal 'device-9', captured_origin('i1')
  end

  test 'a rejected op captures nothing and rings nothing' do
    push_ops(DummyReplica, user: @user, ops: [
                        { id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'NopeItem', data: {} }
                      ], origin: 'device-1')

    assert_empty @rings
  end
end
