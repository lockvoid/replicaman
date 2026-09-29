require 'test_helper'
require 'rack/test'
require 'zlib'

class RackTest < ActiveSupport::TestCase
  include Rack::Test::Methods

  def app
    DummyReplica
  end

  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @rival = User.create!(id: 'u2', name: 'Rival')
    @client = ProtocolClient.new(@user)
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Mine') }
    DummyReplica.document(:boards, 'b9').create(user_id: 'u2') { it.get_map('meta').set('name', 'Theirs') }
  end

  test 'POST /pull transfers an owned checkpoint and its changes' do
    post '/pull', JSON.generate(@client.headers.merge('shard' => 'user')), http_headers
    assert_predicate last_response, :ok?
    response = JSON.parse(last_response.body)
    frames = response.fetch('frames')
    assert_equal ['b1'], frames.pluck('id')
    cursor = response.fetch('cursor')
    refute response.fetch('more')

    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'over http')
    post '/pull', JSON.generate(@client.headers.merge('shard' => 'user', 'cursor' => cursor)), http_headers
    assert_predicate last_response, :ok?
    frames = JSON.parse(last_response.body).fetch('frames')
    assert_equal ['i1'], frames.pluck('id')
    assert_equal 'over http', frames.first.fetch('data').fetch('body')
  end

  test 'POST /push applies ops and answers verdicts' do
    ops = [{ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
             data: { boardId: 'b1', rank: 'a', body: 'pushed' } }]
    post '/push', JSON.generate(push_body(*ops)), { 'HTTP_X_USER_ID' => 'u1', 'CONTENT_TYPE' => 'application/json' }

    assert_predicate last_response, :ok?
    assert_equal [{ 'id' => DomainClient.uuid('op1'), 'outcome' => 'accepted' }], JSON.parse(last_response.body).fetch('verdicts')
    assert_equal 'pushed', Item.find('i1').body
  end

  test 'the replica endpoint accepts the compressed requests its clients send' do
    op = { id: 'gzip', op: 'row.create', stream: 'items', row_id: 'gzip-row', type: 'TextItem',
           data: { boardId: 'b1', rank: 'z', body: 'compressed' } }
    post '/push', Zlib.gzip(JSON.generate(push_body(op))),
         { 'HTTP_X_USER_ID' => 'u1', 'CONTENT_TYPE' => 'application/json', 'HTTP_CONTENT_ENCODING' => 'gzip' }
    assert_equal 200, last_response.status
    assert_equal 'compressed', Item.find('gzip-row').body
  end

  test 'malformed operation envelopes refuse the entire request before applying any mutation' do
    valid = { id: 'good', op: 'row.create', stream: 'items', row_id: 'valid', type: 'TextItem',
              data: { boardId: 'b1', rank: 'z', body: 'valid' } }
    invalid = [nil, 7, [], {}, valid.merge(id: ''), valid.merge(data: []), valid.merge(row_id: nil)]
    invalid.each do |bad|
      post '/push', JSON.generate(push_body(valid, bad)), { 'HTTP_X_USER_ID' => 'u1', 'CONTENT_TYPE' => 'application/json' }
      assert_equal 400, last_response.status
      refute Item.exists?('valid')
    end
    post '/push', JSON.generate(push_body(valid, valid)), { 'HTTP_X_USER_ID' => 'u1', 'CONTENT_TYPE' => 'application/json' }
    assert_equal 400, last_response.status, 'duplicate IDs make client verdict matching ambiguous'
    refute Item.exists?('valid')
  end

  test 'no user, no door — 401 on both endpoints' do
    post '/pull', '{}'
    assert_equal 401, last_response.status

    post '/push', JSON.generate(ops: []), { 'CONTENT_TYPE' => 'application/json' }
    assert_equal 401, last_response.status

    post '/pull', '{}', { 'HTTP_X_USER_ID' => 'ghost' }
    assert_equal 401, last_response.status, 'an unknown user id authenticates nobody'
  end

  test 'push scope auth: writing into another user replica is a rejected verdict, not transport' do
    doc = Loro::Doc.new(peer_id: 11)
    doc.get_map('meta').set('name', 'hijack')
    ops = [{ id: 'op1', op: 'doc.delta', stream: 'boards', row_id: 'b9', codec: 'loro@1',
             payload: Base64.strict_encode64(doc.export_updates) }]
    post '/push', JSON.generate(push_body(*ops)), { 'HTTP_X_USER_ID' => 'u1', 'CONTENT_TYPE' => 'application/json' }

    assert_predicate last_response, :ok?, 'a verdict rides HTTP 200 — the client parks it, never retries it'
    assert_equal [{ 'id' => DomainClient.uuid('op1'), 'outcome' => 'rejected', 'reason' => 'row is outside your replica' }],
                 JSON.parse(last_response.body).fetch('verdicts')
  end

  test 'GET /manifest serves the schema artifact without auth, and can be switched off' do
    get '/manifest'

    assert_predicate last_response, :ok?
    assert_equal JSON.parse(ReplicaMan::Manifest.new(DummyReplica).to_json),
                 JSON.parse(last_response.body)

    begin
      DummyReplica.serve_manifest = false
      get '/manifest'
      assert_equal 404, last_response.status, 'production turns introspection off'
    ensure
      DummyReplica.serve_manifest = true
    end
  end

  test 'malformed and oversized envelopes fail before processing an operation' do
    post '/pull', JSON.generate(@client.headers.merge('shard' => 'user', 'cursor' => [])), http_headers
    assert_equal 400, last_response.status

    body = @client.headers.merge('ops' => Array.new(ReplicaMan::Protocol::MAX_OPERATIONS + 1, {}))
    post '/push', JSON.generate(body), http_headers
    assert_equal 400, last_response.status

    post '/push', 'not json', http_headers
    assert_equal 400, last_response.status

    post '/push', JSON.generate(@client.headers.merge('ops' => 'nope')), http_headers
    assert_equal 400, last_response.status
    assert_equal 0, ReplicaMan::Operation.count
  end

  test 'anything else is 404' do
    get '/nope', {}, { 'HTTP_X_USER_ID' => 'u1' }
    assert_equal 404, last_response.status
  end

  test 'a deadlocked push answers 503 with Retry-After and applies nothing' do
    Items::TextItem.create!(id: 'first', board_id: 'b1', rank: 'a', body: 'one')
    Items::TextItem.create!(id: 'second', board_id: 'b1', rank: 'b', body: 'two')
    locked = Queue.new
    rival = Thread.new do
      Item.transaction do
        Item.lock.find('first')
        locked << true
        sleep 0.01 until Item.connection.select_value(
          "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'"
        ).positive?
        Item.lock.find('second')
      end
    end
    locked.pop

    ops = [{ id: 'second', op: 'row.patch', stream: 'items', row_id: 'second', data: { body: 'changed' } },
           { id: 'first', op: 'row.patch', stream: 'items', row_id: 'first', data: { body: 'changed' } }]
    post '/push', JSON.generate(push_body(*ops)), http_headers
    rival.join

    assert_equal 503, last_response.status
    assert_equal '1', last_response.headers['retry-after']
    assert_equal 'Contention', JSON.parse(last_response.body).fetch('error')
    assert_equal %w[one two], Item.order(:id).map(&:body)
    assert_equal 0, ReplicaMan::Operation.count
  end

  private

  def http_headers
    { 'HTTP_X_USER_ID' => @user.id, 'CONTENT_TYPE' => 'application/json' }
  end

  def push_body(*operations)
    prepared = operations.map do |op|
      next op unless op.is_a?(Hash)

      snapshot = ReplicaMan::Snapshot.find_by(stream: op.fetch(:stream, nil), row_id: op.fetch(:row_id, nil))
      incarnation = op.fetch(:op, nil) == 'row.create' ? SecureRandom.uuid : snapshot&.incarnation || SecureRandom.uuid
      id = op.fetch(:id, nil)
      op.merge(incarnation: incarnation, id: id.is_a?(String) && !id.empty? ? DomainClient.uuid(id) : id)
    end
    @client.headers.merge('ops' => prepared)
  end
end
