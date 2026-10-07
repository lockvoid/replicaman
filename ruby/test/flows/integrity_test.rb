require 'test_helper'
require_relative '../support/protocol_client'

class IntegrityTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'integrity-owner', name: 'Owner')
    @other = User.create!(id: 'integrity-other', name: 'Other')
    @client = ProtocolClient.new(@user)
  end

  def verify(cursor, client: @client, user: @user, shard: 'user')
    body = client.headers.merge('cursor' => cursor, 'shard' => shard)
    ReplicaMan::Integrity.call(DummyReplica, user: user, body: body)
  end

  test 'frozen vectors pin the digest of an ordered view' do
    path = File.expand_path('../../../protocol/fixtures/integrity.json', __dir__)
    fixture = JSON.parse(File.read(path))
    rows = fixture.fetch('rows').reverse.sort_by { [it.fetch('stream').b, it.fetch('id').b] }

    summary = ReplicaMan::Integrity.digest(rows.map { it.values_at('stream', 'id', 'incarnation', 'revision') })

    assert_equal({ digest: fixture.fetch('view_digest'), count: '3' }, summary)
  end

  test 'verification answers for the current head only' do
    job = Job.create!(id: 'verify-job', user: @user, state: 'queued')
    cursor = @client.pull.fetch(:cursor)
    initial = verify(cursor)
    assert_equal '1', initial.fetch(:count)
    assert_equal cursor, initial.fetch(:cursor)

    job.update!(state: 'running')
    error = assert_raises(ReplicaMan::Protocol::Error) { verify(cursor) }
    assert_equal 'CursorBehind', error.code

    newer = @client.pull(cursor: cursor).fetch(:cursor)
    refute_equal initial.fetch(:digest), verify(newer).fetch(:digest)
  end

  test 'a compaction keeps a caught-up client verifiable' do
    DummyReplica.document(:boards, 'b1').create(user_id: @user.id) { it.get_map('meta').set('name', 'Plans') }
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'Edited') }
    cursor = @client.pull.fetch(:cursor)

    DummyReplica.document(:boards, 'b1').compact

    error = assert_raises(ReplicaMan::Protocol::Error) { verify(cursor) }
    assert_equal 'CursorBehind', error.code, 'the compaction moved the head: the client pulls first'
    response = @client.pull(cursor: cursor)
    assert_equal ['doc.snapshot'], response.fetch(:frames).map { it.fetch(:frame) }
    frame = response.fetch(:frames).first
    expected = ReplicaMan::Integrity.digest([['boards', 'b1', frame.fetch(:incarnation), frame.fetch(:revision)]])
    assert_equal expected.fetch(:digest), verify(response.fetch(:cursor)).fetch(:digest)
  end

  test 'verification refuses a foreign cursor and an unknown shard' do
    Job.create!(id: 'private-job', user: @user, state: 'queued')
    cursor = @client.pull.fetch(:cursor)

    error = assert_raises(ReplicaMan::Protocol::Error) { verify(cursor, client: ProtocolClient.new(@other), user: @other) }
    assert_equal 'CursorInvalid', error.code
    assert_raises(ReplicaMan::InvalidRequest) { verify(cursor, shard: 'catalog') }
  end

  test 'the server orders live members by their UTF-8 bytes' do
    %w[z a é/🙂].each { Job.create!(id: it, user: @user, state: 'queued') }
    buckets = DummyReplica.buckets(@user, 'user')
    live = ReplicaMan::Snapshot.where(bucket: buckets, deleted_at: nil).to_a
    expected = live.sort_by { [it.stream.b, it.row_id.b] }.map { [it.stream, it.row_id, it.incarnation, it.revision.to_s] }

    summary = ActiveRecord::Base.transaction { ReplicaMan::Integrity.summarize(DummyReplica, buckets) }
    assert_equal ReplicaMan::Integrity.digest(expected), summary
  end

  test "authenticated HTTP verification counts only the caller's members" do
    Job.create!(id: 'mine', user: @user, state: 'queued')
    Job.create!(id: 'theirs', user: @other, state: 'queued')
    cursor = @client.pull.fetch(:cursor)
    body = @client.headers.merge('cursor' => cursor, 'shard' => 'user')
    response = Rack::MockRequest.new(DummyReplica).post('/verify', 'HTTP_X_USER_ID' => @user.id,
      'CONTENT_TYPE' => 'application/json', input: JSON.generate(body))
    assert_equal 200, response.status
    parsed = JSON.parse(response.body)
    assert_equal cursor, parsed.fetch('cursor')
    assert_equal '1', parsed.fetch('count')
  end
end
