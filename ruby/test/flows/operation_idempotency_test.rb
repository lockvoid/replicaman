require 'test_helper'
require_relative '../support/protocol_client'

class OperationIdempotencyTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'One')
    DummyReplica.document(:boards, 'b1').create(user_id: @user.id) do |doc|
      doc.get_map('meta').set('name', 'Board')
    end
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'Original')
    @client = ProtocolClient.new(@user)
  end

  def patch(name, body, incarnation: @client.incarnation('items', 'i1'), group: nil)
    operation = {
      'id' => DomainClient.uuid(name), 'op' => 'row.patch', 'stream' => 'items', 'row_id' => 'i1',
      'incarnation' => incarnation, 'data' => { 'body' => body }
    }
    operation['group'] = DomainClient.uuid(group) if group
    operation
  end

  test 'a retried operation returns its stored verdict without repeating the mutation' do
    first = patch('first', 'First')
    @client.push(first)
    @client.push(patch('later', 'Later'))
    retried = @client.push(first)

    assert_equal 'Later', Item.find('i1').body
    assert_equal [{ id: first.fetch('id'), outcome: 'accepted' }], retried.fetch(:verdicts)
    assert_equal 2, ReplicaMan::Operation.count
  end

  test 'a rejected group rolls back every member and keeps the refusal for retries' do
    group = [patch('valid', 'Temporary', group: 'g'), patch('invalid', 'No', incarnation: 'old-life', group: 'g')]
    response = @client.push(*group)

    assert_equal 'Original', Item.find('i1').body
    assert_equal %w[rejected rejected], response.fetch(:verdicts).map { it.fetch(:outcome) }
    assert_equal response, @client.push(*group)
  end

  test 'a group must be contiguous' do
    error = assert_raises(ReplicaMan::InvalidRequest) do
      @client.push(patch('a', 'A', group: 'g'), patch('b', 'B'), patch('c', 'C', group: 'g'))
    end

    assert_equal 'an operation group must be contiguous', error.message
    assert_equal 0, ReplicaMan::Operation.count
  end

  test 'old-incarnation edits cannot change a replacement at the same address' do
    old = patch('offline', 'Old offline edit')
    Item.find('i1').destroy!
    Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'Replacement')

    verdict = @client.push(old).fetch(:verdicts).first

    assert_equal 'rejected', verdict.fetch(:outcome)
    assert_equal 'Replacement', Item.find('i1').body
  end

  test 'a transient failure rolls back the claim, the mutation and its capture' do
    previous = DummyReplica.after_apply
    DummyReplica.after_apply { |**| raise IOError, 'domain temporarily unavailable' }
    operation = patch('retry', 'Retry succeeded')
    before = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').position

    assert_raises(IOError) { @client.push(operation) }

    assert_equal 'Original', Item.find('i1').body
    assert_equal 0, ReplicaMan::Operation.count
    assert_equal before, ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').position

    DummyReplica.instance_variable_set(:@after_apply, previous)
    @client.push(operation)
    assert_equal 'Retry succeeded', Item.find('i1').body
  ensure
    DummyReplica.instance_variable_set(:@after_apply, previous)
  end

  test 'a document birth missing codec or seed returns a durable refusal' do
    missing = [
      { 'id' => DomainClient.uuid('no-codec'), 'op' => 'row.create', 'stream' => 'boards', 'row_id' => 'missing-codec',
        'incarnation' => 'missing-codec-life', 'seed' => Base64.strict_encode64('invalid') },
      { 'id' => DomainClient.uuid('no-seed'), 'op' => 'row.create', 'stream' => 'boards', 'row_id' => 'missing-seed',
        'incarnation' => 'missing-seed-life', 'codec' => DummyReplica.codec.name }
    ]
    response = @client.push(*missing)

    assert_equal %w[rejected rejected], response.fetch(:verdicts).map { it.fetch(:outcome) }
    assert_nil Board.find_by(id: 'missing-codec')
    assert_nil Board.find_by(id: 'missing-seed')
    assert_equal response, @client.push(*missing)
  end

  test 'an operation id must be a UUID' do
    error = assert_raises(ReplicaMan::InvalidRequest) { @client.push(patch('x', 'X').merge('id' => 'not-a-uuid')) }

    assert_equal 'operation id must be a UUID', error.message
  end
end
