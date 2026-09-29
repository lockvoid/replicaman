require 'test_helper'

class RebirthTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'owner', name: 'Owner')
    create_board!(id: 'parent', user: @user, name: 'Parent')
    @client = ProtocolClient.new(@user)
  end

  def birth(incarnation, replaces: nil)
    operation = {
      'id' => SecureRandom.uuid, 'op' => 'row.create', 'stream' => 'items',
      'row_id' => 'job', 'incarnation' => incarnation, 'type' => 'TextItem',
      'data' => { 'boardId' => 'parent', 'rank' => 'a', 'body' => 'saved' }
    }
    operation['replaces'] = replaces if replaces
    operation
  end

  def submit(operation)
    response = @client.push(operation)
    [operation, JSON.parse(JSON.generate(response.fetch(:verdicts).first))]
  end

  def delete_and_collect
    Item.find('job').destroy!
    DummyReplica.gc(window: 0.seconds)
    tombstone = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'job')
    assert tombstone.deleted_at
    assert_empty tombstone.data
    tombstone.incarnation
  end

  test 'an unprocessed old birth cannot resurrect a compacted tombstone' do
    delayed = birth('offline-before-deletion')
    assert_equal 'accepted', submit(birth('first')).last.fetch('outcome')
    assert_equal 'first', delete_and_collect

    assert_equal 'rejected', submit(delayed).last.fetch('outcome')
    assert_nil Item.find_by(id: 'job')
    assert_equal 'first', @client.incarnation('items', 'job')
  end

  test 'recreation names the latest deleted lifetime and a replay never recreates again' do
    assert_equal 'accepted', submit(birth('first')).last.fetch('outcome')
    delete_and_collect
    delayed = birth('stale-replacement', replaces: 'first')

    request, verdict = submit(birth('second', replaces: 'first'))
    assert_equal 'accepted', verdict.fetch('outcome')
    assert_equal 'second', delete_and_collect

    replay = @client.push(request).fetch(:verdicts)
    assert_equal [verdict], JSON.parse(JSON.generate(replay))
    assert_nil Item.find_by(id: 'job')
    assert_equal 'rejected', submit(delayed).last.fetch('outcome')
    assert_equal 'accepted', submit(birth('third', replaces: 'second')).last.fetch('outcome')
    assert_equal 'third', @client.incarnation('items', 'job')
  end

  test 'a replacement cannot invent its predecessor or reuse its deleted identity' do
    assert_equal 'rejected', submit(birth('new', replaces: 'unknown')).last.fetch('outcome')
    assert_equal 'accepted', submit(birth('first')).last.fetch('outcome')
    delete_and_collect
    assert_equal 'rejected', submit(birth('first', replaces: 'first')).last.fetch('outcome')
    assert_nil Item.find_by(id: 'job')
  end
end
