require 'test_helper'
require_relative '../support/protocol_client'

class BucketPullTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'owner', name: 'Owner')
    @other = User.create!(id: 'other', name: 'Other')
    @client = ProtocolClient.new(@user)
  end

  def frames(response)
    response.fetch(:frames).map { [it.fetch(:frame), it.fetch(:id)] }
  end

  test 'a row lives in the bucket it was born in: changing its owner is refused' do
    job = Job.create!(id: 'j1', user: @user, state: 'queued')

    error = assert_raises(ReplicaMan::Refused) { job.update!(user: @other) }

    assert_equal "a row's owner cannot change: jobs/j1", error.message
    assert_equal @user.id, Job.find('j1').user_id
    assert_empty ProtocolClient.new(@other).pull.fetch(:frames)
  end

  test 'a document born and edited between pulls arrives as one complete baseline' do
    initial = @client.pull
    DummyReplica.document(:boards, 'new').create(user_id: @user.id) { it.get_map('meta').set('name', 'Initial') }
    DummyReplica.document(:boards, 'new').edit { it.get_map('meta').set('name', 'Edited') }

    response = @client.pull(cursor: initial.fetch(:cursor))

    assert_equal [%w[doc.snapshot new]], frames(response)
    assert_equal 'Edited', response.fetch(:frames).first.fetch(:data).fetch('name')
  end

  test 'a document edited after the cursor arrives as its new deltas and fields' do
    DummyReplica.document(:boards, 'b1').create(user_id: @user.id) { it.get_map('meta').set('name', 'Initial') }
    initial = @client.pull
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'Edited') }

    response = @client.pull(cursor: initial.fetch(:cursor))

    assert_equal [%w[doc.delta b1], %w[row.set b1]], frames(response)
    assert_equal 'Edited', response.fetch(:frames).last.fetch(:data).fetch('name')
  end

  test 'the limit counts entities: a document and its deltas are one' do
    DummyReplica.document(:boards, 'b1').create(user_id: @user.id) { it.get_map('meta').set('name', 'Initial') }
    initial = @client.pull
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'Once') }
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'Twice') }
    Job.create!(id: 'j1', user: @user, state: 'queued')

    response = @client.pull(cursor: initial.fetch(:cursor), limit: 2)

    assert_equal [%w[doc.delta b1], %w[doc.delta b1], %w[row.set b1], %w[row.set j1]], frames(response)
    assert_equal false, response.fetch(:more)
  end

  test 'compacted history sends the baseline instead of deltas the client no longer can get' do
    DummyReplica.document(:boards, 'b1').create(user_id: @user.id) { it.get_map('meta').set('name', 'Initial') }
    initial = @client.pull
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('name', 'Edited') }
    DummyReplica.document(:boards, 'b1').compact

    assert_equal [%w[doc.snapshot b1]], frames(@client.pull(cursor: initial.fetch(:cursor)))
  end

  test 'an unchanged pull returns no frames and the same cursor' do
    Job.create!(id: 'a', user: @user, state: 'queued')
    initial = @client.pull
    response = @client.pull(cursor: initial.fetch(:cursor))

    assert_empty response.fetch(:frames)
    assert_equal initial.fetch(:cursor), response.fetch(:cursor)
    assert_equal false, response.fetch(:more)
  end

  test 'a deletion arrives as a delete and a bootstrap never sees the tombstone' do
    job = Job.create!(id: 'a', user: @user, state: 'queued')
    initial = @client.pull
    job.destroy!

    assert_equal [%w[row.delete a]], frames(@client.pull(cursor: initial.fetch(:cursor)))
    assert_empty ProtocolClient.new(@user).pull.fetch(:frames)
  end

  test 'a write that leaves the replicated row unchanged takes no revision and sends no frame' do
    job = Job.create!(id: 'a', user: @user, state: 'queued')
    initial = @client.pull
    before = ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'a').slice(:revision, :position)

    job.update!(state: 'queued')
    DummyReplica.transaction { Job.where(id: 'a').update_all(state: 'queued') }

    assert_equal before, ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'a').slice(:revision, :position)
    assert_empty @client.pull(cursor: initial.fetch(:cursor)).fetch(:frames)
  end

  test 'a malformed or foreign cursor is refused' do
    Job.create!(id: 'a', user: @user, state: 'queued')
    foreign = ProtocolClient.new(@other).pull.fetch(:cursor)

    ['not a cursor', foreign].each do |cursor|
      error = assert_raises(ReplicaMan::Protocol::Error) { @client.pull(cursor: cursor) }
      assert_equal 'CursorInvalid', error.code
    end
  end

  test 'a large entity arrives inline in its own page' do
    Job.create!(id: 'big', user: @user, state: 'queued', payload: { 'blob' => 'x' * ReplicaMan::Protocol::PAGE_BYTES })
    Job.create!(id: 'small', user: @user, state: 'queued')

    first = @client.pull
    second = @client.pull(cursor: first.fetch(:cursor))

    assert_equal [%w[row.set big]], frames(first)
    assert first.fetch(:more)
    assert_equal [%w[row.set small]], frames(second)
  end
end
