require 'test_helper'
require_relative '../support/protocol_client'

class CaptureEnforcementTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'owner', name: 'Owner')
    Job.create!(id: 'j1', user: @user, state: 'queued')
    @client = ProtocolClient.new(@user)
  end

  test 'callback-bypassing SQL cannot commit without capture' do
    assert_raises(ActiveRecord::StatementInvalid) do
      Job.where(id: 'j1').update_all(state: 'done')
    end

    assert_equal 'queued', Job.find('j1').state
  end

  test 'the host transaction captures raw updates in the same commit' do
    initial = @client.pull

    DummyReplica.transaction do
      Job.where(id: 'j1').update_all(state: 'done')
    end

    response = @client.pull(cursor: initial.fetch(:cursor))
    assert_equal 'done', @client.frames(response).first.fetch('data').fetch('state')
  end

  test 'delete and recreate in one transaction produces a distinct lifetime' do
    previous = @client.incarnation('jobs', 'j1')

    DummyReplica.transaction do
      Job.where(id: 'j1').delete_all
      Job.create!(id: 'j1', user: @user, state: 'running')
    end

    refute_equal previous, @client.incarnation('jobs', 'j1')
  end

  test 'a failed savepoint does not leak its changes into the outer commit' do
    DummyReplica.transaction do
      Job.where(id: 'j1').update_all(state: 'running')

      Job.transaction(requires_new: true) do
        Job.where(id: 'j1').update_all(state: 'done')
        raise ActiveRecord::Rollback
      end
    end

    assert_equal 'running', Job.find('j1').state
    snapshot = ReplicaMan::Snapshot.find_by!(namespace: DummyReplica.namespace, stream: 'jobs', row_id: 'j1')
    assert_equal 'running', snapshot.data.fetch('state')
  end
end
