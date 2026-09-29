require 'test_helper'
require 'rack/mock'

class RequestAuthorizationTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'One')
    @client = ProtocolClient.new(@user)
    @prior = DummyReplica.authorize
    @request = Rack::MockRequest.new(DummyReplica)
  end

  teardown { DummyReplica.instance_variable_set(:@authorizer, @prior) }

  test 'pull revalidation runs after the repeatable-read transaction is pinned' do
    observed = nil
    DummyReplica.authorize do |request:, user:, operation:|
      connection = ActiveRecord::Base.connection
      observed = [operation, request.path_info, connection.transaction_open?, connection.select_value('SHOW transaction_isolation')]
      user
    end
    result = @request.post('/pull', 'HTTP_X_USER_ID' => @user.id, input: JSON.generate(@client.headers.merge('shard' => 'user')))
    assert_equal 200, result.status
    assert_equal [:pull, '/pull', true, 'repeatable read'], observed
  end

  test 'revocation at push admission leaves neither a row nor a durable result' do
    DummyReplica.authorize do |operation:, **|
      assert_equal :push, operation
      assert ActiveRecord::Base.connection.transaction_open?
      raise ReplicaMan::Unauthorized
    end
    op = { id: DomainClient.uuid('blocked'), op: 'row.create', stream: 'boards', row_id: 'b1', incarnation: SecureRandom.uuid, data: {} }
    result = @request.post('/push', 'HTTP_X_USER_ID' => @user.id, input: JSON.generate(@client.headers.merge('ops' => [op])))
    assert_equal 401, result.status
    assert_empty ReplicaMan::Operation.all
    refute Board.exists?('b1')
  end

  test 'authorization cannot silently replace the authenticated principal' do
    other = User.create!(id: 'u2', name: 'Two')
    DummyReplica.authorize { |**| other }
    assert_equal 401, @request.post('/pull', 'HTTP_X_USER_ID' => @user.id, input: JSON.generate(@client.headers.merge('shard' => 'user'))).status
  end

  test 'malformed pull fields are client errors' do
    [{ limit: 'wat' }, { limit: 0 }, { cursor: [] }, { shard: [] }, { shard: 'missing' }].each do |fields|
      body = @client.headers.merge('shard' => 'user').merge(fields.stringify_keys)
      result = @request.post('/pull', 'HTTP_X_USER_ID' => @user.id, input: JSON.generate(body))
      assert_equal 400, result.status, fields.inspect
    end
  end
end
