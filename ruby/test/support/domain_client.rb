require_relative 'protocol_client'

# Domain-flow tests use actual protocol pushes. Readable test ids map to stable
# UUIDs, so a retried test operation is the same operation on the wire.
class DomainClient
  NAMESPACE = 'b7a6f1c2-5d3e-4f60-9a8b-0c1d2e3f4a5b'.freeze

  def initialize(replica, user)
    @client = ProtocolClient.new(user, replica: replica)
    @replica = replica
    @operations = {}
    @identities = {}
  end

  def push(operations, origin: nil)
    prepared = operations.map { prepare(it.deep_stringify_keys) }
    names = prepared.to_h { [it.fetch('id'), it.fetch('name')] }
    response = @client.push(*prepared.map { it.except('name') }, origin: origin)

    verdicts = response.fetch(:verdicts).map do |verdict|
      verdict.deep_symbolize_keys.merge(id: names.fetch(verdict.fetch(:id)))
    end
    { verdicts: verdicts }
  end

  def self.uuid(name)
    Digest::UUID.uuid_v5(NAMESPACE, name)
  end

  private

  def prepare(operation)
    name = operation.fetch('id')
    address = [operation.fetch('stream'), operation.fetch('row_id')]
    saved = @operations.fetch(name, nil)
    identity = saved&.fetch(:incarnation) || operation.fetch('incarnation', nil)
    identity ||= if operation.fetch('op') == 'row.create'
      SecureRandom.uuid
    else
      @identities.fetch(address) do
        ReplicaMan::Snapshot.find_by(namespace: @replica.namespace,
          stream: address.first, row_id: address.last)&.incarnation || SecureRandom.uuid
      end
    end
    @identities[address] = identity
    predecessor = saved ? saved.fetch(:replaces) : predecessor_for(operation, address)
    @operations[name] = { incarnation: identity, replaces: predecessor }
    operation = operation.merge('id' => self.class.uuid(name), 'name' => name, 'incarnation' => identity)
    operation['group'] = self.class.uuid("group:#{operation.fetch('group')}") if operation.key?('group')
    operation['replaces'] = predecessor if predecessor
    operation
  end

  def predecessor_for(operation, address)
    return operation.fetch('replaces') if operation.key?('replaces')
    return unless operation.fetch('op') == 'row.create'

    snapshot = ReplicaMan::Snapshot.find_by(namespace: @replica.namespace, stream: address.first, row_id: address.last)
    snapshot.incarnation if snapshot&.deleted_at
  end
end
