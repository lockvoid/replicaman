# Builds real wire envelopes. It never applies domain mutations or computes
# expected outcomes; the server and PostgreSQL execute every tested history.
class ProtocolClient
  attr_reader :headers

  def initialize(user, replica: DummyReplica)
    @user = user
    @replica = replica
    @headers = ReplicaMan::Protocol.header(replica).stringify_keys
  end

  def push(*operations, origin: nil)
    @replica.push(user: @user, body: @headers.merge('ops' => operations), origin: origin)
  end

  def pull(cursor: nil, limit: 500, shard: 'user')
    body = @headers.merge('shard' => shard, 'limit' => limit)
    body['cursor'] = cursor if cursor

    @replica.pull(user: @user, body: body)
  end

  # Follows `more` like a native client and returns every frame of the round.
  def checkpoint(cursor: nil, limit: 500, shard: 'user')
    response = pull(cursor: cursor, limit: limit, shard: shard)
    reset = response.fetch(:reset)
    frames = []
    pages = 1

    loop do
      frames.concat(response.fetch(:frames).map { JSON.parse(JSON.generate(it)).symbolize_keys })
      break unless response.fetch(:more)

      pages += 1
      response = pull(cursor: response.fetch(:cursor), limit: limit, shard: shard)
    end

    { cursor: response.fetch(:cursor), frames: frames, reset: reset, pages: pages, more: false }
  end

  # One response's frames with string keys, as a client decodes the wire.
  def frames(response)
    JSON.parse(JSON.generate(response.fetch(:frames)))
  end

  def incarnation(stream, row_id)
    ReplicaMan::Snapshot.find_by!(namespace: @replica.namespace, stream: stream, row_id: row_id).incarnation
  end
end
