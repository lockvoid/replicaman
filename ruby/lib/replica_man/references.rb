require 'digest'

module ReplicaMan
  module References
    module_function

    def addresses(stream, row_id, data)
      stream.references.filter_map do |spec|
        prefix = spec.fetch(:keyPrefix, nil)
        next if prefix && !row_id.start_with?(prefix)

        id = if spec.key?(:keySegment)
          row_id.split('/', -1).fetch(spec.fetch(:keySegment), nil)
        else
          data.fetch(spec.fetch(:field), nil)
        end
        next if id.nil? && spec.fetch(:optional)
        raise Refused, "missing reference: #{spec.fetch(:name)}" unless id.is_a?(String) && !id.empty?

        { 'name' => spec.fetch(:name), 'stream' => spec.fetch(:stream), 'id' => id }
      end
    end

    def verify!(stream, op)
      snapshot = Snapshot.find_by(namespace: stream.replica.namespace, stream: stream.stream_name, row_id: op.row_id)
      data = (snapshot&.data || {}).merge(op.data)
      expected = addresses(stream, op.row_id, data)
      named = op.references.select { it.key?('name') }
      unless named.map { it.fetch('name') }.sort == expected.map { it.fetch('name') }.sort
        raise Refused, 'operation must carry exactly its declared references'
      end

      expected.each do |address|
        actual = named.find { it.fetch('name') == address.fetch('name') }
        unless actual.slice('name', 'stream', 'id') == address
          raise Refused, "reference address changed: #{address.fetch('name')}"
        end
      end

      op.references.sort_by { [it.fetch('stream'), it.fetch('id')] }.each do |reference|
        parent_stream = stream.replica.streams.fetch(reference.fetch('stream').to_sym) do
          raise Refused, 'unknown reference stream'
        end
        EntityFence.lock(parent_stream, reference.fetch('id'))
        parent = Snapshot.find_by(namespace: stream.replica.namespace,
                                  stream: parent_stream.stream_name, row_id: reference.fetch('id'))
        unless parent && parent.deleted_at.nil? && parent.incarnation == reference.fetch('incarnation')
          raise Refused, 'referenced entity incarnation is no longer current'
        end
        unless stream.replica.buckets(op.user, parent_stream.shard).include?(parent.bucket)
          raise Refused, 'row is outside your replica'
        end
      end
    end

    def derived_incarnation(stream, row_id, references)
      name = stream.lifetime_from
      return unless name

      parent = references.find { it.fetch('name', nil) == name }
      return unless parent # A key prefix can restrict the declaration to one row kind.

      parts = ['replicaman:derived:1', stream.replica.namespace, stream.stream_name,
               row_id, parent.fetch('stream'), parent.fetch('id'), parent.fetch('incarnation')]
      'derived:' + Digest::SHA256.hexdigest(parts.map { "#{it.bytesize}:#{it}" }.join)
    end

    def captured_incarnation(stream, row_id, data)
      return unless stream.lifetime_from

      references = addresses(stream, row_id, data).map do |address|
        parent_stream = stream.replica.streams.fetch(address.fetch('stream').to_sym)
        EntityFence.lock(parent_stream, address.fetch('id'))
        parent = Snapshot.find_by!(namespace: stream.replica.namespace,
                                   stream: address.fetch('stream'), row_id: address.fetch('id'))
        raise Refused, 'cannot capture a child of a deleted entity' if parent.deleted_at

        address.merge('incarnation' => parent.incarnation)
      end
      derived_incarnation(stream, row_id, references)
    end

    # Parents receive their lifetime before dependent births in the same commit.
    # Sorting the roots also keeps lock acquisition deterministic.
    def capture_order(entries)
      pending = entries.to_h { |entry| [address_key(entry), entry] }
      ordered = []
      visiting = Set.new
      visit = lambda do |key|
        entry = pending.fetch(key, nil)
        return unless entry
        raise Stream::Invalid, 'cyclic derived entity lifetimes' unless visiting.add?(key)

        stream = entry.fetch(:stream_ref)
        if stream.lifetime_from && !entry.fetch(:deleted)
          addresses(stream, entry.fetch(:row_id), entry.fetch(:data)).each do |parent|
            visit.call([stream.replica.namespace, parent.fetch('stream'), parent.fetch('id')])
          end
        end
        visiting.delete(key)
        pending.delete(key)
        ordered << entry
      end
      pending.keys.sort.each { visit.call(it) }
      ordered
    end

    def address_key(entry)
      [entry.fetch(:replica).namespace, entry.fetch(:stream).to_s, entry.fetch(:row_id).to_s]
    end
  end
end
