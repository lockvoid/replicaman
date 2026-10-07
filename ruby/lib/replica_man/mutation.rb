module ReplicaMan
  class Mutation
    def initialize(replica, origin: nil)
      @replica = replica
      @origin = origin
    end

    def apply(op)
      stream = @replica.streams.fetch(op.stream_name.to_sym) do
        raise Refused, "unknown stream: #{op.stream_name}"
      end

      raise Refused, "stream #{stream.stream_name} is readonly" if stream.readonly
      unless %w[row.create row.patch row.delete doc.delta].include?(op.verb)
        raise Refused, "unknown op: #{op.verb}"
      end

      EntityFence.lock(stream, op.row_id)
      return if op.verb == 'row.delete' && ended?(stream, op)

      References.verify!(stream, op)
      verify_lifetime!(stream, op)
      verify_fields!(stream, op)

      Capture.with_op_origin(@origin, stream: stream.stream_name, row_id: op.row_id,
                             incarnation: op.verb == 'row.create' ? op.incarnation : nil, user: op.user) do
        aftermath = apply_to_stream(stream, op)

        if aftermath.respond_to?(:call)
          raise ArgumentError, 'Enqueue a job inside the mutation instead of returning a callback'
        end

        Capture.flush
      end
    end

    private

    # Deleting a lifetime that already ended asks for nothing: a delete is idempotent across devices.
    def ended?(stream, op)
      snapshot = snapshots(stream).find_by(row_id: op.row_id)
      snapshot.present? && snapshot.deleted_at.present? && snapshot.incarnation == op.incarnation
    end

    def verify_lifetime!(stream, op)
      snapshot = snapshots(stream).find_by(row_id: op.row_id)

      if op.verb == 'row.create'
        verify_birth!(stream, op, snapshot)
      elsif snapshot.nil? || snapshot.deleted_at || snapshot.incarnation != op.incarnation
        raise Refused, 'entity incarnation is no longer current'
      end

      return unless op.expected
      raise Refused, 'expected state requires an existing entity' unless snapshot && snapshot.deleted_at.nil?

      expected = op.expected
      if expected.key?('revision') && expected.fetch('revision') != snapshot.revision.to_s
        raise Refused, 'entity revision changed'
      end

      expected.fetch('fields', {}).each do |field, value|
        unless snapshot.data.key?(field) && snapshot.data.fetch(field) == value
          raise Refused, "expected field changed: #{field}"
        end
      end
    end

    def verify_birth!(stream, op, snapshot)
      derived = References.derived_incarnation(stream, op.row_id, op.references)
      raise Refused, 'incorrect derived entity incarnation' if derived && derived != op.incarnation

      unless snapshot
        raise Refused, 'replacement predecessor is unknown' if op.replaces

        return
      end

      unless snapshot.deleted_at
        raise Refused, 'entity already exists with another incarnation' if snapshot.incarnation != op.incarnation

        return
      end

      if derived
        raise Refused, 'derived entity was deleted in this parent lifetime' if snapshot.incarnation == derived
      elsif op.replaces != snapshot.incarnation || op.incarnation == snapshot.incarnation
        raise Refused, 'recreation requires the last deleted incarnation'
      end
    end

    def verify_fields!(stream, op)
      if op.verb == 'row.create' && stream.door_variant && op.type
        unless op.type == stream.wire_type(stream.door_variant)
          raise Refused, "#{op.type.inspect} rows are server-authored"
        end
      end

      return unless %w[row.create row.patch].include?(op.verb)

      missing = stream.preconditions - op.data.keys
      raise Refused, "#{op.verb} of #{stream.stream_name} must carry #{missing.join(', ')}" if missing.any?
    end

    def apply_to_stream(stream, op)
      case op.verb
      when 'row.create'
        create(stream, op)
      when 'row.patch'
        stream.normalizer.patch(@replica, stream, op)
      when 'row.delete'
        stream.normalizer.delete(@replica, stream, op)
      when 'doc.delta'
        stream.normalizer.delta(@replica, stream, op)
      else
        raise Refused, "unknown op: #{op.verb}"
      end
    end

    def create(stream, op)
      existing = stream.locate(op.row_id, lock: true)

      if existing
        stream.member!(op.user, existing)
        stream.normalizer.create_existing(@replica, stream, op, existing)
      else
        stream.normalizer.create(@replica, stream, op)
      end
    end

    def snapshots(stream)
      Snapshot.where(namespace: @replica.namespace, stream: stream.stream_name)
    end
  end
end
