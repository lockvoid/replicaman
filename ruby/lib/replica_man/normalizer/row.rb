module ReplicaMan
  class Normalizer
    class Row < Normalizer
      def create(replica, stream, op)
        existing = stream.locate(op.row_id, lock: true)
        return collision!(stream, op, existing) if existing

        refuse!(op)
        write(stream, op, build(stream, op))
      end

      def patch(replica, stream, op)
        record = stream.locate(op.row_id, lock: true) || raise(Refused, "unknown row: #{op.row_id}")
        stream.member!(op.user, record)

        refuse!(op)
        write(stream, op, record)
      end

      def delete(replica, stream, op)
        record = stream.locate(op.row_id, lock: true)
        return if record.nil?

        stream.member!(op.user, record)
        refuse!(op)
        destroy_row(stream, record)
      end

      def destroy_row(stream, record)
        record.destroy!
      end

      private

      def collision!(stream, op, existing)
        stream.member!(op.user, existing)
        raise Refused, "row already exists: #{op.row_id}"
      end

      def build(stream, op)
        klass = stream.sti ? stream.variant_class(op.type) || raise(Refused, "unknown type: #{op.type}") : stream.model
        klass.new(stream.key => op.row_id)
      end

      def write(stream, op, record)
        attributes = stream.decode(record.class, op.data)
        drain_intake(stream, op, record, attributes)
        record.assign_attributes(normalize(attributes.except(*stream.intake_names)))
        stream.member!(op.user, record)
        record.save!
      end

      def drain_intake(stream, op, record, attributes)
        stream.intake_specs.each do |spec|
          name = spec.fetch(:name)
          spec.fetch(:intake).call(record, attributes.fetch(name), op) if attributes.key?(name)
        end
      end
    end
  end
end
