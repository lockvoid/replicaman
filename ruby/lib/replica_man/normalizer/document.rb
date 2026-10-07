module ReplicaMan
  class Normalizer
    class Document < Normalizer
      COMPACT_EVERY = 64

      def compact_every
        COMPACT_EVERY
      end

      def project(doc, op: nil)
        {}
      end

      def reflect(stream, doc)
        stream.reflected_paths.to_h { |name, path| [name.to_sym, dig(doc, path)] }
      end

      def create(replica, stream, op)
        codec = codec!(replica, op)

        existing = ReplicaMan::Snapshot.find_by(namespace: stream.replica.namespace, stream: stream.stream_name, row_id: op.row_id)
        if existing && existing.deleted_at.nil? && stream.model.exists?(stream.key => op.row_id)
          stream.member!(op.user, existing)
          raise Refused, "row already exists: #{op.row_id}"
        end

        refuse!(op)
        doc = seed(replica, codec.load(op.seed), op)
        materialize(replica, stream, op.row_id, doc, reflect(stream, doc).merge(project(doc, op: op)))
      end

      def seed(replica, doc, op)
        doc
      end

      def delta(replica, stream, op)
        codec = codec!(replica, op)

        row = lock_fold(stream, op.row_id)
        raise Refused, "unknown row: #{op.row_id}" if row.nil? || row.deleted_at || row.document.nil?

        stream.member!(op.user, row)
        refuse!(op)
        return if ReplicaMan::Delta.exists?(namespace: stream.replica.namespace, stream: stream.stream_name, row_id: op.row_id, payload: op.payload)

        doc = codec.load(row.document)
        codec.merge(doc, op.payload)
        append_delta(replica, stream, op.row_id, op.payload)
        repair(replica, stream, doc, op.row_id)
        refold(replica, stream, op.row_id, doc)
        reproject(stream, op.row_id, doc)
        auto_compact(replica, stream, op.row_id, doc)
      end

      def patch(replica, stream, op)
        raise Refused, "stream #{stream.stream_name} is a document stream — push deltas, not row ops"
      end

      def delete(replica, stream, op)
        row = lock_fold(stream, op.row_id)
        raise Refused, "unknown row: #{op.row_id}" if row.nil?
        return if row.deleted_at

        stream.member!(op.user, row)
        refuse!(op)
        record = stream.locate(op.row_id)
        record ? destroy_row(stream, record) : Capture.record_deletion(stream, op.row_id)
      end

      def destroy_row(stream, record)
        record.destroy!
      end

      def materialize(replica, stream, row_id, doc, attributes)
        ActiveRecord::Base.transaction do
          EntityFence.lock(stream, row_id)
          write_fold(replica, stream, row_id, doc)
          stream.model.create!(stream.key => row_id, **attributes)
        end
      end

      def write_fold(replica, stream, row_id, doc)
        connection.execute(<<~SQL)
          INSERT INTO replica_man_snapshots (namespace, stream, row_id, codec, document)
          VALUES (#{quote(replica.namespace)}, #{quote(stream.stream_name)}, #{quote(row_id)}, #{quote(replica.codec.name)}, #{bytea(replica.codec.fold(doc))})
          ON CONFLICT (namespace, stream, row_id) DO UPDATE
          SET codec = EXCLUDED.codec, document = EXCLUDED.document, document_position = NULL, deleted_at = NULL, updated_at = now()
        SQL
      end

      def append_delta(replica, stream, row_id, payload)
        seq = ReplicaMan::Delta.where(namespace: stream.replica.namespace, stream: stream.stream_name, row_id: row_id).maximum(:seq).to_i + 1

        connection.execute(<<~SQL)
          INSERT INTO replica_man_deltas (namespace, stream, row_id, seq, payload)
          VALUES (#{quote(replica.namespace)}, #{quote(stream.stream_name)}, #{quote(row_id)}, #{seq}, #{bytea(payload)})
        SQL
      end

      def refold(replica, stream, row_id, doc)
        connection.execute(<<~SQL)
          UPDATE replica_man_snapshots
          SET document = #{bytea(replica.codec.fold(doc))}, updated_at = now()
          WHERE namespace = #{quote(replica.namespace)} AND stream = #{quote(stream.stream_name)} AND row_id = #{quote(row_id)}
        SQL
      end

      def lock_fold(stream, row_id)
        EntityFence.lock(stream, row_id)
        ReplicaMan::Snapshot.lock.find_by(namespace: stream.replica.namespace, stream: stream.stream_name, row_id: row_id)
      end

      def reproject(stream, row_id, doc)
        stream.locate(row_id).update!(**reflect(stream, doc), **project(doc))
      end

      # Compaction is a captured change: the fold's new axis is the position the capture claims.
      def compact(replica, stream, row_id)
        replica.transaction do
          row = lock_fold(stream, row_id) || raise(ActiveRecord::RecordNotFound, "no fold for #{stream.stream_name}/#{row_id}")
          next if row.deleted_at

          fold_tail(replica, stream, row_id, replica.codec.load(row.document))
          record = stream.locate(row_id) || raise(ActiveRecord::RecordNotFound, "no row for #{stream.stream_name}/#{row_id}")
          Capture.record(stream, record)
        end
      end

      def auto_compact(replica, stream, row_id, doc)
        return if ReplicaMan::Delta.where(namespace: stream.replica.namespace, stream: stream.stream_name, row_id: row_id).count < compact_every

        fold_tail(replica, stream, row_id, doc)
      end

      private

      # The folded history is reachable only through the fold: the axis moves to
      # the position this transaction claims at commit, so every cursor behind
      # the fold is handed the document.
      def fold_tail(replica, stream, row_id, doc)
        folded = ReplicaMan::Delta.where(namespace: stream.replica.namespace, stream: stream.stream_name, row_id: row_id).delete_all
        folded_bytes = replica.codec.fold(doc)
        connection.execute(<<~SQL)
          UPDATE replica_man_snapshots
          SET document = #{bytea(folded_bytes)}, document_position = #{Capture::UNCLAIMED}, updated_at = now()
          WHERE namespace = #{quote(replica.namespace)} AND stream = #{quote(stream.stream_name)} AND row_id = #{quote(row_id)}
        SQL
        Frames.validate_snapshot!(replica, stream.stream_name, row_id)
        Rails.logger.info("[replica_man] compact stream=#{stream.stream_name} row=#{row_id} folded=#{folded} fold_bytes=#{folded_bytes.bytesize}")
      end

      def repair(replica, stream, doc, row_id)
        before = replica.codec.version(doc)
        normalize(doc)
        return if replica.codec.version(doc) == before

        append_delta(replica, stream, row_id, replica.codec.diff(doc, since: before))
      end

      def dig(doc, path)
        *maps, key = path
        maps.drop(1).reduce(doc.get_map(maps.first)) { |map, name| map&.get_map(name) }&.get(key)
      end

      def codec!(replica, op)
        raise Refused, "unknown codec: #{op.codec}" unless op.codec == replica.codec.name

        replica.codec
      end
    end
  end
end
