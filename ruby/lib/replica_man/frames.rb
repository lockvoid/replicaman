require 'base64'

module ReplicaMan
  module Frames
    # Enforce this in the domain transaction. Discovering an oversized entity
    # during pull is too late: every new client would be unable to bootstrap.
    def self.validate_snapshot!(replica, stream, row_id)
      return if size(replica, stream, row_id) <= Protocol::ENTITY_BYTES

      raise Refused, "entity exceeds the #{Protocol::ENTITY_BYTES}-byte synchronization limit"
    end

    # The baseline frame's byte size; the database measures the fold instead of loading it.
    def self.size(replica, stream, row_id)
      row = Snapshot.select(*(Snapshot.column_names - %w[document]), 'octet_length(document) AS document_bytes')
        .find_by!(namespace: replica.namespace, stream: stream, row_id: row_id)
      frame = frame(replica, row, '')
      JSON.generate(frame).bytesize + (frame.key?(:snapshot) ? 4 * ((row.document_bytes.to_i + 2) / 3) : 0)
    end

    def self.baseline(replica, row)
      frame(replica, row, Base64.strict_encode64(row.document.to_s))
    end

    def self.frame(replica, row, snapshot)
      frame = { stream: row.stream, id: row.row_id, incarnation: row.incarnation, revision: row.revision.to_s }
      return frame.merge(frame: 'row.delete') if row.deleted_at

      if replica.streams.fetch(row.stream.to_sym).document?
        frame.merge(frame: 'doc.snapshot', codec: row.codec, snapshot: snapshot, data: row.data)
      else
        frame.merge(frame: 'row.set', type: row.row_type, data: row.data)
      end
    end
  end
end
