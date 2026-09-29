module ReplicaMan
  class DocumentHandle
    def initialize(replica, stream_name, row_id)
      @replica = replica
      @stream = replica.streams.fetch(stream_name.to_sym) { raise ArgumentError, "unknown stream: #{stream_name}" }
      raise ArgumentError, "#{stream_name} is not a document stream" unless @stream.document?

      @row_id = row_id.to_s
    end

    def create(**attributes)
      doc = codec.blank
      yield doc if block_given?
      doc.commit

      normalizer.materialize(@replica, @stream, @row_id, doc, { **normalizer.reflect(@stream, doc), **normalizer.project(doc), **attributes })
      doc
    end

    def edit
      ActiveRecord::Base.transaction do
        row = normalizer.lock_fold(@stream, @row_id) ||
              raise(ActiveRecord::RecordNotFound, "no fold for #{@stream.stream_name}/#{@row_id}")
        doc = codec.load(row.document)
        before = codec.version(doc)
        yield doc
        doc.commit
        break doc if codec.version(doc) == before

        normalizer.append_delta(@replica, @stream, @row_id, codec.diff(doc, since: before))
        normalizer.refold(@replica, @stream, @row_id, doc)
        normalizer.reproject(@stream, @row_id, doc)
        normalizer.auto_compact(@replica, @stream, @row_id, doc)
        doc
      end
    end

    def compact
      normalizer.compact(@replica, @stream, @row_id)
    end

    private

    def codec
      @replica.codec
    end

    def normalizer
      @stream.normalizer
    end
  end
end
