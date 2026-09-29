require 'json'

module ReplicaMan
  class Manifest
    class Stale < StandardError; end

    def initialize(replica)
      @replica = replica
    end

    def to_h
      eager_load!
      refuse_stale_schema!

      { version: 1, namespace: @replica.namespace, schemaVersion: @replica.schema_version, streams: streams }
    end

    def to_json(*)
      JSON.pretty_generate(to_h) << "\n"
    end

    def write(path)
      File.write(path, to_json)
    end

    private

    def eager_load!
      Rails.application.eager_load! if defined?(Rails.application) && Rails.application
    end

    def refuse_stale_schema!
      return if @replica.streams.values.all?(&:introspectable?)

      raise Stale, 'manifest refused: the database is behind its migrations or unreachable — migrate, then regenerate'
    end

    def streams
      @replica.streams.values.sort_by(&:stream_name).map { entry(it) }
    end

    def entry(stream)
      { name: stream.stream_name, lane: stream.lane }.tap do |entry|
        entry[:codec] = codec_name if stream.document?
        entry[:readonly] = stream.readonly
        entry[:shard] = stream.shard
        entry[:sti] = stream.sti
        entry[:columns] = stream.wire_columns
        entry[:references] = stream.references if stream.references.any?
        entry[:lifetimeFrom] = stream.lifetime_from if stream.lifetime_from
        entry[:indexes] = stream.indexes if stream.indexes.any?
        entry[:variants] = stream.variants if stream.sti
        if stream.document_schema?
          entry[:shapes] = stream.document_shapes
          entry[:default] = stream.document_default
        end
      end
    end

    def codec_name
      @replica.codec.name
    end
  end
end
