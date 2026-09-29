require 'loro'
require_relative 'loro/writer'

module ReplicaMan
  module Loro
    CODEC = 'loro@1'.freeze

    SERVER_PEER = 1

    def self.install(replica, **)
      replica.codecs[CODEC] = Codec.new
    end

    class Codec
      def name
        CODEC
      end

      def blank
        ::Loro::Doc.new(peer_id: SERVER_PEER)
      end

      def load(blob)
        ::Loro::Doc.from_snapshot(blob, peer_id: SERVER_PEER)
      rescue ::Loro::Error => e
        raise Refused, e.message
      end

      def fold(doc)
        doc.export_snapshot
      end

      def version(doc)
        doc.version_vector
      end

      def diff(doc, since:)
        doc.export_updates(since: since)
      end

      def merge(doc, payload)
        raise Refused, 'delta depends on changes the server has not seen' if doc.import(payload).fetch(:pending)

        doc
      rescue ::Loro::Error => e
        raise Refused, e.message
      end
    end
  end
end
