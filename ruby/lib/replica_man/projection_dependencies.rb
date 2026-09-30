module ReplicaMan
  # A projection may read another model. Queue affected children for capture in the
  # parent's transaction, including bulk SQL writes that bypass Rails callbacks.
  module ProjectionDependencies
    class Dependency
      attr_reader :via, :fields

      def initialize(parent, via:, fields:)
        @parent = parent
        @via = via.to_s
        @fields = Array(fields).map(&:to_s).uniq.freeze
        raise Stream::Invalid, 'a projection dependency needs changed fields' if @fields.empty?
      end

      def parent
        @parent.is_a?(String) ? @parent.constantize : @parent
      end

      def validate!(stream)
        unless parent.is_a?(Class) && parent < ActiveRecord::Base && parent.primary_key.is_a?(String)
          raise Stream::Invalid, 'a projection dependency needs an ActiveRecord model with one primary key'
        end

        missing = fields - parent.column_names
        raise Stream::Invalid, "unknown dependency fields: #{missing.join(', ')}" unless missing.empty?
        unless stream.model.column_names.include?(via)
          raise Stream::Invalid, "unknown dependency foreign key: #{stream.model}.#{via}"
        end
      end
    end
  end
end
