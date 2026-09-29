require 'base64'

module ReplicaMan
  class Commit
    class Uncommitted < StandardError; end

    attr_reader :value

    def self.current
      ActiveSupport::IsolatedExecutionState[:replica_man_commit]
    end

    def self.capture(replica, user:)
      previous = current
      commit = new(replica, user, previous)
      ActiveSupport::IsolatedExecutionState[:replica_man_commit] = commit
      commit.instance_variable_set(:@value, yield)
      commit
    ensure
      ActiveSupport::IsolatedExecutionState[:replica_man_commit] = previous
    end

    def initialize(replica, user, parent)
      @replica, @user, @parent = replica, user, parent
      @addresses = Set.new
    end

    def record(stream, row_id)
      @addresses.add([stream, row_id]) if @replica.streams.key?(stream.to_sym)
      @parent&.record(stream, row_id)
    end

    def encode
      raise Uncommitted, 'ReplicaMan commits can only be encoded after the command commits' if ActiveRecord::Base.connection.transaction_open?

      @encoded ||= begin
        shards = @addresses.map { |name, _| @replica.streams.fetch(name.to_sym).shard }.uniq.sort
        Base64.strict_encode64(JSON.generate(Protocol.header(@replica).merge(shards: shards)))
      end
    end
  end
end
