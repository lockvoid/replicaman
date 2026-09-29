module ReplicaMan
  class Normalizer
    extend ActiveSupport::Autoload

    autoload :Document
    autoload :Row

    def create(replica, stream, op)
      raise NotImplementedError
    end

    # An explicit domain merge for a deterministic identity shared with the
    # server may override this. Push holds the row lock and checks membership
    # before calling it. Ordinary creates never acknowledge unsaved data.
    def create_existing(replica, stream, op, record)
      raise Refused, "row already exists: #{op.row_id}"
    end

    def delta(replica, stream, op)
      raise Refused, "stream #{stream.stream_name} is a row stream — push row ops, not deltas"
    end

    def normalize(state)
      state
    end

    def refuse?(op)
      false
    end

    def refuse!(op)
      refusal = refuse?(op)
      raise Refused, (refusal.is_a?(String) ? refusal : 'refused') if refusal
    end

    private

    def connection
      ActiveRecord::Base.connection
    end

    def quote(value)
      connection.quote(value)
    end

    def bytea(bytes)
      "'\\x#{bytes.unpack1('H*')}'::bytea"
    end
  end
end
