module ReplicaMan
  # What a failure inside an operation means for the push. The same bytes meet
  # a data or integrity error, or a host exception, again on every retry: that
  # is the operation's verdict. Contention, a lost connection and exhausted
  # resources belong to the request: it rolls back and the client retries.
  module Failure
    TRANSIENT = [
      ActiveRecord::TransactionRollbackError,
      ActiveRecord::LockWaitTimeout,
      ActiveRecord::QueryAborted,
      ActiveRecord::ConnectionNotEstablished,
      ActiveRecord::NoDatabaseError,
      Timeout::Error,
      IOError,
      SystemCallError,
      SocketError,
    ].freeze

    # SQLSTATE classes: connection, transaction rollback, resources, program limits,
    # operator intervention, system error, internal error.
    TRANSIENT_SQLSTATE = %w[08 40 53 54 57 58 XX].freeze

    def self.transient?(error)
      TRANSIENT.any? { error.is_a?(it) } || TRANSIENT_SQLSTATE.include?(sqlstate(error).to_s[0, 2])
    end

    def self.reason(error)
      case error
      when ActiveRecord::RecordInvalid then error.record.errors.full_messages.join('; ')
      when ActiveRecord::RecordNotUnique then 'a unique field is already taken'
      else "#{error.class.name}: #{error.message.lines.first.to_s.strip}"
      end
    end

    def self.report(error, operations)
      Rails.logger.error(
        "[replica_man] #{operations.map(&:id).join(',')} refused by a failure the same operation repeats: " \
        "#{error.class}: #{error.message.lines.first.to_s.strip}\n#{Array(error.backtrace).first(8).join("\n")}"
      )
    end

    def self.sqlstate(error)
      [error, error.cause].compact.each do |candidate|
        result = candidate.respond_to?(:result) ? candidate.result : nil
        state = result.respond_to?(:error_field) ? result.error_field(PG::PG_DIAG_SQLSTATE) : nil
        return state if state
      end
      nil
    end
  end
end
