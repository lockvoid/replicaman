module ReplicaMan
  # Applies a client's queue in order, in one transaction. Each operation (or
  # declared group) claims its id in `operations`, then runs in a savepoint: a
  # retry, or a concurrent duplicate waiting on that row, gets the stored verdict.
  class Push
    UUID = /\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/i

    def self.call(replica, user:, body:, origin: nil, &admit)
      new(replica, user, body, origin).call(&admit)
    end

    def initialize(replica, user, body, origin)
      @replica = replica
      @user = user
      @body = body
      @origin = origin
      @rejected_count = 0
      @applied = []
    end

    def call(&admit)
      validate_request!
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      verdicts = ActiveRecord::Base.transaction do
        Capture.batch do
          @user = admit.call if admit
          applied = groups.flat_map { apply(it) }
          after_apply
          applied
        end
      end

      elapsed = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1000).round
      Rails.logger.info("[replica_man] push ops=#{@operations.size} rejected=#{@rejected_count} ms=#{elapsed}")
      Protocol.header(@replica).merge(verdicts: verdicts)
    end

    private

    def validate_request!
      Protocol.validate!(@replica, @body)
      RequestBody.require_fields!(@body, %w[ops])

      raw = @body.fetch('ops')
      Op.validate_batch!(raw, limit: Protocol::MAX_OPERATIONS)
      raw.each do |operation|
        raise InvalidRequest, 'operation id must be a UUID' unless operation.fetch('id').match?(UUID)
        if operation.key?('group') && !operation.fetch('group').match?(UUID)
          raise InvalidRequest, 'operation group must be a UUID'
        end
      end

      @operations = raw.map { Op.new(it, user: @user) }
    end

    def groups
      grouped = @operations.slice_when { |left, right| left.group.nil? || left.group != right.group }.to_a
      seen = Set.new
      grouped.each do |group|
        next if group.first.group.nil?
        raise InvalidRequest, 'an operation group must be contiguous' unless seen.add?(group.first.group)
      end
      grouped
    end

    def apply(group)
      claimed = claim(group)
      return stored(group) if claimed.empty?
      raise InvalidRequest, 'an operation group changed after it was applied' unless claimed.size == group.size

      verdicts = execute(group)
      if verdicts.first.fetch(:outcome) == 'rejected'
        Operation.where(op_id: group.map(&:id)).update_all(outcome: 'rejected', reason: verdicts.first.fetch(:reason))
      else
        @applied.concat(group)
      end
      verdicts
    end

    def claim(group)
      rows = group.map do |operation|
        { op_id: operation.id, namespace: @replica.namespace, author: Protocol.principal(@user),
          body_sha256: operation.digest, outcome: 'accepted' }
      end
      Operation.insert_all(rows, unique_by: :op_id, returning: :op_id).rows.flatten
    end

    def stored(group)
      operations = Operation.where(op_id: group.map(&:id)).index_by(&:op_id)
      group.map do |operation|
        stored = operations.fetch(operation.id)
        if stored.namespace != @replica.namespace || stored.author != Protocol.principal(@user)
          next { id: operation.id, outcome: 'rejected', reason: 'operation id belongs to another replica' }
        end
        raise Protocol::Error.new('MutationChanged', id: operation.id) unless stored.body_sha256 == operation.digest

        { id: operation.id, outcome: stored.outcome, reason: stored.reason }.compact
      end
    end

    def execute(operations)
      mutation = Mutation.new(@replica, origin: @origin)

      # A refusal rolls back the group's domain changes; its claimed ids stay
      # with the recorded refusal. Infrastructure failures escape and retry.
      ActiveRecord::Base.transaction(requires_new: true) do
        lock_entities(operations)
        operations.each { mutation.apply(it) }
      end

      operations.map { { id: it.id, outcome: 'accepted' } }
    rescue Refused => error
      rejected(operations, error.message)
    rescue ActiveRecord::RecordInvalid => error
      rejected(operations, error.record.errors.full_messages.join('; '))
    rescue ActiveModel::RangeError, ActiveRecord::RangeError => error
      rejected(operations, error.message)
    rescue ActiveRecord::RecordNotUnique
      rejected(operations, 'a unique field is already taken')
    end

    def lock_entities(operations)
      addresses = operations.flat_map do |operation|
        references = operation.references.map { [it.fetch('stream'), it.fetch('id')] }
        [[operation.stream_name, operation.row_id], *references]
      end

      addresses.uniq.sort.each do |name, row_id|
        stream = @replica.streams.fetch(name.to_sym) { raise Refused, "unknown stream: #{name}" }
        EntityFence.lock(stream, row_id)
      end
    end

    def rejected(operations, reason)
      @rejected_count += operations.size
      reason = reason.byteslice(0, 1024).scrub
      operations.map { { id: it.id, outcome: 'rejected', reason: reason } }
    end

    def after_apply
      return if @applied.empty?

      @replica.after_apply&.call(operations: @applied.freeze, user: @user, origin: @origin)
      Capture.flush
    end
  end
end
