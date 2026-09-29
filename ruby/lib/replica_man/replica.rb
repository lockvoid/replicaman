module ReplicaMan
  class Replica
    PARTITION_LOCK = 0x7265706c

    class << self
      attr_writer :serve_manifest

      def namespace(value = nil)
        @namespace = Protocol.identifier!(value, 'namespace') if value
        @namespace || raise('Declare a stable ReplicaMan namespace')
      end

      # Keep this value outside the database backup. Rotate it before serving a
      # restored or forked database; existing clients then enter recovery.
      def dataset_epoch(value = nil)
        @dataset_epoch = value if value
        epoch_file = ENV.fetch('REPLICAMAN_DATASET_EPOCH_FILE', nil)
        if epoch_file
          raise 'Configure either dataset_epoch or REPLICAMAN_DATASET_EPOCH_FILE, not both' if @dataset_epoch

          value = File.binread(epoch_file, 130).strip
          raise Protocol::Error.new('DatasetUnavailable', status: 503) if value.empty?

          return Protocol.identifier!(value, 'dataset epoch')
        end

        Protocol.identifier!(@dataset_epoch || ENV.fetch('REPLICAMAN_DATASET_EPOCH', nil), 'dataset epoch')
      end

      def schema_version(value = nil)
        @schema_version = Integer(value) if value
        @schema_version || 1
      end

      def streams
        @streams ||= {}
      end

      def stream(klass)
        klass.replica = self
        klass.validate!
        streams[klass.stream_name.to_sym] = klass.tap { Capture.install(it) }
      end

      def doorbell(&block)
        @doorbell = block if block
        @doorbell
      end

      # Runs inside the push transaction for newly applied operations only,
      # so a lost-reply retry does not repeat what it writes.
      def after_apply(&block)
        @after_apply = block if block
        @after_apply
      end

      def authenticate(&block)
        @authenticator = block
      end

      # Revalidate a request inside its actual database transaction. Pull pins
      # its MVCC frontier first; push admits before touching receipts or rows.
      # Return the same principal, or raise ReplicaMan::Unauthorized.
      def authorize(&block)
        @authorizer = block if block
        @authorizer
      end

      def origin(&block)
        @origin = block if block
        @origin
      end

      def authenticator
        @authenticator
      end

      def use(plugin, **options)
        plugins << [plugin, options]
        plugin.install(self, **options)
      end

      def plugins
        @plugins ||= []
      end

      def codecs
        @codecs ||= {}
      end

      def codec
        codecs.values.first || raise("#{self} has no codec registered — `use ReplicaMan::Loro`")
      end

      def document(stream_name, row_id)
        DocumentHandle.new(self, stream_name, row_id)
      end

      def call(env)
        request = Rack::Request.new(env)
        return manifest_response if request.request_method == 'GET' && request.path_info == '/manifest'

        user = authenticator&.call(request)
        return respond(401, error: 'unauthenticated') unless user

        case [request.request_method, request.path_info]
        when ['POST', '/pull']
          pull_response(request, user)
        when ['POST', '/push']
          push_response(request, user)
        when ['POST', '/verify']
          body = JSON.parse(RequestBody.read(request))
          result = Integrity.call(self, user: user, body: body) do
            admitted_user(request, user, :pull)
          end
          respond(200, result)
        else
          respond(404, error: 'not found')
        end
      rescue JSON::ParserError
        respond(400, error: 'malformed json')
      rescue Protocol::Error => error
        respond(error.status, error: error.code, message: error.message, **error.details)
      rescue InvalidRequest => error
        respond(error.status, error: error.message)
      rescue Unauthorized
        respond(401, error: 'unauthenticated')
      rescue ActiveRecord::TransactionRollbackError, ActiveRecord::LockWaitTimeout => error
        Rails.logger.warn("[replica_man] contention: #{error.message.lines.first.strip}")
        respond(503, { error: 'Contention', message: 'a concurrent write held what this request needed' }, 'retry-after' => '1')
      end

      def pull(user:, body:, &admit)
        Pull.call(self, user: user, body: body, &admit)
      end

      # The buckets a principal reads for a shard: its own and, when a stream of
      # the shard declares a shared owner, the shared bucket.
      def buckets(user, shard)
        members = streams.values.select { it.shard == shard }
        raise InvalidRequest, 'unknown shard' if members.empty?

        own = "#{shard}:#{user.id}"
        members.any?(&:shared?) ? [own, "#{shard}:*"] : [own]
      end

      def capture(user:, &block)
        Commit.capture(self, user: user, &block)
      end

      def transaction
        ActiveRecord::Base.transaction do
          Capture.batch do
            value = yield
            Capture.flush
            value
          end
        end
      end

      def recapture(stream_name, row_id)
        stream = streams.fetch(stream_name.to_sym)

        transaction do
          EntityFence.lock(stream, row_id)
          record = stream.locate(row_id) || raise(ActiveRecord::RecordNotFound)
          Capture.record(stream, record)
        end
      end

      def push(user:, body:, origin: nil, &admit)
        Push.call(self, user: user, body: body, origin: origin, &admit)
      end

      def manifest
        Manifest.new(self).to_h
      end

      def serve_manifest?
        return @serve_manifest unless @serve_manifest.nil?

        !(defined?(Rails.env) && Rails.env.production?)
      end

      # Partitions and capture triggers take table locks: run this from the migration step, never on boot.
      def install!
        return unless Snapshot.table_exists?

        connection = Snapshot.connection
        return unless connection.select_value(
          "SELECT relkind FROM pg_class WHERE relname = 'replica_man_snapshots'"
        ) == 'p'

        connection.transaction do
          connection.execute("SELECT pg_advisory_xact_lock(#{PARTITION_LOCK})")
          streams.each_key do |name|
            %w[replica_man_snapshots replica_man_deltas].each do |table|
              carve(connection, table, name)
            end
          end

          CaptureHooks.install(self)
        end
      end

      # What install! would still create; empty once the database matches the declared streams.
      def uninstalled
        connection = Snapshot.connection
        streams.values.flat_map do |stream|
          partitions = %w[replica_man_snapshots replica_man_deltas].map { "#{it}_#{stream.stream_name}" }
            .reject { connection.select_value("SELECT to_regclass(#{connection.quote(it)})::text") }
          trigger = CaptureHooks.trigger_name(self, stream)
          installed = connection.select_value(<<~SQL)
            SELECT EXISTS (SELECT FROM pg_trigger WHERE tgname = #{connection.quote(trigger)}
                           AND tgrelid = to_regclass(#{connection.quote(stream.model.table_name)}))
          SQL
          installed ? partitions : partitions + ["#{trigger} on #{stream.model.table_name}"]
        end
      end

      # Release old payloads in bounded batches. Entity identity is a permanent
      # fence within this dataset, so GC never makes a deleted address look new.
      def gc(window:, limit: 500)
        unless limit.is_a?(Integer) && (1..10_000).cover?(limit)
          raise ArgumentError, 'GC limit must be an integer from 1 to 10000'
        end

        cutoff = Time.current - window
        Snapshot.transaction do
          dead = Snapshot.where(namespace: namespace, stream: streams.keys.map(&:to_s), deleted_at: ...cutoff)
            .where("data <> '{}'::jsonb OR document IS NOT NULL")
            .select(:namespace, :stream, :row_id)
            .order(:stream, :row_id).limit(limit).lock.to_a
          next 0 if dead.empty?

          dead.group_by { streams.fetch(it.stream.to_sym) }.each do |stream, rows|
            addresses = { namespace: namespace, stream: stream.stream_name, row_id: rows.map(&:row_id) }
            Delta.where(addresses).delete_all if stream.document?
            Snapshot.where(addresses).update_all(data: {}, document: nil)
          end

          Rails.logger.info("[replica_man] gc payloads_released=#{dead.size}")
          dead.size
        end
      end

      private

      def carve(connection, table, name)
        return if connection.select_value("SELECT to_regclass('#{table}_#{name}')::text")

        stream = connection.quote(name.to_s)
        attempts = 0
        begin
          attempts += 1
          connection.transaction(requires_new: true) do
            lagged = connection.select_value("SELECT EXISTS (SELECT FROM #{table}_default WHERE stream = #{stream})")
            if lagged
              connection.execute(<<~SQL)
                CREATE TEMP TABLE #{table}_#{name}_carve ON COMMIT DROP AS
                SELECT * FROM #{table}_default WHERE stream = #{stream}
              SQL
              connection.execute("DELETE FROM #{table}_default WHERE stream = #{stream}")
            end
            connection.execute("CREATE TABLE #{table}_#{name} PARTITION OF #{table} FOR VALUES IN (#{stream})")
            connection.execute("INSERT INTO #{table} SELECT * FROM #{table}_#{name}_carve") if lagged
          end
        rescue ActiveRecord::CheckViolation
          retry if attempts == 1

          Rails.logger.warn("[replica_man] carve #{table}/#{name} lost the writer race twice — stream stays on the default partition until the next install")
        end
      end

      def pull_response(request, user)
        body = request.post? ? JSON.parse(RequestBody.read(request)) : request.params

        result = pull(user: user, body: body) do
          admitted_user(request, user, :pull)
        end
        respond(200, result)
      end

      def push_response(request, user)
        body = JSON.parse(RequestBody.read(request))
        raise InvalidRequest, 'push body must be an object' unless body.is_a?(Hash)
        result = push(user: user, body: body, origin: origin&.call(request)) do
          admitted_user(request, user, :push)
        end
        respond(200, result)
      end

      def admitted_user(request, user, operation)
        return user unless authorize

        admitted = authorize.call(request: request, user: user, operation: operation)
        raise Unauthorized unless admitted && admitted.class.base_class == user.class.base_class && admitted.id == user.id

        admitted
      end

      def manifest_response
        return respond(404, error: 'not found') unless serve_manifest?

        respond(200, manifest)
      end

      def respond(status, payload, headers = {})
        [status, { 'content-type' => 'application/json', **headers }, [JSON.generate(payload)]]
      end
    end
  end
end
