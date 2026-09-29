require 'json'
require 'net/http'
require 'open3'
require 'optparse'
require 'pathname'
require 'puma'
require 'puma/server'
require 'rack'
require 'securerandom'
require 'socket'
require 'time'
require 'zlib'

# Independent, real-process conformance: three native worker processes against
# the Rails server over HTTP, each through its own fault proxy. Every run owns its
# PostgreSQL database, SQLite stores and logs under build/e2e.
module Harness
  ROOT = Pathname(__dir__).parent
  RUN = ROOT.join('build/e2e', SecureRandom.hex(6))
  DATABASE = "replicaman_e2e_#{RUN.basename}".freeze
  BINARIES = {
    'swift' => ROOT.join('swift/.build/debug/ReplicaManE2EWorker'),
    'kotlin' => ROOT.join('kotlin/e2e-worker/build/install/e2e-worker/bin/e2e-worker'),
    'rust' => ROOT.join('rust/target/debug/replicaman-e2e-worker'),
  }.freeze

  class Failure < StandardError; end

  module_function

  def check(condition, message)
    raise Failure, message unless condition
  end

  def pass(message)
    puts "PASS #{message}"
    $stdout.flush
  end

  def free_port
    TCPServer.open('127.0.0.1', 0) { it.addr[1] }
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def item(worker, ident, **data)
    worker.call('save', id: ident, type: 'TextItem', data: { boardId: 'b1', rank: '1', body: 'initial', **data })
  end

  def converge(workers)
    workers.each { it.call('pull') }
    states = workers.map { it.call('inspect') }
    check(states.all? { it.fetch('rows') == states.first.fetch('rows') }, 'row replicas diverged')
    check(states.all? { it['document'] == states.first['document'] }, 'document replicas diverged')
    states
  end

  def store(worker)
    stores = Dir[worker.home.join('**/*.sqlite').to_s]
    check(stores.size == 1, "#{worker.language}: expected one owner store, found #{stores}")
    Pathname(stores.first)
  end

  # Statements run through the sqlite3 shell; a SELECT answers rows as JSON objects.
  def sqlite(path, sql, *binds)
    statement = binds.reduce(sql) { |text, value| text.sub('?') { "'#{value.to_s.gsub("'", "''")}'" } }
    output, status = Open3.capture2('sqlite3', '-json', path.to_s, statement)
    check(status.success?, "sqlite3 failed for #{statement}")
    output.strip.empty? ? [] : JSON.parse(output)
  end

  def psql(sql)
    output, status = Open3.capture2('psql', '-At', '-d', DATABASE, '-c', sql)
    check(status.success?, "psql failed for #{sql}")
    output.lines(chomp: true)
  end

  def rss_kib(pid)
    output, status = Open3.capture2('ps', '-o', 'rss=', '-p', pid.to_s)
    check(status.success?, "ps failed for #{pid}")
    Integer(output.strip)
  end

  def options(description)
    settings = { clients: %w[swift kotlin rust] }
    parser = OptionParser.new(description)
    parser.on('--clients LIST', 'Three native clients; repetition permits single-platform histories') do
      settings[:clients] = it.split(',')
    end
    yield parser, settings if block_given?
    parser.parse!
    settings
  end

  def prepare
    RUN.mkpath
    puts "Evidence: #{RUN}"
    $stdout.flush
  end

  def binary(language)
    BINARIES.fetch(language).tap { check(it.exist?, 'Build every requested worker first: mise run e2e:workers') }
  end

  # Runs histories against a fresh server and three workers; cleanup always runs.
  def run(clients, page_limit: 500)
    check(clients.size == 3, 'The histories require three independent client stores')
    prepare
    binaries = clients.map { binary(it) }
    system('createdb', DATABASE, exception: true)
    server = Server.new(free_port)
    workers = []
    begin
      server.start
      clients.each_with_index do |language, index|
        label = clients.count(language) == 1 ? language : "#{language}#{index + 1}"
        workers << Worker.new(label, binaries[index], server.backend, page_limit: page_limit)
      end
      yield workers, server
      puts 'ALL E2E SCENARIOS PASSED'
    ensure
      workers.each(&:close)
      server.stop
      server.close
      system('dropdb', '--if-exists', '--force', DATABASE, exception: true)
    end
  end

  class Proxy
    attr_accessor :offline, :block_push, :lose_push, :corrupt_pull, :corrupt_verdict, :retry_after
    attr_reader :backend, :events, :url, :retry_deadline

    HOP_BY_HOP = %w[HTTP_HOST HTTP_CONNECTION HTTP_ACCEPT_ENCODING HTTP_VERSION].freeze

    def initialize(backend, language)
      @backend = backend
      @events = []
      @log = RUN.join("#{language}-http.jsonl").open('w')
      @server = Puma::Server.new(self, nil, min_threads: 0, max_threads: 8, log_writer: Puma::LogWriter.null)
      @server.add_tcp_listener('127.0.0.1', 0)
      @url = "http://127.0.0.1:#{@server.connected_ports.first}"
      @server.run
    end

    def call(env)
      request = Rack::Request.new(env)
      body = request.body.read
      requested_at = Harness.monotonic
      push = request.path.end_with?('/push')

      if retry_after && push
        status, value = retry_after
        self.retry_after = nil
        seconds = value.match?(/\A\d+\z/) ? Float(value) : Time.httpdate(value) - Time.now
        @retry_deadline = requested_at + [seconds, 0].max
        return reply(status, '{"error":"injected backpressure"}', 'retry-after' => value)
      end
      return reply(503, '{"error":"injected offline"}') if offline || (block_push && push)

      status, payload = forward(request, body)
      record(request, body, status, payload, requested_at)
      return reply(status, payload) unless status == 200

      if push && corrupt_verdict
        kind = corrupt_verdict
        self.corrupt_verdict = nil
        data = JSON.parse(payload)
        reply(200, JSON.generate(data.merge('verdicts' => corrupt(kind, data.fetch('verdicts')))))
      elsif push && lose_push
        self.lose_push = false
        reply(503, '{"error":"server committed; reply lost"}')
      elsif request.path.end_with?('/pull') && corrupt_pull
        self.corrupt_pull = false
        data = JSON.parse(payload)
        bad = { 'stream' => 'items', 'id' => 'bad', 'incarnation' => 'bad', 'frame' => 'future.frame' }
        reply(200, JSON.generate(data.merge('frames' => data.fetch('frames') + [bad])))
      else
        reply(status, payload)
      end
    end

    def close
      @server.stop(true)
      @log.close
    end

    private

    def forward(request, body)
      forwarded = Net::HTTPGenericRequest.new(request.request_method, !body.empty?, true, request.fullpath)
      request.env.each do |name, value|
        next unless name == 'CONTENT_TYPE' || (name.start_with?('HTTP_') && !HOP_BY_HOP.include?(name))

        forwarded[name.delete_prefix('HTTP_').split('_').map(&:capitalize).join('-')] = value
      end
      forwarded.body = body unless body.empty?
      response = Net::HTTP.start('127.0.0.1', backend, read_timeout: 30) { it.request(forwarded) }
      [response.code.to_i, response.body.to_s]
    end

    def record(request, body, status, payload, requested_at)
      event = { 'method' => request.request_method, 'path' => request.path, 'status' => status, 'requested_at' => requested_at }
      unless body.empty?
        event['request'] = JSON.parse(request.get_header('HTTP_CONTENT_ENCODING') == 'gzip' ? Zlib.gunzip(body) : body)
      end
      event['response'] = payload.start_with?('{') ? JSON.parse(payload) : payload
      events << event
      @log.puts(JSON.generate(event))
      @log.flush
    end

    def corrupt(kind, verdicts)
      case kind
      when 'missing'
        verdicts[0...-1]
      when 'duplicate'
        [verdicts.first, *verdicts]
      when 'partial_group'
        [*verdicts[0...-1], verdicts.last.merge('outcome' => 'rejected', 'reason' => 'injected')]
      when 'foreign'
        [*verdicts[0...-1], verdicts.last.merge('id' => SecureRandom.uuid)]
      else
        raise ArgumentError, "unknown verdict fault #{kind}"
      end
    end

    def reply(status, payload, headers = {})
      [status, { 'content-type' => 'application/json', **headers }, [payload]]
    end
  end

  class Server
    attr_reader :backend, :environment, :log, :epoch_file, :pid

    def initialize(backend)
      @backend = backend
      @log = RUN.join('server.log').open('w')
      @epoch_file = RUN.join('dataset-epoch')
      @epoch_file.write("test-dataset-1\n")
      @environment = {
        'DATABASE_URL' => "postgresql:///#{DATABASE}", 'RAILS_ENV' => 'test', 'RAILS_MAX_THREADS' => '12',
        'REPLICAMAN_DATASET_EPOCH_FILE' => epoch_file.to_s,
      }
    end

    def start(initialize: true)
      environment = @environment.merge('REPLICAMAN_E2E_INITIALIZE' => initialize ? '1' : '0')
      @pid = Process.spawn(environment, 'bundle', 'exec', 'puma', ROOT.join('e2e/server/config.ru').to_s,
                           '-b', "tcp://127.0.0.1:#{backend}", '-t', '0:8',
                           chdir: ROOT.join('ruby').to_s, out: log, err: log, pgroup: true)
      deadline = Harness.monotonic + 45
      loop do
        Harness.check(Process.waitpid(pid, Process::WNOHANG).nil?, "Rails failed to start; see #{RUN.join('server.log')}")
        begin
          response = Net::HTTP.get_response(URI("http://127.0.0.1:#{backend}/__ready"))
          Harness.check(response.code == '200', 'server readiness failed')
          return
        rescue Errno::ECONNREFUSED
          Harness.check(Harness.monotonic < deadline, 'Rails startup timed out')
          sleep 0.1
        end
      end
    end

    def stop
      return unless pid

      if Process.waitpid(pid, Process::WNOHANG).nil?
        Process.kill('TERM', -pid)
        unless reaped?(10)
          Process.kill('KILL', -pid)
          Process.waitpid(pid)
        end
      end
      @pid = nil
    end

    def close
      log.close
    end

    def backup(path)
      system('pg_dump', '--format=custom', '--file', path.to_s, DATABASE, exception: true)
    end

    def restore(path)
      stop
      system('dropdb', '--force', DATABASE, exception: true)
      system('createdb', DATABASE, exception: true)
      restore_command(path)
      start(initialize: false)
    end

    def restore_command(path, exception: true)
      system(environment, 'bundle', 'exec', 'ruby', '-Ilib', 'exe/replicaman-restore',
             '--backup', path.to_s, '--epoch-file', epoch_file.to_s, '--service-stopped',
             chdir: ROOT.join('ruby').to_s, out: log, err: log, exception: exception)
    end

    def script(name, *arguments)
      system(environment, 'bundle', 'exec', 'ruby', ROOT.join('e2e/server', name).to_s, *arguments.map(&:to_s),
             chdir: ROOT.join('ruby').to_s, out: log, err: log, exception: true)
    end

    private

    def reaped?(seconds)
      deadline = Harness.monotonic + seconds
      until Process.waitpid(pid, Process::WNOHANG)
        return false if Harness.monotonic > deadline

        sleep 0.05
      end
      true
    end
  end

  class Worker
    attr_reader :language, :binary, :proxy
    attr_accessor :home, :command_timeout

    def initialize(language, binary, backend, page_limit: 500, home: nil)
      @language = language
      @binary = binary
      @proxy = Proxy.new(backend, language)
      @home = home || RUN.join("#{language}-store")
      @owner = 42
      @page_limit = page_limit
      @command_timeout = 45
      @stderr = RUN.join("#{language}.log").open('a')
      start
    end

    def pid
      @process&.pid
    end

    def start(owner: nil)
      @owner = owner if owner
      environment = {
        'REPLICAMAN_PAGE_LIMIT' => @page_limit.to_s,
        'JAVA_OPTS' => "#{ENV.fetch('JAVA_OPTS', '')} -Djna.library.path=#{ROOT.join('kotlin/libraries/loro/build/host')}",
      }
      @input, output, @process = Open3.popen2(environment, binary.to_s, proxy.url, home.to_s, @owner.to_s,
                                              err: @stderr, pgroup: true)
      @lines = Queue.new
      lines = @lines
      @reader = Thread.new do
        output.each_line { lines << it }
        output.close
        lines << nil
      end
      ready = receive
      Harness.check(ready['ready'], "#{language}: #{ready}")
    end

    def call(command, expect: true, **fields)
      request = { 'command' => command, **fields.transform_keys(&:to_s) }
      @input.puts(JSON.generate(request))
      @input.flush
      answer = receive
      Harness.check(answer['ok'], "#{language}: #{request}: #{answer}") if expect
      answer
    end

    def crash
      return unless @process

      Process.kill('KILL', -pid) if @process.alive?
      @process.join
      @reader.join
      @input.close
      @process = nil
    end

    def row(ident)
      call('inspect').fetch('rows').find { it['stream'] == 'items' && it['id'] == ident }&.fetch('data')
    end

    def close
      crash
      proxy.close
      @stderr.close
    end

    private

    def receive
      line = @lines.pop(timeout: command_timeout)
      raise Failure, "#{language}: worker timed out or exited; see #{RUN}" if line.nil?

      JSON.parse(line)
    rescue JSON::ParserError
      raise Failure, "#{language}: non-protocol output #{line.inspect}"
    end
  end
end
