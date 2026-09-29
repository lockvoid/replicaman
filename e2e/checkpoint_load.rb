require_relative 'harness'

# Real Rails capture and native publication at 1k/10k/100k rows.
module CheckpointLoad
  module_function

  def verify(worker, total)
    database = Harness.store(worker)
    Harness.check(Harness.sqlite(database, 'PRAGMA quick_check') == [{ 'quick_check' => 'ok' }], 'SQLite integrity failed')
    counts = Harness.sqlite(database, <<~SQL).first
      SELECT COUNT(*) AS count, SUM(CASE
          WHEN json_extract(data, '$.body') = '#{'x' * 256}'
           AND json_extract(data, '$.boardId') = 'b1'
           AND row_id = 'load-' || json_extract(data, '$.rank')
          THEN 0 ELSE 1 END) AS invalid
      FROM snapshots WHERE stream = 'items'
    SQL
    Harness.check(counts == { 'count' => total, 'invalid' => 0 }, "a pull lost or changed captured rows: #{counts}")
    Harness.check(Harness.sqlite(database, 'SELECT COUNT(*) AS count FROM downloads') == [{ 'count' => 0 }],
                  'a published round kept its download')
    cursor = Harness.sqlite(database, "SELECT cursor FROM checkpoints WHERE shard = 'user'").first.fetch('cursor')
    [cursor, Dir[worker.home.join('**/*').to_s].sum { File.file?(it) ? File.size(it) : 0 }]
  end

  def call(workers, server, milestones, command_timeout)
    results = []
    previous = 0
    milestones.each do |target|
      capture_start = Harness.monotonic
      server.script('seed_load.rb', previous, target)
      capture_seconds = Harness.monotonic - capture_start
      workers.each do |worker|
        worker.command_timeout = command_timeout
        start = Harness.monotonic
        peak_client = Harness.rss_kib(worker.pid)
        peak_server = Harness.rss_kib(server.pid)
        pages = 0
        loop do
          response = worker.call('pull_page')
          pages += 1
          peak_client = [peak_client, Harness.rss_kib(worker.pid)].max
          peak_server = [peak_server, Harness.rss_kib(server.pid)].max
          break if response['applied'].positive?

          Harness.check(pages < 10_000, 'a round did not finish within its page bound')
        end
        seconds = Harness.monotonic - start
        cursor, disk_bytes = verify(worker, target)
        started = Harness.monotonic
        worker.call('verify')
        integrity_seconds = Harness.monotonic - started
        started = Harness.monotonic
        Harness.check(worker.call('pull_page')['applied'].zero?, 'an unchanged pull invented a change')
        unchanged_seconds = Harness.monotonic - started
        worker.crash
        worker.start
        Harness.check(verify(worker, target).first == cursor, 'reopening changed the published cursor')
        worker.call('verify')
        result = {
          language: worker.language, rows: target, capture_seconds: capture_seconds, pull_seconds: seconds,
          unchanged_seconds: unchanged_seconds, integrity_seconds: integrity_seconds, pages: pages,
          sampled_client_rss_kib: peak_client, sampled_server_rss_kib: peak_server, client_disk_bytes: disk_bytes,
          database_bytes: Integer(Harness.psql('SELECT pg_database_size(current_database())').first),
        }
        results << result
        Harness::RUN.join('checkpoint-load-results.json').write("#{JSON.pretty_generate(results)}\n")
        puts JSON.generate(result)
        $stdout.flush
      end
      previous = target
    end
    Harness.pass 'real capture, round publication and crash recovery at every load milestone'
  end
end

if $PROGRAM_NAME == __FILE__
  arguments = { rows: [1000, 10_000, 100_000], command_timeout: 120.0 }
  settings = Harness.options('Real Rails capture and native publication at load milestones.') do |parser, _|
    parser.on('--rows LIST') { arguments[:rows] = it.split(',').map { Integer(it) }.uniq.sort }
    parser.on('--command-timeout SECONDS', Float, 'Per-command deadline, including the final publication') do
      arguments[:command_timeout] = it
    end
  end
  Harness.check(arguments[:command_timeout].positive?, 'command timeout must be positive')
  Harness.check(arguments[:rows].any? && arguments[:rows].first.positive? && arguments[:rows].last <= 1_000_000, 'invalid row counts')
  Harness.run(settings.fetch(:clients), page_limit: 1000) do |workers, server|
    CheckpointLoad.call(workers, server, arguments[:rows], arguments[:command_timeout])
  end
end
