require_relative 'harness'

# Durable native authoring at 1k/10k/100k rows, then SIGKILL recovery. A host
# resource probe, not a device benchmark: RSS is sampled between batches.
module Load
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
    Harness.check(counts == { 'count' => total, 'invalid' => 0 }, "saved rows lost or changed during the load: #{counts}")
    Harness.check(Harness.sqlite(database, "SELECT COUNT(*) AS count FROM intents WHERE state = 'owed'") == [{ 'count' => total }],
                  'a durable row lost its outbound intent')
    Dir[worker.home.join('**/*').to_s].sum { File.file?(it) ? File.size(it) : 0 }
  end

  def measure(language, milestones)
    worker = Harness::Worker.new(language, Harness.binary(language), Harness.free_port)
    count = 0
    milestones.map do |target|
      started = Harness.monotonic
      samples = [Harness.rss_kib(worker.pid)]
      slowest = 0
      while count < target
        batch = [100, target - count].min
        tick = Harness.monotonic
        worker.call('load', start: count, count: batch)
        slowest = [slowest, Harness.monotonic - tick].max
        count += batch
        samples << Harness.rss_kib(worker.pid)
      end
      status = worker.call('statistics')
      Harness.check(status['queued'] == target, 'status aggregate lost queued work')
      disk_bytes = verify(worker, target)
      elapsed = Harness.monotonic - started
      worker.crash
      reopen = Harness.monotonic
      worker.start
      reopen_seconds = Harness.monotonic - reopen
      Harness.check(worker.call('statistics') == status, 'crash/reopen changed durable backlog')
      verify(worker, target)
      {
        language: language, rows: target, stage_seconds: elapsed, slowest_batch_seconds: slowest,
        sampled_rss_kib: samples.max, disk_bytes: disk_bytes, journal_bytes: status['journalBytes'], reopen_seconds: reopen_seconds,
      }.tap do
        puts JSON.generate(it)
        $stdout.flush
      end
    end
  ensure
    worker&.close
  end
end

if $PROGRAM_NAME == __FILE__
  rows = [1000, 10_000, 100_000]
  settings = Harness.options('Durable native authoring load and crash recovery.') do |parser, _|
    parser.on('--rows LIST') { rows = it.split(',').map { Integer(it) }.uniq.sort }
  end
  Harness.check(rows.any? && rows.first.positive? && rows.last <= 1_000_000, 'invalid row counts')
  Harness.prepare
  results = []
  settings.fetch(:clients).each do |language|
    results.concat(Load.measure(language, rows))
    Harness::RUN.join('load-results.json').write("#{JSON.pretty_generate(results)}\n")
  end
  Harness.pass 'durable native load and crash recovery'
end
