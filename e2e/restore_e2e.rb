require_relative 'harness'

# A real PostgreSQL backup/restore must fence every client's later history.
module RestoreHistories
  module_function

  def database_ids
    Harness.psql('SELECT id FROM items ORDER BY id')
  end

  def failure_guards(server, backup)
    epoch = server.epoch_file.binread
    server.stop
    Harness.check(server.restore_command(backup, exception: false) == false, 'restore replaced a nonempty target')
    Harness.check(server.epoch_file.binread == epoch, 'preflight refusal changed the active fence')

    broken = Harness::RUN.join('broken.dump')
    broken.binwrite('not a PostgreSQL archive')
    system('dropdb', '--force', Harness::DATABASE, exception: true)
    system('createdb', Harness::DATABASE, exception: true)
    Harness.check(server.restore_command(broken, exception: false) == false, 'malformed backup reported success')
    Harness.check(server.epoch_file.binread.empty?, 'failed restore resumed the previous epoch')
    Harness.pass 'restore refuses nonempty targets and leaves synchronization fenced after a failed import'
  end

  def call(workers, server)
    Harness.converge(workers)
    Harness.item(workers.first, 'before-backup', body: 'retained in backup')
    Harness.converge(workers)
    backup = Harness::RUN.join('authoritative.dump')
    server.backup(backup)
    previous_epoch = server.epoch_file.read.strip

    before = workers.to_h do |worker|
      Harness.item(worker, "after-backup-#{worker.language}", body: 'accepted after backup')
      worker.call('drain')
      worker.call('pull')
      Harness.item(worker, "offline-#{worker.language}", body: 'never sent')
      [worker.language, worker.call('inspect')]
    end

    failure_guards(server, backup)
    server.restore(backup)
    Harness.check(server.epoch_file.read.strip != previous_epoch, 'restore reused the dataset epoch')
    authoritative = database_ids
    Harness.check(authoritative.include?('before-backup'), 'restore lost data present in the backup')
    Harness.check(authoritative.none? { it.start_with?('after-backup-', 'offline-') },
                  'restored database already includes later client history')

    workers.each do |worker|
      %w[drain pull].each do |command|
        result = worker.call(command, expect: false)
        Harness.check(!result['ok'] && result['error'].include?('DatasetChanged'),
                      "#{worker.language}: restored history did not fence #{command}: #{result}")
      end
      Harness.check(worker.call('inspect') == before.fetch(worker.language), 'restore refusal changed local evidence')
      worker.crash
      worker.start
      result = worker.call('drain', expect: false)
      Harness.check(!result['ok'] && result['error'].include?('DatasetChanged'), 'restart bypassed dataset fence')
      Harness.check(worker.call('inspect') == before.fetch(worker.language), 'restart lost retained local work')
    end

    Harness.check(database_ids == authoritative, 'old clients replayed mutations into restored history')
    fresh = Harness::Worker.new("fresh-#{workers.first.language}", workers.first.binary, server.backend)
    begin
      fresh.call('pull')
      Harness.check(fresh.row('before-backup')['body'] == 'retained in backup', 'fresh bootstrap failed')
      Harness.item(fresh, 'fresh-after-restore', body: 'new epoch')
      fresh.call('drain')
      Harness.check(database_ids.include?('fresh-after-restore'), 'new dataset cannot admit new writers')
    ensure
      fresh.close
    end
    Harness::RUN.join('restore-result.json').write("#{JSON.pretty_generate(
      'old_epoch' => previous_epoch, 'new_epoch' => server.epoch_file.read.strip,
      'clients' => workers.map(&:language), 'retained_local_state' => true
    )}\n")
    Harness.pass 'PostgreSQL backup/restore fences old sessions before and after SIGKILL; local authoring survives; fresh writers work'
  end
end

if $PROGRAM_NAME == __FILE__
  settings = Harness.options('A real PostgreSQL backup/restore must fence every client\'s later history.')
  Harness.run(settings.fetch(:clients)) { |workers, server| RestoreHistories.call(workers, server) }
end
