require_relative 'harness'

# Core histories: durability across SIGKILL, lost replies, collisions, field
# merges, Loro history, malformed verdicts and pages, principal isolation.
module CoreHistories
  module_function

  def call(workers)
    first, second, third = workers
    Harness.converge(workers)
    Harness.pass 'initial authenticated bootstrap'

    workers.each do |worker|
      worker.proxy.offline = true
      Harness.item(worker, "durable-#{worker.language}", body: worker.language)
      worker.crash
      worker.start
      Harness.check(worker.row("durable-#{worker.language}")['body'] == worker.language, 'acknowledged local save lost')
      Harness.check(worker.call('inspect')['pending'].positive?, 'outbound journal lost on process death')
      worker.proxy.offline = false
      worker.call('drain')
    end
    Harness.converge(workers)
    workers.product(workers).each do |observer, author|
      Harness.check(observer.row("durable-#{author.language}")['body'] == author.language, 'durable write never arrived')
    end
    Harness.pass 'SIGKILL after offline save; reopened SQLite and journal'

    workers.each do |worker|
      %w[missing duplicate foreign].each do |kind|
        2.times { Harness.item(worker, "verdict-#{worker.language}-#{kind}-#{it}") }
        worker.proxy.corrupt_verdict = kind
        Harness.check(!worker.call('drain', expect: false)['ok'], "#{worker.language}: malformed #{kind} verdict accepted")
        Harness.check(worker.call('inspect')['pending'] == 2, "#{worker.language}: malformed verdict partially acknowledged a batch")
        worker.call('drain')
      end
    end
    Harness.converge(workers)
    Harness.pass 'malformed verdict batches retain every operation for safe retry'

    [[first, second], [second, third], [third, first]].each do |author, other|
      ident = "receipt-#{author.language}"
      Harness.item(author, ident)
      author.call('drain')
      other.call('pull')
      author.call('save', id: ident, data: { body: 'first' })
      author.proxy.lose_push = true
      Harness.check(!author.call('drain', expect: false)['ok'], 'fault failed to lose committed reply')
      other.call('pull')
      other.call('save', id: ident, data: { body: 'later' })
      other.call('drain')
      author.crash
      author.start
      author.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(ident)['body'] == 'later' }, 'replayed patch overwrote later write')
    end
    Harness.pass 'lost committed replies and replay after another writer, all clients'

    workers.each_with_index do |loser, index|
      winner = workers[(index + 1) % workers.size]
      ident = "collision-#{loser.language}"
      Harness.item(winner, ident, body: 'first durable birth')
      Harness.item(loser, ident, body: 'conflicting birth')
      winner.call('drain')
      loser.call('drain')
      Harness.check(loser.row(ident).nil?, 'a colliding create was falsely accepted')
      verdicts = loser.proxy.events.select { it['path'].include?('push') && it['response'].is_a?(Hash) && it['response']['verdicts']&.any? }
      Harness.check(verdicts.last['response']['verdicts'].first['outcome'] == 'rejected', 'server acknowledged unsaved authoring')
      Harness.converge(workers)
      Harness.check(loser.row(ident)['body'] == 'first durable birth', 'identity collision changed the winner')
    end
    Harness.pass 'colliding row identities reject unsaved authoring, all clients'

    workers.each do |worker|
      ident = "delete-after-lost-reply-#{worker.language}"
      Harness.item(worker, ident, body: 'briefly created')
      worker.proxy.lose_push = true
      Harness.check(!worker.call('drain', expect: false)['ok'], 'missing committed reply loss')
      worker.crash
      worker.start
      worker.call('delete', id: ident)
      worker.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(ident).nil? }, "deleting an ambiguously committed birth was discarded: #{worker.language}")
    end
    Harness.pass 'delete after committed-reply loss and process death, all clients'

    Harness.item(first, 'no-resurrection')
    first.proxy.lose_push = true
    Harness.check(!first.call('drain', expect: false)['ok'], 'missing lost-reply fault')
    second.call('pull')
    second.call('delete', id: 'no-resurrection')
    second.call('drain')
    first.call('drain')
    Harness.converge(workers)
    Harness.check(workers.all? { it.row('no-resurrection').nil? }, 'replayed create resurrected deleted data')
    Harness.pass 'delayed create replay cannot resurrect a deleted row'

    Harness.item(first, 'merge-fields', label: 'idea')
    first.call('drain')
    Harness.converge(workers)
    [[first, 'body', 'offline body'], [second, 'rank', '27'], [third, 'label', 'task']].each do |worker, field, value|
      worker.proxy.offline = true
      worker.call('save', id: 'merge-fields', data: { field => value })
    end
    workers.reverse_each do |worker|
      worker.proxy.offline = false
      worker.call('drain')
    end
    Harness.converge(workers)
    row = first.row('merge-fields')
    Harness.check(row.values_at('body', 'rank', 'label') == ['offline body', '27', 'task'], 'independent fields clobbered')
    Harness.pass 'offline concurrent edits to distinct row fields'

    workers.each do |worker|
      worker.proxy.offline = true
      worker.call('edit', key: worker.language, value: 'preserved')
      worker.call('edit', key: 'name', value: worker.language)
    end
    third.crash
    third.start
    workers.each do |worker|
      worker.proxy.offline = false
      worker.call('drain')
    end
    states = Harness.converge(workers)
    Harness.check(workers.all? { states.first['document'][it.language] == 'preserved' }, 'CRDT history lost')
    Harness.check(workers.map(&:language).include?(states.first['document']['name']), 'invalid CRDT conflict winner')
    Harness.pass 'concurrent Loro edits, conflicting map keys and crash/reopen'

    workers.each do |worker|
      worker.proxy.block_push = true
      worker.call('edit', key: "reset-#{worker.language}", value: 'unsent')
      Harness.item(worker, "reset-#{worker.language}")
      worker.call('reset')
      worker.call('pull')
      Harness.check(worker.call('inspect')['document']["reset-#{worker.language}"] == 'unsent',
                    "#{worker.language}: checkpoint reset erased local document history")
      Harness.check(!worker.row("reset-#{worker.language}").nil?, "#{worker.language}: reset erased an unsent row")
      worker.crash
      worker.start
      worker.proxy.block_push = false
      worker.call('drain')
    end
    Harness.converge(workers)
    Harness.pass 'replacement checkpoints preserve unsent rows and document history'

    workers.each do |worker|
      worker.proxy.offline = true
      worker.call('rich', value: "#{worker.language}🙂")
      worker.crash
      worker.start
      worker.proxy.offline = false
      worker.call('drain')
    end
    states = Harness.converge(workers)
    Harness.check(states.all? { it['body'] == states.first['body'] && it['labels'] == states.first['labels'] }, 'text/list history diverged')
    workers.each do |worker|
      Harness.check(states.first['body'].scan("#{worker.language}🙂").size == 1, 'text lost or duplicated a Unicode edit')
    end
    Harness.check(states.first['labels'].sort == workers.map { "#{it.language}🙂" }.sort, 'list lost an insertion')
    Harness.pass 'concurrent Unicode text and list edits across native bindings and restarts'

    workers.each do |worker|
      worker.proxy.block_push = true
      key = "recovered-#{worker.language}"
      worker.call('rebuild', key: key)
      Harness.check(worker.call('inspect')['pending'].positive?, 'explicit rebuild was not journaled')
      worker.call('reset')
      worker.call('pull')
      Harness.check(worker.call('inspect')['document'][key] == 'recovered', 'reset erased the rebuilt history')
      worker.crash
      worker.start
      worker.proxy.block_push = false
      worker.call('drain')
      Harness.converge(workers)
    end
    states = Harness.converge(workers)
    Harness.check(workers.all? { states.first['document']["recovered-#{it.language}"] == 'recovered' },
                  'rebuilt history never reached other clients')
    Harness.pass 'explicit document rebuilding survives reset and process death and reaches other clients'

    workers.each do |worker|
      ident = "foreign-#{worker.language}"
      Harness.item(worker, ident, boardId: 'b9')
      worker.call('drain')
      Harness.check(worker.row(ident).nil?, 'rejected write remained authoritative')
      verdicts = worker.proxy.events.select { it['path'].include?('push') && it['response'].is_a?(Hash) && it['response']['verdicts']&.any? }
      Harness.check(verdicts.last['response']['verdicts'].first['outcome'] == 'rejected',
                    'the server never refused the foreign write')
      Harness.check(Harness.psql("SELECT count(*) FROM items WHERE id = '#{ident}'") == ['0'], 'the foreign write reached the server')
      Harness.check(worker.call('inspect')['pending'].zero?, 'rejected journal never settled')
    end
    Harness.pass 'server rejects cross-principal writes'

    [[first, second], [second, third], [third, first]].each do |target, author|
      ident = "checkpoint-#{target.language}"
      Harness.item(author, ident)
      author.call('drain')
      target.proxy.corrupt_pull = true
      Harness.check(!target.call('pull', expect: false)['ok'], 'invalid frame did not fail page')
      Harness.check(target.row(ident).nil?, 'failed checkpoint applied a partial page')
      target.call('pull')
      Harness.check(!target.row(ident).nil?, 'failed page advanced cursor past a valid row')
    end
    Harness.converge(workers)
    Harness.pass 'malformed pull page is atomic and retryable, all clients'

    random = Random.new(0x5EED)
    expected = { 'body' => 'initial', 'rank' => '1', 'label' => 'idea' }
    Harness.item(first, 'randomized', **expected.transform_keys(&:to_sym))
    first.call('drain')
    Harness.converge(workers)
    12.times do |round|
      order = workers.shuffle(random: random)
      edits = workers.map do |worker|
        field = %w[body rank].sample(random: random)
        value = "#{round}-#{worker.language}-#{random.rand(100_000)}"
        worker.call('save', id: 'randomized', data: { field => value })
        [worker, field, value]
      end
      order.each do |worker|
        worker.call('drain')
        edits.each { |author, field, value| expected[field] = value if author.equal?(worker) }
      end
      Harness.converge(workers)
      actual = first.row('randomized')
      Harness.check(expected.all? { |field, value| actual[field] == value },
                    "seeded field history diverged in round #{round}: #{actual}, #{expected}")
    end
    Harness.pass 'seeded concurrent row histories (12 rounds, independent oracle)'

    workers.each do |worker|
      worker.crash
      worker.start(owner: 43)
      Harness.check(worker.call('inspect')['rows'].empty?, "previous principal's rows leaked before pull")
      worker.call('pull')
      rows = worker.call('inspect')['rows']
      Harness.check(rows.map { [it['stream'], it['id']] } == [%w[boards b9]], 'principal isolation failed')
      worker.crash
      worker.start(owner: 42)
      Harness.check(!worker.row('randomized').nil?, 'switching owner erased original store')
    end
    Harness.pass 'owner store isolation and return to original owner'
  end
end

if $PROGRAM_NAME == __FILE__
  settings = Harness.options('Independent real-process conformance; requires all three workers built.')
  Harness.run(settings.fetch(:clients)) { |workers| CoreHistories.call(workers) }
end
