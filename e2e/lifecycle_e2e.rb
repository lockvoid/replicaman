require 'rbconfig'
require_relative 'harness'

# Seeded lifecycle histories checked against a small independent row model.
# Each episode owns its row address, so deleting episodes is a valid shrink.
# Values, delivery orders and faults are saved before execution; --shrink
# replays candidates in fresh real databases.
module LifecycleHistories
  KINDS = %w[conflict hold rebirth restore refusal].freeze

  class Model
    attr_reader :rows

    def initialize
      @rows = {}
    end

    def put(ident, **fields)
      (rows[ident] ||= { 'body' => 'initial', 'rank' => '1' }).merge!(fields.transform_keys(&:to_s))
    end

    def verify(workers)
      workers.each do |worker|
        actual = worker.call('inspect')['rows']
          .select { it['stream'] == 'items' && it['id'].start_with?('model-') }
          .to_h { [it['id'], it['data'].slice('body', 'rank')] }
        Harness.check(actual == rows, "model view mismatch: #{worker.language}: #{actual} != #{rows}")
      end
    end
  end

  module_function

  def generate(seed, count)
    random = Random.new(seed)
    selected = KINDS.product([0, 1, 2])
    selected += Array.new([0, count - selected.size].max) { [KINDS.sample(random: random), random.rand(3)] }
    selected.shuffle(random: random).each_with_index.map do |(kind, author), index|
      {
        'id' => index, 'kind' => kind, 'author' => author, 'order' => [0, 1, 2].shuffle(random: random),
        'fields' => Array.new(3) { %w[body rank].sample(random: random) },
        'values' => Array.new(3) { random.rand(1_000_000).to_s },
        'lose_reply' => [true, false].sample(random: random), 'crash' => [true, false].sample(random: random),
      }
    end.then { { 'seed' => seed, 'events' => it } }
  end

  def deliver(worker, event)
    if event['lose_reply']
      worker.proxy.lose_push = true
      Harness.check(!worker.call('drain', expect: false)['ok'], 'lost reply was not injected')
    else
      worker.call('drain')
    end
    if event['crash']
      worker.crash
      worker.start
    end
    worker.call('drain')
  end

  def conflict(workers, event, model, ident)
    workers.each_with_index { |worker, index| worker.call('save', id: ident, data: { event['fields'][index] => event['values'][index] }) }
    lost = workers[event['order'].first] if event['lose_reply']
    event['order'].each do |index|
      worker = workers[index]
      if worker.equal?(lost)
        worker.proxy.lose_push = true
        Harness.check(!worker.call('drain', expect: false)['ok'], 'lost concurrent reply was not injected')
      else
        worker.call('drain')
      end
      model.put(ident, event['fields'][index].to_sym => event['values'][index])
    end
    return unless lost

    if event['crash']
      lost.crash
      lost.start
    end
    lost.call('drain')
  end

  def hold(workers, event, model, ident)
    author = workers[event['author']]
    Harness.item(author, ident, rank: 'hold:0', body: 'first held value')
    author.call('save', id: ident, data: { rank: 'hold:1', body: event['values'][0] })
    author.call('drain')
    (workers - [author]).each do |peer|
      peer.call('pull')
      Harness.check(peer.row(ident).nil?, 'held birth escaped before release')
    end
    author.crash
    author.start
    Harness.check(author.row(ident)['body'] == event['values'][0], 'held value changed after crash')
    author.call('reset')
    author.call('pull')
    Harness.check(author.row(ident)['body'] == event['values'][0], 'a baseline lost the held value')
    author.call('save', id: ident, data: { rank: '1' })
    deliver(author, event)
    model.put(ident, body: event['values'][0])
  end

  def rebirth(workers, server, event, model, ident)
    author = workers[event['author']]
    stale = workers[(event['author'] + 1) % 3]
    stale.call('save', id: ident, data: { body: 'stale lifetime' })
    author.call('delete', id: ident)
    author.call('drain')
    author.call('pull')
    server.script('lifecycle.rb', 'compact')
    Harness.item(author, ident, body: event['values'][0])
    deliver(author, event)
    stale.call('drain')
    model.put(ident, body: event['values'][0])
    refused = Harness.sqlite(Harness.store(stale), "SELECT payload FROM intents WHERE row_id = ? AND state = 'refused'", ident)
    Harness.check(refused.any? { it['payload'].include?('stale lifetime') }, 'stale incarnation refusal lost its evidence')
  end

  def restore(workers, server, event)
    backup = Harness::RUN.join("model-#{event['id']}.dump")
    server.backup(backup)
    snapshots = workers.each_with_index.map do |worker, index|
      Harness.item(worker, "after-backup-#{event['id']}-#{index}", body: 'accepted after backup')
      worker.call('drain')
      Harness.item(worker, "offline-#{event['id']}-#{index}", body: 'only local copy')
      worker.call('inspect')
    end
    server.restore(backup)
    workers.each_index do |index|
      worker = workers[index]
      result = worker.call('drain', expect: false)
      Harness.check(!result['ok'] && result['error'].include?('DatasetChanged'), 'restore failed to fence an old writer')
      Harness.check(worker.call('inspect') == snapshots[index], 'restore fence changed retained local state')
      worker.crash
      worker.start
      Harness.check(worker.call('inspect') == snapshots[index], 'restored dataset caused local loss after restart')
      worker.close
      workers[index] = Harness::Worker.new("epoch-#{event['id']}-#{index}", worker.binary, server.backend)
    end
  end

  def histories(workers, server, history)
    Harness::RUN.join('lifecycle-history.json').write("#{JSON.pretty_generate(history)}\n")
    model = Model.new
    Harness.converge(workers)
    history['events'].each do |event|
      ident = "model-#{event['id']}"
      author = workers[event['author']]
      if %w[conflict rebirth].include?(event['kind'])
        Harness.item(author, ident)
        author.call('drain')
        Harness.converge(workers)
        model.put(ident)
      end
      case event['kind']
      when 'conflict'
        conflict(workers, event, model, ident)
      when 'hold'
        hold(workers, event, model, ident)
      when 'rebirth'
        rebirth(workers, server, event, model, ident)
      when 'restore'
        restore(workers, server, event)
      when 'refusal'
        Harness.item(author, ident, rank: '')
        deliver(author, event)
      else
        raise ArgumentError, "Unknown lifecycle event #{event['kind']}"
      end
      Harness.converge(workers)
      model.verify(workers)
      Harness.pass "lifecycle seed=#{history['seed']} event=#{event['id']} kind=#{event['kind']}"
    end
  end

  # Delta-debugs complete episodes; every candidate runs real native processes.
  def shrink(path, clients)
    history = JSON.parse(path.read)
    candidate_path = path.dirname.join('lifecycle-candidate.json')
    smallest = path.dirname.join('lifecycle-minimal.json')
    expected = path.dirname.join('lifecycle-failure.txt').read.split(':').first
    observed = path.dirname.join('lifecycle-candidate-failure.txt')
    events = history['events']
    width = [1, events.size / 2].max
    attempts = 0
    path.dirname.join('lifecycle-shrink.log').open('w') do |log|
      while events.any? && attempts < 40
        reduced = false
        (0...events.size).step(width).each do |start|
          candidate = events[0...start] + events[(start + width)..]
          next if candidate.empty?

          candidate_path.write("#{JSON.pretty_generate(history.merge('events' => candidate))}\n")
          observed.delete if observed.exist?
          system(RbConfig.ruby, __FILE__, '--clients', clients.join(','), '--replay', candidate_path.to_s,
                 '--failure-out', observed.to_s, out: log, err: log)
          attempts += 1
          next unless $?.exitstatus == 2 && observed.exist? && observed.read.split(':').first == expected

          events = candidate
          reduced = true
          smallest.write("#{JSON.pretty_generate(history.merge('events' => events))}\n")
          break
        end
        next if reduced
        break if width == 1

        width = [1, width / 2].max
      end
    end
    smallest.write("#{JSON.pretty_generate(history.merge('events' => events))}\n")
    puts "Shrunk #{history['events'].size} episodes to #{events.size}: #{smallest}"
  end
end

if $PROGRAM_NAME == __FILE__
  arguments = { seed: 0xC0FFEE, episodes: 18 }
  settings = Harness.options('Seeded lifecycle histories against an independent row model.') do |parser, _|
    parser.on('--seed N') { arguments[:seed] = Integer(it) }
    parser.on('--episodes N', Integer) { arguments[:episodes] = it }
    parser.on('--replay PATH') { arguments[:replay] = Pathname(it) }
    parser.on('--shrink PATH') { arguments[:shrink] = Pathname(it) }
    parser.on('--failure-out PATH') { arguments[:failure_out] = Pathname(it) }
  end

  if arguments[:shrink]
    LifecycleHistories.shrink(arguments[:shrink], settings.fetch(:clients))
  else
    history = arguments[:replay] ? JSON.parse(arguments[:replay].read) : LifecycleHistories.generate(arguments[:seed], arguments[:episodes])
    begin
      Harness.run(settings.fetch(:clients)) { |workers, server| LifecycleHistories.histories(workers, server, history) }
    rescue Harness::Failure => e
      Harness::RUN.join('lifecycle-failure.txt').write("#{e.message}\n")
      arguments[:failure_out]&.write("#{e.message}\n")
      warn "FAILED: #{e.message}\nReplay: #{Harness::RUN.join('lifecycle-history.json')}"
      exit 2
    end
  end
end
