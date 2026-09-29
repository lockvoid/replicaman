require_relative 'harness'

# Pull rounds, atomic groups, copied stores, stored-base verification and
# backpressure against the real Rails server, one frame per page.
module ProtocolHistories
  module_function

  def pushes(worker)
    worker.proxy.events.select { it['path'].end_with?('/push') && it['request'] }
  end

  def pulls(worker)
    worker.proxy.events.select { it['path'].end_with?('/pull') && it['request'] }
  end

  def atomic(workers)
    Harness.converge(workers)
    workers.each do |author|
      members = lambda do |prefix, invalid: false|
        [
          { id: "#{prefix}-a", type: 'TextItem', data: { boardId: 'b1', rank: '1', body: 'first member' } },
          { id: "#{prefix}-b", type: 'TextItem', data: { boardId: 'b1', rank: invalid ? '' : '2', body: 'second member' } },
        ]
      end

      prefix = "atomic-retry-#{author.language}"
      author.call('atomic', members: members.call(prefix))
      author.proxy.lose_push = true
      Harness.check(!author.call('drain', expect: false)['ok'], 'group reply loss was not injected')
      Harness.check(author.call('inspect')['pending'] == 2, 'lost group reply discarded a member')
      sent = pushes(author).last['request']['ops']
      Harness.check(sent.size == 2 && sent.map { it['group'] }.uniq.size == 1 && sent.first['group'],
                    'an atomic action became independent operations')
      author.crash
      author.start
      author.call('drain')
      Harness.check(pushes(author).last['request']['ops'] == sent, "restart changed the group's identity or bytes")
      Harness.converge(workers)
      Harness.check(workers.all? { it.row("#{prefix}-a") && it.row("#{prefix}-b") }, 'accepted group did not become authoritative')

      prefix = "atomic-refusal-#{author.language}"
      author.call('atomic', members: members.call(prefix, invalid: true))
      author.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row("#{prefix}-a").nil? && it.row("#{prefix}-b").nil? },
                    'a refused member left another group member committed')
      refused = pushes(author).last['response']['verdicts']
      Harness.check(refused.size == 2 && refused.all? { it['outcome'] == 'rejected' }, 'the server did not refuse the entire action')

      prefix = "atomic-corrupt-#{author.language}"
      author.call('atomic', members: members.call(prefix))
      author.proxy.corrupt_verdict = 'partial_group'
      Harness.check(!author.call('drain', expect: false)['ok'], 'a mixed verdict was accepted for an atomic action')
      Harness.check(author.call('inspect')['pending'] == 2, 'mixed verdict consumed part of an atomic action')
      author.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row("#{prefix}-a") && it.row("#{prefix}-b") },
                    'the whole accepted result could not replay after response corruption')
    end
    Harness.pass 'atomic native actions survive lost replies/restart; domain refusal rolls back all members; mixed verdicts consume nothing'
  end

  def copies(workers)
    Harness.converge(workers)
    workers.each do |worker|
      ident = "copied-#{worker.language}"
      original = worker.home
      Harness.item(worker, ident, body: 'sent from two copies')
      worker.proxy.block_push = true
      Harness.check(!worker.call('drain', expect: false)['ok'], 'blocked push was not injected')
      clone = original.sub_ext('-copy')
      destination = clone.join(Harness.store(worker).relative_path_from(original))
      destination.dirname.mkpath
      Harness.sqlite(Harness.store(worker), ".backup '#{destination}'")
      worker.proxy.block_push = false

      worker.crash
      worker.home = clone
      worker.start
      worker.call('drain')
      first = pushes(worker).last
      worker.crash
      worker.home = original
      worker.start
      worker.call('drain')
      replay = pushes(worker).last
      Harness.check(replay['request']['ops'] == first['request']['ops'], 'the copies sent different operations')
      Harness.check(replay['response'] == first['response'], 'a replayed operation got a different verdict')
      Harness.check(worker.call('inspect')['pending'].zero?, 'the replayed operation never settled')
      Harness.check(Harness.psql("SELECT count(*) FROM items WHERE id = '#{ident}'") == ['1'], 'a copied operation ran twice')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(ident)['body'] == 'sent from two copies' }, 'the copied operation never arrived')
    end
    Harness.pass 'a store copied with frozen operations replays them idempotently: one execution, one verdict'
  end

  def integrity(workers)
    Harness.converge(workers)
    workers.each do |target|
      ident = "integrity-#{target.language}"
      Harness.item(target, ident, body: 'authoritative')
      target.call('drain')
      Harness.converge(workers)

      local = "integrity-local-#{target.language}"
      Harness.item(target, local, rank: 'hold:offline', body: 'keep this unsent edit')
      target.call('verify')
      retained = target.call('inspect')
      database = Harness.store(target)
      saved = Harness::RUN.join("integrity-#{target.language}.sqlite")
      held = Harness.sqlite(database, "SELECT * FROM holds WHERE stream = 'items' AND row_id = ?", local)
      Harness.check(held.size == 1, 'the offline edit was not durably held')

      [
        ["UPDATE base SET data = '{}' WHERE stream = 'items' AND row_id = ?", ident],
        ["UPDATE base SET fold = X'FF' WHERE stream = 'boards' AND row_id = ?", 'b1'],
        ["DELETE FROM base WHERE stream = 'items' AND row_id = ?", ident],
      ].each do |statement, argument|
        Harness.sqlite(database, "ATTACH '#{saved}' AS saved; DROP TABLE IF EXISTS saved.base; " \
                                 'CREATE TABLE saved.base AS SELECT * FROM main.base;')
        changed = Harness.sqlite(database, "#{statement}; SELECT changes() AS changed;", argument)
        Harness.check(changed == [{ 'changed' => 1 }], 'integrity fault did not change exactly one stored row')

        result = target.call('verify', expect: false)
        Harness.check(!result['ok'] && ['Authoritative row integrity failed', 'ReplicaDiverged'].any? { result['error'].include?(it) },
                      "stored corruption passed verification: #{result}")
        Harness.check(target.call('inspect') == retained, 'failed verification changed local authoring or cursor')
        target.crash
        target.start
        Harness.check(!target.call('verify', expect: false)['ok'], 'restart concealed stored corruption')
        Harness.check(target.call('inspect') == retained, 'restart lost unsent authoring')
        Harness.check(Harness.sqlite(database, "SELECT * FROM holds WHERE stream = 'items' AND row_id = ?", local) == held,
                      'verification or restart changed the durable hold')

        # Only the injected fault is undone: these are the original SQLite bytes.
        Harness.sqlite(database, "ATTACH '#{saved}' AS saved; DELETE FROM main.base; " \
                                 'INSERT INTO main.base SELECT * FROM saved.base;')
        target.call('verify')
      end

      Harness.sqlite(database, "DELETE FROM base WHERE stream = 'items' AND row_id = ?", ident)
      target.call('reset')
      target.call('pull')
      target.call('verify')
      Harness.check(target.row(ident)['body'] == 'authoritative', 'a baseline did not rebuild the damaged base')
      Harness.check(target.row(local)['body'] == 'keep this unsent edit', 'a baseline discarded the held edit')
      Harness.check(Harness.sqlite(database, "SELECT count(*) AS held FROM holds WHERE stream = 'items' AND row_id = ?", local) ==
                    [{ 'held' => 1 }], 'a baseline released the durable hold')
      target.call('save', id: local, data: { rank: '1' })
      target.call('drain')
      Harness.converge(workers)
    end
    Harness.pass 'stored row/fold corruption and missing membership fail verification; restart and explicit rebuild retain unsent edits'
  end

  def backpressure(workers)
    Harness.converge(workers)
    workers.each do |target|
      [429, 503].each do |status|
        ident = "backpressure-#{status}-#{target.language}"
        Harness.item(target, ident)
        target.proxy.retry_after = [status, status == 429 ? '1' : (Time.now + 2).httpdate]
        Harness.check(!target.call('drain', expect: false)['ok'], 'backpressure did not fail the request')
        Harness.check(target.call('inspect')['pending'].positive?, 'backpressure acknowledged unsent work')
        target.call('drain')
        Harness.check(pushes(target).last['requested_at'] >= target.proxy.retry_deadline - 0.025,
                      'the native transport retried before the server deadline')
        Harness.check(target.call('inspect')['pending'].zero?, 'the retry did not settle')
      end
    end
    Harness.converge(workers)
    Harness.pass 'HTTP 429/503 Retry-After seconds and dates delay native retries without acknowledging unsent work'
  end

  def rounds(workers)
    Harness.converge(workers)
    workers.each_with_index do |target, index|
      author = workers[(index + 1) % workers.size]
      prefix = "partial-#{target.language}"
      before = target.call('inspect')
      4.times { Harness.item(author, "#{prefix}-#{it}", body: 'first commit') }
      author.call('drain')

      target.call('pull_page')
      staged = target.call('inspect')
      Harness.check(staged['rows'] == before['rows'] && staged['cursor'] == before['cursor'], 'a partial round became visible')
      seen = pulls(target).size
      cursor = pulls(target).last['response']['cursor']
      author.call('save', id: "#{prefix}-0", data: { body: 'later commit' })
      author.call('drain')

      target.crash
      target.start
      target.call('pull')
      Harness.check(pulls(target)[seen]['request']['cursor'] == cursor, 'process death discarded staged progress')
      Harness.check(target.row("#{prefix}-0")['body'] == 'later commit', 'a round published without a later commit')
      Harness.check((1..3).all? { target.row("#{prefix}-#{it}") }, 'a resumed round lost a staged row')
      Harness.converge(workers)
    end
    Harness.pass 'partial rounds stay invisible, resume after SIGKILL and publish the latest committed rows'

    workers.each_with_index do |target, index|
      author = workers[(index + 1) % workers.size]
      ident = "visibility-#{target.language}"
      Harness.item(author, ident, body: 'initial')
      author.call('drain')
      Harness.converge(workers)
      author.call('save', id: ident, data: { body: 'round predates acceptance' })
      3.times { Harness.item(author, "#{ident}-extra-#{it}") }
      author.call('drain')
      target.call('pull_page')
      target.call('save', id: ident, data: { body: 'accepted while downloading' })
      target.call('drain')
      Harness.check(target.call('inspect')['pending'].zero?, 'accepted intent did not settle')
      target.crash
      target.start
      target.call('pull')
      Harness.check(target.row(ident)['body'] == 'accepted while downloading', 'an older round erased accepted authoring')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(ident)['body'] == 'accepted while downloading' }, 'accepted authoring never became authoritative')
    end
    Harness.pass 'accepted authoring survives an older staged round and process death'

    workers.each_with_index do |author, index|
      observer = workers[(index + 1) % workers.size]
      ident = "large-#{author.language}"
      content = "#{"native history \u{1F642} " * 24_000}#{author.language}"
      Harness.item(author, ident, body: content)
      author.proxy.lose_push = true
      Harness.check(!author.call('drain', expect: false)['ok'], 'large push reply loss was not injected')
      Harness.check(author.call('inspect')['pending'].positive?, 'a lost reply discarded authoring')
      author.crash
      author.start
      author.call('drain')

      cursor = observer.call('inspect')['cursor']
      observer.proxy.corrupt_pull = true
      Harness.check(!observer.call('pull', expect: false)['ok'], 'a corrupt page passed validation')
      Harness.check(observer.row(ident).nil? && observer.call('inspect')['cursor'] == cursor, 'a corrupt page published partial state')
      observer.crash
      observer.start
      observer.call('pull')
      Harness.check(observer.row(ident)['body'] == content, 'a large row could not recover after a corrupt page')
      Harness.converge(workers)
    end
    Harness.pass 'large rows retry after lost replies; corrupt pages fail atomically and recover'

    workers.each_with_index do |author, index|
      other = workers[(index + 1) % workers.size]
      ident = "rebirth-#{author.language}"
      Harness.item(author, ident, body: 'first')
      author.call('drain')
      Harness.converge(workers)
      author.call('delete', id: ident)
      author.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(ident).nil? }, 'deleted lifetime remained visible')

      Harness.item(author, ident, body: 'replacement')
      author.proxy.lose_push = true
      Harness.check(!author.call('drain', expect: false)['ok'], 'recreation reply loss was not injected')
      author.crash
      author.start
      author.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(ident)['body'] == 'replacement' }, 'recreated lifetime did not survive a lost reply and restart')

      author.call('delete', id: ident)
      Harness.item(author, ident, body: 'cancel this unsubmitted birth')
      author.call('delete', id: ident)
      Harness.item(author, ident, body: 'final lifetime')
      author.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(ident)['body'] == 'final lifetime' },
                    "cancelling an unsent recreation erased the previous lifetime's delete")

      delayed = "delayed-birth-#{author.language}"
      Harness.item(author, delayed, body: 'old offline birth')
      Harness.item(other, delayed, body: 'committed birth')
      other.call('drain')
      other.call('delete', id: delayed)
      other.call('drain')
      author.call('drain')
      Harness.converge(workers)
      Harness.check(workers.all? { it.row(delayed).nil? }, 'late unprocessed birth resurrected a tombstone')
    end
    Harness.pass 'explicit recreation survives lost replies and restart; cancelled and delayed births cannot erase lifetime fences'
  end

  def call(workers)
    copies(workers)
    atomic(workers)
    integrity(workers)
    backpressure(workers)
    rounds(workers)
  end
end

if $PROGRAM_NAME == __FILE__
  only = nil
  settings = Harness.options('Pull rounds, atomic groups, copies, verification and backpressure.') do |parser, _|
    parser.on('--only NAME', %w[atomic copies integrity backpressure rounds]) { only = it }
  end
  Harness.run(settings.fetch(:clients), page_limit: 1) do |workers|
    only ? ProtocolHistories.public_send(only, workers) : ProtocolHistories.call(workers)
  end
end
