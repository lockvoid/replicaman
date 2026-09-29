require 'test_helper'

class PullTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @rival = User.create!(id: 'u2', name: 'Rival')
  end

  def create_board(id, user, name)
    DummyReplica.document(:boards, id).create(user_id: user.id) do |doc|
      doc.get_map('meta').set('name', name)
    end
  end

  test 'blank cursor bootstraps the full scoped state with a frontier handoff' do
    create_board('b1', @user, 'Plans')
    item = Items::TextItem.create!(id: 'i1', board_id: 'b1', rank: 'a', body: 'hello')
    Job.create!(id: 'j1', user: @user, state: 'queued')
    Items::PhotoItem.create!(id: 'i2', board_id: 'b1', rank: 'b', width: 3).destroy!

    create_board('b9', @rival, 'Not yours')
    Items::TextItem.create!(id: 'i9', board_id: 'b9', rank: 'a', body: 'secret')
    Job.create!(id: 'j9', user: @rival, state: 'queued')

    result = pull_checkpoint(DummyReplica, user: @user, cursor: nil)

    assert result.fetch(:cursor).present?, 'the committed checkpoint becomes the next read cursor'
    assert result[:reset], 'a bootstrap response says so — the importer replaces its world, not merges'

    frames = result[:frames]
    assert_equal %w[b1], frames.select { it[:frame] == 'doc.snapshot' }.map { it[:id] },
                 'my board arrives as a doc.snapshot frame; the rival board does not'

    doc_frame = frames.find { it[:frame] == 'doc.snapshot' }
    assert_equal 'boards', doc_frame[:stream]
    assert_equal 'loro@1', doc_frame[:codec]
    assert_equal({ 'userId' => 'u1', 'name' => 'Plans' }, doc_frame[:data],
                 'the snapshot frame carries the server projection so import stays dumb and uniform')
    doc = Loro::Doc.from_snapshot(Base64.strict_decode64(doc_frame[:snapshot]))
    assert_equal 'Plans', doc.get_map('meta').get('name'), 'and the full fold'

    row_ids = frames.select { it[:frame] == 'row.set' }.map { it[:id] }
    assert_equal %w[i1 j1], row_ids.sort, 'row.set frames for my items and jobs only, tombstones excluded'

    item_frame = frames.find { it[:id] == 'i1' }
    assert_equal 'TextItem', item_frame[:type]
    assert_equal({ 'boardId' => 'b1', 'label' => nil, 'rank' => 'a', 'body' => 'hello', 'rankBadge' => '#a' }, item_frame[:data])

    item.update!(rank: 'z')
    tail = pull_checkpoint(DummyReplica, user: @user, cursor: result[:cursor])
    assert_equal ['i1'], tail[:frames].map { it[:id] },
                 'a write after bootstrap arrives on the next pull — snapshot + tail, no gap'
    assert_equal 'z', tail[:frames].first[:data]['rank']
  end

  test 'a concurrent uncommitted transaction cannot be skipped by the cursor' do
    create_board('b1', @user, 'Plans')
    cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

    held = Queue.new
    release = Queue.new
    thread = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do |connection|
        connection.transaction do
          Items::TextItem.create!(id: 'iA', board_id: 'b1', rank: 'a', body: 'held')
          held << :held
          release.pop
        end
      end
    end

    held.pop
    Items::TextItem.create!(id: 'iB', board_id: 'b1', rank: 'b', body: 'after')

    parked = pull_checkpoint(DummyReplica, user: @user, cursor: cursor)
    assert_equal ['iB'], parked.fetch(:frames).pluck(:id),
                 'a later commit can be read without waiting for an unrelated open transaction'

    release << :go
    assert thread.join(10), 'the held transaction must finish'

    settled = pull_checkpoint(DummyReplica, user: @user, cursor: parked.fetch(:cursor))
    assert_equal ['iA'], settled.fetch(:frames).pluck(:id),
                 'the earlier transaction remains visible after its later commit'
  ensure
    release&.push(:go)
    thread&.join(5)
  end

  test 'a large commit arrives in bounded pages and a write between pages still arrives' do
    create_board('b1', @user, 'Plans')
    client = ProtocolClient.new(@user)
    cursor = client.checkpoint.fetch(:cursor)
    ActiveRecord::Base.transaction do
      Items::TextItem.create!(id: 'x1', board_id: 'b1', rank: 'a', body: 'one')
      Items::TextItem.create!(id: 'x2', board_id: 'b1', rank: 'b', body: 'two')
      Job.create!(id: 'xj', user: @user, state: 'queued')
    end
    first = client.pull(cursor: cursor, limit: 2)
    assert_equal 2, first.fetch(:frames).size
    assert first.fetch(:more)

    Item.find('x2').update!(body: 'after the first page')
    rest = client.checkpoint(cursor: first.fetch(:cursor), limit: 2)
    latest = (client.frames(first) + JSON.parse(JSON.generate(rest.fetch(:frames)))).to_h { [it.fetch('id'), it] }

    assert_equal %w[x1 x2 xj], latest.keys.sort
    assert_equal 'after the first page', latest.fetch('x2').fetch('data').fetch('body')
  end

  test 'a retained document baseline can receive later deltas and its new projection' do
    create_board('b1', @user, 'Plans')
    DummyReplica.document(:boards, 'b1').edit { it.get_map('meta').set('color', 'blue') }
    cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

    DummyReplica.document(:boards, 'b1').edit do |doc|
      doc.get_map('meta').set('name', 'Renamed')
    end

    tail = pull_checkpoint(DummyReplica, user: @user, cursor: cursor)
    frames = tail[:frames].group_by { it[:frame] }
    assert frames['doc.delta']&.any?, 'the edit ships as deltas'
    assert_equal ['b1'], (frames['row.set'] || []).map { it[:id] },
                 'the fresh projection tails as an ordinary row frame'
    assert_equal 'Renamed', frames['row.set'].first[:data]['name']
    assert_nil frames['doc.snapshot'], 'the fold does not reship on a projection change'
  end

  test 'one checkpoint preserves every row of a commit across many bounded pages' do
    create_board('b1', @user, 'Plans')
    cursor = pull_checkpoint(DummyReplica, user: @user, cursor: nil)[:cursor]

    ActiveRecord::Base.transaction do
      17.times { Items::TextItem.create!(id: "g#{it}", board_id: 'b1', rank: "r#{it}", body: 'bulk') }
    end

    result = pull_checkpoint(DummyReplica, user: @user, cursor: cursor, limit: 1)
    assert_equal 17, result.fetch(:frames).size
    assert_equal 17, result.fetch(:pages), 'the complete checkpoint spans bounded pages'
  end

  test 'a bootstrap reads live rows only and still reaches the bucket head' do
    Job.create!(id: 'j1', user: @user, state: 'queued')
    Job.create!(id: 'j2', user: @user, state: 'queued').destroy!

    reads = snapshot_reads { @result = pull_checkpoint(DummyReplica, user: @user, cursor: nil) }

    refute_empty reads
    assert reads.all? { it.include?('"deleted_at" IS NULL') }, reads.join("\n")
    assert_empty pull_checkpoint(DummyReplica, user: @user, cursor: @result[:cursor])[:frames],
                 'the tombstone tail is behind the returned cursor'
  end

  private

  def snapshot_reads
    reads = []
    subscriber = ActiveSupport::Notifications.subscribe('sql.active_record') do |*, payload|
      reads << payload[:sql] if payload[:name] == 'ReplicaMan::Snapshot Load'
    end
    yield
    reads
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end
end
