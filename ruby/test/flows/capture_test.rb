require 'test_helper'

class CaptureTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @board = create_board!(id: 'b1', user: @user, name: 'Plans')
  end

  test 'a flush upserts its rows in one canonical order, whatever order they were captured in' do
    upserted = []
    subscriber = ActiveSupport::Notifications.subscribe('sql.active_record') do |*, payload|
      next unless payload[:name] == 'ReplicaMan::Capture'

      upserted << payload[:binds][1, 2]
    end

    begin
      ActiveRecord::Base.transaction do
        Items::TextItem.create!(id: 'i3', board: @board, rank: 'c', body: 'third')
        Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'first')
        Items::TextItem.create!(id: 'i2', board: @board, rank: 'b', body: 'second')
      end
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    assert_equal [%w[items i1], %w[items i2], %w[items i3]], upserted,
                 'captured i3, i1, i2 — written in (stream, row_id) order'
  end

  test 'a declared model save upserts one snapshot row stamped with a bucket position' do
    item = Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'hello')

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1')
    assert_equal 'TextItem', snapshot.row_type, 'STI type ships demodulized'
    assert_equal({ 'boardId' => 'b1', 'label' => nil, 'rank' => 'a', 'body' => 'hello', 'rankBadge' => '#a' }, snapshot.data,
                 'camelized columns plus flattened store keys plus computed virtuals, no id/type/metadata')
    assert_equal "user:#{@user.id}", snapshot.bucket
    assert snapshot.position.present?
    assert_nil snapshot.deleted_at

    first_position = snapshot.position
    item.update!(rank: 'b')

    assert_equal 1, ReplicaMan::Snapshot.where(stream: 'items').count, 'updates overwrite in place'
    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1')
    assert_equal 'b', snapshot.data['rank']
    assert_operator snapshot.position, :>, first_position, 'a new write takes a later position'
  end

  test 'a union payload captures in wire spelling, inverse to the door' do
    Job.create!(
      id: 'j1', user: @user, state: 'queued',
      payload: { 'kind' => 'ready', 'display_name' => 'Anna',
                 'metrics' => { 'sample_count' => 3, 'mean_confidence' => 0.9 },
                 'history' => [{ 'sample_count' => 1, 'mean_confidence' => 0.5 }] }
    )

    assert_equal(
      { 'kind' => 'ready', 'displayName' => 'Anna',
        'metrics' => { 'meanConfidence' => 0.9, 'sampleCount' => 3 },
        'history' => [{ 'meanConfidence' => 0.5, 'sampleCount' => 1 }] },
      ReplicaMan::Snapshot.find_by!(stream: 'jobs', row_id: 'j1').data['payload'],
      'a union payload is stored in the model spelling (the host door underscores on the way in), ' \
      'but a generated client decode requires its declared wire keys — shipping storage verbatim ' \
      'makes every multi-word required field throw and the row silently unreadable'
    )
  end

  test 'capture serializes ROW TRUTH, never the writing instance memory' do
    Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'first')

    stale = Items::TextItem.find('i1')
    Items::TextItem.find('i1').update!(body: 'second')

    stale.update!(rank: 'b')

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1')
    assert_equal 'b', snapshot.data['rank']
    assert_equal 'second', snapshot.data['body'],
                 'the snapshot must carry the row as it commits, not the stale instance'
  end

  test 'capture shares the business transaction' do
    ActiveRecord::Base.transaction do
      Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'atomic')
      ReplicaMan::Capture.flush
      assert ReplicaMan::Snapshot.exists?(stream: 'items', row_id: 'i1'),
             'capture must ride the SAME transaction — a crash window between commit and flush loses rows'
      raise ActiveRecord::Rollback
    end

    assert_nil ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'i1')
  end

  test 'rows committed together take consecutive positions in their bucket' do
    ActiveRecord::Base.transaction do
      create_board!(id: 'b2', user: @user, name: 'Second')
      Items::TextItem.create!(id: 'i2', board_id: 'b2', rank: 'a', body: 'x')
      Job.create!(id: 'j2', user: @user, state: 'queued')
    end

    positions = ReplicaMan::Snapshot.where(row_id: %w[b2 i2 j2]).pluck(:position).sort
    assert_equal 3, positions.uniq.size
    assert_equal positions.first + 2, positions.last, 'one bucket, no gaps'
  end

  test 'one post-commit doorbell rings per touched shard, not per captured row' do
    rings = []
    DummyReplica.doorbell do |shard:, captures:|
      rings << [
        shard,
        captures.map { [it[:stream], it[:row_id]] }.sort,
        ReplicaMan::Snapshot.where(row_id: %w[b2 i2 j2]).count
      ]
    end

    ActiveRecord::Base.transaction do
      create_board!(id: 'b2', user: @user, name: 'Second')
      Items::TextItem.create!(id: 'i2', board_id: 'b2', rank: 'a', body: 'x')
      Job.create!(id: 'j2', user: @user, state: 'queued')

      assert_empty rings, 'the doorbell must not escape before the business transaction commits'
    end

    assert_equal [
      [
        'user',
        [%w[boards b2], %w[items i2], %w[jobs j2]],
        3
      ]
    ], rings,
                 'one flushed transaction rings once for the user shard after all captured rows commit'
  ensure
    DummyReplica.doorbell { }
  end

  test 'destroy tombstones the snapshot row, keeping its last data' do
    item = Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', body: 'bye')
    item.destroy!

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1')
    assert snapshot.deleted_at.present?, 'deletion is a tombstone, not a vanish — pull must emit row.delete'
    assert_equal 'bye', snapshot.data['body'], 'membership stays decidable from the last state'
  end

  test 'a crash before commit leaves neither the row nor its capture' do
    assert_raises(RuntimeError) do
      ActiveRecord::Base.transaction do
        Items::TextItem.create!(id: 'i9', board: @board, rank: 'a', body: 'x')
        raise 'boom'
      end
    end

    assert_nil Item.find_by(id: 'i9')
    assert_nil ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'i9'),
               'capture is atomic with the business write — no stranded, no missing'
  end

  test 'a document row rides two axes: every save moves the row position, only a new fold moves the fold position' do
    board = create_board!(id: 'bx', user: @user, name: 'Flip')
    born = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'bx')
    assert_equal born.position, born.document_position, 'a birth ships its fold'

    board.update!(name: 'Still live')
    saved = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'bx')
    assert_operator saved.position, :>, born.position, 'a projection change tails out as an ordinary row frame'
    assert_equal born.document_position, saved.document_position, 'an ordinary projection change must not reship the doc'

    board.destroy!
    dead = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'bx')
    assert dead.deleted_at.present?
    assert_operator dead.position, :>, saved.position, 'the tombstone reaches every holder'

    create_board!(id: 'bx', user: @user, name: 'Back from the dead')
    revived = ReplicaMan::Snapshot.find_by!(stream: 'boards', row_id: 'bx')
    assert_nil revived.deleted_at
    assert_operator revived.document_position, :>, dead.position,
                    'the dead -> live flip must ship the new fold — an undelete a tail never hears is a ghost'
  end

  test 'a rolled-back transaction captures nothing, and does not poison the next commit' do
    ActiveRecord::Base.transaction do
      Items::TextItem.create!(id: 'i9', board: @board, rank: 'a', body: 'x')
      raise ActiveRecord::Rollback
    end

    assert_nil ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'i9')

    Items::TextItem.create!(id: 'i10', board: @board, rank: 'a', body: 'y')

    assert_nil ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'i9'),
               'the discarded buffer must not leak into the next flush'
    assert ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'i10')
  end

  test 'undeclared models leave no trace' do
    assert_equal 0, ReplicaMan::Snapshot.where(row_id: 'u1').count, 'User is not a stream'
  end

  test 'a keyed stream captures under its natural key, never the surrogate id' do
    ticket = Ticket.create!(code: 'tk/1/a', user: @user, note: 'first')

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'tickets', row_id: 'tk/1/a')
    assert_equal({ 'userId' => 'u1', 'note' => 'first' }, snapshot.data,
                 'neither the surrogate id nor the key column ride the data — the key IS the row identity')

    ticket.destroy!
    assert ReplicaMan::Snapshot.find_by!(stream: 'tickets', row_id: 'tk/1/a').deleted_at
    assert_equal 0, ReplicaMan::Snapshot.where(stream: 'tickets', row_id: ticket.id.to_s).count
  end

  test 'a row with no owner stays out of every replica' do
    Items::TextItem.new(id: 'orphan', board_id: 'nowhere', rank: 'z', body: 'lost').save!(validate: false)

    assert_nil ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'orphan')
  end

  test 'a row whose owner goes away leaves its replica as a deletion' do
    item = Items::TextItem.create!(id: 'i9', board: @board, rank: 'z', body: 'kept')

    DummyReplica.transaction { item.update_column(:board_id, 'nowhere') }

    snapshot = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i9')
    assert snapshot.deleted_at, 'the owner that saw the row is told it is gone'
    assert_equal 'user:u1', snapshot.bucket
  end
end
