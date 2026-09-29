require 'test_helper'

class BackfillTest < ActiveSupport::TestCase
  test 'backfill serializes existing row-lane rows, skips the document lane, and is idempotent' do
    uncaptured_fixture(Board, Item, Job, Ticket) do
      User.insert_all([{ id: 'u1', name: 'D' }])
      Board.insert_all([{ id: 'b1', user_id: 'u1', name: 'Quiet' }])
      Item.insert_all([
        { id: 'i1', board_id: 'b1', type: 'Items::TextItem', rank: 'a', metadata: { body: 'old row' } },
        { id: 'i2', board_id: 'b1', type: 'Items::PhotoItem', rank: 'b', metadata: { width: 4 } }
      ])
      Job.insert_all([{ id: 'j1', user_id: 'u1', state: 'queued' }])
      Ticket.insert_all([{ code: 'tk/1', user_id: 'u1', note: 'old' }])
    end
    assert_equal 0, ReplicaMan::Snapshot.count, 'insert_all bypasses capture — the pre-replica world'

    counts = ReplicaMan::Backfill.call(DummyReplica)

    assert_equal({ exports: 0, item_templates: 0, items: 2, jobs: 1, tallies: 0, themes: 0, tickets: 1, workflows: 0 }, counts)
    assert_equal 4, ReplicaMan::Snapshot.count, 'boards (document lane) are not backfilled'
    assert ReplicaMan::Snapshot.find_by(stream: 'tickets', row_id: 'tk/1'), 'keyed streams backfill under their key'

    text = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1')
    assert_equal 'TextItem', text.row_type
    assert_equal({ 'boardId' => 'b1', 'label' => nil, 'rank' => 'a', 'body' => 'old row', 'rankBadge' => '#a' }, text.data,
                 'virtuals backfill computed, like any column')
    assert text.position.present?

    again = ReplicaMan::Backfill.call(DummyReplica)
    assert_equal({ exports: 0, item_templates: 0, items: 2, jobs: 1, tallies: 0, themes: 0, tickets: 1, workflows: 0 }, again)
    assert_equal 4, ReplicaMan::Snapshot.count, 'idempotent — upserts, never duplicates'
  end
  test 'a committed batch can resume after interruption without skipping an address' do
    user = User.create!(id: 'batch-owner', name: 'Owner')
    uncaptured_fixture(Ticket) do
      Ticket.insert_all!(5.times.map { |index| { code: "ticket/#{index}", user_id: user.id, note: 'old' } })
    end
    checkpoint = nil
    interruption = Class.new(StandardError)

    assert_raises(interruption) do
      ReplicaMan::Backfill.call(DummyReplica, batch_size: 2) do |cursor|
        checkpoint = cursor
        raise interruption
      end
    end
    assert_equal 2, ReplicaMan::Snapshot.where(stream: 'tickets').count

    counts = ReplicaMan::Backfill.call(DummyReplica, batch_size: 2, after: checkpoint)
    assert_equal 3, counts.fetch(:tickets)
    assert_equal Ticket.order(:code).pluck(:code),
                 ReplicaMan::Snapshot.where(stream: 'tickets').order(:row_id).pluck(:row_id)
  end
end
