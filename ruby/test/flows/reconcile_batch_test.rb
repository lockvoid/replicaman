require 'test_helper'

class ReconcileBatchTest < ActiveSupport::TestCase
  ROWS = 40

  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    @board = create_board!(id: 'b1', user: @user, name: 'Plans')
    ROWS.times do |i|
      Items::TextItem.create!(id: "i#{i}", board: @board, rank: i.to_s, body: 'row')
    end
  end

  test 'a quiet sweep costs batched queries, not one per row' do
    selects = []
    subscription = ActiveSupport::Notifications.subscribe('sql.active_record') do |*, payload|
      selects << payload[:sql] if payload[:sql].to_s.match?(/\ASELECT/i)
    end

    counts = ReplicaMan::Reconcile.new(DummyReplica.streams[:items]).call

    assert_equal({ recaptured: 0, tombstoned: 0, missing_fold: 0 }, counts, 'callback-captured rows are already true')
    assert_operator selects.size, :<=, 10,
                    "quiet sweep over #{ROWS} rows must be batched, ran #{selects.size} SELECTs"
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription)
  end
end
