require 'test_helper'

class PushBatchTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Mine') }
    push_ops(DummyReplica, user: @user, ops: []) # Register the client before measuring push.
  end

  teardown do
    DummyReplica.doorbell { }
  end

  def ops(*specs)
    specs.map.with_index do |(row_id, body), i|
      { id: "op#{i}", op: 'row.create', stream: 'items', row_id: row_id, type: 'TextItem',
        data: { boardId: 'b1', rank: "r#{i}", body: body }.compact }
    end
  end

  test 'a push rings ONCE, carrying every accepted capture with its origin' do
    rings = []
    DummyReplica.doorbell do |shard:, captures:|
      rings << { shard: shard, rows: captures.map { [it[:row_id], it[:origin]] }.sort }
    end

    verdicts = push_ops(DummyReplica,
      user: @user,
      ops: ops(%w[i1 hello], ['i2', nil], %w[i3 world]),
      origin: 'device-A'
    )[:verdicts]

    assert_equal %w[accepted rejected accepted], verdicts.map { it[:outcome] }
    assert_equal 1, rings.size, 'one push = one business commit = ONE ring, not one per op'
    assert_equal 'user', rings.first[:shard]
    assert_equal [%w[i1 device-A], %w[i3 device-A]], rings.first[:rows],
                 'the ring carries every ACCEPTED capture, each origin-stamped; the rejected op never captured'
  end

  test 'accepted ops take consecutive positions — the batch is one consistent cut' do
    push_ops(DummyReplica, user: @user, ops: ops(%w[i1 a], %w[i2 b], %w[i3 c]))

    positions = ReplicaMan::Snapshot.where(stream: 'items', row_id: %w[i1 i2 i3]).order(:row_id).pluck(:position)
    assert_equal positions.first.step(by: 1).first(3), positions, 'one push commits its captures together, in order'
  end

  test 'a rejected op leaves no row, no snapshot, and poisons nothing' do
    verdicts = push_ops(DummyReplica,
      user: @user,
      ops: ops(%w[i1 first], ['bad', nil], %w[i3 third])
    )[:verdicts]

    assert_equal %w[accepted rejected accepted], verdicts.map { it[:outcome] }
    assert_equal 'first', Item.find('i1').body
    assert_equal 'third', Item.find('i3').body
    assert_nil Item.find_by(id: 'bad')
    assert_nil ReplicaMan::Snapshot.find_by(stream: 'items', row_id: 'bad'),
               'the savepoint rollback discards the rejected op capture'
  end

  test 'the batch is one transaction: a single BEGIN and COMMIT, savepoints inside' do
    statements = []
    sub = ActiveSupport::Notifications.subscribe('sql.active_record') do |*, payload|
      statements << payload[:sql] if payload[:sql].match?(/\A(BEGIN|COMMIT|SAVEPOINT|RELEASE)/i)
    end
    push_ops(DummyReplica, user: @user, ops: ops(%w[i1 a], %w[i2 b], %w[i3 c]))
    ActiveSupport::Notifications.unsubscribe(sub)

    begins = statements.count { it.match?(/\ABEGIN/i) }
    commits = statements.count { it.match?(/\ACOMMIT/i) }
    savepoints = statements.count { it.match?(/\ASAVEPOINT/i) }
    assert_equal 1, begins, "one BEGIN for the whole push, saw: #{statements.inspect}"
    assert_equal 1, commits, 'one COMMIT for the whole push'
    assert_operator savepoints, :>=, 2, 'later ops nest as savepoints inside the batch transaction'
  end
end
