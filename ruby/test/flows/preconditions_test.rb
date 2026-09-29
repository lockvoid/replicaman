require 'test_helper'

class PreconditionsTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    push({ id: 'op1', op: 'row.create', stream: 'tallies', row_id: 't1',
           data: { userId: 'u1', count: 1, version: 3, status: 'open' } })
  end

  def push(*ops)
    push_ops(DummyReplica, user: @user, ops: ops)[:verdicts]
  end

  test 'a patch without its preconditions is refused whole — the row it would overwrite never lends them' do
    verdicts = push({ id: 'op2', op: 'row.patch', stream: 'tallies', row_id: 't1', data: { count: 2 } },
                    { id: 'op3', op: 'row.patch', stream: 'tallies', row_id: 't1', data: { count: 3, version: 3 } })

    assert_equal [{ id: 'op2', outcome: 'rejected', reason: 'row.patch of tallies must carry status, version' },
                  { id: 'op3', outcome: 'rejected', reason: 'row.patch of tallies must carry status' }], verdicts
    assert_equal 1, Tally.find('t1').count
  end

  test 'a create without its preconditions is refused' do
    verdicts = push({ id: 'op2', op: 'row.create', stream: 'tallies', row_id: 't2', data: { userId: 'u1', count: 1 } })

    assert_equal [{ id: 'op2', outcome: 'rejected', reason: 'row.create of tallies must carry status, version' }], verdicts
    assert_nil Tally.find_by(id: 't2')
  end

  test 'a patch carrying its preconditions lands' do
    verdicts = push({ id: 'op2', op: 'row.patch', stream: 'tallies', row_id: 't1', data: { count: 2, version: 3, status: 'open' } })

    assert_equal [{ id: 'op2', outcome: 'accepted' }], verdicts
    assert_equal 2, Tally.find('t1').count
  end

  test 'a precondition is pushed by declaration — it rides every write' do
    error = assert_raises(ReplicaMan::Stream::Invalid) do
      Class.new(ReplicaMan::Stream) do
        def self.name
          'Streams::Tallies'
        end

        attribute :version, push: false, precondition: true
      end
    end

    assert_match(/precondition/, error.message)
  end
end
