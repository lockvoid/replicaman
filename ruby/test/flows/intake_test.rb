require 'test_helper'

class IntakeTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
  end

  def tickets(**stamp)
    Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'tickets'
      end
      model 'Ticket'
      key :code
      owner :user_id
      door ReplicaMan::Normalizer::Row
      attribute :user_id, :note
      attribute :stamp, :string, push: true, **stamp
    end.tap(&:validate!)
  end

  def create(stream, data)
    op = ReplicaMan::Op.new({ id: 'op1', op: 'row.create', stream: 'tickets', row_id: 'tk-1', data: }, user: @user)
    ReplicaMan::Normalizer::Row.new.create(DummyReplica, stream, op)
  end

  test 'a pushed intake field reaches its handler with the record, the value and the op' do
    stream = tickets(pull: false, intake: ->(ticket, value, op) { ticket.note = "#{value} from #{op.user.id}" })

    create(stream, { userId: 'u1', stamp: 'v1' })

    assert_equal 'v1 from u1', Ticket.find_by!(code: 'tk-1').note
  end

  test 'a refusing intake handler writes nothing' do
    stream = tickets(pull: false, intake: ->(*) { raise ReplicaMan::Refused, 'not that' })

    error = assert_raises(ReplicaMan::Refused) { create(stream, { userId: 'u1', stamp: 'v1' }) }

    assert_equal 'not that', error.message
    assert_nil Ticket.find_by(code: 'tk-1')
  end

  test 'an intake field that was not pushed never calls its handler' do
    stream = tickets(pull: false, intake: ->(*) { flunk 'called without a pushed value' })

    create(stream, { userId: 'u1', note: 'plain' })

    assert_equal 'plain', Ticket.find_by!(code: 'tk-1').note
  end

  test 'an intake field may echo a computed value back' do
    stream = tickets(pull: -> { "echo:#{it.note}" }, intake: ->(ticket, value, _op) { ticket.note = value })

    create(stream, { userId: 'u1', stamp: 'hello' })

    assert_equal 'echo:hello', stream.serialize(Ticket.find_by!(code: 'tk-1'))['stamp']
  end

  test 'a pushed computed attribute needs an intake handler' do
    error = assert_raises(ReplicaMan::Stream::Invalid) { tickets(pull: -> { it.note }) }

    assert_match(/never pushed/, error.message)
  end

  test 'an intake handler needs push: true' do
    error = assert_raises(ReplicaMan::Stream::Invalid) do
      Class.new(ReplicaMan::Stream) do
        def self.stream_name
          'tickets'
        end
        door ReplicaMan::Normalizer::Row
        attribute :stamp, :string, pull: false, intake: ->(*) { }
      end
    end

    assert_match(/intake/, error.message)
  end
end
