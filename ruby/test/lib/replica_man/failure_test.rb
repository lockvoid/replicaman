require 'test_helper'

class FailureTest < ActiveSupport::TestCase
  FakeResult = Struct.new(:sqlstate) do
    def error_field(_field)
      sqlstate
    end
  end

  FakeError = Class.new(StandardError) { attr_accessor :result }

  test 'contention and lost connections belong to the request' do
    [ActiveRecord::Deadlocked.new('deadlock'), ActiveRecord::SerializationFailure.new('serial'),
     ActiveRecord::LockWaitTimeout.new('lock'), ActiveRecord::StatementTimeout.new('timeout'),
     ActiveRecord::ConnectionNotEstablished.new('gone'), Errno::ECONNRESET.new].each do |error|
      assert ReplicaMan::Failure.transient?(error), error.class.name
    end
  end

  test 'data and integrity errors and host exceptions belong to the operation' do
    [ActiveRecord::NotNullViolation.new('null'), ActiveRecord::InvalidForeignKey.new('fk'),
     ActiveRecord::ValueTooLong.new('long'), ActiveRecord::RecordNotFound.new('missing'),
     KeyError.new('key'), NoMethodError.new('nil')].each do |error|
      refute ReplicaMan::Failure.transient?(error), error.class.name
    end
  end

  test 'an unmapped PostgreSQL error is classified by its SQLSTATE class, through a wrapper too' do
    exhausted = FakeError.new('too many connections').tap { it.result = FakeResult.new('53300') }
    assert ReplicaMan::Failure.transient?(exhausted)

    wrapped = begin
      begin
        raise exhausted
      rescue FakeError
        raise ActiveRecord::StatementInvalid, 'wrapped'
      end
    rescue ActiveRecord::StatementInvalid => error
      error
    end
    assert ReplicaMan::Failure.transient?(wrapped), 'the cause carries the state'

    malformed = FakeError.new('invalid input syntax').tap { it.result = FakeResult.new('22P02') }
    refute ReplicaMan::Failure.transient?(malformed)
  end

  test 'the reason names the failure the client parks' do
    assert_equal 'KeyError: key not found', ReplicaMan::Failure.reason(KeyError.new("key not found\nmore"))
    assert_equal 'a unique field is already taken', ReplicaMan::Failure.reason(ActiveRecord::RecordNotUnique.new('dup'))
  end
end
