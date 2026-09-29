require 'test_helper'

class UnionTest < ActiveSupport::TestCase
  ResultInput = Struct.new(:kind, :value, keyword_init: true)

  class Result
    include StoreModel::Model
    attribute :kind, :string
    attribute :value, :integer
  end

  def type
    ReplicaMan.union(by: :kind, variants: { 'result' => Result }).to_type
  end

  test 'string keys, symbol keys, and hash-convertible inputs select the same variant' do
    inputs = [
      { 'kind' => 'result', 'value' => 17 },
      { kind: 'result', value: 17 },
      ResultInput.new(kind: 'result', value: 17),
      '{"kind":"result","value":17}'
    ]

    inputs.each do |input|
      result = type.cast(input)
      assert_instance_of Result, result
      assert_equal 17, result.value
    end
  end

  test 'missing and unknown discriminators remain decoding errors' do
    [{}, { kind: 'unknown' }, ResultInput.new(value: 17)].each do |input|
      assert_raises(StoreModel::Types::ExpandWrapperError) { type.cast(input) }
    end
  end
end
