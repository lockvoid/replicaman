require 'test_helper'

class LoroFreeTest < ActiveSupport::TestCase
  test 'a rows-only replica loads without loro entering the process' do
    script = <<~RUBY
      require 'bundler/setup'
      $LOAD_PATH.unshift #{File.expand_path('../../lib', __dir__).inspect}
      require 'rails'
      require 'replica_man'

      class Item < ActiveRecord::Base; end

      class Items < ReplicaMan::Stream
        owner ->(item) { item.board_id }
        attribute :rank
      end

      Class.new(ReplicaMan::Replica) do
        namespace 'rows-only-test'
        stream Items
      end

      # Bundler evaluates the path gem's gemspec (loro/version.rb) — the
      # RUNTIME is the boundary: no loro.rb, no native extension, no Doc.
      raise 'the loro runtime leaked into the rows-only world' if defined?(Loro::Doc)
      raise 'loro.rb was required' if $LOADED_FEATURES.any? { |f| f.end_with?('/loro.rb') || f.include?('loro_rb') }
      raise 'the row lane must not need a codec' unless ReplicaMan::Normalizer::Row.new.is_a?(ReplicaMan::Normalizer)

      print 'rows-only world clean'
    RUBY

    output = IO.popen([RbConfig.ruby, '-e', script], err: [:child, :out], &:read)

    assert $?.success?, "the rows-only process died:\n#{output}"
    assert_includes output, 'rows-only world clean'
  end
end
