ENV['RAILS_ENV'] = 'test'
raise 'Load probe requires its disposable database' unless ENV.fetch('DATABASE_URL').match?(%r{/replicaman_e2e_[a-z0-9]+\z})
require_relative '../../ruby/test/dummy/config/environment'
Rails.application.eager_load!

start = Integer(ARGV.fetch(0))
finish = Integer(ARGV.fetch(1))
raise 'Invalid load range' unless start >= 0 && finish > start && finish <= 1_000_000
(start...finish).each_slice(500) do |indices|
  rows = indices.map do |index|
    { id: "load-#{index}", board_id: 'b1', rank: index.to_s, metadata: { body: 'x' * 256 } }
  end
  DummyReplica.transaction { Items::TextItem.insert_all!(rows) }
end
puts "Captured #{finish - start} domain rows"

