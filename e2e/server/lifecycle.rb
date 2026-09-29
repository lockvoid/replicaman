ENV['RAILS_ENV'] = 'test'
raise 'Lifecycle history requires its disposable database' unless ENV.fetch('DATABASE_URL').match?(%r{/replicaman_e2e_[a-z0-9]+\z})
require_relative '../../ruby/test/dummy/config/environment'
Rails.application.eager_load!

case ARGV.fetch(0)
when 'compact'
  DummyReplica.gc(window: 0.seconds, limit: 100)
else
  raise 'Unknown lifecycle action'
end
