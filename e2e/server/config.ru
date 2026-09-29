ENV['RAILS_ENV'] = 'test'
raise 'E2E needs its own disposable database' unless ENV.fetch('DATABASE_URL').match?(%r{/replicaman_e2e_[a-z0-9]+\z})
require_relative '../../ruby/test/dummy/config/environment'

ActiveRecord::Migration.verbose = false
ActiveRecord::MigrationContext.new([File.expand_path('../../ruby/test/dummy/db/migrate', __dir__)]).migrate
Rails.application.eager_load!
DummyReplica.install!
unless ENV.fetch('REPLICAMAN_E2E_INITIALIZE', '1') == '0'
  User.create!(id: '42', name: 'First')
  User.create!(id: '43', name: 'Second')
  DummyReplica.document(:boards, 'b1').create(user_id: '42') { it.get_map('meta').set('name', 'Initial') }
  DummyReplica.document(:boards, 'b9').create(user_id: '43') { it.get_map('meta').set('name', 'Private') }
end

run lambda { |env|
  Rails.application.executor.wrap do
    ActiveRecord::Base.connection_pool.with_connection do
      if env['PATH_INFO'] == '/__ready'
        [200, { 'content-type' => 'application/json' }, ['{"ready":true}']]
      else
        DummyReplica.call(env)
      end
    end
  end
}
