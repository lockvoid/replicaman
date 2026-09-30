require 'test_helper'
require 'rails/generators/test_case'
require 'generators/replica_man/migration/migration_generator'

class MigrationGeneratorTest < Rails::Generators::TestCase
  tests ReplicaMan::Generators::MigrationGenerator
  destination File.expand_path('../../../../../tmp/generators', __dir__)
  setup :prepare_destination

  test 'writes the migration a declared stream still needs' do
    rolled_back do |connection|
      ReplicaMan::Schema::CaptureTrigger.new(namespace: 'replicaman-test', stream: 'jobs', table: 'jobs', key: 'id').drop(connection)
      run_generator
    end

    assert_migration 'db/migrate/add_replica_stream_jobs.rb',
                     /def change\n    create_replica_capture :jobs, namespace: 'replicaman-test', table: :jobs, key: :id\n  end/
  end

  test 'writes nothing while the database carries every declared stream' do
    assert_match(/already carries every declared replica stream/, run_generator)
    assert_empty Dir[File.join(destination_root, 'db/migrate/*')]
  end

  test 'refuses to plan against a database behind its migrations' do
    pool = ReplicaMan::Snapshot.connection_pool
    pending = Struct.new(:needs_migration?).new(true)
    pool.define_singleton_method(:migration_context) { pending }

    assert_match(/Pending migrations/, capture(:stderr) { run_generator })
    assert_empty Dir[File.join(destination_root, 'db/migrate/*')]
  ensure
    pool.singleton_class.remove_method(:migration_context)
  end
end
