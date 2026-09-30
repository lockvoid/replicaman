require 'test_helper'

class StreamMigrationsTest < ActiveSupport::TestCase
  MIGRATIONS = [File.expand_path('../dummy/db/migrate', __dir__)].freeze

  test 'migrating an empty database installs every declared stream' do
    with_empty_database do |pool|
      ActiveRecord::MigrationContext.new(MIGRATIONS, pool.schema_migration, pool.internal_metadata).migrate

      assert_empty stream_changes
    end
  end

  private

  def with_empty_database
    original = ActiveRecord::Base.connection_db_config
    empty = ActiveRecord::DatabaseConfigurations::HashConfig.new(
      original.env_name, 'empty', original.configuration_hash.merge(database: "#{original.database}_empty")
    )
    recreate(empty.database)
    ActiveRecord::Base.establish_connection(empty)
    yield ActiveRecord::Base.connection_pool
  ensure
    ActiveRecord::Base.establish_connection(original)
    ActiveRecord::Base.connection.drop_database(empty.database)
  end

  def recreate(database)
    ActiveRecord::Base.connection.drop_database(database)
    ActiveRecord::Base.connection.create_database(database)
  end
end
