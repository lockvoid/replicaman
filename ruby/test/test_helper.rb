ENV['RAILS_ENV'] = 'test'

require_relative './dummy/config/environment'

begin
  ActiveRecord::Base.connection.execute('SELECT 1')
rescue ActiveRecord::NoDatabaseError
  ActiveRecord::Tasks::DatabaseTasks.create_current
end

ActiveRecord::Migration.verbose = false
ActiveRecord::MigrationContext.new([File.expand_path('dummy/db/migrate', __dir__)]).migrate

DummyReplica.install!

require 'rails/test_help'
require_relative 'support/domain_client'

class ActiveSupport::TestCase
  self.use_transactional_tests = false

  def push_ops(replica, user:, ops:, origin: nil)
    @domain_clients ||= {}
    client = @domain_clients[[replica, user.id]] ||= DomainClient.new(replica, user)
    client.push(ops, origin: origin)
  end

  def pull_checkpoint(replica, user:, **options)
    @read_clients ||= {}
    client = @read_clients[[replica, user.id]] ||= ProtocolClient.new(user, replica: replica)
    client.checkpoint(**options)
  end

  # Repair tests deliberately construct a database predating capture hooks.
  # Trigger DDL is transactional: failure rolls it back with the fixture writes.
  # Production bulk writes must use Replica.transaction and keep enforcement on.
  def uncaptured_fixture(*models)
    connection = ActiveRecord::Base.connection
    tables = models.map(&:table_name).uniq.map { connection.quote_table_name(it) }
    connection.transaction do
      tables.each { connection.execute("ALTER TABLE #{it} DISABLE TRIGGER USER") }
      result = yield
      tables.each { connection.execute("ALTER TABLE #{it} ENABLE TRIGGER USER") }
      result
    end
  end

  def create_board!(**attributes)
    id = attributes.fetch(:id)
    values = attributes.except(:id)
    DummyReplica.document(:boards, id).create(**values) do |doc|
      doc.get_map('meta').set('name', values.fetch(:name)) if values.key?(:name)
    end
    Board.find(id)
  end

  setup do
    ReplicaMan::Operation.delete_all
    ReplicaMan::Bucket.delete_all
    ReplicaMan::Delta.delete_all
    ReplicaMan::Snapshot.delete_all
    DummyReplica.transaction do
      Item.delete_all
      Board.delete_all
      Job.delete_all
      Ticket.delete_all
      Tally.delete_all
      User.delete_all
    end

    ReplicaMan::Snapshot.delete_all
  end
end
