require 'rails/generators'
require 'rails/generators/active_record/migration'

module ReplicaMan
  module Generators
    # bin/rails g replica_man:migration — the migration that brings the database's stream partitions
    # and triggers in line with the declared streams. Run it after adding, changing or removing a stream.
    class MigrationGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      VERBS = { %w[create] => 'add', %w[drop] => 'remove' }.freeze

      source_root File.expand_path('templates', __dir__)
      desc 'Write the migration that makes the database carry exactly the declared replica streams'

      def create_migration_file
        @changes = schema_changes
        return say('The database already carries every declared replica stream') if @changes.empty?

        migration_template 'migration.rb.tt', File.join(db_migrate_path, "#{migration_name}.rb")
      end

      private

      def schema_changes
        Rails.application.eager_load!
        raise Rails::Generators::Error, 'Pending migrations: run bin/rails db:migrate first' if pending_migrations?

        replicas.each { it.streams.each_value(&:validate_schema!) }
        ReplicaMan::Schema::Plan.new(replicas).changes
      end

      def replicas
        ReplicaMan::Replica.descendants.select { it.name && it.streams.any? }
      end

      def pending_migrations?
        ReplicaMan::Snapshot.connection_pool.migration_context.needs_migration?
      end

      def migration_name
        base = migration_base_name
        (1..).lazy.map { it == 1 ? base : "#{base}_#{it}" }.find { !self.class.migration_exists?(db_migrate_path, it) }
      end

      def migration_base_name
        verb = VERBS.fetch(@changes.map { it.command.to_s.split('_').first }.uniq, 'change')
        streams = @changes.map { it.object.stream }.uniq
        streams.one? ? "#{verb}_replica_stream_#{streams.first}" : "#{verb}_replica_streams"
      end
    end
  end
end
