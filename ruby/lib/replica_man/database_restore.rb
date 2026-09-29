require 'pg'
require 'securerandom'
require 'tempfile'

module ReplicaMan
  # Restore into an empty PostgreSQL database while synchronization is stopped.
  # The epoch file lives outside database backups and is shared by server nodes.
  class DatabaseRestore
    def self.call(database_url:, backup:, epoch_file:)
      raise ArgumentError, 'Backup must be a regular file' unless File.file?(backup)
      raise ArgumentError, 'Epoch file must be outside the backup' if File.expand_path(backup) == File.expand_path(epoch_file)

      File.open("#{epoch_file}.lock", File::RDWR | File::CREAT, 0o600) do |lock|
        raise 'Another restore owns the epoch file' unless lock.flock(File::LOCK_EX | File::LOCK_NB)

        require_empty_database!(database_url)
        # A failed restore leaves this fence unavailable. It must never resume
        # serving the old epoch against a partially changed authoritative world.
        replace_file(epoch_file, '')
        system('pg_restore', '--single-transaction', '--exit-on-error',
               '--no-owner', '--no-privileges', '--dbname', database_url,
               File.expand_path(backup), exception: true)
        epoch = SecureRandom.uuid
        replace_file(epoch_file, "#{epoch}\n")
        epoch
      end
    end

    def self.require_empty_database!(url)
      PG.connect(url) do |connection|
        count = connection.exec(<<~SQL).first.fetch('count').to_i
          SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
            AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
        SQL
        raise ArgumentError, 'Restore target must be empty; create a new database first' unless count.zero?
      end
    end
    private_class_method :require_empty_database!

    def self.replace_file(path, contents)
      directory = File.dirname(File.expand_path(path))
      Tempfile.create('.replicaman-epoch-', directory) do |file|
        file.write(contents)
        file.flush
        file.fsync
        File.rename(file.path, path)
        File.open(directory, File::RDONLY) { it.fsync }
      end
    end
    private_class_method :replace_file
  end
end
