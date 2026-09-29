namespace :replica_man do
  desc 'Serialize every declared row-lane stream into replica_man_snapshots (idempotent)'
  task :backfill, [:replica] => :environment do |_, args|
    replica = args.fetch(:replica) { abort 'usage: rake replica_man:backfill[AppReplica]' }.constantize

    ReplicaMan::Backfill.call(replica).each do |stream, count|
      puts "#{stream}: #{count} rows"
    end
  end
end
