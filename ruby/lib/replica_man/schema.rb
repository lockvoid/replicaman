module ReplicaMan
  # The PostgreSQL objects a declared stream needs beyond the engine tables. Migrations
  # create them; `bin/rails g replica_man:migration` writes those migrations.
  module Schema
    extend ActiveSupport::Autoload

    PARENTS = %w[replica_man_snapshots replica_man_deltas].freeze

    autoload :CaptureTrigger
    autoload :CommandRecorder
    autoload :DependencyTrigger
    autoload :Partitions
    autoload :Plan
    autoload :Statements
  end
end
