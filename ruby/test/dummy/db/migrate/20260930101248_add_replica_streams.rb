class AddReplicaStreams < ActiveRecord::Migration[8.1]
  def change
    create_replica_partitions :boards
    create_replica_partitions :decks
    create_replica_partitions :exports
    create_replica_partitions :item_templates
    create_replica_partitions :items
    create_replica_partitions :jobs
    create_replica_partitions :tallies
    create_replica_partitions :themes
    create_replica_partitions :tickets
    create_replica_partitions :workflows
    create_replica_capture :boards, namespace: 'replicaman-test', table: :boards, key: :id
    create_replica_capture :decks, namespace: 'replicaman-test', table: :decks, key: :id
    create_replica_capture :exports, namespace: 'replicaman-test', table: :exports, key: :id
    create_replica_capture :item_templates, namespace: 'replicaman-test', table: :item_templates, key: :id
    create_replica_capture :items, namespace: 'replicaman-test', table: :items, key: :id
    create_replica_capture :jobs, namespace: 'replicaman-test', table: :jobs, key: :id
    create_replica_capture :tallies, namespace: 'replicaman-test', table: :tallies, key: :id
    create_replica_capture :themes, namespace: 'replicaman-test', table: :themes, key: :id
    create_replica_capture :tickets, namespace: 'replicaman-test', table: :tickets, key: :code
    create_replica_capture :workflows, namespace: 'replicaman-test', table: :workflows, key: :id
  end
end
