class DummyReplica < ReplicaMan::Replica
  namespace 'replicaman-test'
  dataset_epoch 'test-dataset-1' unless ENV.key?('REPLICAMAN_DATASET_EPOCH_FILE')
  use ReplicaMan::Loro

  authenticate { |request| User.find_by(id: request.get_header('HTTP_X_USER_ID')) }

  origin { |request| request.get_header('HTTP_X_DEVICE_ID').presence }

  stream Streams::Boards
  stream Streams::Decks
  stream Streams::Exports
  stream Streams::ItemTemplates
  stream Streams::Items
  stream Streams::Jobs
  stream Streams::Tallies
  stream Streams::Themes
  stream Streams::Tickets
  stream Streams::Workflows
end
