class Streams::Workflows < ReplicaMan::Stream
  shard :global
  owner ->(_workflow) { 'catalog' }, shared: -> { 'catalog' }

  attribute :version
  attribute :graph, WorkflowShapes::Graph
end
