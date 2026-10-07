require 'replica_man/version'
require 'replica_man/engine'
require 'replica_man/refused'
require 'replica_man/request_body'

require 'active_record'
require 'active_support'

module ReplicaMan
  class Unauthorized < StandardError; end

  extend ActiveSupport::Autoload

  autoload :Backfill
  autoload :Commit
  autoload :Frames
  autoload :Capture
  autoload :CaptureHooks
  autoload :DocumentHandle
  autoload :References
  autoload :ProjectionDependencies
  autoload :EntityFence
  autoload :Failure
  autoload :Protocol
  autoload :Buckets
  autoload :Cursor
  autoload :Integrity
  autoload :Mutation
  autoload :Pull
  autoload :DatabaseCursor
  autoload :Loro
  autoload :Manifest
  autoload :Normalizer
  autoload :Op
  autoload :Push
  autoload :Reconcile
  autoload :Replica
  autoload :Schema
  autoload :Stream
end

require 'replica_man/union'
