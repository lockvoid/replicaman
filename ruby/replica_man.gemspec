require_relative 'lib/replica_man/version'

Gem::Specification.new do |spec|
  spec.name = 'replica_man'
  spec.version = ReplicaMan::VERSION
  spec.authors = ['LockVoid Labs']
  spec.license = 'MIT'
  spec.required_ruby_version = '>= 3.4'
  spec.email = ['dev@lockvoid.com']
  spec.homepage = 'https://lockvoid.com/opensource/replicaman'
  spec.summary = 'Client replicas for Rails apps: declarative streams, CRDT merge, per-platform codegen.'
  spec.description = 'ReplicaMan keeps client replicas convergent with a Rails app. The app declares ' \
                     'a Replica class (streams + normalizers + auth); the engine owns transport, ' \
                     'cursors, delta storage and the schema manifest that drives client codegen.'

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    Dir['{app,config,db,lib,exe}/**/*', 'LICENSE', 'Rakefile', 'README.md', 'docs/COMMITS.md']
  end

  spec.bindir = 'exe'
  spec.executables = ['replicaman-restore']

  spec.add_dependency 'pg', '>= 1.5'
  spec.add_dependency 'rails', '>= 8'
end
