#!/usr/bin/env ruby
# Regenerates (or, with --check, verifies) every committed codegen output through the public CLI.

ROOT = File.expand_path('..', __dir__)
CONFIG = ['--config', 'codegen/tests/fixtures/consumer-codegen.json'].freeze
CONSUMER = ['--manifest', 'protocol/fixtures/consumer-manifest.json', '--name', 'SampleReplica'].freeze
NOTES = ['--manifest', 'examples/notes/manifest.json', '--name', 'NotesReplica'].freeze

JOBS = [
  ['--language', 'swift', '--manifest', 'swift/Tests/ReplicaManTests/Fixtures/manifest.json',
   '--out', 'swift/Tests/ReplicaManTests/Generated', '--name', 'Replica'],
  ['--language', 'swift', *CONSUMER,
   '--out', 'swift/Sources/ReplicaManGeneratedContract', '--document-out', 'swift/Sources/ReplicaManGeneratedContract'],
  ['--language', 'kotlin', '--manifest', 'kotlin/libraries/replicaman/src/test/resources/manifest.json',
   '--out', 'kotlin/libraries/replicaman/src/test/kotlin/io/replicaman/generated/dummy',
   '--package', 'io.replicaman.generated.dummy', '--name', 'Replica',
   '--document-out', 'kotlin/libraries/replicaman/src/test/resources/generated/dummy/documents'],
  ['--language', 'kotlin', *CONSUMER,
   '--out', 'kotlin/libraries/replicaman/src/test/kotlin/io/replicaman/generated/app', '--package', 'io.replicaman.generated.app',
   '--document-out', 'kotlin/libraries/replicaman/src/test/generated-documents', '--document-package', 'io.replicaman.generated.app.documents'],
  ['--language', 'rust', *CONSUMER,
   '--out', 'rust/crates/replicaman-generated-contract/src/generated', '--document-out', 'rust/crates/replicaman-generated-contract/src/documents'],
  ['--language', 'swift', *NOTES, '--out', 'swift/Sources/ReplicaManNotesExample/Generated'],
  ['--language', 'kotlin', *NOTES, '--out', 'kotlin/samples/notes/src/main/kotlin/example/generated',
   '--package', 'example.generated'],
  ['--language', 'rust', *NOTES, '--out', 'rust/examples/notes/src/generated'],
].freeze

check = ARGV.include?('--check') ? ['--check'] : []
JOBS.each do |job|
  system('ruby', 'codegen/bin/replica-codegen', *CONFIG, *job, *check, chdir: ROOT, exception: true)
end
