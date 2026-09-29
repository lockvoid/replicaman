#!/usr/bin/env ruby
# Authors the frozen CRDT convergence corpus over a neutral board document and writes its verbatim copies.
# Usage: ruby tools/generate_crdt_corpus.rb

$LOAD_PATH.unshift(File.expand_path('../ruby/vendor/loro/lib', __dir__))

require 'digest'
require 'fileutils'
require 'json'
require 'loro'

ROOT = File.expand_path('..', __dir__)
SOURCE = 'protocol/fixtures/crdt_convergence'
COPIES = %w[
  swift/Tests/ReplicaManLoroTests/Fixtures/crdt_convergence
  rust/crates/replicaman-loro/tests/fixtures/crdt_convergence
].freeze
NOTE = 'Frozen manifest of the cross-platform merge corpus, written with its verbatim copies by ' \
       'tools/generate_crdt_corpus.rb; this index pins WHICH cases and WHICH bytes a client is grading, ' \
       'so a partial re-copy or an uncopied new case fails instead of passing quietly.'

BASE_PEER = 1
LEFT_PEER = 11
RIGHT_PEER = 22
MERGE_PEER = 99
BLOBS = %w[base.bin left.bin right.bin].freeze

REGISTRIES = %w[cards columns].freeze
ROOTS = %w[cards columns settings meta].freeze
CARD = '01j9x7k2m4p6r8t0v2w4y6z8a0'

BOARD = {
  'cards' => {
    CARD => {
      'column_key' => 'todo',
      'title' => 'First card',
      'position' => 0.0,
      'progress' => 0.0,
      'estimate' => 3,
      'weight' => 1.0,
      'done' => false,
    },
  },
  'columns' => {
    'todo' => { 'name' => 'To do', 'rank' => 1000, 'collapsed' => false },
    'doing' => { 'name' => 'Doing', 'rank' => 2000, 'collapsed' => false },
    'done' => { 'name' => 'Done', 'rank' => 3000, 'collapsed' => true },
  },
  'settings' => {
    'theme' => 'light',
    'density' => 'comfortable',
    'show_avatars' => true,
    'wip_limit' => 0,
    'week_start' => 'monday',
  },
  'meta' => { 'name' => 'Fixture' },
}.freeze

LEFT_FRAME = { 'x' => 0.1, 'y' => 0.5, 'width' => 1.0, 'height' => 1.0 }.freeze
RIGHT_FRAME = { 'x' => 0.5, 'y' => 0.9, 'width' => 2.0, 'height' => 3.0 }.freeze
BANNER = { 'enabled' => true, 'align' => 'bottom-left', 'opacity' => 0.8, 'image_id' => nil }.freeze
FUTURE_FIELD = { 'nested' => [1, 2], 'flag' => true }.freeze

CASES = {
  'field_wise_merge' => {
    description: 'Two devices edit different fields of one card. Both edits land.',
    left: { 'cards' => { CARD => { 'progress' => 0.25 } } },
    right: { 'cards' => { CARD => { 'position' => 9.5 } } },
    merged: { 'cards' => { CARD => { 'progress' => 0.25, 'position' => 9.5 } } },
  },
  'same_field_lww' => {
    description: 'Two devices write the same field. A deterministic winner, no conflict UI.',
    left: { 'cards' => { CARD => { 'progress' => 0.25 } } },
    right: { 'cards' => { CARD => { 'progress' => 0.75 } } },
    merged: { 'cards' => { CARD => { 'progress' => 0.75 } } },
  },
  'delete_vs_edit' => {
    description: 'One device deletes a card while another edits it. The delete wins.',
    left: { 'cards' => { CARD => nil } },
    right: { 'cards' => { CARD => { 'progress' => 0.25 } } },
    merged: { 'cards' => { CARD => nil } },
  },
  'sub_object_replace_whole' => {
    description: 'Concurrent frame writes. One wins WHOLE — never x from one and y from the other.',
    left: { 'cards' => { CARD => { 'frame' => LEFT_FRAME } } },
    right: { 'cards' => { CARD => { 'frame' => RIGHT_FRAME } } },
    merged: { 'cards' => { CARD => { 'frame' => RIGHT_FRAME } } },
  },
  'numeric_fields_lww' => {
    description: 'estimate/weight merge as ordinary LWW fields beside a concurrent progress edit.',
    left: { 'cards' => { CARD => { 'estimate' => 8, 'weight' => 2.5 } } },
    right: { 'cards' => { CARD => { 'progress' => 0.5 } } },
    merged: { 'cards' => { CARD => { 'estimate' => 8, 'weight' => 2.5, 'progress' => 0.5 } } },
  },
  'settings_field_wise' => {
    description: 'Two devices change different settings. Both land; a sub-object replaces whole.',
    left: { 'settings' => { 'wip_limit' => 5 } },
    right: { 'settings' => { 'banner' => BANNER } },
    merged: { 'settings' => { 'wip_limit' => 5, 'banner' => BANNER } },
  },
  'meta_name_lww' => {
    description: 'Two devices rename the board offline. A deterministic winner, and a concurrent card edit is untouched.',
    left: { 'meta' => { 'name' => 'Left Board' } },
    right: { 'meta' => { 'name' => 'Right Board' }, 'cards' => { CARD => { 'progress' => 0.5 } } },
    merged: { 'meta' => { 'name' => 'Right Board' }, 'cards' => { CARD => { 'progress' => 0.5 } } },
  },
  'concurrent_same_key_create' => {
    description: 'Two devices create the same well-known key offline. Both sets of edits survive.',
    left: {
      'cards' => { 'welcome' => { 'template_id' => 'welcome-template', 'progress' => 0.25 } },
      'columns' => { 'archive' => { 'collapsed' => true } },
    },
    right: {
      'cards' => { 'welcome' => { 'weight' => 0.5 } },
      'columns' => { 'archive' => { 'color' => '#ff0000' } },
    },
    merged: {
      'cards' => { 'welcome' => { 'template_id' => 'welcome-template', 'progress' => 0.25, 'weight' => 0.5 } },
      'columns' => { 'archive' => { 'collapsed' => true, 'color' => '#ff0000' } },
    },
  },
  'unknown_fields_roundtrip' => {
    description: 'A field neither platform models survives an edit from a peer that has never heard of it.',
    left: { 'cards' => { CARD => { 'field_from_the_future' => FUTURE_FIELD } } },
    right: { 'cards' => { CARD => { 'progress' => 0.5 } } },
    merged: { 'cards' => { CARD => { 'field_from_the_future' => FUTURE_FIELD, 'progress' => 0.5 } } },
  },
}.freeze

def write(doc, patch)
  patch.each do |root, changes|
    map = doc.get_map(root)
    changes.each do |key, value|
      if REGISTRIES.include?(root)
        write_entry(map, key, value)
      else
        map.set(key, value)
      end
    end
  end
  doc.commit
end

def write_entry(registry, key, fields)
  if fields
    entry = registry.ensure_mergeable_map(key)
    fields.each { |field, value| entry.set(field, value) }
  else
    registry.delete(key)
  end
end

def base_history
  doc = Loro::Doc.new(peer_id: BASE_PEER)
  write(doc, BOARD)
  doc.export_updates
end

def edit_history(base, peer, patch)
  doc = Loro::Doc.new(peer_id: peer)
  doc.import(base)
  before = doc.version_vector
  write(doc, patch)
  doc.export_updates(since: before)
end

def merged_state(blobs)
  doc = Loro::Doc.new(peer_id: MERGE_PEER)
  blobs.each { doc.import(it) }
  doc.to_h
end

def patched(state, patch)
  state.merge(patch) do |root, current, changes|
    REGISTRIES.include?(root) ? patched_registry(current, changes) : current.merge(changes)
  end
end

def patched_registry(entries, changes)
  changes.each_with_object(entries.dup) do |(key, fields), result|
    if fields
      result[key] = result.fetch(key, {}).merge(fields)
    else
      result.delete(key)
    end
  end
end

def canonical(value)
  case value
  when Hash
    value.sort.to_h { |key, nested| [key, canonical(nested)] }
  when Array
    value.map { canonical(it) }
  else
    value
  end
end

def projection(state)
  ROOTS.to_h { [it, projected_root(it, state.fetch(it, {}))] }
end

def projected_root(root, value)
  return canonical(value) unless REGISTRIES.include?(root)

  value.sort.map { |key, fields| canonical(fields.merge('key' => key)) }
end

def json(value)
  "#{JSON.pretty_generate(value)}\n"
end

def expected(name, kase, blobs)
  merged = json(projection(merged_state(blobs)))
  declared = json(projection(patched(BOARD, kase.fetch(:merged))))
  abort("#{name}: the merge is not the declared outcome\nmerged:\n#{merged}declared:\n#{declared}") unless merged == declared
  merged
end

def manifest(name, kase)
  json('name' => name, 'description' => kase.fetch(:description), 'blobs' => BLOBS,
       'left_peer' => LEFT_PEER, 'right_peer' => RIGHT_PEER)
end

def case_files(name, kase, base)
  edits = [edit_history(base, LEFT_PEER, kase.fetch(:left)), edit_history(base, RIGHT_PEER, kase.fetch(:right))]
  blobs = BLOBS.zip([base, *edits]).to_h
  blobs.merge('expected.json' => expected(name, kase, blobs.values), 'manifest.json' => manifest(name, kase))
end

def index(files)
  cases = files.keys.sort.group_by { File.dirname(it) }.transform_values do |paths|
    paths.to_h { [File.basename(it), Digest::SHA256.hexdigest(files.fetch(it))] }
  end
  json('cases' => cases, 'note' => NOTE, 'source' => SOURCE)
end

def corpus
  base = base_history
  files = CASES.flat_map do |name, kase|
    case_files(name, kase, base).map { |file, bytes| ["#{name}/#{file}", bytes] }
  end.to_h
  files.merge('INDEX.json' => index(files))
end

def write_corpus(files)
  [SOURCE, *COPIES].each do |directory|
    root = File.join(ROOT, directory)
    FileUtils.rm_rf(root)
    files.each do |path, bytes|
      FileUtils.mkdir_p(File.dirname(File.join(root, path)))
      File.binwrite(File.join(root, path), bytes)
    end
  end
end

write_corpus(corpus)
puts "Wrote #{CASES.size} cases to #{SOURCE} and #{COPIES.join(', ')}"
