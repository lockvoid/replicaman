require 'test_helper'

class ManifestTest < ActiveSupport::TestCase
  test 'the manifest derives the full wire shape from declarations plus AR introspection' do
    manifest = DummyReplica.manifest

    assert_equal 1, manifest[:version]
    assert_equal %w[boards decks exports item_templates items jobs tallies themes tickets workflows], manifest[:streams].map { it[:name] }

    boards, items, jobs, tallies, tickets = manifest[:streams].index_by { it[:name] }.values_at(*%w[boards items jobs tallies tickets])

    assert_equal 'document', boards[:lane]
    assert_equal 'loro@1', boards[:codec]
    assert_equal false, boards[:readonly]
    assert_equal 'user', boards[:shard]
    assert_equal false, boards[:sti]
    assert_equal [
      { name: 'name', type: 'string', null: true, push: false, pull: true, reflects: %w[meta name] },
      { name: 'userId', type: 'string', null: false, push: true, pull: true }
    ], boards[:columns], 'a row attribute the document also declares is its reflection — never pushed, ' \
                         'the path it reflects ships so the client derives it from its own document'
    assert_equal({
      'meta' => {
        type: 'object',
        fields: [{ name: 'name', type: 'string', null: false, default: 'Untitled' }],
      },
    }, boards[:shapes], 'document roots are source-model declarations introspected by the manifest')
    assert_equal({ 'meta' => { 'name' => 'Untitled' } }, boards[:default],
                 'the document seed is manifest truth beside its shapes')
    assert_nil boards[:variants], 'a non-STI stream carries no variants key'

    assert_equal 'row', items[:lane]
    assert_equal true, items[:sti]
    assert_equal [
      { name: 'annotation', type: 'string', null: true, push: true, pull: false },
      { name: 'boardId', type: 'string', null: false, push: true, pull: true },
      { name: 'label', type: 'string', null: true, enum: %w[idea task], push: true, pull: true },
      { name: 'rank', type: 'string', null: false, push: true, pull: true },
      { name: 'rankBadge', type: 'string', null: true, push: false, pull: true }
    ], items[:columns], 'id, the STI discriminator and the typed store container never ship as columns; ' \
                        'declared store keys ship as typed base columns (always nullable); ' \
                        'an intake attribute ships push-only (pull: false), a computed one pull-only — ' \
                        'directions are DECLARED per attribute, codegen needs no heuristic'
    assert_equal [
      { type: 'PhotoItem',
        columns: [{ name: 'caption', type: 'string', null: true, enum: %w[wide square], push: true, pull: true },
                  { name: 'width', type: 'integer', null: true, push: true, pull: true }] },
      { type: 'TextItem', columns: [{ name: 'body', type: 'string', null: false, push: true, pull: true }] }
    ], items[:variants], 'variant-declared store keys appear only on their variant, demodulized wire names, ' \
                         "nullable exactly as the variant's own validators declare"

    assert_equal 'row', jobs[:lane]
    assert_equal true, jobs[:readonly], 'no door reads as readonly — the flag is derived, never declared'
    assert_equal [
      { name: 'payload', type: 'jsonb', null: true, push: false, pull: true,
        shapes: {
          discriminator: 'kind',
          variants: [
            {
              value: 'empty',
              type: 'Empty',
              fields: [{ name: 'reasonCode', type: 'string', null: true }],
            },
            {
              value: 'ready',
              type: 'Ready',
              fields: [
                { name: 'displayName', type: 'string', null: false },
                {
                  name: 'history',
                  type: 'array',
                  null: false,
                  default: [],
                  items: {
                    type: 'object',
                    fields: [
                      { name: 'meanConfidence', type: 'float', null: true },
                      { name: 'sampleCount', type: 'integer', null: true }
                    ],
                  },
                },
                {
                  name: 'metrics',
                  type: 'object',
                  null: true,
                  fields: [
                    { name: 'meanConfidence', type: 'float', null: true },
                    { name: 'sampleCount', type: 'integer', null: true }
                  ],
                }
              ],
            }
          ],
        } },
      { name: 'priority', type: 'string', null: false, enum: %w[low high], push: false, pull: true },
      { name: 'state', type: 'string', null: false, enum: %w[queued running done], push: false, pull: true },
      { name: 'summary', type: 'json', null: true, push: false, pull: true,
        shapes: {
          type: 'object',
          fields: [
            { name: 'active', type: 'boolean', null: false, default: false },
            {
              name: 'labels',
              type: 'array',
              null: true,
              default: [],
              items: { type: 'string', enum: %w[system custom] },
            },
            {
              name: 'metricsByKey',
              type: 'map',
              null: false,
              default: {},
              values: {
                type: 'object',
                fields: [
                  { name: 'meanConfidence', type: 'float', null: true },
                  { name: 'sampleCount', type: 'integer', null: true }
                ],
              },
            },
            { name: 'state', type: 'string', null: true, enum: %w[queued running done], default: 'queued' },
            { name: 'token', type: 'string', null: true }
          ],
        } },
      { name: 'tags', type: 'array', null: false, items: { type: 'string' }, push: false, pull: true },
      { name: 'userId', type: 'string', null: false, push: false, pull: true }
    ], jobs[:columns], 'a computed value declares its shape source as its type ' \
                       '(`attribute :summary, JobPayloads::Summary, pull: ->`); a union column carries ' \
                       'the union declared AT THE TYPE; a Postgres array column ships its item type'

    assert_equal [
      { name: 'note', type: 'string', null: true, push: false, pull: true },
      { name: 'userId', type: 'string', null: false, push: false, pull: true }
    ], tickets[:columns], 'neither the surrogate id nor the natural key ship as columns'

    assert_equal [
      { name: 'count', type: 'integer', null: false, push: true, pull: true },
      { name: 'status', type: 'string', null: false, push: true, pull: true, precondition: true },
      { name: 'userId', type: 'string', null: false, push: true, pull: true },
      { name: 'version', type: 'integer', null: false, push: true, pull: true, precondition: true }
    ], tallies[:columns], 'a precondition rides every write of its row — the client carries it, changed or not'
  end

  test "the committed consumer manifest is the dummy server's manifest, byte for byte" do
    fixture = File.expand_path('../../../protocol/fixtures/consumer-manifest.json', __dir__)

    assert_equal File.read(fixture), ReplicaMan::Manifest.new(DummyReplica).to_json,
                 'every client builds its codegen contract from this declaration — regenerate the fixture, never hand-edit it'
  end

  test 'declared indexes ride the manifest as sorted (field, structure) derivatives' do
    streams = DummyReplica.manifest[:streams].index_by { it[:name] }

    assert_equal [{ field: 'boardId', kind: 'btree' }], streams.fetch('items')[:indexes],
                 'an index is a READ derivative of a pulled attribute — the client read-plan, reviewed here; ' \
                 'kind names the physical structure (btree by default), never an operator'
    assert_equal [
      { field: 'note', kind: 'fts5' },
      { field: 'userId', kind: 'btree' }
    ], streams.fetch('tickets')[:indexes], 'full text is its own structure; order is (field, kind)'
    assert_nil streams.fetch('jobs')[:indexes], 'a stream with no index declares no key at all'
    assert_nil streams.fetch('boards')[:indexes]
  end

  %w[pull push].each do |direction|
    test "#{direction} translates a declared shape without rewriting typed map identities" do
      stream = Class.new(ReplicaMan::Stream) do
        def self.stream_name
          'jobs'
        end
        door ReplicaMan::Normalizer::Row
        attribute :payload, JobPayloads::Summary
      end
      stored = { 'state' => 'queued', 'active' => false,
                 'metrics_by_key' => { 'utterance_one' => { 'sample_count' => 2, 'mean_confidence' => 0.9 } } }
      wire = { 'state' => 'queued', 'active' => false,
               'metricsByKey' => { 'utterance_one' => { 'sampleCount' => 2, 'meanConfidence' => 0.9 } } }

      if direction == 'pull'
        assert_equal wire, stream.serialize(Job.new(payload: stored)).fetch('payload')
      else
        assert_equal stored, stream.decode(Job, { 'payload' => wire }).fetch('payload')
      end
    end
  end

  test 'a union translates its selected variant and nested arrays in both directions' do
    stream = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'jobs'
      end
      door ReplicaMan::Normalizer::Row
      attribute :payload, JobPayloads::Polymorphic
    end
    stored = { 'kind' => 'ready', 'display_name' => 'Ready',
               'history' => [{ 'sample_count' => 2, 'mean_confidence' => 0.9 }], 'metrics' => nil }
    wire = { 'kind' => 'ready', 'displayName' => 'Ready',
             'history' => [{ 'sampleCount' => 2, 'meanConfidence' => 0.9 }], 'metrics' => nil }
    assert_equal wire, stream.serialize(Job.new(payload: stored)).fetch('payload')
    assert_equal stored, stream.decode(Job, { 'payload' => wire }).fetch('payload')
    assert_nil stream.decode(Job, { 'payload' => nil }).fetch('payload')
  end

  test 'the manifest refuses a schema behind its migrations' do
    context = Job.connection_pool.migration_context
    context.define_singleton_method(:needs_migration?) { true }
    Job.connection_pool.define_singleton_method(:migration_context) { context }

    error = assert_raises(ReplicaMan::Manifest::Stale) { DummyReplica.manifest }
    assert_match(/behind its migrations/, error.message)
  ensure
    Job.connection_pool.singleton_class.send(:remove_method, :migration_context)
  end

  test 'a json field inside a declared shape ships the value its model holds' do
    shape = Class.new do
      include StoreModel::Model

      def self.name
        'ProbeShape'
      end

      attribute :source, :string
      attribute :options, ActiveRecord::Type::Json.new, default: -> { {} }
    end
    held = { 'blur_hash' => 1, 'slotOne' => { 'a_b' => 2 } }
    stream = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'jobs'
      end
      attribute :probe, shape, pull: ->(_job) { shape.new(source: 's', options: held) }
    end

    wire = stream.serialize(Job.new).fetch('probe')

    assert_equal 's', wire.fetch('source')
    assert_equal held, wire.fetch('options')
  end
end
