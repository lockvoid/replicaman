require 'test_helper'

class DeclarationTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u1', name: 'Ada')
    DummyReplica.document(:boards, 'b1').create(user_id: 'u1') { it.get_map('meta').set('name', 'Plans') }
    @board = Board.find('b1')
  end

  def stream
    DummyReplica.streams[:items]
  end

  def push(*ops)
    push_ops(DummyReplica, user: @user, ops: ops)[:verdicts]
  end

  test 'an undeclared store key never serializes' do
    item = Items::TextItem.create!(id: 'i1', board: @board, rank: 'a', label: 'idea',
                                   internal_note: 'provider secret', body: 'x')

    data = stream.serialize(item)

    assert_equal 'idea', data['label']
    assert_not_includes data.keys, 'internalNote'
  end

  test 'a client cannot write through an undeclared store key' do
    error = assert_raises(ReplicaMan::Refused) do
      stream.decode(Items::TextItem, { 'internalNote' => 'sneak' })
    end

    assert_match(/unknown column/, error.message)
  end

  test 'a declared push: false attribute is refused inbound with the same unknown-column shape' do
    guarded = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'items'
      end
      owner ->(item) { item.board&.user_id }
      door ReplicaMan::Normalizer::Row
      attribute :board_id
      attribute :rank, push: false
    end
    guarded.validate!

    error = assert_raises(ReplicaMan::Refused) { guarded.decode(Item, { 'rank' => 'z' }) }

    assert_match(/unknown column: rank/, error.message)
    assert_equal({ 'board_id' => 'b2' }, guarded.decode(Item, { 'boardId' => 'b2' }),
                 'declared push attributes still decode')
  end

  test 'the row identity rides the op envelope — pushing it through data is refused' do
    error = assert_raises(ReplicaMan::Refused) { stream.decode(Items::TextItem, { 'id' => 'i9' }) }

    assert_match(/unknown column: id/, error.message)
  end

  test 'an intake attribute is admitted, drained at the door, and never stored or served' do
    verdicts = push({ id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'TextItem',
                      data: { boardId: 'b1', rank: 'a', body: 'hello', annotation: 'composer-only bytes' } })

    assert_equal [{ id: 'op1', outcome: 'accepted' }], verdicts, 'an intake key is admitted, not refused'
    data = ReplicaMan::Snapshot.find_by!(stream: 'items', row_id: 'i1').data
    assert_not_includes data.keys, 'annotation', 'nothing stores an intake value'

    frames = pull_checkpoint(DummyReplica, user: @user)[:frames]
    item_frame = frames.find { it[:stream] == 'items' && it[:id] == 'i1' }
    assert_not_includes item_frame[:data].keys, 'annotation', 'nothing serves an intake value'
  end

  test 'the door drains an intake value from the decoded attributes' do
    drained = nil
    draining_door = Class.new(ReplicaMan::Normalizer::Row) do
      define_method(:create) do |replica, stream, op|
        klass = stream.variant_class(op.type) || stream.model
        drained = stream.decode(klass, op.data)['annotation']
        super(replica, stream, op)
      end
    end
    drainer = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'items'
      end
      owner ->(item) { item.board&.user_id }
      attribute :id, :board_id, :rank, :label
      attribute :annotation, :string, pull: false
      variant Items::TextItem do
        attribute :body
      end
    end
    drainer.door(draining_door)
    drainer.validate!
    drainer.replica = DummyReplica

    op = ReplicaMan::Op.new(
      { 'id' => 'op1', 'op' => 'row.create', 'stream' => 'items', 'row_id' => 'i7', 'type' => 'TextItem',
        'data' => { 'boardId' => 'b1', 'rank' => 'a', 'body' => 'x', 'annotation' => 'poster bytes' } },
      user: @user
    )
    drainer.normalizer.create(DummyReplica, drainer, op)

    assert_equal 'poster bytes', drained, 'op.data still carries the intake value for the door'
    assert_nil Item.find('i7').read_attribute(:metadata)['annotation'], 'the row writer never saw it'
  end

  def misdeclared(&body)
    klass = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'items'
      end
      door ReplicaMan::Normalizer::Row
    end
    klass.class_eval(&body)
    klass
  end

  test 'a declared name matching neither column nor store key fails the boot' do
    bogus = misdeclared { attribute :nonsense }

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/declares unknown attribute: nonsense/, error.message)
  end

  test 'a scalar column refuses a declared type — introspection owns it' do
    bogus = misdeclared { attribute :rank, :string }

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/column type is introspected/, error.message)
  end

  test 'a typed store key refuses a declared type — introspection owns it' do
    bogus = misdeclared { attribute :label, :string }

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/typed store key is introspected/, error.message)
  end

  test 'a json column refuses a scalar type — its type positional is a shape source' do
    bogus = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'jobs'
      end
      attribute :payload, :json
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/shape source/, error.message)
  end

  test 'a columnless attribute requires a pull direction' do
    undirected = misdeclared { attribute :ghost, :string }
    error = assert_raises(ReplicaMan::Stream::Invalid) { undirected.validate_schema! }
    assert_match(/declare pull: false \(intake\) or a pull lambda/, error.message)
  end

  test 'push: never takes a lambda — inbound judgment is the door monopoly' do
    error = assert_raises(ReplicaMan::Stream::Invalid) do
      misdeclared { attribute :rank, push: -> { } }
    end
    assert_match(/push: is boolean only/, error.message)
  end

  test 'a document block needs a document door' do
    bogus = misdeclared do
      document do
        attribute :rank
      end
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate! }
    assert_match(/a document block needs a document door/, error.message)
  end

  test 'a document door needs its document declared' do
    bogus = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'boards'
      end
      door BoardNormalizer
      attribute :user_id
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate! }
    assert_match(/needs its document declared/, error.message)
  end

  test 'a document attribute travels in the deltas — it takes no directions' do
    error = assert_raises(ReplicaMan::Stream::Invalid) do
      misdeclared do
        document do
          attribute :rank, push: true
        end
      end
    end
    assert_match(/travels in the document's deltas/, error.message)
  end

  test 'a document attribute names an attribute of the model' do
    bogus = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'boards'
      end
      door BoardNormalizer
      document do
        attribute :nonsense
      end
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/document attribute :nonsense is not an attribute of Board/, error.message)
  end

  test 'push flags on a doorless stream fail the boot — no door, no inbound' do
    bogus = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'tickets'
      end
      attribute :note, push: false
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate! }
    assert_match(/has no door/, error.message)
  end

  test 'the identity column takes no type and no directions' do
    bogus = misdeclared { attribute :id, push: false }

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/identity rides the op envelope/, error.message)
  end

  test 'the STI discriminator is never declared — it rides op.type' do
    bogus = misdeclared { attribute :type, :string, pull: false }

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/STI discriminator rides op.type/, error.message)
  end

  test 'a stream must be named after its table — one name, zero aliases' do
    bogus = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'board_rows'
      end
      model 'Board'
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate! }
    assert_match(/must be named after its table 'boards'/, error.message)
  end

  test 'a variant must be a subclass of the stream model' do
    bogus = misdeclared { variant Board }

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate! }
    assert_match(/variant Board is not a Item subclass/, error.message)
  end

  test 'door variant: restricts client creates to the declared subclass' do
    restricted = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'items'
      end
      owner ->(item) { item.board&.user_id }
      door ReplicaMan::Normalizer::Row, variant: Items::TextItem
      attribute :id, :board_id, :rank, :label
      variant Items::TextItem do
        attribute :body
      end
      variant Items::PhotoItem do
        attribute :caption, :width
      end
    end
    restricted.validate!
    world = Class.new(ReplicaMan::Replica)
    world.namespace DummyReplica.namespace
    world.dataset_epoch 'test'
    world.use(ReplicaMan::Loro)
    world.stream(restricted)

    refused = push_ops(world, user: @user, ops: [
      { id: 'op1', op: 'row.create', stream: 'items', row_id: 'i1', type: 'PhotoItem',
        data: { boardId: 'b1', rank: 'a', caption: 'wide', width: 3 } }
    ])[:verdicts].sole
    assert_equal 'rejected', refused[:outcome]
    assert_match(/"PhotoItem" rows are server-authored/, refused[:reason])

    accepted = push_ops(world, user: @user, ops: [
      { id: 'op2', op: 'row.create', stream: 'items', row_id: 'i2', type: 'TextItem',
        data: { boardId: 'b1', rank: 'a', body: 'raw fact' } }
    ])[:verdicts].sole
    assert_equal 'accepted', accepted[:outcome]
  end

  test 'the model infers from the stream name' do
    assert_equal Board, Streams::Boards.model
    assert_equal Ticket, Streams::Tickets.model, 'flat names classify directly'
  end

  test 'an index names a pulled base attribute — declare the attribute first' do
    bogus = misdeclared { index :ghost }

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/index :ghost .*declare the attribute first/, error.message)

    intake = misdeclared { attribute :annotation, :string, pull: false; index :annotation }
    error = assert_raises(ReplicaMan::Stream::Invalid) { intake.validate_schema! }
    assert_match(/index :annotation .*declare the attribute first/, error.message,
                 'an intake attribute is never stored, so nothing exists to index')
  end

  test 'an index kind names a structure: btree or fts5' do
    error = assert_raises(ReplicaMan::Stream::Invalid) { misdeclared { index :rank, kind: :hash } }
    assert_match(/index kind :hash/, error.message)
  end

  test 'a full-text index needs a string value' do
    bogus = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'jobs'
      end
      attribute :tags
      index :tags, kind: :fts5
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { bogus.validate_schema! }
    assert_match(/index :tags \(fts5\).*cannot be indexed/, error.message)
  end

  test 'declaring a stream reads no database; the schema check propagates an unavailable one' do
    indexed = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'jobs'
      end
      owner :user_id
      attribute :rank
      index :rank
    end
    refused = proc { raise ActiveRecord::ConnectionNotEstablished, 'connection to server at "127.0.0.1", port 5432 failed' }
    %i[table_exists? columns_hash primary_key].each { Job.define_singleton_method(it, &refused) }

    assert_nothing_raised { indexed.validate! }
    assert_raises(ActiveRecord::ConnectionNotEstablished) { indexed.validate_schema! }
  ensure
    %i[table_exists? columns_hash primary_key].each { Job.singleton_class.send(:remove_method, it) }
  end

  test 'the manifest checks the declarations against the migrated schema' do
    error = assert_raises(ReplicaMan::Stream::Invalid) { with_unknown_attribute { ReplicaMan::Manifest.new(DummyReplica).to_h } }
    assert_match(/declares unknown attribute: nonsense/, error.message)
  end

  test 'the schema check refuses an attribute the migrated schema does not have' do
    ahead = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'jobs'
      end
      attribute :added_by_the_pending_migration
    end

    error = assert_raises(ReplicaMan::Stream::Invalid) { ahead.validate_schema! }
    assert_match(/unknown attribute: added_by_the_pending_migration/, error.message)
  end

  private

  def with_unknown_attribute
    bogus = Class.new(ReplicaMan::Stream) do
      def self.stream_name
        'jobs'
      end
      owner :user_id
      attribute :nonsense
    end
    streams = DummyReplica.streams
    DummyReplica.define_singleton_method(:streams) { streams.merge(jobs: bogus) }
    yield
  ensure
    DummyReplica.singleton_class.send(:remove_method, :streams)
  end
end
