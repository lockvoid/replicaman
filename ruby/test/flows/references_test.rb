require 'test_helper'

class ReferencesTest < ActiveSupport::TestCase
  setup do
    @user = User.create!(id: 'u', name: 'Owner')
    @board = create_board!(id: 'parent', user: @user, name: 'First lifetime')
    @client = ProtocolClient.new(@user)
    @original_references = Streams::Items.references.dup
    @original_lifetime = Streams::Items.lifetime_from
    Streams::Items.reference :board_id, stream: :boards
  end

  teardown do
    Streams::Items.instance_variable_set(:@references, @original_references)
    Streams::Items.instance_variable_set(:@lifetime_from, @original_lifetime)
  end

  def parent_reference
    { 'name' => 'boardId', 'stream' => 'boards', 'id' => @board.id,
      'incarnation' => @client.incarnation('boards', @board.id) }
  end

  def birth(id, references: [parent_reference], incarnation: SecureRandom.uuid)
    { 'id' => "op-#{id}", 'op' => 'row.create', 'stream' => 'items', 'row_id' => id,
      'incarnation' => incarnation, 'type' => 'TextItem',
      'data' => { 'boardId' => @board.id, 'rank' => 'a', 'body' => 'offline authoring' },
      'references' => references }
  end

  def outcome(operation)
    @client.push(operation.merge('id' => DomainClient.uuid(operation.fetch('id')))).fetch(:verdicts).first.fetch(:outcome)
  end

  test 'a declaration cannot be bypassed by omitting or substituting the parent reference' do
    assert_equal 'rejected', outcome(birth('missing', references: []))
    wrong = parent_reference.merge('id' => 'different')
    assert_equal 'rejected', outcome(birth('wrong', references: [wrong]))
    assert_equal 'accepted', outcome(birth('valid'))
    assert_equal ['valid'], Item.pluck(:id)
  end

  test 'a frozen child cannot attach to a replacement parent with the same business ID' do
    pending = birth('offline')
    first_lifetime = parent_reference.fetch('incarnation')
    @board.destroy!
    @board = create_board!(id: 'parent', user: @user, name: 'Replacement')
    refute_equal first_lifetime, parent_reference.fetch('incarnation')

    assert_equal 'rejected', outcome(pending)
    assert_nil Item.find_by(id: 'offline')
    assert_equal 'accepted', outcome(birth('new-authoring'))
  end

  test 'derived births use the same identity on the server and an offline client' do
    Streams::Items.lifetime_from :board_id
    parent = parent_reference
    parts = ['replicaman:derived:1', DummyReplica.namespace, 'items', 'derived',
             'boards', @board.id, parent.fetch('incarnation')]
    expected = 'derived:' + Digest::SHA256.hexdigest(parts.map { "#{it.bytesize}:#{it}" }.join)
    Items::TextItem.create!(id: 'derived', board: @board, rank: 'a', body: 'server')
    assert_equal expected, @client.incarnation('items', 'derived')

    patch = { 'id' => 'patch', 'op' => 'row.patch', 'stream' => 'items', 'row_id' => 'derived',
              'incarnation' => expected, 'references' => [parent], 'data' => { 'body' => 'offline' } }
    assert_equal 'accepted', outcome(patch)
    assert_equal 'offline', Item.find('derived').body
  end

  test 'a parent created in the same transaction receives its lifetime before child capture' do
    Streams::Items.lifetime_from :board_id
    DummyReplica.transaction do
      @board = create_board!(id: 'new-parent', user: @user, name: 'New')
      Items::TextItem.create!(id: 'child', board: @board, rank: 'a', body: 'dependent')
    end
    parent = parent_reference
    expected = ReplicaMan::References.derived_incarnation(Streams::Items, 'child', [parent])
    assert_equal expected, @client.incarnation('items', 'child')
    refute_nil expected
  end

  test 'a derived tombstone cannot be resurrected during the same parent lifetime' do
    Streams::Items.lifetime_from :board_id
    operation = birth('derived', incarnation: ReplicaMan::References.derived_incarnation(
      Streams::Items, 'derived', [parent_reference]))
    assert_equal 'accepted', outcome(operation)
    Item.find('derived').destroy!
    DummyReplica.gc(window: 0.seconds)
    operation['id'] = 'late-birth'
    assert_equal 'rejected', outcome(operation)
    assert_nil Item.find_by(id: 'derived')
  end
end
