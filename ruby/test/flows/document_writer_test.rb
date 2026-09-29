require 'test_helper'
require 'replica_man/loro'

class DocumentWriterTest < ActiveSupport::TestCase
  Writer = ReplicaMan::Loro::Writer

  test 'a stale save preserves unseen additions and peer deletions without echoing unchanged fields' do
    doc = Loro::Doc.new(peer_id: 7)
    base = { 'a' => { 'value' => 1 }, 'b' => { 'value' => 2 }, 'd' => { 'value' => 4 } }
    Writer.write_registry(doc, 'items', base)
    doc.get_map('items').delete('b')
    Writer.write_field(doc.get_map('items').ensure_mergeable_map('a'), 'value', 9)
    Writer.write_field(doc.get_map('items').ensure_mergeable_map('c'), 'value', 3)
    Writer.write_registry(doc, 'items', base.slice('a', 'b'), base: base)
    assert_equal({ 'a' => { 'value' => 9 }, 'c' => { 'value' => 3 } }, doc.to_h['items'])
  end

  test 'independent same-key creation merges the two field sets' do
    left = Loro::Doc.new(peer_id: 7)
    right = Loro::Doc.new(peer_id: 8)
    Writer.write_registry(left, 'items', { 'shared' => { 'title' => 'left' } })
    Writer.write_registry(right, 'items', { 'shared' => { 'count' => 2 } })
    left.import(right.export_snapshot)
    right.import(left.export_snapshot)
    assert_equal({ 'shared' => { 'title' => 'left', 'count' => 2 } }, left.to_h['items'])
    assert_equal left.to_h, right.to_h
  end

  test 'a refused child reaches the transaction owner without replacing its value' do
    doc = Loro::Doc.new(peer_id: 7)
    doc.get_map('items').set('broken', 'peer value')
    assert_raises(Loro::Error) { Writer.write_registry(doc, 'items', { 'broken' => { 'title' => 'unsaved' } }) }
    assert_equal({ 'broken' => 'peer value' }, doc.to_h['items'])
  end

  test 'idle saves and null writes to absent fields produce no operations' do
    doc = Loro::Doc.new(peer_id: 7)
    entries = { 'a' => { 'value' => 1 } }
    Writer.write_registry(doc, 'items', entries)
    doc.commit
    before = doc.version_vector
    Writer.write_registry(doc, 'items', entries, base: entries)
    Writer.write_field(doc.get_map('items').ensure_mergeable_map('a'), 'absent', nil)
    doc.commit
    assert_equal before, doc.version_vector
    assert_equal({ 'value' => 1 }, doc.to_h['items']['a'])
  end

  test 'an authoritative empty registry deletes live entries' do
    doc = Loro::Doc.new(peer_id: 7)
    Writer.write_registry(doc, 'items', { 'a' => { 'value' => 1 } })
    Writer.write_registry(doc, 'items', {})
    assert_empty doc.to_h['items']
  end
end
