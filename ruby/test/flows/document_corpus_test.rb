require 'test_helper'
require 'digest'

class DocumentCorpusTest < ActiveSupport::TestCase
  ROOT = File.expand_path('../../../protocol/fixtures/crdt_convergence', __dir__)
  INDEX = JSON.parse(File.read(File.join(ROOT, 'INDEX.json'))).fetch('cases')
  raise 'the frozen nine-case corpus must execute' unless INDEX.size == 9

  INDEX.each do |name, files|
    test "#{name}: server codec preserves the frozen projection across reorder, replay and reopen" do
      directory = File.join(ROOT, name)
      files.each do |file, digest|
        assert_equal digest, Digest::SHA256.file(File.join(directory, file)).hexdigest, "#{name}/#{file} changed"
      end
      manifest = JSON.parse(File.read(File.join(directory, 'manifest.json')))
      blobs = manifest.fetch('blobs').map { File.binread(File.join(directory, it)) }
      assert_equal 3, blobs.size
      expected = JSON.parse(File.read(File.join(directory, 'expected.json'))).transform_values do |value|
        value.is_a?(Array) ? value.to_h { |entry| [entry.fetch('key'), entry.except('key')] } : value
      end
      codec = ReplicaMan::Loro::Codec.new
      [[0, 1, 2, 0, 1, 2], [0, 2, 1, 2, 1, 0]].each do |order|
        doc = codec.blank
        order.each { codec.merge(doc, blobs[it]) }
        reopened = codec.load(codec.fold(doc)).to_h
        expected.each_key { reopened[it] ||= {} }
        assert_equal expected, reopened, "#{name}: order #{order}"
      end
    end
  end
end
