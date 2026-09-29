require 'minitest/autorun'
require 'json'
require 'tmpdir'
require 'open3'
require_relative '../lib/manifest'

class CompilerTest < Minitest::Test
  ROOT = File.expand_path('../..', __dir__)
  CLI = File.join(ROOT, 'codegen/bin/replica-codegen')

  def manifest(name = 'notes')
    { 'version' => 1, 'namespace' => 'example.notes', 'schemaVersion' => 7, 'streams' => [{ 'name' => name, 'lane' => 'row', 'shard' => 'user',
      'readonly' => false, 'sti' => false, 'columns' => [
        { 'name' => 'title', 'type' => 'string', 'null' => false, 'push' => true, 'pull' => true },
        { 'name' => 'count', 'type' => 'bigint', 'null' => true, 'push' => true, 'pull' => true },
      ] }] }
  end

  def document_stream(name)
    { 'name' => name, 'lane' => 'document', 'codec' => 'loro@1', 'shard' => 'user', 'readonly' => false, 'sti' => false,
      'columns' => [{ 'name' => 'title', 'type' => 'string', 'null' => true, 'push' => false, 'pull' => true, 'reflects' => %w[meta title] }],
      'shapes' => { 'meta' => { 'type' => 'object', 'fields' => [{ 'name' => 'title', 'type' => 'string', 'null' => false, 'default' => 'Untitled' }] } },
      'default' => { 'meta' => { 'title' => 'Untitled' } } }
  end

  def union_manifest
    variants = { 'ThumbnailResult' => 'thumbnail', 'PreviewResult' => 'preview' }.map do |type, value|
      { 'type' => type, 'value' => value, 'fields' => [{ 'name' => 'width', 'type' => 'integer', 'null' => false }] }
    end
    manifest.tap do
      it['streams'][0]['columns'] << { 'name' => 'result', 'type' => 'jsonb', 'null' => true, 'push' => true, 'pull' => true,
                                       'shapes' => { 'discriminator' => 'kind', 'variants' => variants } }
    end
  end

  def generated_source(dir)
    Dir.glob('**/*', base: dir).select { File.file?(File.join(dir, it)) }.sort.map { File.read(File.join(dir, it)) }.join
  end

  def generate(language, input, out, *options)
    command = [RbConfig.ruby, CLI, '--language', language, '--manifest', input, '--out', out]
    command += ['--package', 'example.generated'] if language == 'kotlin'
    Open3.capture3(*command, *options)
  end

  def test_every_language_refuses_an_invalid_manifest_before_changing_output
    %w[swift kotlin rust].each do |language|
      Dir.mktmpdir do |dir|
        input, out = File.join(dir, 'manifest.json'), File.join(dir, 'out')
        File.write(input, JSON.generate(manifest))
        _, error, status = generate(language, input, out)
        assert status.success?, error
        original = Dir.glob('*', base: out).to_h { [it, File.binread(File.join(out, it))] }
        broken = manifest('../escape')
        File.write(input, JSON.generate(broken))
        _, _, status = generate(language, input, out)
        refute status.success?, language
        assert_equal original, Dir.glob('*', base: out).to_h { [it, File.binread(File.join(out, it))] }
        refute File.exist?(File.join(dir, 'escape'))
      end
    end
  end

  def test_missing_or_invalid_protocol_identity_is_refused
    [nil, '', 'space in namespace', 'a' * 129].each do |namespace|
      input = manifest.merge('namespace' => namespace)
      assert_raises(ReplicaCodegen::InvalidManifest) { ReplicaCodegen::Manifest.new(input).compile }
    end
    [nil, 0, -1, '1', 2_147_483_648].each do |version|
      input = manifest.merge('schemaVersion' => version)
      assert_raises(ReplicaCodegen::InvalidManifest) { ReplicaCodegen::Manifest.new(input).compile }
    end
  end

  def test_stale_generated_files_are_removed_but_handwritten_files_survive
    %w[swift kotlin rust].each do |language|
      Dir.mktmpdir do |dir|
        input, out = File.join(dir, 'manifest.json'), File.join(dir, 'out')
        File.write(input, JSON.generate(manifest))
        assert generate(language, input, out).last.success?
        File.write(File.join(out, 'handwritten.txt'), 'keep')
        File.write(input, JSON.generate(manifest('tasks')))
        refute generate(language, input, out, '--check').last.success?
        assert generate(language, input, out).last.success?
        assert generate(language, input, out, '--check').last.success?
        assert_equal 'keep', File.read(File.join(out, 'handwritten.txt'))
        refute Dir.glob('*', base: out).any? { it.match?(/\Anote\./i) }
      end
    end
  end

  def test_every_rust_document_declares_its_own_meta_type
    Dir.mktmpdir do |dir|
      input, documents = File.join(dir, 'manifest.json'), File.join(dir, 'documents')
      File.write(input, JSON.generate(manifest.merge('streams' => [document_stream('boards'), document_stream('decks')])))
      _, error, status = generate('rust', input, File.join(dir, 'rows'), '--document-out', documents)
      assert status.success?, error
      declared = generated_source(documents).scan(/^pub (?:enum|struct|trait) (\w+)/).flatten
      assert_equal declared.uniq, declared
      assert_includes declared, 'BoardDocumentMeta'
      assert_includes declared, 'DeckDocumentMeta'
    end
  end

  def test_union_variants_keep_their_wire_type_names_unless_the_config_renames_them
    renames = { 'variants' => { 'notes' => { 'ThumbnailResult' => 'Thumbnail', 'PreviewResult' => 'Preview' } } }
    %w[swift kotlin rust].each do |language|
      Dir.mktmpdir do |dir|
        input, config = File.join(dir, 'manifest.json'), File.join(dir, 'config.json')
        File.write(input, JSON.generate(union_manifest))
        File.write(config, JSON.generate(renames))
        assert generate(language, input, File.join(dir, 'wire')).last.success?, language
        assert_includes generated_source(File.join(dir, 'wire')), 'ThumbnailResult', language
        assert generate(language, input, File.join(dir, 'renamed'), '--config', config).last.success?, language
        renamed = generated_source(File.join(dir, 'renamed'))
        assert_includes renamed, 'Thumbnail', language
        refute_includes renamed, 'ThumbnailResult', language
      end
    end
  end

  def test_two_union_variants_renamed_alike_are_refused
    clash = { 'variants' => { 'notes' => { 'ThumbnailResult' => 'Image', 'PreviewResult' => 'Image' } } }
    assert_raises(ReplicaCodegen::InvalidManifest) { ReplicaCodegen::Manifest.new(union_manifest, clash).compile }
  end

  def test_semantics_are_shared_and_not_product_specific
    assert_equal 'Bus', ReplicaCodegen::Manifest.new(manifest('buses')).compile['streams'][0]['modelName']
    invalid = manifest
    invalid['streams'][0]['columns'][0]['pull'] = false
    assert_raises(ReplicaCodegen::InvalidManifest) { ReplicaCodegen::Manifest.new(invalid).compile }
    invalid = manifest
    invalid['streams'] << invalid['streams'].first.dup
    assert_raises(ReplicaCodegen::InvalidManifest) { ReplicaCodegen::Manifest.new(invalid).compile }
  end
end
