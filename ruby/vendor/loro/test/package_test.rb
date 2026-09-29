# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require "tmpdir"

class PackageTest < Minitest::Test
  ROOT = File.expand_path("..", __dir__)

  def test_built_gem_contains_sources_but_no_generated_cargo_artifacts
    Dir.mktmpdir("loro-package") do |directory|
      gem_path = File.join(directory, "loro.gem")
      stdout, stderr, status = Open3.capture3(
        Gem.ruby, "-S", "gem", "build", "loro.gemspec", "--output", gem_path,
        chdir: ROOT
      )
      assert status.success?, "gem build failed:\n#{stdout}\n#{stderr}"

      unpack = File.join(directory, "unpack")
      _, unpack_stderr, unpack_status = Open3.capture3(
        Gem.ruby, "-S", "gem", "unpack", gem_path, "--target", unpack
      )
      assert unpack_status.success?, "gem unpack failed: #{unpack_stderr}"

      files = Dir[File.join(unpack, "**/*")].select { File.file?(it) }
      relative = files.map { it.delete_prefix("#{unpack}/loro/") }
      refute relative.any? { it.include?("/target/") || it.start_with?("target/") }
      assert_includes relative, "ext/loro_rb/Cargo.lock"
      assert_includes relative, "ext/loro_rb/src/lib.rs"
      assert_includes relative, "lib/loro.rb"
    end
  end
end
