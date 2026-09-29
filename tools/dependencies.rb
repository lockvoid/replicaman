#!/usr/bin/env ruby
# Records resolved Rust/native dependencies and their distributed license files.

require 'digest'
require 'fileutils'
require 'json'
require 'open3'

ROOT = File.expand_path('..', __dir__)
OUT = File.join(ROOT, 'build/dependencies')
MANIFESTS = ['rust/Cargo.toml', 'kotlin/libraries/loro/rust/Cargo.toml', 'ruby/vendor/loro/ext/loro_rb/Cargo.toml'].freeze
LICENSE_NAMES = /\A(LICEN[CS]E|COPYING|NOTICE|AUTHORS)/i
UPSTREAM = File.join(ROOT, 'tools/upstream-licenses')

def metadata(manifest)
  output, status = Open3.capture2('cargo', 'metadata', '--locked', '--format-version', '1',
                                  '--manifest-path', File.join(ROOT, manifest), chdir: ROOT)
  abort("cargo metadata failed for #{manifest}") unless status.success?
  JSON.parse(output)
end

def license_files(package)
  directory = File.dirname(package.fetch('manifest_path'))
  files = Dir.children(directory).select { it.match?(LICENSE_NAMES) }.map { File.join(directory, it) }
  declared = package.fetch('license_file', nil)
  files << File.join(directory, declared) if declared && File.file?(File.join(directory, declared))
  files = files.select { File.file?(it) }.uniq.sort
  return files unless files.empty?

  key = package.fetch('name') == 'serde_columnar_derive' ? 'serde_columnar@0.3.14' : "#{package.fetch('name')}@#{package.fetch('version')}"
  upstream = File.join(UPSTREAM, key)
  File.directory?(upstream) ? Dir.children(upstream).map { File.join(upstream, it) }.select { File.file?(it) }.sort : []
end

FileUtils.mkdir_p(OUT)
packages = {}
locks = {}
notices = []

MANIFESTS.each do |manifest|
  lock = File.join(File.dirname(File.join(ROOT, manifest)), 'Cargo.lock')
  locks[lock.delete_prefix("#{ROOT}/")] = Digest::SHA256.file(lock).hexdigest

  metadata(manifest).fetch('packages').each do |package|
    next unless package.fetch('source', nil)

    key = "#{package.fetch('name')}@#{package.fetch('version')}"
    if packages.key?(key)
      packages.fetch(key).fetch('workspaces') << manifest
      next
    end

    files = license_files(package)
    packages[key] = {
      'name' => package.fetch('name'), 'version' => package.fetch('version'),
      'license' => package.fetch('license', nil), 'repository' => package.fetch('repository', nil),
      'source' => package.fetch('source'), 'workspaces' => [manifest],
      'license_files' => files.map { File.basename(it) },
    }
    notices << "\n#{'=' * 72}\n#{key}\nLicense: #{package.fetch('license', nil)}\nSource: #{package.fetch('repository', nil)}\n"
    files.each { notices << "\n#{File.basename(it)}\n#{File.read(it, encoding: 'UTF-8', invalid: :replace, undef: :replace)}" }
  end
end

report = { 'lock_sha256' => locks, 'packages' => packages.keys.sort.map { packages.fetch(it) } }
File.write(File.join(OUT, 'rust-native.json'), "#{JSON.pretty_generate(report)}\n")
File.write(File.join(OUT, 'THIRD-PARTY-NOTICES.txt'),
           "ReplicaMan dependency license inventory. Includes build/test dependencies conservatively.\n#{notices.join("\n")}")

# Android consumes both the binding JAR and the native AAR; each keeps its own notices.
resources = File.join(OUT, 'resources/META-INF/replicaman-loro-binding')
FileUtils.rm_rf(File.join(OUT, 'resources'))
FileUtils.mkdir_p(resources)
%w[THIRD-PARTY-NOTICES.txt rust-native.json].each { FileUtils.cp(File.join(OUT, it), resources) }
android = File.join(ROOT, 'kotlin/libraries/loro-android/src/main/resources/META-INF/replicaman-loro-android')
FileUtils.rm_rf(File.dirname(android) + '/replicaman')
FileUtils.mkdir_p(android)
Dir.children(resources).each { FileUtils.cp(File.join(resources, it), android) }

puts "Recorded #{packages.size} resolved dependencies and their distributed license files in #{OUT}"
