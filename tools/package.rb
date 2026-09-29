#!/usr/bin/env ruby
# Builds and inspects local release artifacts. Never uploads a package.

require 'digest'
require 'fileutils'
require 'json'
require 'open3'
require 'optparse'
require 'tmpdir'

ROOT = File.expand_path('..', __dir__)
VERSION = File.read(File.join(ROOT, 'VERSION')).strip
OUT = File.join(ROOT, 'build/packages')

def run(*command, chdir: ROOT, env: {})
  puts command.join(' ')
  system(env, *command, chdir: chdir, exception: true)
end

def read(path)
  File.read(File.join(ROOT, path))
end

def names_in(archive)
  output, status = Open3.capture2('unzip', '-Z1', archive)
  abort("cannot list #{archive}") unless status.success?
  output.lines(chomp: true)
end

def tree(root)
  Dir.glob('**/*', base: root).select { File.file?(File.join(root, it)) }.sort.to_h { [it, File.binread(File.join(root, it))] }
end

def source_contract
  {
    'rust/Cargo.toml' => /version = "([^"]+)"/,
    'ruby/lib/replica_man/version.rb' => /VERSION = '([^']+)'/,
    'ruby/vendor/loro/lib/loro/version.rb' => /VERSION = "([^"]+)"/,
  }.each do |path, pattern|
    abort("version drift: #{path}") unless read(path)[pattern, 1] == VERSION
  end

  license = read('LICENSE')
  %w[rust/crates/replicaman rust/crates/replicaman-loro ruby ruby/vendor/loro].each do |mod|
    abort("license drift: #{mod}") unless read("#{mod}/LICENSE") == license
  end

  %w[pull-decode.json stored-values.json journal-decode.json integrity.json].each do |name|
    abort("fixture drift: #{name}") unless read("protocol/fixtures/#{name}") == read("rust/crates/replicaman/tests/fixtures/#{name}")
  end

  corpus = File.join(ROOT, 'protocol/fixtures/crdt_convergence')
  frozen = JSON.parse(File.read(File.join(corpus, 'INDEX.json'))).fetch('cases')
  abort('review every native runner when extending the frozen corpus') unless frozen.size == 9
  frozen.each do |kase, files|
    files.each do |name, digest|
      abort("frozen corpus drift: #{kase}/#{name}") unless Digest::SHA256.file(File.join(corpus, kase, name)).hexdigest == digest
    end
  end
  %w[swift/Tests/ReplicaManLoroTests/Fixtures/crdt_convergence rust/crates/replicaman-loro/tests/fixtures/crdt_convergence].each do |copy|
    abort("shared document fixture drift: #{copy}") unless tree(File.join(ROOT, copy)) == tree(corpus)
  end
end

def verify_installed_binding(artifact)
  gem_path = Open3.capture2('ruby', '-rrubygems', '-e', 'puts Gem.path.join(File::PATH_SEPARATOR)').first.strip
  Dir.mktmpdir('replicaman-gem-') do |home|
    env = { 'GEM_HOME' => home, 'GEM_PATH' => [home, gem_path].join(File::PATH_SEPARATOR) }
    run('gem', 'install', '--local', '--no-document', artifact, env: env)
    run('ruby', '-e', <<~RUBY, VERSION, env: env)
      gem "loro", ARGV.fetch(0)
      require "loro"
      abort "loaded the source tree instead of the installed gem" unless
        File.realpath(Gem.loaded_specs.fetch("loro").full_gem_path).start_with?(File.realpath(ENV.fetch("GEM_HOME")) + "/")
      doc = Loro::Doc.new(peer_id: 9)
      doc.get_map("meta").set("value", 9_007_199_254_740_993)
      doc.commit
      peer = Loro::Doc.from_snapshot(doc.export_snapshot, peer_id: 10)
      abort "installed binding lost the value" unless peer.get_map("meta").get("value") == 9_007_199_254_740_993
      puts "PASS installed Ruby binding round trip"
    RUBY
  end
end

def package_rust
  run('cargo', 'package', '-p', 'replicaman', '-p', 'replicaman-loro', '--allow-dirty', '--locked',
      chdir: File.join(ROOT, 'rust'))
  {
    'replicaman' => %w[tests/fixtures/pull-decode.json LICENSE],
    'replicaman-loro' => %w[tests/fixtures/crdt_convergence/INDEX.json LICENSE],
  }.map do |name, required|
    crate = File.join(ROOT, "rust/target/package/#{name}-#{VERSION}.crate")
    listing = Open3.capture2('tar', '-tzf', crate).first.lines(chomp: true)
    required.each do |suffix|
      abort("#{name} crate lacks #{suffix}") unless listing.any? { it.end_with?(suffix) }
    end
    FileUtils.cp(crate, OUT)
    File.join(OUT, File.basename(crate))
  end
end

def package_ruby
  [%w[ruby replica_man], %w[ruby/vendor/loro loro]].map do |directory, name|
    target = File.join(OUT, "#{name}-#{VERSION}.gem")
    run('gem', 'build', "#{name}.gemspec", '--output', target, chdir: File.join(ROOT, directory))
    run('ruby', '-rrubygems/package', '-e', <<~RUBY, target)
      files = Gem::Package.new(ARGV.fetch(0)).spec.files
      abort "missing license" unless files.include?("LICENSE")
      abort "private build files" if files.any? { it.match?(%r{(?:^|/)(?:tmp|target|test|\\.env)(?:/|$)}) }
    RUBY
    if name == 'replica_man'
      run('ruby', '-rrubygems/package', '-e', <<~RUBY, target)
        spec = Gem::Package.new(ARGV.fetch(0)).spec
        abort "restore operator is absent" unless spec.files.include?("exe/replicaman-restore")
        abort "restore command is not installed" unless spec.executables.include?("replicaman-restore")
      RUBY
    end
    verify_installed_binding(target) if name == 'loro'
    target
  end
end

def package_jvm(android:)
  run('ruby', 'tools/dependencies.rb')
  modules = %w[replicaman replicaman-loro loro]
  modules << 'loro-android' if android
  run('./gradlew', *(android ? ['-Pandroid=true'] : []),
      *modules.map { ":#{it}:publishLibraryPublicationToStagingRepository" }, chdir: File.join(ROOT, 'kotlin'))

  repository = File.join(ROOT, 'kotlin/build/maven/io/replicaman')
  modules.flat_map do |mod|
    suffix = mod == 'loro-android' ? 'aar' : 'jar'
    artifact = File.join(repository, mod, VERSION, "#{mod}-#{VERSION}.#{suffix}")
    abort("missing #{artifact}") unless File.file?(artifact)
    names = names_in(artifact)
    abort("empty archive #{artifact}") if names.empty?

    if suffix == 'aar'
      abort('AAR lacks native library') unless names.include?('jni/arm64-v8a/libloro_kotlin.so')
      Dir.mktmpdir do |dir|
        classes = File.join(dir, 'classes.jar')
        File.binwrite(classes, Open3.capture2('unzip', '-p', artifact, 'classes.jar', binmode: true).first)
        abort('AAR lacks notices') unless names_in(classes).include?('META-INF/replicaman-loro-android/THIRD-PARTY-NOTICES.txt')
      end
    else
      abort('JAR lacks classes') unless names.any? { it.end_with?('.class') }
      abort('production JAR contains test fixture writers') if names.any? { it.start_with?('io/replicaman/testing/') }
      if mod == 'loro' && !names.include?('META-INF/replicaman-loro-binding/THIRD-PARTY-NOTICES.txt')
        abort('binding JAR lacks notices')
      end
    end

    Dir.children(File.dirname(artifact)).map { File.join(File.dirname(artifact), it) }
       .select { File.extname(it).match?(/\A\.(pom|module|jar|aar)\z/) }.sort
  end
end

def package_generator
  target = File.join(OUT, "replicaman-codegen-#{VERSION}.tar.gz")
  prefix = "replicaman-codegen-#{VERSION}"
  Dir.mktmpdir do |dir|
    staged = File.join(dir, prefix)
    FileUtils.mkdir_p(staged)
    %w[bin lib emitters README.md].each { FileUtils.cp_r(File.join(ROOT, 'codegen', it), staged) }
    FileUtils.cp(File.join(ROOT, 'LICENSE'), staged)
    run('tar', '-czf', target, prefix, chdir: dir)
  end
  target
end

options = {}
OptionParser.new do |parser|
  parser.on('--check') { options[:check] = true }
  parser.on('--rust') { options[:rust] = true }
  parser.on('--ruby') { options[:ruby] = true }
  parser.on('--jvm') { options[:jvm] = true }
  parser.on('--android') { options[:android] = true }
end.parse!

source_contract
if options.fetch(:check, false)
  puts 'PASS versions, licenses and shared packaged fixtures'
  exit
end

FileUtils.mkdir_p(OUT)
artifacts = []
artifacts.concat(package_rust) if options.fetch(:rust, false)
artifacts.concat(package_ruby) if options.fetch(:ruby, false)
if options.fetch(:jvm, false) || options.fetch(:android, false)
  artifacts.concat(package_jvm(android: options.fetch(:android, false)))
end
artifacts << package_generator

checksums = artifacts.to_h { [it.delete_prefix("#{ROOT}/"), Digest::SHA256.file(it).hexdigest] }
File.write(File.join(OUT, 'artifacts.json'), "#{JSON.pretty_generate('version' => VERSION, 'sha256' => checksums)}\n")
puts 'PASS local release artifacts; nothing uploaded'
