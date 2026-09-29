# frozen_string_literal: true

require_relative "lib/loro/version"

Gem::Specification.new do |spec|
  spec.name = "loro"
  spec.version = Loro::VERSION
  spec.authors = ["LockVoid Labs"]
  spec.summary = "Ruby bindings for Loro CRDTs"
  spec.description = <<~DESC
    Native-extension Ruby bindings for the Loro CRDT library (loro-dev/loro),
    wrapping the core Rust crate via magnus. Curated surface: Doc lifecycle,
    binary import/export (updates, snapshot, shallow snapshot), version vectors,
    Map containers, and deep-value materialization. The server-side peer for
    CRDT document sync.
  DESC
  spec.homepage = "https://loro.dev"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 4.0"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/loro_rb/*.{rb,toml,lock}",
    "ext/loro_rb/src/**/*.rs",
    "README.md",
    "LICENSE"
  ]
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/loro_rb/extconf.rb"]
  spec.add_dependency "rb_sys", "~> 0.9"

  spec.metadata["loro_crate_version"] = "1.13.6"
end
