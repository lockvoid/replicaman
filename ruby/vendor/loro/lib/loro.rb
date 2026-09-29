# frozen_string_literal: true

require_relative "loro/version"

begin
  require "loro/loro_rb"
rescue LoadError => e
  raise LoadError, "loro native extension is not built (#{e.message}). " \
                   "Reinstall the gem with a Rust toolchain, or run `bundle exec rake compile` in ruby/vendor/loro."
end
