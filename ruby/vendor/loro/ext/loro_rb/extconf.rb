# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("loro/loro_rb") do |config|
  config.env["RB_SYS_CARGO_TARGET_DIR"] = File.join(__dir__, "target")
  config.extra_rustflags += %w[-C link-dead-code=yes] if RUBY_PLATFORM.include?("darwin")
end

if RUBY_PLATFORM.include?("darwin")
  # rb_sys emits an empty Mach-O install name. Apple's current toolchain can
  # produce an unloadable bundle from it. Apply the nonempty name at the shared
  # gem-install boundary, so gem install and rake compile build the same binary.
  makefile = File.read("Makefile")
  File.write("Makefile", makefile.gsub('-id "" $(DLLIB)', '-id "$(DLLIB)" $(DLLIB)'))
end
