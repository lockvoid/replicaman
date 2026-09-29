# Dependency and binding record

The root lockfiles and binding lockfiles describe the tested set. Matching a
marketing version across bindings is not evidence of binary compatibility.

| Component | Resolved version in this tree |
| --- | --- |
| Swift Loro package / binary core | 1.13.3, exact SwiftPM pin / 1.13.7 |
| Rust Loro crate / internal core | 1.13.7 / 1.13.7 |
| Kotlin Loro FFI / underlying Loro core | 1.13.7 / 1.13.7 |
| Ruby Loro crate / internal core | 1.13.7 / 1.13.7 |
| Swift GRDB / yyjson | 7.11.1 / 0.12.0 |
| Kotlin / coroutines / serialization | 2.4.10 / 1.11.0 / 1.11.0 |
| AndroidX SQLite / OkHttp / JNA | 2.7.0 / 5.5.0 / 5.19.1 |
| Kotlin native bridge generator | UniFFI 0.31.1 |

The nine document fixtures and live multi-client E2E exercise this exact set,
including maps, Unicode text, lists, concurrent edits, snapshots, resets, rebuilds,
restarts and server import/export. Unsupported codec/container features are not
implied to have been verified by a compatible version label. Upgrade a binding
and regenerate its interface in the same change; rerun every native codec suite
and E2E before changing the supported set.

ReplicaMan source is MIT. Dependencies retain their own licenses. SwiftPM,
RubyGems, Maven and Cargo metadata remain authoritative for separately obtained
dependencies. `tools/dependencies.rb` produces a resolved Rust/native inventory
with lockfile hashes and copies available upstream license/notice files into
`build/dependencies`. Package assembly includes that inventory and notice text in
the Kotlin binding jar and Android AAR.

Some crates omit license files from their published source archives. The cache
under `tools/upstream-licenses` retains notices retrieved from their declared
upstream repository at the archive's recorded source revision, with URLs in
`index.json`. `generic-btree` 0.10.7 declares MIT in its crate metadata but provides
no license text at the recorded source revision; its declaration and source are
recorded in the inventory. This is an inventory, not a replacement for reviewing
upstream redistribution obligations before a public binary release. Build/test
and non-Android dependency notices are conservatively included as well.

The generator archive uses only Ruby's standard library at runtime. The Rails
engine's optional Loro gem compiles its native extension from its packaged Rust
source and lockfile. JVM consumers supply JNA and the platform-specific native
library; Android consumers use the companion AAR. Only arm64-v8a is built by the
current Android script.
