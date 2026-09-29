# loro-ruby

`loro` is a small native Ruby binding around the Loro CRDT Rust crate. It gives a
server-side Ruby peer document lifecycle, binary update and snapshot exchange,
encoded version metadata, map mutation, and deep materialization into ordinary Ruby
hashes. The API is deliberately curated rather than a mirror of the entire Rust API.

## Build

Ruby 4.0 or newer, a C toolchain, and rustup are required. The exact Rust toolchain is
pinned in `rust-toolchain.toml` and is selected automatically by rustup.

From this directory:

```sh
bundle install
bundle exec rake compile
bundle exec rake
```

In the host Rails application, `bin/setup` builds the extension or it can be rebuilt
with `bin/rails loro:compile`. Bundler does not compile extensions for path-sourced
gems, so this explicit build step is required.

## Quick reference

```ruby
doc = Loro::Doc.new(peer_id: 1)
settings = doc.get_map("settings")
settings.set("enabled", true)
settings.set("labels", ["one", "two"])

snapshot = doc.export_snapshot
vector = doc.version_vector
delta = doc.export_updates(since: vector)
plain_hash = doc.to_h

copy = Loro::Doc.from_snapshot(snapshot, peer_id: 2)
status = copy.import(delta) # => {pending: false}
```

`Loro::Doc` provides `new`, `from_snapshot`, peer-id access, `import`, `import_batch`,
update/full-snapshot/shallow-snapshot exports, `version_vector`, `frontiers`, `commit`,
`get_map`, and `to_h`.

Loro reserves the maximum u64 value for internal use, so explicit peer ids may use
`0..2**64-2`. With the pinned Loro auto-commit behavior, changing `peer_id` after local
edits commits those edits before switching the peer used by later operations.

`Loro::Map` provides `set`, `get`, `get_map`, `ensure_mergeable_map`, `delete`, `key?`, `keys`,
`size`, and `to_h`. Map keys may be strings or symbols. Values may be `nil`, booleans,
i64-range integers, floats, strings, symbols, arrays, and hashes with string/symbol
keys. Binary-encoded Ruby strings are stored as binary values; all binary exports are
returned as `Encoding::BINARY` strings.

Errors from the native library use `Loro::Error`. Corrupt imports use
`Loro::ImportError`, and unsupported Ruby values use `Loro::TypeError`.

## Threading and lifetime

A document and its map handles must not be used concurrently from multiple Ruby
threads. Use one document per request or job. Every call holds the GVL. Handles keep
their underlying document storage alive and require no explicit close operation.

## Out of scope for v1

Text, MovableList, Tree, and Counter containers; subscriptions and change events;
checkout, time travel, forks, and detached mode; undo management; GVL release;
precompiled binary gems; UniFFI bindings; detailed import version ranges; JSON patch
or diff export; awareness and ephemeral storage; and application-specific document
schemas or update persistence.
