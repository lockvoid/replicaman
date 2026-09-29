# Shared conformance fixtures

`fixtures/pull-decode.json` contains valid and invalid wire envelopes. Every
client must accept or refuse the whole envelope as specified; dropping a frame
and retaining its cursor is a failure.

`fixtures/stored-values.json` covers stored JSON shape, exact integer limits,
and invalid values. It exercises real native value decoders.

`fixtures/crdt_convergence` is a frozen nine-case Loro corpus over a neutral
board document. Swift, Kotlin, Rust and Rails merge the same binary histories in
different orders, replay duplicates, reopen snapshots, and compare against the
committed expected values. `INDEX.json` records hashes and provenance. The
oracle is never regenerated during acceptance runs.

`ruby tools/generate_crdt_corpus.rb` authors the corpus with the vendored Ruby
binding. The board has a `meta` name, a `settings` map and two registries of
mergeable entries, `cards` and `columns`; every case shares one base history
(peer 1), adds two concurrent edits of it (peers 11 and 22) and declares the
outcome the merge must reach, and the tool writes nothing unless the Loro merge
equals every declaration. It writes each case's `base.bin`, `left.bin`,
`right.bin`, `expected.json` and `manifest.json`, plus `INDEX.json`, here and
verbatim into the SwiftPM and Cargo copies. Peers are fixed and no timestamps
are recorded, so a rerun reproduces every byte; run it only to change the
corpus, and review the diff.

SwiftPM and packaged Cargo tests keep identical resource copies. Packaging checks
every byte against this directory. Extending the corpus requires updating the
explicit case inventory in every native runner and reviewing the expected result.

`fixtures/consumer-manifest.json` is the manifest of the gem's test server
(`ruby/test/dummy`, `DummyReplica`), generated as `SampleReplica`; the Rails suite
holds it byte-identical to the declaration. The minimal public example uses
`examples/notes/manifest.json`.
Neither fixture requires private services or sibling application repositories.
