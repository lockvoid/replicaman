# Verification

Tests have three levels. Unit contracts cover values, generated types, gate decisions, and wire parsing. Integration tests use real SQLite, PostgreSQL transactions, and Loro history. End-to-end tests run three separate native client processes against Rails over HTTP, with a fault proxy and an independent conflict oracle.

## Toolchains

`mise.toml` pins Ruby 4.0.5, Java 17 and Rust 1.98.1; Swift comes from Xcode 27 (Swift 6.4). PostgreSQL must be running locally, and the current role must be allowed to create databases.

Install Ruby dependencies with `bundle install` in `ruby`, build the Ruby extension with `bundle exec rake loro:compile` in `ruby`, and the Kotlin host library with `mise run build:loro-kotlin`.

```sh
mise run test          # every suite, one at a time
mise run test:swift    # or test:kotlin, test:rust, test:rails, test:loro-ruby, test:codegen
mise run e2e:workers   # the three native workers the E2E scenarios drive
mise run examples
```

## End-to-end suite

```sh
mise run e2e           # builds the workers, then every required history
```

The harness (`e2e/harness.rb`, Ruby with the gem bundle) starts Rails/Puma against a new PostgreSQL database, routes each native process through its own fault proxy, and records client/server logs in `build/e2e/`. It kills workers with SIGKILL after acknowledged offline writes and reopens their actual SQLite files. Scenarios cover lost committed replies, malformed verdict sets, deletion after ambiguous creation, cross-owner isolation, concurrent field, Unicode text and list edits, explicit document rebuilding, reset with pending work, rejected authorization, malformed pull pages, and seeded conflict histories. Database and process cleanup runs even when an assertion fails.

One history runs alone with the gem bundle, for example
`BUNDLE_GEMFILE=ruby/Gemfile bundle exec ruby e2e/protocol_e2e.rb --only rounds --clients rust,rust,rust`.

No live product services, product database, existing emulator, or private application source is required for these tests. The fixture server is test infrastructure, not a production deployment example with authentication already solved.

## Test maintenance

Add a behavioral regression for a safety defect before changing its implementation. Assert saved state, journal state, cursor position, and independently observed server state where relevant. Faults must occur at the actual transaction or transport boundary. Avoid asserting incidental call counts unless that count is itself the contract, such as exactly-once effects or no gate work during drains.

Shared fixtures must execute in every native client. The required generator gate also checks every embedded copy of the shared SQLite schema. Generated output must also compile; textual equality alone does not establish correctness. Missing tools, skipped required suites, and zero tests are failures of the verification gate. Performance probes and platform device tests supplement these gates; passing them does not prove every possible interleaving or hardware power-loss behavior.

Kotlin consumers needing SQLite fault injection can declare
`testImplementation(testFixtures(project(":replicaman")))` and import
`io.replicaman.testing.fixtureWrite`. The fixture writer is absent from the
production jar; application code authors through engine transactions.


## Restore and resource probes

```sh
mise run e2e:load      # e2e/load.rb and the full 100k e2e/checkpoint_load.rb
```

The restore suite runs a real `pg_dump`/`pg_restore` cycle through the packaged
operator command. It checks the nonempty-target and failed-import fences,
client refusal before/after process death, retained offline data, and fresh
writer admission into the new dataset.

The load probe authors 1,000, 10,000, and 100,000 rows through each native client's
transaction API, in 100-row local batches. Each milestone checks SQLite integrity,
every row's contents, outbound-intent counts, aggregate status, and SIGKILL/reopen.
It records elapsed time, sampled process RSS, SQLite file bytes, and reopening
time. These are host-machine authoring measurements, not mobile-device memory
budgets or server checkpoint throughput measurements.

The pull probe captures real domain rows on Rails at the same milestones,
then pulls and atomically publishes them on all three native clients. It
checks every projected row, authoritative integrity before/after restart, an
unchanged pull, and SIGKILL/reopen with the exact published cursor. Results include
server capture time, complete pull time, integrity scan time,
unchanged-pull time, page count, sampled server/client RSS, and database size.
The extended 100k probe allows 120 seconds per command because the final command
decodes and atomically publishes the whole round. The required 1k/10k gate retains
its 45-second command deadline. This is a harness deadline, not an HTTP retry or
a mobile-device performance guarantee.
The regular E2E gate runs the 1k/10k pulls; run the full 100k probe before
release or when changing storage, bucket queries, or round publication.
The E2E gate also includes protocol and real backup/restore histories.


## Reproducible lifecycle histories

`BUNDLE_GEMFILE=ruby/Gemfile bundle exec ruby e2e/lifecycle_e2e.rb --seed 0xC0FFEE` runs every lifecycle episode
with each native client as author. The reference model is a dictionary of
authoritative row fields plus visibility; it contains no ReplicaMan reducer,
sequence allocator or SQL publication code. Native processes and PostgreSQL
execute conflicts, loss/reordering, holds, baseline replacement,
deletion/recreation after GC, domain refusals and real backup/restore.

Every run writes `lifecycle-history.json` before execution. Replay a saved history:

~~~sh
BUNDLE_GEMFILE=ruby/Gemfile bundle exec ruby e2e/lifecycle_e2e.rb --replay build/e2e/RUN/lifecycle-history.json
BUNDLE_GEMFILE=ruby/Gemfile bundle exec ruby e2e/lifecycle_e2e.rb --shrink build/e2e/RUN/lifecycle-history.json
~~~

Shrinking requires a failed run's `lifecycle-failure.txt`. It removes whole
self-contained episodes, recreates the real server and clients for each candidate,
and retains only candidates reproducing the same failure category. Evidence and
`lifecycle-minimal.json` remain beside the original history. Infrastructure or
syntax failures do not count as a reproduced property failure. The default
18-episode seed is included in the required E2E gate.

The GitHub workflow uses the published
[`xcode-27` runner image](https://github.com/actions/runner-images/blob/main/images/macos/xcode-27-arm64-Readme.md)
for the verified Swift toolchain. Hosted CI execution remains a release check;
local workflow linting does not substitute for a hosted run.
