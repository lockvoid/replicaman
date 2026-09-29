# Inspecting and exporting retained local work

Recovery branches contain bytes that must not automatically replay: for example,
an edit to a replaced entity lifetime, a refused submission, or an uncertain
submission belonging to a previous principal. A transport error alone keeps the
immutable submission retryable; it is not permission to mint new operation ids.

Swift/Kotlin expose `recoveryRecords`, `recoveryParts`, `recoveryChunk` and
`exportRecovery` on the state store. Rust uses their snake_case equivalents.
Lists paginate by the last returned identifier/part. A chunk is at most 256 KiB.
The export callback only writes bytes; it must not reenter the store. Run file
I/O outside the UI thread. The entire export reads one consistent SQLite snapshot.

Export propagates storage and destination failures. Only publish/share a completed
file, and retain the source archive until the user explicitly decides otherwise.
`removeRecoveryRecord` / `remove_recovery_record` is the separate destructive API.
The iOS and Android Replica screens list retained branches and offer export;
exporting never dismisses them.

## JSON Lines format

The UTF-8 export consists of newline-terminated JSON objects in this order:

1. A `record`: `format` is `replicaman-recovery`, `version` is 1; metadata names
   `id`, `stream`, `row_id`, `incarnation`, `reason`, and `created_at` (Unix seconds).
2. For each part, a `part` object names `kind`, `key`, and `bytes` (decimal string).
   Parts are ordered by `(kind, key)`. Empty parts have zero chunks.
3. Zero or more `chunk` objects follow that part: `offset` is a decimal byte offset,
   `content` is canonical base64, and `sha256` hashes the decoded chunk bytes.
   Chunks are contiguous, nonempty and no larger than 262,144 decoded bytes.
4. A final `complete` object gives `parts` (integer) and total `bytes` (decimal string).

An inspector must require the supported format/version, verify every checksum,
offset and part length, and require the completion totals. Missing completion means
truncation, even if all preceding JSON objects parse. Do not interpret a part's key
as a filesystem path; it is an opaque identifier and may contain slashes.

Parts include raw row data/metadata, document fold/acknowledged history/codec/peer,
intents in every state (accepted ones included) with their preimages and state
metadata, hold metadata, references, frozen submission bytes, and dataset context. Malformed original
JSON is deliberately preserved as raw bytes instead of decoded and discarded.

This is an inspection/recovery format, not a command replay format. Resolve the
principal, dataset, incarnation and domain intent before making a fresh authorized
application action. Importing old mutation IDs or CRDT peers into another lifetime
would bypass the protection that caused recovery in the first place.
