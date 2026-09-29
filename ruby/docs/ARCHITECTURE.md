# Architecture

The maintained invariants are in the monorepo's
[protocol contract](../../docs/PROTOCOL.md).

The implementation uses two lanes: field patches for rows and opaque mergeable
history for documents. The same transaction contains domain mutation, capture,
and sequenced writer results. Each native client commits its local state with the work it
owes the server. Language implementations share fixtures and process-level
conformance tests, while keeping native storage and scheduling idioms.
