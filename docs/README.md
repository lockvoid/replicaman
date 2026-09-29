# Documentation

ReplicaMan keeps application data in a durable local SQLite store and synchronizes it with the rows your Rails database gives each account. Use generated models for application code, row transactions for records, and the optional Loro plugin for collaborative documents.

## Get started

1. **[Installation](./INSTALLATION.md)** — add the packages for your platform.
2. **[Rails server](./SERVER.md)** — expose an authenticated stream over an ActiveRecord model.
3. **[Models and code generation](./MODELS.md)** — generate the native schema and models.
4. **[Setup](./SETUP.md)** — open an owner store and connect the client.

To try local persistence first, run the **[Notes example](../examples/notes/README.md)**. It needs no server.

## Build your application

| Guide | What it covers |
| --- | --- |
| [Queries and observation](./QUERIES.md) | Local reads, indexed queries, and live results |
| [Mutations and transactions](./MUTATIONS.md) | Create, update, delete, and atomic server actions |
| [Documents and Loro](./DOCUMENTS.md) | Document creation, managed edits, projections, and undo |
| [Synchronization](./SYNC.md) | Push, pull, scheduling, refusals, and status |
| [Sync gates](./GATES.md) | Hold writes until uploads or application policies allow them |
| [Storage and account lifecycle](./STORAGE.md) | Durability, owner isolation, credentials, and database ownership |
| [Recovery and diagnostics](./RECOVERY.md) | Inspect retained work, export it, and verify stored state |
| [Command responses](./COMMITS.md) | Refresh replicas after a separate application command |

## Understand the model

**[Keynotes](./KEYNOTES.md)** explains how the pieces fit together: local intent, immutable submissions, server decisions, and atomic pull rounds. The guides use the same concepts across all clients while keeping their native APIs explicit.

## Work on ReplicaMan

**[Protocol](./PROTOCOL.md)** is the wire and durability contract for implementers; **[Recovery export](./RECOVERY_EXPORT.md)** the archive format; **[Dependencies](./DEPENDENCIES.md)** the tested dependency set. Build commands live in **[Testing](../e2e/README.md)**.
