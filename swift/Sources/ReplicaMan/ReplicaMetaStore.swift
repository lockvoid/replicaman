import Foundation
import GRDB

/// The store's identity and what it synchronizes: the dataset learned from
/// its first pull and the next local submission sequence.
struct ReplicaMeta: Sendable {
    let store: String
    let dataset: String?
    let nextSequence: Int64
}

extension ReplicaStateStore {
    /// The layout this build reads and writes: the store's `PRAGMA user_version`.
    static let format = 3

    /// Stamp a fresh file, open a current one; any other store is refused in
    /// the open transaction, so its bytes stay as they were.
    static func prepareSynchronization(_ db: Database) throws {
        let version = try Int.fetchOne(db, sql: "PRAGMA user_version") ?? 0
        let fresh = try version == 0
            && Bool.fetchOne(db, sql: "SELECT NOT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table')") == true
        guard fresh || version >= Self.format else {
            throw ReplicaError.storage("Unsupported earlier store format; open a fresh store")
        }
        guard version <= Self.format else {
            throw ReplicaError.storage("Store format \(version) is newer than this build; upgrade required")
        }
        try db.execute(sql: ReplicaSyncSchema.sql)
        if fresh {
            try db.execute(sql: "PRAGMA user_version = \(Self.format)")
        }
        try db.execute(sql: "INSERT OR IGNORE INTO meta (id, store_id) VALUES (1, ?)", arguments: [UUID().uuidString])
    }

    func requireSchema(_ schema: ReplicaSchema) throws {
        try pool.write { db in
            guard let row = try Row.fetchOne(db, sql: "SELECT namespace, schema_version FROM meta WHERE id = 1") else {
                throw ReplicaError.storage("Missing durable store identity")
            }
            let namespace: String? = row["namespace"]
            let version: Int? = row["schema_version"]
            guard (namespace == nil && version == nil) || (namespace == schema.namespace && version == schema.version) else {
                throw ReplicaError.storage("This store belongs to another namespace or schema")
            }
            try db.execute(sql: "UPDATE meta SET namespace = ?, schema_version = ? WHERE id = 1",
                arguments: [schema.namespace, schema.version])
        }
    }

    func meta(_ db: Database) throws -> ReplicaMeta {
        guard let row = try Row.fetchOne(db, sql: "SELECT store_id, dataset, next_sequence FROM meta WHERE id = 1") else {
            throw ReplicaError.storage("Missing durable store identity")
        }
        return ReplicaMeta(store: row["store_id"], dataset: row["dataset"], nextSequence: row["next_sequence"])
    }

    /// The first answer names the dataset; every later answer must repeat it.
    func adoptDataset(_ db: Database, _ dataset: String) throws {
        if let known = try meta(db).dataset {
            guard known == dataset else {
                throw ReplicaError.protocolFailure(code: "DatasetChanged", message: "The authoritative dataset changed")
            }
            return
        }
        try db.execute(sql: "UPDATE meta SET dataset = ? WHERE id = 1", arguments: [dataset])
    }

    func incarnation(_ db: Database, stream: String, id: String) throws -> String? {
        try String.fetchOne(db,
            sql: "SELECT incarnation FROM entities WHERE stream = ? AND row_id = ?",
            arguments: [stream, id])
    }

    func setIncarnation(_ db: Database, stream: String, id: String, shard: String, incarnation: String) throws {
        if let previous = try self.incarnation(db, stream: stream, id: id), previous != incarnation {
            try db.execute(sql: "DELETE FROM entity_references WHERE stream = ? AND row_id = ?", arguments: [stream, id])
        }
        try db.execute(sql: """
            INSERT INTO entities (stream, row_id, shard, incarnation) VALUES (?, ?, ?, ?)
            ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard,
                predecessor = CASE WHEN entities.incarnation = excluded.incarnation THEN entities.predecessor END,
                incarnation = excluded.incarnation
            """, arguments: [stream, id, shard, incarnation])
    }

    func references(_ db: Database, stream: String, id: String) throws -> [ReplicaReference] {
        try Row.fetchAll(db, sql: """
            SELECT name, target_stream, target_id, target_incarnation FROM entity_references
            WHERE stream = ? AND row_id = ? ORDER BY name
            """, arguments: [stream, id]).map {
            ReplicaReference(name: $0["name"], stream: $0["target_stream"], id: $0["target_id"],
                             incarnation: $0["target_incarnation"])
        }
    }

    func predecessor(_ db: Database, stream: String, id: String) throws -> String? {
        try String.fetchOne(db,
            sql: "SELECT predecessor FROM entities WHERE stream = ? AND row_id = ?",
            arguments: [stream, id])
    }

    /// Removing an unsubmitted birth restores the address's last known lifetime.
    /// A later birth must not claim to replace a lifetime the server never saw.
    func cancelUnsentBirth(_ db: Database, stream: String, id: String) throws {
        try db.execute(sql: "DELETE FROM entity_references WHERE stream = ? AND row_id = ?", arguments: [stream, id])
        if let previous = try predecessor(db, stream: stream, id: id) {
            try db.execute(sql: "UPDATE entities SET incarnation = ?, predecessor = NULL WHERE stream = ? AND row_id = ?",
                arguments: [previous, stream, id])
        } else {
            try db.execute(sql: "DELETE FROM entities WHERE stream = ? AND row_id = ?", arguments: [stream, id])
        }
    }

    func identify(
        _ db: Database, operation: ReplicaOp, schema: ReplicaSchema,
        birth: Bool = false, preimage: Data? = nil
    ) throws -> ReplicaOp {
        var operation = operation
        let spec = schema.spec(operation.stream)
        let current = try incarnation(db, stream: operation.stream, id: operation.rowId)
        var baseline = try snapshot(db, stream: operation.stream, rowId: operation.rowId)?.data
        // Deletes have already removed the visible row. Its durable preimage
        // still names the required references, including after a held release.
        if baseline == nil, let preimage,
           case .row(_, _, let data) = try ReplicaPreimage.decode(preimage) {
            baseline = data
        }
        let data = (baseline ?? [:]).merging(operation.data ?? [:]) { _, new in new }
        operation.references = try (spec?.references ?? []).compactMap { reference in
            guard let id = try reference.target(rowId: operation.rowId, data: data) else { return nil }
            guard try snapshot(db, stream: reference.stream, rowId: id) != nil,
                  let lifetime = try incarnation(db, stream: reference.stream, id: id) else {
                throw ReplicaError.storage("Missing reference target: \(reference.stream)/\(id)")
            }
            let bound = birth ? nil : try String.fetchOne(db, sql: """
                SELECT target_incarnation FROM entity_references
                WHERE stream = ? AND row_id = ? AND name = ? AND target_stream = ? AND target_id = ?
                """, arguments: [operation.stream, operation.rowId, reference.name, reference.stream, id])
            if let bound, bound != lifetime {
                throw ReplicaError.storage("Referenced parent lifetime changed; recovery is required")
            }
            return ReplicaReference(name: reference.name, stream: reference.stream, id: id, incarnation: lifetime)
        }

        let parent = operation.references.first { $0.name == spec?.lifetimeFrom }
        let derived = parent.map {
            ReplicaLifetime.derived(namespace: schema.namespace, stream: operation.stream, id: operation.rowId, parent: $0)
        }
        if !birth, let derived, derived != current {
            throw ReplicaError.storage("The parent lifetime changed; this child needs recovery")
        }
        guard let identity = birth ? (derived ?? UUID().uuidString) : current else {
            throw ReplicaError.storage("Missing entity incarnation for \(operation.stream)/\(operation.rowId)")
        }

        operation.incarnation = identity
        try setIncarnation(db, stream: operation.stream, id: operation.rowId, shard: spec?.shard ?? "user", incarnation: identity)
        if birth {
            try db.execute(sql: "UPDATE entities SET predecessor = ? WHERE stream = ? AND row_id = ?",
                arguments: [current, operation.stream, operation.rowId])
        }
        operation.replaces = operation.verb == ReplicaOp.Verb.rowCreate && derived == nil
            ? try predecessor(db, stream: operation.stream, id: operation.rowId)
            : nil
        try db.execute(sql: "DELETE FROM entity_references WHERE stream = ? AND row_id = ?",
            arguments: [operation.stream, operation.rowId])
        for reference in operation.references {
            try db.execute(sql: "INSERT INTO entity_references VALUES (?, ?, ?, ?, ?, ?)",
                arguments: [operation.stream, operation.rowId, reference.name, reference.stream, reference.id, reference.incarnation])
        }
        return operation
    }
}
