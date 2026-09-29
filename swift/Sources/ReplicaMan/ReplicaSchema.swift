import Foundation

/// One declared stream, as the manifest describes it. The manifest knows the
/// Which drain a write leaves on. Claimed by an ACTION, never by a stream:
/// the same stream carries a foreground write and a background one (a chat
/// send authors elements; so does an import), so stream granularity cannot
/// express "someone is waiting on this one".
///
/// Lanes drain concurrently, so an interactive write OVERTAKES a bulk backlog.
/// Order holds within a lane only — which is safe because the engine keeps two
/// invariants automatically: a row's later ops join its pending lane, and an
/// interactive op promotes any pending row it names.
public enum ReplicaLane: String, Sendable, CaseIterable {
    /// A human is waiting on this write — a tap, a send, a foreground edit.
    case interactive
    /// Background work: imports, cook fan-out, catalog fill. The default.
    case bulk
}

/// lane and the direction — that is what drives the engine's `row.delete`
/// cascade and lets codegen omit write verbs entirely on readonly streams
/// (the client *can't* express the mistake).
public struct ReplicaStreamSpec: Sendable, Equatable {
    public enum Lane: String, Sendable {
        case row
        case document
    }

    public var name: String
    public var lane: Lane
    public var readonly: Bool
    public var shard: String
    /// Document-lane codec wire name (`loro@1`); nil on row streams.
    public var codec: String?
    /// The row fields a document stream's document owns (the manifest
    /// column's `reflects:`).
    public var reflections: [ReplicaReflection]
    /// The provenance columns the engine fills on a document stream's row.
    public var stamp: ReplicaStamp?
    /// The fields every patch of this stream carries, changed or not — the
    /// server judges the write against them (the manifest column's
    /// `precondition`).
    public var preconditions: [String]
    /// The fields a device may send — the manifest's `push` columns, every
    /// variant's included; the server refuses any other. A row a gate held
    /// leaves as these fields of its state. Nil: every field.
    public var references: [ReplicaReferenceSpec]
    public var lifetimeFrom: String?
    public var pushed: Set<String>?

    public init(
        name: String, lane: Lane, readonly: Bool = false, shard: String = "user", codec: String? = nil,
        reflections: [ReplicaReflection] = [], stamp: ReplicaStamp? = nil, preconditions: [String] = [],
        pushed: Set<String>? = nil, references: [ReplicaReferenceSpec] = [], lifetimeFrom: String? = nil
    ) {
        self.name = name
        self.lane = lane
        self.readonly = readonly
        self.shard = shard
        self.codec = codec
        self.reflections = reflections
        self.stamp = stamp
        self.preconditions = preconditions
        self.pushed = pushed
        self.references = references
        self.lifetimeFrom = lifetimeFrom
    }
}

/// A row field its document owns: the row reads the document's value at
/// `path` (`["meta", "name"]` — a root map, the maps under it, the key), and
/// the engine writes it whenever the document moves; nobody else writes it.
public struct ReplicaReflection: Sendable, Equatable, Hashable {
    public var field: String
    public var path: [String]

    public init(field: String, path: [String]) {
        self.field = field
        self.path = path
    }
}

/// One physical read structure the store builds over a pulled field — the
/// manifest's `indexes:` entry. `btree` is an index over a generated column
/// (`json_extract(data, '$.field')`), partial per stream; `fts5` is a
/// shadow table kept by triggers. Indexes are DERIVATIVES of `data`: the
/// store reconciles declared ↔ actual at every open, so there are no
/// migrations — only the manifest.
public struct ReplicaIndexSpec: Sendable, Equatable, Hashable {
    public var stream: String
    public var field: String
    public var kind: ReplicaIndexKind

    public init(stream: String, field: String, kind: ReplicaIndexKind = .btree) {
        self.stream = stream
        self.field = field
        self.kind = kind
    }

    /// The generated column — one per FIELD, shared by every stream that
    /// indexes it (the expression is stream-agnostic). So is the btree:
    /// `(stream, ix_field)`, one per field — two equalities always beat the
    /// primary key's one for the planner, stats or no stats (a partial
    /// `WHERE stream = …` index lost that race the moment `sqlite_stat1`
    /// existed for the PK). The fts5 table is per (stream, field): its
    /// triggers are stream-gated.
    var column: String { "ix_\(field)" }
    var btreeIndex: String { "idx_\(field)" }
    var ftsTable: String { "fts_\(stream)_\(field)" }
}

public enum ReplicaIndexKind: String, Sendable {
    case btree
    case fts5
}

/// A generated per-stream field enum: the raw value is the wire field name.
/// Only indexed fields are cases, so a predicate over anything else does
/// not compile.
public protocol ReplicaIndexedField: RawRepresentable, Sendable, Hashable where RawValue == String {}

/// The `Field` of a model with no indexes — nothing to scope on.
public enum ReplicaNoField: ReplicaIndexedField {
    public init?(rawValue: String) { nil }

    public var rawValue: String {
        switch self {}
    }
}

/// The engine's map of the replica: stream specs plus the shard list, in
/// declaration order. Generated code ships one of these; tests build them by
/// hand.
public struct ReplicaSchema: Sendable {
    public let namespace: String
    public let version: Int
    public let specs: [ReplicaStreamSpec]
    /// Shards in first-appearance order — pulled independently, one cursor
    /// each.
    public let shards: [String]
    /// Every declared read structure, in manifest order.
    public let indexes: [ReplicaIndexSpec]

    private let byName: [String: ReplicaStreamSpec]

    public init(streams: [ReplicaStreamSpec], indexes: [ReplicaIndexSpec] = [], namespace: String = "replicaman", version: Int = 1) {
        self.namespace = namespace
        self.version = version
        self.specs = streams
        self.indexes = indexes
        self.byName = Dictionary(uniqueKeysWithValues: streams.map { ($0.name, $0) })
        var seen: [String] = []
        for spec in streams where !seen.contains(spec.shard) {
            seen.append(spec.shard)
        }
        self.shards = seen.isEmpty ? ["user"] : seen
    }

    public func spec(_ name: String) -> ReplicaStreamSpec? {
        byName[name]
    }

    /// The lane an incoming frame's stream belongs to. Unknown streams are
    /// row-lane by definition (nothing to cascade) — the importer stays
    /// total.
    public func lane(of stream: String) -> ReplicaStreamSpec.Lane {
        byName[stream]?.lane ?? .row
    }

    public func streams(shard: String) -> [String] {
        specs.filter { $0.shard == shard }.map(\.name)
    }
}
