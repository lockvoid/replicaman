import Foundation

/// The document-lane merge protocol behind an opaque-bytes seam: folds,
/// payloads and version vectors are all `Data`, so the core neither imports
/// nor understands loro — `ReplicaManLoro` is the one plugin that does, and
/// a rows-only consumer never links it (module-graph enforcement).
///
/// Contracts the engine leans on:
/// - `merge` is a real CRDT merge (idempotent, order-free) and REFUSES a
///   payload whose causal deps the fold has not seen (`missingCausalDeps`) —
///   a silently-parked edit would be absent from every future fold.
/// - `diff(fold, since:)` returns exactly the ops past `since` — the
///   per-document supersede (one pending merged delta) is literally
///   `diff(fold, since: acked)`.
/// - `payloadVersion` reads a payload's end version WITHOUT applying it —
///   how acked advances on server frames and accepted pushes alike.
public protocol ReplicaCodec: Sendable {
    /// Wire name (`loro@1`) — matched against frame/op `codec` fields.
    var name: String { get }

    /// Merge an update or snapshot payload into a fold; nil fold = a fresh
    /// document born from the payload. `reflections` are read off the merged
    /// document while it is open — an absent path reads `.null`.
    func merge(fold: Data?, payload: Data, reflecting reflections: [ReplicaReflection]) throws -> ReplicaMerge

    /// Updates past `version` (nil = everything).
    func diff(fold: Data, since version: Data?) throws -> Data

    /// The fold's own version vector, encoded.
    func version(fold: Data) throws -> Data

    /// The end version covered by an update/snapshot payload, encoded —
    /// read from the blob's metadata, never by applying it.
    func payloadVersion(_ payload: Data) throws -> Data

    /// Union of two encoded version vectors.
    func mergeVersions(_ a: Data?, _ b: Data) throws -> Data

    /// True when an update payload carries no ops — an empty diff is not
    /// worth a journal entry.
    func isEmptyDiff(_ payload: Data) -> Bool
}

/// A merged fold and what its reflected paths read, keyed by row field.
public struct ReplicaMerge: Sendable, Equatable {
    public var fold: Data
    public var reflected: [String: ReplicaValue]

    public init(fold: Data, reflected: [String: ReplicaValue]) {
        self.fold = fold
        self.reflected = reflected
    }
}

/// Whether this store maintains full editable documents or only server-authored
/// row projections. Switching modes requires a fresh bootstrap.
public enum ReplicaDocumentMode: Sendable {
    case replicated
    case projectionsOnly
}
