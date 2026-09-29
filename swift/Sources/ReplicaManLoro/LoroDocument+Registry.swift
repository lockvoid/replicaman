import Foundation
import ReplicaMan

/// A document's keyed REGISTRIES — a root map of `key → entry`, each entry a
/// mergeable map whose fields merge one by one (the agent sets `volume` while
/// the person drags `start`), everything below a field a plain value that
/// replaces whole — and its plain root maps. A writer writes relative to what
/// it READ (`base`): that is what makes its silence interpretable.
extension LoroDocument {
    /// The registry under `root` as the writer means it now, against the
    /// registry it read. A key it saw and dropped is deleted; a key it never
    /// saw is not its business; a key it saw that a peer has since deleted
    /// stays deleted (the tombstone wins — echoing a stale read is not an
    /// intent to resurrect); a field equal to what it read is left alone, so
    /// a peer's newer value is never out-voted by a stale echo. `base: nil`
    /// is an authoritative writer: the document is its own base, and an empty
    /// `entries` clears the registry.
    public func writeRegistry(
        _ root: String,
        _ entries: [String: [String: ReplicaValue]],
        base: [String: [String: ReplicaValue]]?
    ) throws {
        let live = Set(keys(at: [root]))
        let deletable = base.map { Set($0.keys) } ?? live
        for stale in deletable.subtracting(entries.keys) {
            try delete(at: [root, stale])
        }
        for (key, fields) in entries {
            if base?[key] != nil, !live.contains(key) { continue }
            try writeEntry(root, key, fields, base: base?[key])
        }
    }

    /// One entry's fields, each against what the writer read. A slot a peer's
    /// bytes hold as a plain value, not a mergeable map, refuses the write —
    /// that refusal propagates to the owner. Inside an engine edit closure it
    /// rolls back the entire action; a raw document must be discarded on error.
    public func writeEntry(
        _ root: String,
        _ key: String,
        _ fields: [String: ReplicaValue],
        base: [String: ReplicaValue]?
    ) throws {
        for (field, value) in fields {
            if let seen = base?[field], seen == value { continue }
            try write(value, at: [root, key, field])
        }
    }

    /// A plain root map's fields, each against what the writer read.
    public func writeFields(_ root: String, _ fields: [String: ReplicaValue], base: [String: ReplicaValue]?) throws {
        for (field, value) in fields {
            if let seen = base?[field], seen == value { continue }
            try write(value, at: [root, field])
        }
    }

    /// A document's birth from one complete value: every root in `roots`, the
    /// ones named in `registries` as keyed registries of mergeable entries,
    /// the rest as plain maps. A birth is not an edit — its nulls stand.
    public func seed(_ roots: [String: ReplicaValue], registries: Set<String>) throws {
        for (root, value) in roots {
            for (key, member) in value.object ?? [:] {
                if registries.contains(root) {
                    for (field, fieldValue) in member.object ?? [:] {
                        try seed(fieldValue, at: [root, key, field])
                    }
                } else {
                    try seed(member, at: [root, key])
                }
            }
        }
    }
}

extension ReplicaValue {
    /// A registry of a materialized document (`LoroDocument.value`):
    /// `key → fields` — what a writer read, handed back as its `base`.
    public func registry(_ root: String) -> [String: [String: ReplicaValue]] {
        (self[root]?.object ?? [:]).mapValues { $0.object ?? [:] }
    }
}
