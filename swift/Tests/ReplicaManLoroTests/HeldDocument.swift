import Foundation
@testable import ReplicaManLoro
@testable import ReplicaMan

/// A document held the way the engine holds one: opened through the engine's
/// own codec, one commit per edit (the engine commits after every
/// `updateDocument` body), transport through the codec's own doors.
struct HeldDocument {
    let codec = LoroReplicaCodec()
    let live: LoroDocument

    init(peer: UInt64 = 10, fold: Data? = nil) throws {
        live = try codec.open(fold: fold, peer: peer)
    }

    /// The document's version vector — committing what is pending, as the
    /// engine does at the end of every edit.
    var version: Data { codec.version(live) }

    /// One edit, one commit.
    func edit(_ body: (LoroDocument) throws -> Void) throws {
        try body(live)
        _ = codec.version(live)
    }

    func importing(_ blobs: [Data]) throws {
        try codec.importDeltas(live, blobs)
    }

    /// Everything a peer at `version` has not seen.
    func delta(since version: Data) throws -> Data {
        try codec.exportDelta(live, since: version)
    }

    func snapshot() throws -> Data {
        try codec.snapshot(live)
    }

    /// Hand everything this document has produced to `other`.
    func sync(to other: HeldDocument) throws {
        try other.importing([try delta(since: other.version)])
    }

    var canUndo: Bool { codec.canUndo(live) }
    var canRedo: Bool { codec.canRedo(live) }

    func undo() throws -> Bool {
        let stepped = try codec.undo(live)
        _ = codec.version(live)
        return stepped
    }

    func redo() throws -> Bool {
        let stepped = try codec.redo(live)
        _ = codec.version(live)
        return stepped
    }

    /// The registry under `root`: key → fields.
    func registry(_ root: String) -> [String: [String: ReplicaValue]] {
        (live.value[root]?.object ?? [:]).mapValues { $0.object ?? [:] }
    }

    func entry(_ root: String, _ key: String) -> [String: ReplicaValue]? {
        registry(root)[key]
    }
}

/// A clip entry as the timeline writes one.
func clipFields(start: Double, duration: Double = 4, volume: Double? = nil) -> [String: ReplicaValue] {
    var fields: [String: ReplicaValue] = [
        "track_key": .string("video"),
        "start": .number(start),
        "duration": .number(duration),
    ]
    if let volume { fields["volume"] = .number(volume) }
    return fields
}
