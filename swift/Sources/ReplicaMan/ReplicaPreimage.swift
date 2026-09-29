import Foundation

/// What a client write displaced, captured with its journal
/// entry (client-local, never on the wire) so a rejected verdict can undo
/// the write atomically in the verdict transaction.
enum ReplicaPreimage: Codable {
    /// The row did not exist — a rejected create removes it.
    case absent
    /// A patch touched exactly these fields: `values` restore, `missing`
    /// were introduced by the patch and are removed. Interim server fields
    /// stay untouched.
    case fields(values: [String: ReplicaValue], missing: [String])
    /// A delete displaced this whole row — restore it byte-identical.
    case row(shard: String, type: String?, data: [String: ReplicaValue])

    func encoded() throws -> Data {
        try ReplicaJSON.encoder().encode(self)
    }

    static func decode(_ data: Data) throws -> ReplicaPreimage {
        try ReplicaJSON.decoder().decode(ReplicaPreimage.self, from: data)
    }

    /// Reconstruct the row before a queued suffix, newest displacement first.
    func undoing(_ images: [ReplicaPreimage]) -> ReplicaPreimage {
        images.reversed().reduce(self) { state, image in
            switch image {
            case .absent, .row: return image
            case .fields(let values, let missing):
                guard case .row(let shard, let type, var data) = state else { return state }
                for key in missing { data.removeValue(forKey: key) }
                data.merge(values) { _, old in old }
                return .row(shard: shard, type: type, data: data)
            }
        }
    }

    func forRelease(_ op: ReplicaOp) -> ReplicaPreimage {
        if op.verb == ReplicaOp.Verb.rowCreate { return .absent }
        guard op.verb == ReplicaOp.Verb.rowPatch, case .row(_, _, let data) = self else { return self }
        let touched = op.data ?? [:]
        return .fields(values: data.filter { touched[$0.key] != nil }, missing: touched.keys.filter { data[$0] == nil })
    }
}

