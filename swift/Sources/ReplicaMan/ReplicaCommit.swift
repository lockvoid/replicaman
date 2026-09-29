import Foundation

/// Capture before sending a command. Its reply may refresh only this engine's owner.
public struct ReplicaCommitSession: Sendable {
    let engine: UUID
    let binding: UInt64
}

/// A command reply names what to refresh, never a second route around a pull
/// round. Its dataset binding refuses replies from a restored server.
struct ReplicaCommit {
    let shards: [String]

    init(_ encoded: String, schema: ReplicaSchema, dataset: String?) throws {
        struct Hint: Decodable { let shards: [String] }
        let bytes = try ReplicaProtocol.binary(encoded, limit: 64 * 1024)
        let header = try ReplicaJSON.decoder().decode(ReplicaProtocolHeader.self, from: bytes)
        try header.validate(schema: schema, dataset: dataset)
        let hint = try ReplicaJSON.decoder().decode(Hint.self, from: bytes)
        guard Set(hint.shards).count == hint.shards.count,
              hint.shards.allSatisfy({ schema.shards.contains($0) }) else {
            throw ReplicaError.invalidCommit
        }
        shards = hint.shards
    }
}
