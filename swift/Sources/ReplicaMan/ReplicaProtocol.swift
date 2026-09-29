import Foundation
import CryptoKit

public enum ReplicaEndpoint: String, Sendable {
    case pull
    case push
    case verify
}

enum ReplicaProtocol {
    static let version = 2
    /// One push carries at most this many operations; so does one atomic action.
    static let maxOperations = 100
    /// The server reads request bodies up to 32 MiB; the envelope around a
    /// push's operations stays under 1 KiB.
    static let operationBytes = 32 * 1024 * 1024 - 1024
    /// A page stops after about 256 KiB of frames but completes its last
    /// entity, which is at most 32 MiB.
    static let responseBytes = 64 * 1024 * 1024

    static func isDigest(_ value: String) -> Bool {
        value.count == 64 && value.utf8.allSatisfy { (48...57).contains($0) || (97...102).contains($0) }
    }

    static func digest(_ content: Data) -> String {
        SHA256.hash(data: content).map { String(format: "%02x", $0) }.joined()
    }

    static func counter(_ text: String) throws -> Int64 {
        guard !text.isEmpty, text.count <= 19,
              text == "0" || text.first != "0",
              text.utf8.allSatisfy({ (48...57).contains($0) }),
              let value = Int64(text), value >= 0 else {
            throw ReplicaError.protocolFailure(code: "InvalidResponse", message: "Expected a decimal int64 counter")
        }

        return value
    }

    static func binary(_ text: String, limit: Int) throws -> Data {
        guard text.utf8.count <= (limit + 2) / 3 * 4,
              let bytes = Data(base64Encoded: text), bytes.count <= limit,
              bytes.base64EncodedString() == text else {
            throw ReplicaError.protocolFailure(code: "InvalidResponse", message: "Invalid or oversized base64 content")
        }

        return bytes
    }

    static func invalidResponse(_ message: String) -> ReplicaError {
        .protocolFailure(code: "InvalidResponse", message: message)
    }
}

struct ReplicaProtocolHeader: Decodable, Sendable {
    let `protocol`: Int
    let namespace: String
    let schema: Int
    let dataset: String

    func validate(schema expected: ReplicaSchema, dataset expectedDataset: String?) throws {
        guard self.protocol == ReplicaProtocol.version,
              namespace == expected.namespace, schema == expected.version else {
            throw ReplicaError.protocolFailure(code: "UpgradeRequired", message: "Incompatible replica protocol or schema")
        }

        guard !dataset.isEmpty, expectedDataset == nil || dataset == expectedDataset else {
            throw ReplicaError.protocolFailure(code: "DatasetChanged", message: "The authoritative dataset changed")
        }
    }
}

/// One `/pull` answer: the frames past the request's cursor, read from one
/// database snapshot. A frame that does not decode refuses the whole page;
/// skipping it would move the cursor past a change that was never saved.
struct ReplicaPullPage: Decodable, Sendable {
    let header: ReplicaProtocolHeader
    let shard: String
    let reset: Bool
    let frames: [ReplicaPageFrame]
    let cursor: String
    let more: Bool

    private enum Keys: String, CodingKey {
        case shard, reset, frames, cursor, more
    }

    init(from decoder: Decoder) throws {
        header = try ReplicaProtocolHeader(from: decoder)
        let fields = try decoder.container(keyedBy: Keys.self)
        shard = try fields.decode(String.self, forKey: .shard)
        reset = try fields.decode(Bool.self, forKey: .reset)
        frames = try fields.decode([ReplicaPageFrame].self, forKey: .frames)
        cursor = try fields.decode(String.self, forKey: .cursor)
        more = try fields.decode(Bool.self, forKey: .more)
        guard !shard.isEmpty, !cursor.isEmpty else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath,
                debugDescription: "A pull answer names its shard and its cursor"))
        }
    }

    static func decode(_ data: Data) throws -> ReplicaPullPage {
        try ReplicaJSON.decoder().decode(ReplicaPullPage.self, from: data)
    }
}

/// A frame and the lifetime it belongs to. Every frame but a document delta
/// carries the row's revision; the delta's row arrives as the `row.set` after it.
struct ReplicaPageFrame: Decodable, Sendable {
    let incarnation: String
    let frame: ReplicaFrame

    private enum Keys: String, CodingKey {
        case frame, stream, id, incarnation, revision, type, data, seq, codec, payload, snapshot
    }

    init(from decoder: Decoder) throws {
        let fields = try decoder.container(keyedBy: Keys.self)
        func invalid(_ message: String) -> DecodingError {
            .dataCorrupted(.init(codingPath: fields.codingPath, debugDescription: message))
        }
        let kind = try fields.decode(String.self, forKey: .frame)
        let stream = try fields.decode(String.self, forKey: .stream)
        let id = try fields.decode(String.self, forKey: .id)
        incarnation = try fields.decode(String.self, forKey: .incarnation)
        guard !stream.isEmpty, !id.isEmpty, !incarnation.isEmpty else {
            throw invalid("Replica frames require a nonempty stream, id and incarnation")
        }
        func revision() throws -> Int64 {
            let text = try fields.decode(String.self, forKey: .revision)
            guard let value = Int64(text), value > 0, String(value) == text else {
                throw invalid("Replica revision must be a canonical positive 64-bit decimal string")
            }
            return value
        }
        func codec() throws -> String {
            let name = try fields.decode(String.self, forKey: .codec)
            guard !name.isEmpty else { throw invalid("Document frames name their codec") }
            return name
        }
        func binary(_ key: Keys) throws -> Data {
            let text = try fields.decode(String.self, forKey: key)
            guard let bytes = Data(base64Encoded: text), bytes.base64EncodedString() == text else {
                throw invalid("Document bytes must be canonical base64")
            }
            return bytes
        }
        switch kind {
        case "row.set":
            frame = .rowSet(stream: stream, id: id, type: try fields.decodeIfPresent(String.self, forKey: .type),
                            data: try fields.decode([String: ReplicaValue].self, forKey: .data), revision: try revision())
        case "row.delete":
            frame = .rowDelete(stream: stream, id: id, revision: try revision())
        case "doc.snapshot":
            frame = .docSnapshot(stream: stream, id: id, codec: try codec(), snapshot: try binary(.snapshot),
                                 data: try fields.decode([String: ReplicaValue].self, forKey: .data), revision: try revision())
        case "doc.delta":
            let seq = try fields.decode(Int64.self, forKey: .seq)
            guard seq > 0 else { throw invalid("Document delta sequence starts at one") }
            frame = .docDelta(stream: stream, id: id, seq: seq, codec: try codec(), payload: try binary(.payload))
        default:
            throw invalid("Unknown replica frame kind: \(kind)")
        }
    }
}

/// `/push`'s answer: one verdict per operation, in request order.
struct ReplicaPushAnswer: Decodable, Sendable {
    let verdicts: [ReplicaVerdict]
}

/// `/verify`'s answer: the membership digest of the shard at the cursor sent.
struct ReplicaVerifyAnswer: Decodable, Sendable {
    let shard: String
    let cursor: String
    let count: String
    let digest: String
}
