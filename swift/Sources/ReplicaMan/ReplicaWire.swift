import Foundation

/// The wire grammar, frozen by the server engine (`ruby/lib/replica_man`):
/// `noun.verb`, two nouns (`row`, `doc`), everything addressed by
/// `(stream, id)`. Frames flow down on pull; ops flow up on push and come
/// back as verdicts keyed by the operation id minted when they froze.

// MARK: - Frames (down)

public enum ReplicaFrame: Sendable, Equatable {
    /// Authoritative row state. A revision supersedes only older server state;
    /// local journal entries are rebased on top by the importer.
    case rowSet(stream: String, id: String, type: String?, data: [String: ReplicaValue], revision: Int64? = nil)
    /// Both lanes; on a document stream the ENGINE cascades (fold, owed
    /// journal ops) — driven by the manifest's lane.
    case rowDelete(stream: String, id: String, revision: Int64? = nil)
    /// History the local fold lacks. The document's row, with its revision,
    /// follows in the same answer.
    case docDelta(stream: String, id: String, seq: Int64, codec: String, payload: Data)
    /// Snapshot import is a MERGE into any local doc (loro semantics, never
    /// a blind replace); `data` carries the server projection so the
    /// importer stays dumb.
    case docSnapshot(stream: String, id: String, codec: String, snapshot: Data, data: [String: ReplicaValue], revision: Int64? = nil)

    public var stream: String {
        switch self {
        case .rowSet(let stream, _, _, _, _), .rowDelete(let stream, _, _),
             .docDelta(let stream, _, _, _, _), .docSnapshot(let stream, _, _, _, _, _):
            return stream
        }
    }

    public var id: String {
        switch self {
        case .rowSet(_, let id, _, _, _), .rowDelete(_, let id, _),
             .docDelta(_, let id, _, _, _), .docSnapshot(_, let id, _, _, _, _):
            return id
        }
    }
}

// MARK: - Ops (up)

/// One client op, in the shape the journal persists and the wire carries
/// (`row_id` snake_case, binary fields base64) — serialized byte-stable.
/// In the journal `id` is the entry id; freezing gives the operation a UUID
/// of its own and, for the members of one atomic action, a shared `group`.
public struct ReplicaOp: Sendable, Equatable {
    public var id: String
    public var group: String?
    public var verb: String
    public var stream: String
    public var rowId: String
    public var references: [ReplicaReference]
    public var incarnation: String?
    public var replaces: String?
    public var type: String?
    public var data: [String: ReplicaValue]?
    public var codec: String?
    public var seed: Data?
    public var payload: Data?

    public init(
        id: String, verb: String, stream: String, rowId: String,
        type: String? = nil, data: [String: ReplicaValue]? = nil,
        codec: String? = nil, seed: Data? = nil, payload: Data? = nil, incarnation: String? = nil,
        references: [ReplicaReference] = [], replaces: String? = nil, group: String? = nil
    ) {
        self.id = id
        self.group = group
        self.verb = verb
        self.stream = stream
        self.rowId = rowId
        self.incarnation = incarnation
        self.replaces = replaces
        self.references = references
        self.type = type
        self.data = data
        self.codec = codec
        self.seed = seed
        self.payload = payload
    }

    public enum Verb {
        public static let rowCreate = "row.create"
        public static let rowPatch = "row.patch"
        public static let rowDelete = "row.delete"
        public static let docDelta = "doc.delta"
    }
}

extension ReplicaOp: Codable {
    enum CodingKeys: String, CodingKey {
        case id
        case group
        case verb = "op"
        case stream
        case rowId = "row_id"
        case type
        case data
        case codec
        case seed
        case payload
        case incarnation
        case replaces
        case references
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        group = try container.decodeIfPresent(String.self, forKey: .group)
        verb = try container.decode(String.self, forKey: .verb)
        stream = try container.decode(String.self, forKey: .stream)
        rowId = try container.decode(String.self, forKey: .rowId)
        incarnation = try container.decodeIfPresent(String.self, forKey: .incarnation)
        replaces = try container.decodeIfPresent(String.self, forKey: .replaces)
        references = try container.decodeIfPresent([ReplicaReference].self, forKey: .references) ?? []
        guard references.count <= 64, Set(references.map(\.name)).count == references.count,
              references.allSatisfy({ !$0.name.isEmpty && !$0.stream.isEmpty && !$0.id.isEmpty && !$0.incarnation.isEmpty }) else {
            throw DecodingError.dataCorruptedError(forKey: .references, in: container, debugDescription: "Invalid or duplicate entity references")
        }
        type = try container.decodeIfPresent(String.self, forKey: .type)
        data = try container.decodeIfPresent([String: ReplicaValue].self, forKey: .data)
        codec = try container.decodeIfPresent(String.self, forKey: .codec)
        func binary(_ key: CodingKeys) throws -> Data? {
            guard container.contains(key) else { return nil }
            let text = try container.decode(String.self, forKey: key)
            guard let bytes = Data(base64Encoded: text), bytes.base64EncodedString() == text else {
                throw DecodingError.dataCorruptedError(forKey: key, in: container, debugDescription: "Invalid canonical base64")
            }
            return bytes
        }
        seed = try binary(.seed)
        payload = try binary(.payload)
        guard !id.isEmpty, !stream.isEmpty, !rowId.isEmpty,
              [Verb.rowCreate, Verb.rowPatch, Verb.rowDelete, Verb.docDelta].contains(verb) else {
            throw DecodingError.dataCorruptedError(forKey: .verb, in: container, debugDescription: "Invalid operation identity or verb")
        }
        if group?.isEmpty == true {
            throw DecodingError.dataCorruptedError(forKey: .group, in: container, debugDescription: "Empty operation group")
        }
        if incarnation?.isEmpty == true || replaces?.isEmpty == true || (replaces != nil && verb != Verb.rowCreate) {
            throw DecodingError.dataCorruptedError(forKey: .incarnation, in: container, debugDescription: "Invalid entity lifetime")
        }
        if verb == Verb.rowPatch && data == nil {
            throw DecodingError.dataCorruptedError(forKey: .data, in: container, debugDescription: "Patch requires fields")
        }
        if verb == Verb.docDelta && (payload == nil || codec?.isEmpty != false) {
            throw DecodingError.dataCorruptedError(forKey: .payload, in: container, debugDescription: "Document delta requires payload and codec")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encodeIfPresent(group, forKey: .group)
        try container.encode(verb, forKey: .verb)
        try container.encode(stream, forKey: .stream)
        try container.encode(rowId, forKey: .rowId)
        try container.encodeIfPresent(incarnation, forKey: .incarnation)
        try container.encodeIfPresent(replaces, forKey: .replaces)
        if !references.isEmpty { try container.encode(references, forKey: .references) }
        try container.encodeIfPresent(type, forKey: .type)
        try container.encodeIfPresent(data, forKey: .data)
        try container.encodeIfPresent(codec, forKey: .codec)
        try container.encodeIfPresent(seed.map { $0.base64EncodedString() }, forKey: .seed)
        try container.encodeIfPresent(payload.map { $0.base64EncodedString() }, forKey: .payload)
    }
}

// MARK: - Verdicts

/// The server's word on one op. `rejected` is a VERDICT — the entry parks,
/// never auto-retries; transport failure never produces one (it throws, and
/// the journal retries). Never conflate — v1's core discipline.
public struct ReplicaVerdict: Sendable, Equatable, Codable {
    public enum Outcome: String, Sendable, Codable {
        case accepted
        case rejected
    }

    public let id: String
    public let outcome: Outcome
    public let reason: String?

    public init(id: String, outcome: Outcome, reason: String? = nil) {
        self.id = id
        self.outcome = outcome
        self.reason = reason
    }
}

// MARK: - Shared serialization

public enum ReplicaJSON {
    /// Byte-stable encoding: the journal compares SENT payload bytes against
    /// the entry's current bytes to apply verdicts (an entry replaced
    /// mid-flight is neither deleted nor parked), so encoding must be
    /// deterministic.
    public static func encoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return encoder
    }

    public static func decoder() -> JSONDecoder {
        JSONDecoder()
    }
}
