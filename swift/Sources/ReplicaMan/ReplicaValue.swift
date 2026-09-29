import Foundation

/// A JSON value on ReplicaMan's wire — frame `data`, op `data`, patch
/// fields. Its own small vocabulary on purpose (ported from Syncer v1's
/// `SyncerValue`): payloads are plain domain fields in the server's
/// camelized wire shape, and the engine stores them verbatim.
public enum ReplicaValue: Sendable, Equatable, Hashable {
    case string(String)
    case number(Double)
    /// Signed 64-bit integers retain every bit on the wire and in documents.
    case integer(Int64)
    case bool(Bool)
    case null
    case array([ReplicaValue])
    case object([String: ReplicaValue])

    public var string: String? {
        if case .string(let s) = self { return s }
        return nil
    }

    public var number: Double? {
        switch self {
        case .number(let n): n
        case .integer(let i): Double(i)
        default: nil
        }
    }

    public var int: Int? {
        if case .integer(let i) = self { return Int(exactly: i) }
        guard let n = number else { return nil }
        return Int(exactly: n)
    }

    /// Canonical JSON integer. Safe integers share the ordinary number form;
    /// larger values retain their signed 64-bit representation.
    public static func signedInteger(_ value: Int64) -> ReplicaValue {
        if (-9_007_199_254_740_991...9_007_199_254_740_991).contains(value) { return .number(Double(value)) }
        return .integer(value)
    }

    public var bool: Bool? {
        if case .bool(let b) = self { return b }
        return nil
    }

    public var items: [ReplicaValue]? {
        if case .array(let items) = self { return items }
        return nil
    }

    public var object: [String: ReplicaValue]? {
        if case .object(let fields) = self { return fields }
        return nil
    }

    /// Read a field on an object value.
    public subscript(key: String) -> ReplicaValue? {
        if case .object(let fields) = self { return fields[key] }
        return nil
    }
}

extension ReplicaValue: Codable {
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        // swiftlint:disable no_try_optional - each JSON type is tried in turn; a mismatch is the normal answer
        if container.decodeNil() {
            self = .null
        } else if let b = try? container.decode(Bool.self) {
            self = .bool(b)
        } else if let i = try? container.decode(Int64.self) {
            self = .signedInteger(i)
        } else if let n = try? container.decode(Double.self) {
            self = .number(n)
        } else if let s = try? container.decode(String.self) {
            self = .string(s)
        } else if let a = try? container.decode([ReplicaValue].self) {
            self = .array(a)
        } else {
            self = .object(try container.decode([String: ReplicaValue].self))
        }
        // swiftlint:enable no_try_optional
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let s): try container.encode(s)
        case .number(let n):
            // Whole numbers encode without a trailing `.0` so the bytes match
            // what the server's JSON emits for integer columns.
            if let i = Int64(exactly: n) {
                try container.encode(i)
            } else {
                try container.encode(n)
            }
        case .integer(let i): try container.encode(i)
        case .bool(let b): try container.encode(b)
        case .null: try container.encodeNil()
        case .array(let items): try container.encode(items)
        case .object(let fields): try container.encode(fields)
        }
    }
}
