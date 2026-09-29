import Foundation

/// A declared relationship whose target lifetime travels with each mutation.
public struct ReplicaReferenceSpec: Sendable, Equatable {
    public var name: String
    public var stream: String
    public var field: String?
    public var keySegment: Int?
    public var keyPrefix: String?
    public var optional: Bool

    public init(name: String, stream: String, field: String? = nil,
                keySegment: Int? = nil, keyPrefix: String? = nil, optional: Bool = false) {
        self.name = name
        self.stream = stream
        self.field = field
        self.keySegment = keySegment
        self.keyPrefix = keyPrefix
        self.optional = optional
    }

    func target(rowId: String, data: [String: ReplicaValue]) throws -> String? {
        if let keyPrefix, !rowId.hasPrefix(keyPrefix) { return nil }
        let id: String?
        if let keySegment {
            let parts = rowId.split(separator: "/", omittingEmptySubsequences: false)
            id = parts.indices.contains(keySegment) ? String(parts[keySegment]) : nil
        } else {
            let value = data[field ?? name]
            if optional && (value == nil || value == .null) { return nil }
            id = value?.string
        }
        guard let id, !id.isEmpty else {
            throw ReplicaError.storage("Missing or invalid reference: \(name)")
        }
        return id
    }
}

public struct ReplicaReference: Sendable, Equatable, Codable {
    public var name: String
    public var stream: String
    public var id: String
    public var incarnation: String

    public init(name: String, stream: String, id: String, incarnation: String) {
        self.name = name
        self.stream = stream
        self.id = id
        self.incarnation = incarnation
    }
}

enum ReplicaLifetime {
    static func derived(namespace: String, stream: String, id: String, parent: ReplicaReference) -> String {
        let parts = ["replicaman:derived:1", namespace, stream, id,
                     parent.stream, parent.id, parent.incarnation]
        let content = parts.map { "\($0.utf8.count):\($0)" }.joined()
        return "derived:" + ReplicaProtocol.digest(Data(content.utf8))
    }
}
