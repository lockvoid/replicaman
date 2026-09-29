import CryptoKit
import Foundation
import ReplicaMan

/// One page a scripted server answers a pull with. The server, not the page,
/// decides `reset` (a request without a cursor is a baseline), each frame's
/// lifetime, and the revision of a frame that names none.
public struct ReplicaPullResponse: Sendable, Equatable {
    public var frames: [ReplicaFrame]
    public var cursor: String
    public var more: Bool

    public init(frames: [ReplicaFrame], cursor: String, more: Bool) {
        self.frames = frames
        self.cursor = cursor
        self.more = more
    }
}

/// Test-only scripted server speaking protocol 2 the way the Rails engine
/// does. The transport scripts the pages and the verdicts of operations the
/// server has not seen; the fixture keeps what the server keeps — claimed
/// operation ids with their digests and verdicts, the cursors it issued and
/// the members it served — so retries, groups, cursors and verification
/// behave as they do against PostgreSQL, which the real-server suite covers.
public protocol FixtureTransport: ReplicaTransport {
    var protocolFixture: ProtocolFixture { get }
    func pull(shard: String, cursor: String?, limit: Int) async throws -> ReplicaPullResponse
    func push(_ ops: [ReplicaOp]) async throws -> [ReplicaVerdict]
}

extension FixtureTransport {
    public func exchange(_ endpoint: ReplicaEndpoint, body: Data) async throws -> Data {
        try await protocolFixture.exchange(endpoint, body: body, transport: self)
    }
}

public actor ProtocolFixture {
    /// What a proxy between client and server can do to a push answer.
    public enum VerdictFault: Sendable {
        case missing, duplicate, foreign, mixedGroup
    }

    /// A script the real server could never have produced — a test mistake,
    /// never a connection failure the engine would retry.
    public struct ScriptError: Error, CustomStringConvertible {
        public let description: String
    }

    private struct Claim {
        let digest: Data
        let verdict: ReplicaVerdict
    }

    private struct Address: Hashable {
        let stream: String
        let id: String
    }

    private struct Member {
        let incarnation: String
        let revision: Int64
    }

    /// One shard as served: every cursor issued, the latest of them (the
    /// heads), and the live members at the heads.
    private struct Shard {
        var issued: Set<String> = []
        var head: String?
        var members: [Address: Member] = [:]
    }

    private static let version = 2

    public let namespace: String
    public let schema: Int
    public private(set) var dataset: String
    private var claims: [String: Claim] = [:]
    private var incarnations: [Address: String] = [:]
    private var shards: [String: Shard] = [:]
    private var revision: Int64 = 0
    private var losesPushReply = false
    private var verdictFault: VerdictFault?

    public init(namespace: String = "replicaman", schema: Int = 1, dataset: String = "fixture-dataset") {
        self.namespace = namespace
        self.schema = schema
        self.dataset = dataset
    }

    /// A restored server: every client that synchronized before is fenced.
    public func restore(dataset: String) {
        self.dataset = dataset
    }

    /// The server no longer recognizes any cursor it issued.
    public func forgetCursors() {
        for shard in shards.keys {
            shards[shard]?.issued = []
            shards[shard]?.head = nil
        }
    }

    /// The next push commits, then its answer is lost on the way back.
    public func losePushReply() {
        losesPushReply = true
    }

    /// The next push answer is corrupted after the server committed it.
    public func corruptVerdicts(_ fault: VerdictFault) {
        verdictFault = fault
    }

    public func exchange(_ endpoint: ReplicaEndpoint, body: Data, transport: any FixtureTransport) async throws -> Data {
        let request = try ReplicaJSON.decoder().decode([String: ReplicaValue].self, from: body)
        try admit(request, initial: endpoint == .pull)
        var response: [String: ReplicaValue] = [
            "protocol": .integer(Int64(Self.version)), "namespace": .string(namespace),
            "schema": .integer(Int64(schema)), "dataset": .string(dataset),
        ]

        switch endpoint {
        case .pull:
            response.merge(try await pull(request, transport: transport)) { _, page in page }
        case .push:
            response["verdicts"] = .array(try await push(request, transport: transport))
        case .verify:
            response.merge(try verify(request)) { _, answer in answer }
        }
        return try ReplicaJSON.encoder().encode(response)
    }

    private func admit(_ request: [String: ReplicaValue], initial: Bool) throws {
        guard request["protocol"]?.int == Self.version else { throw Self.refused("UpgradeRequired") }
        guard request["namespace"]?.string == namespace else { throw Self.refused("NamespaceChanged") }
        guard request["schema"]?.int == schema else { throw Self.refused("UpgradeRequired") }
        if initial, (request["dataset"] ?? .null) == .null { return }
        guard request["dataset"]?.string == dataset else { throw Self.refused("DatasetChanged") }
    }

    private func pull(_ request: [String: ReplicaValue], transport: any FixtureTransport) async throws -> [String: ReplicaValue] {
        guard let shard = request["shard"]?.string else { throw Self.invalid("missing request field: shard") }
        guard let limit = request["limit"]?.int, (1...1000).contains(limit) else {
            throw Self.invalid("limit must be an integer from 1 to 1000")
        }
        let cursor = request["cursor"]?.string
        if let cursor, shards[shard]?.issued.contains(cursor) != true { throw Self.refused("CursorInvalid") }

        let page = try await transport.pull(shard: shard, cursor: cursor, limit: limit)
        // A document's deltas and the row that follows them are one entity.
        let entities = page.frames.filter {
            if case .docDelta = $0 { return false }
            return true
        }.count
        guard entities <= limit, entities > 0 || !page.more else {
            throw ScriptError(description: "A scripted page holds 1...\(limit) entities, or none when nothing is left")
        }

        var served = shards[shard] ?? Shard()
        if cursor == nil { served.members = [:] }
        var frames: [ReplicaValue] = []
        for frame in page.frames {
            frames.append(encode(frame, served: &served))
        }
        served.issued.insert(page.cursor)
        served.head = page.cursor
        shards[shard] = served
        return [
            "shard": .string(shard), "reset": .bool(cursor == nil),
            "frames": .array(frames), "cursor": .string(page.cursor), "more": .bool(page.more),
        ]
    }

    private func push(_ request: [String: ReplicaValue], transport: any FixtureTransport) async throws -> [ReplicaValue] {
        guard let raw = request["ops"]?.items else { throw Self.invalid("operations must be an array") }
        guard raw.count <= 100 else { throw Self.invalid("at most 100 operations are allowed") }
        let operations = try raw.map { value -> (op: ReplicaOp, digest: Data) in
            let bytes = try ReplicaJSON.encoder().encode(value)
            return (try ReplicaJSON.decoder().decode(ReplicaOp.self, from: bytes), Data(SHA256.hash(data: bytes)))
        }
        guard operations.allSatisfy({ Self.isUUID($0.op.id) }) else { throw Self.invalid("operation id must be a UUID") }
        guard operations.allSatisfy({ $0.op.group.map(Self.isUUID) ?? true }) else {
            throw Self.invalid("operation group must be a UUID")
        }
        guard Set(operations.map(\.op.id)).count == operations.count else {
            throw Self.invalid("operation IDs must be unique within a submission")
        }

        var groups: [[Int]] = []
        for index in operations.indices {
            if let last = groups.last?.last, let group = operations[index].op.group, operations[last].op.group == group {
                groups[groups.count - 1].append(index)
            } else {
                groups.append([index])
            }
        }
        let named = groups.compactMap { operations[$0[0]].op.group }
        guard Set(named).count == named.count else { throw Self.invalid("an operation group must be contiguous") }

        let scripted = try await transport.push(operations.map(\.op))

        var replays: [[ReplicaVerdict]] = []
        for group in groups {
            let claimed = group.compactMap { claims[operations[$0].op.id] }
            guard claimed.isEmpty || claimed.count == group.count else {
                throw Self.invalid("an operation group changed after it was applied")
            }
            for (index, claim) in zip(group, claimed) where claim.digest != operations[index].digest {
                throw Self.refused("MutationChanged")
            }
            replays.append(claimed.map(\.verdict))
        }

        var verdicts: [ReplicaVerdict] = []
        for (group, replay) in zip(groups, replays) {
            guard replay.isEmpty else {
                verdicts += replay
                continue
            }
            let answered = try group.map { index in
                let id = operations[index].op.id
                guard let verdict = scripted.first(where: { $0.id == id }) else {
                    throw ScriptError(description: "The push script answered no verdict for \(id)")
                }
                return verdict
            }
            // A refused member rolls back its whole group.
            let refusal = answered.first { $0.outcome == .rejected }
            for index in group {
                let op = operations[index].op
                let verdict = refusal.map { ReplicaVerdict(id: op.id, outcome: .rejected, reason: $0.reason) }
                    ?? ReplicaVerdict(id: op.id, outcome: .accepted)
                claims[op.id] = Claim(digest: operations[index].digest, verdict: verdict)
                if verdict.outcome == .accepted {
                    incarnations[Address(stream: op.stream, id: op.rowId)] = op.incarnation
                }
                verdicts.append(verdict)
            }
        }

        if losesPushReply {
            losesPushReply = false
            throw ReplicaError.transport("HTTP 503: server committed; reply lost (fixture)")
        }
        if let fault = verdictFault {
            verdictFault = nil
            verdicts = Self.corrupt(verdicts, fault)
        }
        return verdicts.map(Self.encode)
    }

    /// The digest of the live members at the heads, only for the heads.
    private func verify(_ request: [String: ReplicaValue]) throws -> [String: ReplicaValue] {
        guard let shard = request["shard"]?.string, let cursor = request["cursor"]?.string else {
            throw Self.invalid("missing request field: shard or cursor")
        }
        guard let served = shards[shard], served.issued.contains(cursor) else { throw Self.refused("CursorInvalid") }
        guard served.head == cursor else { throw Self.refused("CursorBehind") }

        var hash = SHA256()
        hash.update(data: Data("replicaman-view\0".utf8))
        let members = served.members.sorted { left, right in
            left.key.stream == right.key.stream
                ? left.key.id.utf8.lexicographicallyPrecedes(right.key.id.utf8)
                : left.key.stream.utf8.lexicographicallyPrecedes(right.key.stream.utf8)
        }
        for (address, member) in members {
            for field in [address.stream, address.id, member.incarnation, String(member.revision)] {
                var length = UInt64(field.utf8.count).bigEndian
                withUnsafeBytes(of: &length) { hash.update(bufferPointer: $0) }
                hash.update(data: Data(field.utf8))
            }
        }
        return [
            "shard": .string(shard), "cursor": .string(cursor), "count": .string(String(members.count)),
            "digest": .string(hash.finalize().map { String(format: "%02x", $0) }.joined()),
        ]
    }

    private static func corrupt(_ verdicts: [ReplicaVerdict], _ fault: VerdictFault) -> [ReplicaVerdict] {
        switch fault {
        case .missing:
            return Array(verdicts.dropLast())
        case .duplicate:
            return [verdicts[0]] + verdicts
        case .foreign:
            return Array(verdicts.dropLast()) + [ReplicaVerdict(id: UUID().uuidString.lowercased(), outcome: .accepted)]
        case .mixedGroup:
            let last = verdicts[verdicts.count - 1]
            return Array(verdicts.dropLast()) + [ReplicaVerdict(id: last.id, outcome: .rejected, reason: "injected")]
        }
    }

    private static func encode(_ verdict: ReplicaVerdict) -> ReplicaValue {
        var fields: [String: ReplicaValue] = ["id": .string(verdict.id), "outcome": .string(verdict.outcome.rawValue)]
        if let reason = verdict.reason { fields["reason"] = .string(reason) }
        return .object(fields)
    }

    private func encode(_ frame: ReplicaFrame, served: inout Shard) -> ReplicaValue {
        let address = Address(stream: frame.stream, id: frame.id)
        let incarnation = incarnations[address] ?? UUID().uuidString
        incarnations[address] = incarnation
        var value: [String: ReplicaValue] = [
            "stream": .string(frame.stream), "id": .string(frame.id), "incarnation": .string(incarnation),
        ]
        switch frame {
        case .rowSet(_, _, let type, let data, let revision):
            let revision = revision ?? nextRevision()
            value["frame"] = .string("row.set")
            value["revision"] = .string(String(revision))
            value["type"] = type.map(ReplicaValue.string) ?? .null
            value["data"] = .object(data)
            served.members[address] = Member(incarnation: incarnation, revision: revision)
        case .rowDelete(_, _, let revision):
            value["frame"] = .string("row.delete")
            value["revision"] = .string(String(revision ?? nextRevision()))
            served.members[address] = nil
        case .docSnapshot(_, _, let codec, let snapshot, let data, let revision):
            let revision = revision ?? nextRevision()
            value["frame"] = .string("doc.snapshot")
            value["revision"] = .string(String(revision))
            value["codec"] = .string(codec)
            value["snapshot"] = .string(snapshot.base64EncodedString())
            value["data"] = .object(data)
            served.members[address] = Member(incarnation: incarnation, revision: revision)
        case .docDelta(_, _, let seq, let codec, let payload):
            value["frame"] = .string("doc.delta")
            value["seq"] = .integer(seq)
            value["codec"] = .string(codec)
            value["payload"] = .string(payload.base64EncodedString())
        }
        return .object(value)
    }

    private func nextRevision() -> Int64 {
        revision += 1
        return revision
    }

    private static func isUUID(_ text: String) -> Bool {
        let bytes = Array(text.utf8)
        guard bytes.count == 36 else { return false }
        return bytes.indices.allSatisfy { index in
            [8, 13, 18, 23].contains(index)
                ? bytes[index] == UInt8(ascii: "-")
                : (48...57).contains(bytes[index]) || (97...102).contains(bytes[index] | 0x20)
        }
    }

    private static func refused(_ code: String) -> ReplicaError {
        .protocolFailure(code: code, message: "HTTP 409: \(code)")
    }

    private static func invalid(_ message: String) -> ReplicaError {
        .protocolFailure(code: message, message: "HTTP 400: \(message)")
    }
}
