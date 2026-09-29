import Foundation

/// A gate the HOST puts on what leaves the device. The engine asks it when a
/// row is WRITTEN — never on a drain — and knows nothing about bytes,
/// settings or accounts: a change goes, is held, or is never sent.
///
/// A held row leaves the journal alone: the engine keeps it in `holds` and
/// asks again only when the gate says its answer may have changed
/// (`changes`), when the row is written again, and when the store opens.
/// A released row leaves as its current state, never as the history of its
/// writes.
public protocol SyncGate: Sendable {
    /// Stable across launches — the holds it placed are released by it.
    var id: String { get }
    /// The stream this gate judges; nil judges every stream. A gate over
    /// every stream is a policy (Cloud Backup): what it lets go does not wait
    /// for the rows it holds.
    var stream: String? { get }
    func judge(_ change: SyncChange) -> SyncVerdict
    /// Fires when something this gate consults moved: a setting flipped,
    /// bytes landed. Every held row of the gate is asked again.
    var changes: AsyncStream<Void> { get }
}

extension SyncGate {
    /// A gate whose answer never changes on its own.
    public var changes: AsyncStream<Void> { AsyncStream { $0.finish() } }
}

public enum SyncVerdict: Sendable, Equatable {
    /// Send the change.
    case push
    /// Hold the whole row: none of its writes leave until the gate lets it go.
    case gate(String)
    /// Never send this change: the device keeps its value, the row's later
    /// writes go on. Never a row's birth — every later write stands on it.
    case discard
}

/// One change as a gate sees it: one write of one row, the values it set and
/// the values it replaced.
public struct SyncChange: Sendable, Equatable {
    public enum Kind: String, Sendable, Equatable {
        case create
        case patch
        case delete
        case document
    }

    public let stream: String
    public let rowId: String
    public let kind: Kind
    /// The value this write set for every field it changed — empty for a
    /// delete and a document.
    public let local: [String: ReplicaValue]
    /// The value each of those fields held before this write — the whole row
    /// a delete displaced. A field the row did not have is absent — every
    /// field of a create.
    public let previous: [String: ReplicaValue]

    public init(
        stream: String, rowId: String, kind: Kind,
        local: [String: ReplicaValue] = [:], previous: [String: ReplicaValue] = [:]
    ) {
        self.stream = stream
        self.rowId = rowId
        self.kind = kind
        self.local = local
        self.previous = previous
    }
}

/// A gate's "ask me again": any number of engines listen, the host fires.
public final class SyncGateSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var listeners: [UUID: AsyncStream<Void>.Continuation] = [:]

    public init() {}

    public var stream: AsyncStream<Void> {
        let id = UUID()
        let (stream, continuation) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
        continuation.onTermination = { [weak self] _ in self?.forget(id) }
        lock.withLock { listeners[id] = continuation }
        return stream
    }

    public func fire() {
        let current = lock.withLock { Array(listeners.values) }
        for listener in current { listener.yield() }
    }

    private func forget(_ id: UUID) {
        _ = lock.withLock { listeners.removeValue(forKey: id) }
    }
}

/// The declared gates applied as one — the gates over every stream, then the
/// change's stream's: a discard by any gate discards (a change never needed is
/// not held for later either); else the first hold holds.
public struct SyncGates: Sendable {
    public enum Outcome: Sendable, Equatable {
        case push
        case hold(gate: String, reason: String)
        case discard
    }

    private let byStream: [String: [any SyncGate]]
    private let everyStream: [any SyncGate]

    public init(_ gates: [any SyncGate]) {
        var byStream: [String: [any SyncGate]] = [:]
        var everyStream: [any SyncGate] = []
        for gate in gates {
            if let stream = gate.stream {
                byStream[stream, default: []].append(gate)
            } else {
                everyStream.append(gate)
            }
        }
        self.byStream = byStream
        self.everyStream = everyStream
    }

    public var isEmpty: Bool { byStream.isEmpty && everyStream.isEmpty }

    /// Whether a row held by this gate makes the rows naming it wait — every
    /// gate but a policy over every stream.
    public func orders(_ gateId: String) -> Bool {
        !everyStream.contains { $0.id == gateId }
    }

    public var all: [any SyncGate] { everyStream + byStream.values.flatMap { $0 } }

    public func judge(_ change: SyncChange) -> Outcome {
        var held: Outcome?
        for gate in everyStream + (byStream[change.stream] ?? []) {
            switch gate.judge(change) {
            case .discard:
                return .discard
            case .gate(let reason):
                held = held ?? .hold(gate: gate.id, reason: reason)
            case .push:
                break
            }
        }
        return held ?? .push
    }
}
