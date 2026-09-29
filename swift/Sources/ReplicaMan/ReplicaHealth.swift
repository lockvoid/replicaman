import Foundation

/// A failure from work without an awaiting caller, such as an observation or
/// an automatic push. Explicit sync and storage calls also throw to their caller.
public enum ReplicaFailureKind: Sendable {
    case transport, storage, upgradeRequired, recoveryRequired, other
}

public struct ReplicaFailure: Sendable {
    public let operation: String
    public let message: String
    public let occurredAt: Date
    public let error: any Error

    public var kind: ReplicaFailureKind {
        switch error {
        case ReplicaError.transport: return .transport
        case ReplicaError.storage: return .storage
        case ReplicaError.protocolFailure(let code, _):
            if code == "UpgradeRequired" { return .upgradeRequired }
            if ["DatasetChanged", "NamespaceChanged", "MutationChanged"].contains(code) {
                return .recoveryRequired
            }
            return .other
        default: return .other
        }
    }
}

public final class ReplicaHealth: @unchecked Sendable {
    private let lock = NSLock()
    private var latest: ReplicaFailure?
    private var listeners: [UUID: AsyncStream<ReplicaFailure>.Continuation] = [:]

    public var lastFailure: ReplicaFailure? {
        lock.lock()
        defer { lock.unlock() }
        return latest
    }

    public func failures() -> AsyncStream<ReplicaFailure> {
        AsyncStream(bufferingPolicy: .bufferingNewest(32)) { continuation in
            let id = UUID()
            lock.lock()
            listeners[id] = continuation
            if let latest { continuation.yield(latest) }
            lock.unlock()
            continuation.onTermination = { [weak self] _ in self?.remove(id) }
        }
    }

    /// Report a failure at a host-owned background boundary without losing its cause.
    public func record(_ error: any Error, operation: String) {
        let failure = ReplicaFailure(operation: operation, message: String(describing: error), occurredAt: Date(), error: error)
        lock.lock()
        latest = failure
        let current = Array(listeners.values)
        lock.unlock()
        for listener in current { listener.yield(failure) }
        Log.logger.error("[health] \(operation, privacy: .public): \(failure.message, privacy: .public)")
    }

    private func remove(_ id: UUID) {
        lock.lock()
        defer { lock.unlock() }
        listeners[id] = nil
    }
}
