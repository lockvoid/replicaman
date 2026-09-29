import Foundation

/// The engine's ONE identity fact: which owner's file this process holds open,
/// and the store over it. `nil` is the whole closed world — no owner, no file,
/// no pool, nothing a write could land in.
///
/// Read without the actor (`store`, `owner`, `database` are all `nonisolated`,
/// and the doc plane loads folds on the main actor without a hop), so the state
/// lives under a lock rather than in actor storage. Every WRITE to it goes
/// through the actor, so a write and a rebind can never interleave.
final class ReplicaBinding: @unchecked Sendable {
    struct Bound {
        var owner: Int
        var store: ReplicaStateStore
        var path: URL
    }

    private let lock = NSLock()
    private var bound: Bound?
    /// Bumped whenever the bound store changes. Watchers ride it: an
    /// observation is only ever valid for one pool, so every change has to
    /// re-arm the ones that were riding the old one.
    private var generation: UInt64 = 0
    private var waiters: [(id: UUID, continuation: CheckedContinuation<Void, Never>)] = []

    var current: Bound? {
        lock.lock()
        defer { lock.unlock() }
        return bound
    }

    var owner: Int? {
        lock.lock()
        defer { lock.unlock() }
        return bound?.owner
    }

    var store: ReplicaStateStore? {
        lock.lock()
        defer { lock.unlock() }
        return bound?.store
    }

    /// The bound store plus the generation it was read at — a watcher arms on
    /// the pair so it can park for the NEXT store without missing one that
    /// arrived while it was arming.
    func snapshot() -> (bound: Bound?, generation: UInt64) {
        lock.lock()
        defer { lock.unlock() }
        return (bound, generation)
    }

    func bind(_ replacement: Bound) {
        resume { bound = replacement }
    }

    @discardableResult
    func unbind() -> Bound? {
        var released: Bound?
        resume {
            released = bound
            bound = nil
        }
        return released
    }

    /// The cold-boot bind: build and publish under the lock, so two first
    /// touches from different threads cannot each open the file.
    func bindIfUnbound(_ make: () throws -> Bound) throws {
        lock.lock()
        guard bound == nil else {
            lock.unlock()
            return
        }
        do {
            let made = try make()
            bound = made
            generation &+= 1
            let woken = waiters
            waiters.removeAll()
            lock.unlock()
            for waiter in woken { waiter.continuation.resume() }
        } catch {
            lock.unlock()
            throw error
        }
    }

    /// The merge's swap: one store out, one store in, ONE generation. A
    /// watcher must never see the unbound moment in between — an empty
    /// picture mid-sign-in is the wipe the preserved world exists to avoid.
    func replace(with replacement: Bound) {
        resume { bound = replacement }
    }

    /// Parks until the bound store differs from the one read at `generation`.
    /// Cancellable: a watcher whose consumer went away must not hold the task
    /// group that is racing it.
    func waitForChange(after generation: UInt64) async {
        let id = UUID()
        await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                lock.lock()
                guard self.generation == generation, !Task.isCancelled else {
                    lock.unlock()
                    continuation.resume()
                    return
                }
                waiters.append((id, continuation))
                lock.unlock()
            }
        } onCancel: {
            release(id)
        }
    }

    private func release(_ id: UUID) {
        lock.lock()
        guard let index = waiters.firstIndex(where: { $0.id == id }) else {
            lock.unlock()
            return
        }
        let waiter = waiters.remove(at: index)
        lock.unlock()
        waiter.continuation.resume()
    }

    private func resume(_ mutate: () -> Void) {
        lock.lock()
        mutate()
        generation &+= 1
        let woken = waiters
        waiters.removeAll()
        lock.unlock()
        for waiter in woken { waiter.continuation.resume() }
    }
}
