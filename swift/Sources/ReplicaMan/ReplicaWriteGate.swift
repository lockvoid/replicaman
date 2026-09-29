import Foundation

/// The identity fence for local transactions, which run on the caller's
/// thread rather than on the engine actor: a transaction enters while the
/// gate is open, and `close()` returns only once every transaction that
/// entered has left — the store under them can then be merged or released.
final class ReplicaWriteGate: @unchecked Sendable {
    private let lock = NSLock()
    private var closed = false
    private var active = 0
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func enter() throws {
        lock.lock()
        defer { lock.unlock() }
        guard !closed else { throw ReplicaError.identityTransitionInProgress }
        active += 1
    }

    func leave() {
        lock.lock()
        active -= 1
        let woken = closed && active == 0 ? waiters : []
        if !woken.isEmpty { waiters.removeAll() }
        lock.unlock()
        for waiter in woken { waiter.resume() }
    }

    func close() async {
        await withCheckedContinuation { continuation in
            lock.lock()
            closed = true
            guard active > 0 else {
                lock.unlock()
                continuation.resume()
                return
            }
            waiters.append(continuation)
            lock.unlock()
        }
    }

    func open() {
        lock.lock()
        closed = false
        lock.unlock()
    }
}
