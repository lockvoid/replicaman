import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// The held documents' lock comes BEFORE the store's writer — a pull, a
/// document edit, a document read take it first — and never inside a write.
/// A document delete that evicted its held copy from within its transaction
/// held the writer while it waited on the lock, and whoever held the lock and
/// needed the writer (or a reader the writer's observers kept pinned) froze
/// the process — documents opening while one of them was deleted.
final class LockOrderTests: XCTestCase {

    func testADocumentDeleteNeverWaitsOnTheHeldDocumentsInsideItsWrite() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.createDoc(stream: "boards", id: "b1", seed: Data("seed".utf8), peer: 7)
        let deleting = DispatchSemaphore(value: 0)
        let signal = SnapshotDeleteSignal(deleting)
        store.pool.add(transactionObserver: signal)

        let locked = DispatchSemaphore(value: 0)
        let wrote = WroteFlag()
        let wroteUnderTheLock = expectation(description: "the lock holder got the writer")
        DispatchQueue.global().async {
            engine.liveDocuments.publishing {
                locked.signal()
                deleting.wait()
                try? store.pool.write { _ in }
            }
            wrote.set()
            wroteUnderTheLock.fulfill()
        }
        await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                locked.wait()
                continuation.resume()
            }
        }

        let deletion = Task { try await engine.deleteRow(stream: "boards", id: "b1") }
        await fulfillment(of: [wroteUnderTheLock], timeout: 3)
        guard wrote.value else { return }

        _ = try await deletion.value
        XCTAssertNil(try store.peekSnapshot("boards", "b1"))
        XCTAssertNil(try engine.docRow(stream: "boards", id: "b1"))
    }
}

/// Signals, from inside the writer's transaction, the first time a snapshot
/// row is deleted.
private final class SnapshotDeleteSignal: TransactionObserver, @unchecked Sendable {
    private let semaphore: DispatchSemaphore
    private let lock = NSLock()
    private var signalled = false

    init(_ semaphore: DispatchSemaphore) {
        self.semaphore = semaphore
    }

    func observes(eventsOfKind eventKind: DatabaseEventKind) -> Bool {
        guard case .delete(let table) = eventKind else { return false }
        return table == "snapshots"
    }

    func databaseDidChange(with event: DatabaseEvent) {
        let first = lock.withLock {
            defer { signalled = true }
            return !signalled
        }
        if first { semaphore.signal() }
    }

    func databaseDidCommit(_ db: Database) {}

    func databaseDidRollback(_ db: Database) {}
}

private final class WroteFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var stored = false

    var value: Bool { lock.withLock { stored } }

    func set() {
        lock.withLock { stored = true }
    }
}
