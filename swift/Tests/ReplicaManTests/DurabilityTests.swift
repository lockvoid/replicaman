import GRDB
import XCTest
@testable import ReplicaMan

final class DurabilityTests: XCTestCase {
    func testSavedWritesRequestDurableWALCommits() throws {
        let store = try Fixture.store()
        try store.pool.writeWithoutTransaction { db in
            XCTAssertEqual(try Int.fetchOne(db, sql: "PRAGMA synchronous"), 2)
            XCTAssertEqual(try Int.fetchOne(db, sql: "PRAGMA fullfsync"), 1)
        }
    }

    func testProjectionStoreCannotBecomeAnEditableReplicaWithAnIncompleteHistory() async throws {
        let directory = Fixture.directory()
        let projections = Fixture.unopenedEngine(in: directory, transport: StubTransport(), codecs: [], documentMode: .projectionsOnly)
        try projections.openForColdBoot(owner: 42)
        try await projections.close()
        let editable = Fixture.unopenedEngine(in: directory, transport: StubTransport())
        XCTAssertThrowsError(try editable.openForColdBoot(owner: 42)) {
            XCTAssertEqual($0 as? ReplicaError, .storage(
                "Document mode belongs to the store. Use a separate store for projection-only replicas."))
        }
    }
}
