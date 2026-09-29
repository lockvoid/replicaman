import ReplicaManTestProtocol
import XCTest
@testable import ReplicaMan

final class SyncStatusTests: XCTestCase {
    func testDeliveryStagesStayUnsettledUntilCheckpointVisibility() async throws {
        let store = try Fixture.store()
        let wire = StubTransport()
        let engine = Fixture.engine(store: store, transport: wire)
        XCTAssertFalse(try store.syncStatus().hasUnsettledWork)
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: ["title": .string("saved")])
        XCTAssertEqual(try store.syncStatus().queuedOperations, 1)
        XCTAssertNotNil(try store.syncStatus().oldestIntentAt)

        try await store.pool.write { db in
            _ = try store.freezeSubmissions(db, lane: nil, limit: 100)
        }
        XCTAssertEqual(try store.syncStatus().submittedGroups, 1)
        try await engine.drain()
        XCTAssertTrue(try store.peekPending().isEmpty)
        let accepted = try store.syncStatus()
        XCTAssertEqual(accepted.submittedGroups, 0)
        XCTAssertEqual(accepted.acceptedOperations, 1)
        XCTAssertTrue(accepted.hasUnsettledWork)
        XCTAssertTrue(try store.containsUnsettledOperation { $0.rowId == "n" })

        await wire.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n", title: "saved")], cursor: "1:", more: false))
        try await engine.pullOnce(shard: "user")
        XCTAssertFalse(try store.syncStatus().hasUnsettledWork)
        XCTAssertFalse(try store.containsUnsettledOperation { $0.rowId == "n" })
    }

    func testCorruptIntentCannotBeMistakenForNoWork() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: [:])
        try await store.pool.write { db in
            try db.execute(sql: "UPDATE intents SET payload = '{broken'")
        }
        XCTAssertTrue(try store.syncStatus().hasUnsettledWork)
        XCTAssertThrowsError(try store.containsUnsettledOperation { _ in false })
        XCTAssertEqual(try store.peekPending().count, 1)
    }
}
