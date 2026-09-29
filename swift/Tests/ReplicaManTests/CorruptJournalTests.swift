import XCTest
import GRDB
@testable import ReplicaMan

final class CorruptJournalTests: XCTestCase {
    func testUnreadableJournalCannotProveThatLocalBlobsAreUnused() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["blob": .string("local:keep")])
        try await store.pool.write { db in
            try db.execute(sql: "UPDATE intents SET payload = '{broken'")
        }
        do {
            _ = try await engine.pendingFieldStrings(stream: "notes", fields: ["blob"])
            XCTFail("corruption was mistaken for an empty keeping set")
        } catch {}
        do {
            _ = try await engine.pendingRowIds(stream: "notes")
            XCTFail("corruption was mistaken for no pending rows")
        } catch {}
        XCTAssertEqual(try store.peekPending().count, 1)
    }
    func testUnreadableRollbackImageRefusesVerdictAndRetainsPendingWrite() async throws {
        let store = try Fixture.store()
        let wire = StubTransport()
        let engine = Fixture.engine(store: store, transport: wire)
        try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("original")])
        _ = try await engine.drain()
        try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("pending")])
        try await store.pool.write { db in try db.execute(sql: "UPDATE intents SET preimage = '{broken' WHERE state <> 'accepted'") }
        await wire.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "denied") } }
        do { _ = try await engine.drain(); XCTFail("unreadable rollback image was treated as absent") } catch {}
        XCTAssertEqual(try store.peekPending().count, 1)
        XCTAssertEqual(try store.peekParked().count, 0)
        XCTAssertEqual(try store.peekSnapshot("notes", "n")?.data["title"], .string("pending"))
    }
}
