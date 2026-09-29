import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 3 — tail apply: `row.set` REPLACES (no merge, no
/// insert-or-update branching), `row.delete` removes, and ordering within a
/// batch is preserved.
final class TailApplyTests: XCTestCase {

    func testRowSetReplacesRowDeleteRemovesOrderPreserved() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
            Fixture.note("n1", title: "first", rank: "a"),
            // Replacement drops fields the new copy doesn't carry — that is
            // what "unconditional replace" means.
            .rowSet(stream: "notes", id: "n1", type: nil, data: ["title": .string("second")]),
            Fixture.note("n2", title: "doomed"),
            .rowDelete(stream: "notes", id: "n2"),
            Fixture.note("n3", title: "last"),
        ], cursor: "7:", more: false))
        try await engine.pullOnce(shard: "user")

        let n1 = try XCTUnwrap(store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(n1.data["title"], .string("second"))
        XCTAssertNil(n1.data["rank"], "row.set replaced the whole copy; the stale field is gone")
        XCTAssertNil(try store.peekSnapshot("notes", "n2"), "set-then-delete within one batch lands deleted")
        XCTAssertEqual(try store.peekSnapshot("notes", "n3")?.data["title"], .string("last"))
    }

    func testPullUntilCaughtUpFollowsMoreAndThreadsTheCursor() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one")], cursor: "5:x", more: true
        ))
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n2", title: "two")], cursor: "9:", more: false
        ))

        let applied = try await engine.pullUntilCaughtUp()
        XCTAssertEqual(applied, 2)

        let events = await transport.events
        let userPulls: [String?] = events.compactMap {
            if case .pull(let shard, let cursor) = $0, shard == "user" { return cursor } else { return nil }
        }
        XCTAssertEqual(userPulls, [nil, "5:x"], "the second page pulls FROM the first page's cursor")
        let cursor = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(cursor, "9:")
        XCTAssertNotNil(try store.peekSnapshot("notes", "n2"))
    }
}
