import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 2 — one pull batch applies ATOMICALLY with its cursor advance. A
/// crash mid-batch leaves the previous checkpoint intact: store unchanged,
/// cursor unmoved, and the same batch re-serves cleanly afterwards.
final class CheckpointAtomicityTests: XCTestCase {

    func testFaultBeforeCommitLeavesPreviousCheckpointIntact() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        struct Fault: Error {}
        await engine.setCheckpointFault { throw Fault() }
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "poisoned"), Fixture.note("n2", title: "half")],
            cursor: "9:", more: false
        ))

        do {
            try await engine.pullOnce(shard: "user")
            XCTFail("the injected fault must surface")
        } catch {}

        XCTAssertEqual(
            try store.peekSnapshot("notes", "n1")?.data["title"], .string("one"),
            "a faulted batch must not leave partial writes"
        )
        XCTAssertNil(try store.peekSnapshot("notes", "n2"))
        let cursor = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(cursor, "5:", "the cursor must not advance past an unapplied batch")

        // The next pull re-serves from the intact checkpoint and lands.
        await engine.setCheckpointFault(nil)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "poisoned"), Fixture.note("n2", title: "half")],
            cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("poisoned"))
        XCTAssertEqual(try store.peekSnapshot("notes", "n2")?.data["title"], .string("half"))
        let advanced = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(advanced, "9:")
    }
}
