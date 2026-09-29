import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 1 — bootstrap with `reset` replaces the server's part of the world;
/// what the journal still owes is the device's and stays, with the journal.
final class BootstrapResetTests: XCTestCase {

    func testResetReplacesTheShardWorldAndJournalSurvives() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        // World v1: two notes on the user shard, one asset on global.
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one"), Fixture.note("n2", title: "two")],
            cursor: "10:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        await transport.queuePull(shard: "global", ReplicaPullResponse(
            frames: [.rowSet(stream: "assets", id: "a1", type: nil, data: ["kind": .string("font")])],
            cursor: "10:", more: false
        ))
        try await engine.pullOnce(shard: "global")

        // Unsent local work: a third note, journaled — and the wire's push
        // side goes dead, so the drain barrier can't discharge it first
        // (the offline shape this matrix item exists for).
        try await engine.saveRow(stream: "notes", id: "n3", type: nil, data: ["title": .string("local")])
        XCTAssertEqual(try store.peekPending().count, 1)
        await transport.failPushes(true)

        try await engine.resetCursors()

        // Forced resnapshot (GC horizon / rebuild): the server's part of the
        // user shard is REPLACED — not merged.
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n4", title: "fresh")],
            cursor: "20:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let userRows = try store.allSnapshots().filter { $0.stream == "notes" }
        XCTAssertEqual(userRows.map(\.rowId).sorted(), ["n3", "n4"],
                       "reset wipes the server's n1/n2 and keeps n3, which the server has never had")
        XCTAssertEqual(try store.peekSnapshot("notes", "n3")?.data["title"], .string("local"))

        XCTAssertEqual(
            try store.peekSnapshot("assets", "a1")?.data["kind"], .string("font"),
            "another shard's rows are untouched by this shard's reset"
        )

        let pending = try store.peekPending()
        XCTAssertEqual(pending.count, 1, "the journal SURVIVES a reset — it still owes n3")
        XCTAssertEqual(try pending.first?.op().rowId, "n3")

        let cursor = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(cursor, "20:", "the reset's cursor is the new checkpoint")
    }
}
