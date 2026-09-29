import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// A foreign identity takes the device: the outgoing user's ENTIRE local
/// world goes — snapshots on every shard, cursors AND the journal. An
/// unpushed op that survived would ride the next identity's bearer: the
/// write-side mirror of the E1.9 cross-identity read leak.
///
/// The wipe is the FILE going away, so there is no wiping pass that could
/// miss a table, and no store for a late write to land in.
final class IdentitySwitchWipeTests: XCTestCase {

    func testRetirementTakesEveryShardTheCursorsAndTheJournalWithTheFile() async throws {
        let directory = Fixture.directory()
        let transport = StubTransport()
        let engine = Fixture.unopenedEngine(in: directory, transport: transport)
        try await engine.open(owner: 1)

        // A signed-in world on both shards…
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one")],
            cursor: "10:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        await transport.queuePull(shard: "global", ReplicaPullResponse(
            frames: [.rowSet(stream: "assets", id: "a1", type: nil, data: ["kind": .string("font")])],
            cursor: "10:", more: false
        ))
        try await engine.pullOnce(shard: "global")

        // …plus an unpushed local op the dead wire cannot discharge.
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("local")])
        await transport.failPushes(true)
        let owedBeforeRetirement = try await engine.pendingOps().count
        XCTAssertEqual(owedBeforeRetirement, 1)

        try await engine.retire()
        try await engine.open(owner: 2)

        let store = try XCTUnwrap(engine.store)
        XCTAssertEqual(try store.allSnapshots().count, 0, "no snapshot survives a foreign switch — either shard")
        XCTAssertEqual(
            try store.peekPending().count, 0,
            "an unpushed op must NEVER ride the next identity's bearer"
        )
        let userCursor = try await engine.currentCursor(shard: "user")
        let globalCursor = try await engine.currentCursor(shard: "global")
        XCTAssertNil(userCursor, "a blank cursor makes the next pull re-snapshot the new identity's world")
        XCTAssertNil(globalCursor)
        XCTAssertFalse(
            FileManager.default.fileExists(atPath: engine.storeURL(owner: 1).path),
            "the outgoing owner's file is the wipe"
        )
    }
}
