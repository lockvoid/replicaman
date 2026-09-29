import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Refused births can be discarded explicitly. Unusable document updates
/// preserve the checkpoint, and explicit resync requests a fresh baseline.
final class ResidualBehaviorTests: XCTestCase {

    func testDeleteOfParkedCreateDiscardsTheEvidenceAndJournalsNothing() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])
        await transport.scriptPush { ops in
            ops.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "refused") }
        }
        try await engine.drain()
        XCTAssertEqual(try store.peekParked().count, 1, "the create parked (and the row reverted)")

        // The user deletes the thing whose create was refused: the parked
        // evidence goes with it, and NO row.delete is journaled — the server
        // never heard the id.
        try await engine.deleteRow(stream: "notes", id: "n1")
        XCTAssertEqual(try store.peekParked().count, 0)
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    func testUnknownDocumentCodecRefusesCheckpointAndResyncRebuilds() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        try await pullBoard(engine, transport, snapshot: "SNAP", cursor: "5:")
        try await assertAlienDeltaKeepsTheCheckpoint(engine, transport)

        try await engine.resyncDocument(stream: "boards", id: "b1")
        XCTAssertNil(try store.peekDoc("boards", "b1"))
        let resynced = try await engine.currentCursor(shard: "user")
        XCTAssertNil(resynced)

        try await pullBoard(engine, transport, snapshot: "SNAP2", cursor: "7:")
        XCTAssertEqual(try store.peekDoc("boards", "b1")?.fold, Data("SNAP2".utf8))
        let rebuilt = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(rebuilt, "7:")
    }

    private func pullBoard(_ engine: ReplicaEngine, _ transport: StubTransport, snapshot: String, cursor: String) async throws {
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data(snapshot.utf8), data: [:])],
            cursor: cursor, more: false
        ))
        try await engine.pullOnce(shard: "user")
    }

    private func assertAlienDeltaKeepsTheCheckpoint(_ engine: ReplicaEngine, _ transport: StubTransport) async throws {
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 1, codec: "alien@9", payload: Data("OPS".utf8))],
            cursor: "6:", more: false
        ))
        do {
            try await engine.pullOnce(shard: "user")
            XCTFail("An unsupported codec must not advance the checkpoint")
        } catch { }
        let retainedCursor = try await engine.currentCursor()
        XCTAssertEqual(retainedCursor, "5:")
    }
}
