import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 6 — peer rotation: a wiped fold's document is reborn with a NEW
/// loro peer. Loro dedups by (peer, counter); a reborn doc reusing its peer
/// would have its edits silently discarded — the v1 lesson, kept.
final class PeerRotationTests: XCTestCase {

    func testExplicitDocumentResyncMintsAFreshPeer() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, minter: Fixture.sequentialMinter(from: 100))

        let snapshot = Data("SNAP-1".utf8)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: snapshot, data: ["name": .string("Plans")])],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let born = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(born.peer, 100, "first birth mints the first peer")

        // Explicitly dropping the fold creates a fresh authoring peer.
        try await engine.resyncDocument(stream: "boards", id: "b1")
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: snapshot, data: ["name": .string("Plans")])],
            cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let reborn = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(reborn.peer, 101, "a reborn doc NEVER reuses its old peer")
        XCTAssertNotEqual(reborn.peer, born.peer)
    }

    func testSurvivingDocKeepsItsPeerAcrossOrdinaryPulls() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, minter: Fixture.sequentialMinter(from: 100))

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SNAP".utf8), data: [:])],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        // An ordinary tail (no reset) touching the same doc must not rotate.
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 1, codec: "stub@1", payload: Data("+d1".utf8))],
            cursor: "6:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let doc = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(doc.peer, 100, "peers are stable while the fold lives")
    }
}
