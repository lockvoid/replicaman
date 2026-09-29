import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 12 — the loro-free core. The MODULE GRAPH is the enforcement:
/// the `ReplicaMan` target (and this whole test target) depends on GRDB
/// alone — no `import Loro` exists on this side of the boundary, and the
/// gate builds the core target in isolation to prove it. This suite adds the
/// functional half: a rows-only consumer (no codec registered at all) still
/// replicates rows and even keeps document PROJECTIONS readable.
final class LoroFreeCoreTests: XCTestCase {

    func testRowsOnlyEngineWithNoCodecReplicates() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        // No codecs at all — the rows-only consumer.
        let engine = Fixture.engine(store: store, transport: transport, codecs: [], documentMode: .projectionsOnly)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [
                Fixture.note("n1", title: "rows work"),
                // Document frames arrive anyway; without a codec the fold is
                // beyond reach, but the projection data still lands in raw
                // truth (stored-and-skipped, never a throw).
                .docSnapshot(stream: "boards", id: "b1", codec: "loro@1", snapshot: Data("OPAQUE".utf8), data: ["name": .string("Plans")]),
                .docDelta(stream: "boards", id: "b1", seq: 1, codec: "loro@1", payload: Data("OPS".utf8)),
            ],
            cursor: "6:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("rows work"))
        XCTAssertEqual(
            try store.peekSnapshot("boards", "b1")?.data["name"], .string("Plans"),
            "the document's projection row is still useful without the codec"
        )
        XCTAssertNil(try store.peekDoc("boards", "b1"), "no codec, no fold — skipped, not thrown")

        // And the row write door works end to end.
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("mine")])
        try await engine.drain()
        XCTAssertEqual(try store.peekPending().count, 0)
    }
}
