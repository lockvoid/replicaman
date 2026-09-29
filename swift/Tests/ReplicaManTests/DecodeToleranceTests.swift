import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Unknown streams refuse checkpoint publication; known streams retain future fields.
final class DecodeToleranceTests: XCTestCase {

    func testKnownStreamRetainsUnknownTypeAndFields() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
            // A known stream with an unknown STI type and an unknown field.
            .rowSet(stream: "notes", id: "n1", type: "HoloNote", data: [
                "title": .string("future"),
                "hologram": .bool(true),
            ]),
            Fixture.note("n2", title: "plain"),
        ], cursor: "4:", more: false))

        // The import itself must not throw.
        try await engine.pullOnce(shard: "user")

        let holo = try XCTUnwrap(store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(holo.data["hologram"], .bool(true), "unknown fields are stored verbatim")

        // The typed layer skips what it cannot decode — and only that.
        let stream = RowStream<TestNote>(engine: engine)
        XCTAssertNil(try stream.find("n1"), "unknown STI type decodes to nothing, not a crash")
        XCTAssertEqual(try stream.list().map(\.id), ["n2"], "typed reads skip the undecodable row")
    }

    func testUnknownStreamRefusesTheWholeCheckpoint() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
            Fixture.note("n1", title: "valid"),
            .rowSet(stream: "widgets", id: "w1", type: nil, data: [:]),
        ], cursor: "4:", more: false))
        do {
            _ = try await engine.pullOnce()
            XCTFail("unsupported schema must refuse publication")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "UpgradeRequired")
        }
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        let cursor = try await engine.currentCursor()
        XCTAssertNil(cursor)
    }

    func testHTTPPullRefusesTheWholePageWhenAFrameIsInvalid() throws {
        func page(_ invalid: String) -> Data {
            Data("""
            {"protocol": 2, "namespace": "replicaman", "schema": 1, "dataset": "fixture-dataset",
             "shard": "user", "reset": false, "cursor": "7:k", "more": true, "frames": [
                {"frame": "row.set", "stream": "notes", "id": "n1", "incarnation": "i1", "revision": "4", "data": {"title": "ok"}},
                {"frame": "doc.delta", "stream": "boards", "id": "b1", "incarnation": "i2", "seq": 3, "codec": "loro@1", "payload": "AAEC"}
                \(invalid)
            ]}
            """.utf8)
        }

        XCTAssertEqual(try ReplicaPullPage.decode(page("")).frames.count, 2)
        XCTAssertThrowsError(try ReplicaPullPage.decode(page(#", {"frame": "row.vanish", "stream": "notes", "id": "n2", "incarnation": "i3"}"#)))
        XCTAssertThrowsError(try ReplicaPullPage.decode(page(
            #", {"frame": "doc.delta", "stream": "boards", "id": "b2", "incarnation": "i4", "seq": 1, "codec": "loro@1", "payload": "%%%not-base64%%%"}"#
        )))
    }
}
