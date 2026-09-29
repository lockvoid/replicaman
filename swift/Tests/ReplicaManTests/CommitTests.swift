import Foundation
import XCTest
@testable import ReplicaMan

/// Command replies name affected shards. Rows arrive only through a complete
/// pull round; the reply itself never installs database state.
final class CommitTests: XCTestCase {
    private func hint(dataset: String = "fixture-dataset", shards: [String] = ["user"]) throws -> String {
        try JSONSerialization.data(withJSONObject: [
            "protocol": 2, "namespace": "replicaman", "schema": 1,
            "dataset": dataset, "shards": shards
        ]).base64EncodedString()
    }

    func testCommandRefreshPublishesTheCheckpointAndItsCursorTogether() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let session = try await engine.commitSession()
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "committed")
        ], cursor: "after-command", more: false))

        try await engine.apply(commit: hint(), session: session)

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("committed"))
        XCTAssertTrue(try store.peekPending().isEmpty)
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "after-command")
    }

    func testCommandRefreshPreservesOfflineAuthoring() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "before", rank: "a")
        ], cursor: "before", more: false))
        try await engine.pullOnce()
        let session = try await engine.commitSession()
        await transport.failPushes(true)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("offline")])
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "remote", rank: "b")
        ], cursor: "after", more: false))

        try await engine.apply(commit: hint(), session: session)

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data, ["title": .string("offline"), "rank": .string("b")])
        XCTAssertEqual(try store.peekPending().count, 1)
        XCTAssertEqual(engine.health.lastFailure?.error as? ReplicaError, .transport("push refused (stub)"))
    }

    func testCommandRefreshContinuesAStagedRoundToTheCommittedRows() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [], cursor: "baseline", more: false))
        try await engine.pullOnce()
        let session = try await engine.commitSession()
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "old")], cursor: "page-one", more: true))
        try await engine.pullOnce()
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "new"), Fixture.note("n2", title: "new")
        ], cursor: "new-cut", more: false))

        try await engine.apply(commit: hint(), session: session)

        let events = await transport.events
        XCTAssertEqual(events.last, .pull(shard: "user", cursor: "page-one"), "the refresh continues the staged round")
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("new"))
        XCTAssertEqual(try store.peekSnapshot("notes", "n2")?.data["title"], .string("new"))
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "new-cut")
    }

    func testCommandSessionCannotCrossEnginesOrOwners() async throws {
        let source = Fixture.engine(store: try Fixture.store(), transport: StubTransport())
        let session = try await source.commitSession()
        let targetStore = try Fixture.store()
        let target = Fixture.engine(store: targetStore, owner: 99, transport: StubTransport())

        do {
            try await target.apply(commit: hint(), session: session)
            XCTFail("An outgoing command was admitted by another store")
        } catch ReplicaError.staleCommit {}

        XCTAssertTrue(try targetStore.allSnapshots().isEmpty)
    }

    func testInvalidDatasetAndUnknownShardsCannotChangePublishedState() async throws {
        for invalid in [try hint(dataset: "restored"), try hint(shards: ["private"]), try hint(shards: ["user", "user"])] {
            let store = try Fixture.store()
            let transport = StubTransport()
            let engine = Fixture.engine(store: store, transport: transport)
            await transport.queuePull(shard: "user", .init(frames: [], cursor: "synchronized", more: false))
            try await engine.pullOnce()
            await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "unasked")], cursor: "later", more: false))
            let session = try await engine.commitSession()
            do {
                try await engine.apply(commit: invalid, session: session)
                XCTFail("Invalid command hint was accepted")
            } catch {
                XCTAssertTrue(try store.allSnapshots().isEmpty)
                let cursor = try await engine.currentCursor()
                XCTAssertEqual(cursor, "synchronized")
            }
        }
    }

    func testResetDuringBootstrapRefusesTheOldResponseEvenWithNoCursor() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let reached = expectation(description: "bootstrap response on the wire")
        let release = AsyncStream<Void>.makeStream()
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "stale")
        ], cursor: "old-bootstrap", more: false))
        await transport.onPull { _ in
            reached.fulfill()
            for await _ in release.stream { break }
        }
        let pull = Task { try await engine.pullOnce() }
        await fulfillment(of: [reached], timeout: 3)
        try await engine.resetCursors()
        release.continuation.yield(())
        _ = try await pull.value

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        let cursor = try await engine.currentCursor()
        XCTAssertNil(cursor)
        await transport.onPull { _ in }
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "fresh")
        ], cursor: "fresh-bootstrap", more: false))
        try await engine.pullOnce()
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("fresh"))
    }

    func testPullPageDecodeRejectsInvalidRevisions() throws {
        XCTAssertEqual(try ReplicaPullPage.decode(page(revision: "1")).frames.map(\.frame),
                       [.rowSet(stream: "notes", id: "n1", type: nil, data: [:], revision: 1)])
        for revision in ["invalid", "0", "-1", "9223372036854775808"] {
            XCTAssertThrowsError(try ReplicaPullPage.decode(page(revision: revision)), revision) {
                guard case DecodingError.dataCorrupted(let context) = $0 else { return XCTFail("\($0)") }
                XCTAssertEqual(context.debugDescription, "Replica revision must be a canonical positive 64-bit decimal string")
            }
        }
    }

    private func page(revision: String) throws -> Data {
        let frame: [String: Any] = ["frame": "row.set", "stream": "notes", "id": "n1", "incarnation": "life-1",
                                    "data": [:], "revision": revision]
        return try JSONSerialization.data(withJSONObject: [
            "protocol": 2, "namespace": "replicaman", "schema": 1, "dataset": "fixture-dataset",
            "shard": "user", "reset": false, "frames": [frame], "cursor": "next", "more": false,
        ])
    }
}
