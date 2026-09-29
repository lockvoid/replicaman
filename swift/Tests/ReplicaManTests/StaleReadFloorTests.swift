import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// The read floor: a materialized read that names the sequence
/// it needs bypasses a cache entry older than it. Without the floor a watch
/// whose observation fired before `commitMutation` evicted the stale entry
/// read the OLD picture, equal to the last delivered one, and the consumer
/// heard nothing until the stream's next commit — History's card sat on
/// «Finalizing» over a store that already held `succeeded`.
final class StaleReadFloorTests: XCTestCase {
    private struct Note: Equatable, Sendable {
        let id: String
        let title: String
    }

    private let decode: @Sendable (String, String?, [String: ReplicaValue]) -> Note? = { id, _, fields in
        Note(id: id, title: fields["title"]?.string ?? "")
    }

    func testAReadAtTheObservedSequenceBypassesAStaleCacheEntry() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [Fixture.note("n1", title: "one")], cursor: "1:", more: false))
        try await engine.pullOnce(shard: "user")

        // The cache is warm at the first sequence.
        let first = try ReplicaReads.materialized(store, stream: "notes", decode: decode)
        XCTAssertEqual(first, [Note(id: "n1", title: "one")])
        let before = try await store.pool.read { try store.changeSequence($0, stream: "notes") }

        // A commit the cache has not been told about yet — the window between
        // the transaction and its `afterNextTransaction` hook.
        try await store.pool.write { db in
            try db.execute(sql: "UPDATE snapshots SET data = ? WHERE stream = 'notes' AND row_id = 'n1'",
                           arguments: ["{\"title\":\"two\"}"])
            try db.execute(sql: "UPDATE stream_meta SET change_seq = change_seq + 1 WHERE stream = 'notes'")
        }
        let after = try await store.pool.read { try store.changeSequence($0, stream: "notes") }
        XCTAssertEqual(after, before + 1)

        // No floor: the stale entry is served (the hazard, stated).
        XCTAssertEqual(try ReplicaReads.materialized(store, stream: "notes", decode: decode), [Note(id: "n1", title: "one")])
        // The floor at the observed sequence: the fresh picture.
        XCTAssertEqual(try ReplicaReads.materialized(store, stream: "notes", minimumSequence: after, decode: decode),
                       [Note(id: "n1", title: "two")])
    }
}
