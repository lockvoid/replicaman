import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 7 — drain-before-pull: a pending create followed by an immediate
/// pull reaches the wire as push FIRST, pull second — so the echo is in the
/// answer and a response can never clobber writes it never saw.
final class DrainBeforePullTests: XCTestCase {

    func testPushIsObservedBeforePull() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        try await engine.pullOnce(shard: "user")

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])
        try await engine.pullOnce(shard: "user")

        let events = await transport.events.dropFirst()
        guard events.count >= 2 else {
            return XCTFail("expected a push and a pull on the wire, saw \(events)")
        }
        guard case .push(let ids) = events[events.startIndex] else {
            return XCTFail("the FIRST wire event must be the drain, saw \(events)")
        }
        XCTAssertEqual(ids.count, 1)
        guard case .pull(let shard, _) = events[events.startIndex + 1] else {
            return XCTFail("the pull follows the drain, saw \(events)")
        }
        XCTAssertEqual(shard, "user")
    }

    /// Only a pull answer names the dataset a push must carry: a store that
    /// never synchronized pulls before its first push, sending no dataset.
    func testANeverSynchronizedStoreLearnsItsDatasetBeforeItsFirstPush() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])

        try await engine.drain()

        let events = await transport.events
        XCTAssertEqual(events.count, 2)
        XCTAssertEqual(events.first, .pull(shard: "user", cursor: nil))
        guard case .push = events.last else { return XCTFail("the push follows the pull, saw \(events)") }
        let dataset = try await store.pool.read { try store.meta($0).dataset }
        XCTAssertEqual(dataset, "fixture-dataset")
        XCTAssertTrue(try store.peekPending().isEmpty)
    }

    func testEmptyJournalPullsWithoutAPush() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.pullOnce(shard: "user")

        let pushes = await transport.pushCount
        XCTAssertEqual(pushes, 0, "a pure-read pull costs one request, not two")
        let pulls = await transport.pullCount
        XCTAssertEqual(pulls, 1)
    }
}
