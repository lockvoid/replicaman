import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// A pulled row snapshot must never regress a row that still owes journal
/// ops: local = server + my unacked ops. Both shapes of the same hole:
///
///   1. the race — a local write lands WHILE a pull is on the wire; the
///      answer carries the server's older state and used to overwrite the
///      newer local row (then the push acks, the doorbell is our own echo,
///      no pull follows — the row stays stale until the next re-run);
///   2. the cold lane — `drainIfWarm` skips a cold bulk lane, the pull
///      proceeds with pending bulk ops in the journal, same overwrite.
final class PullRebaseTests: XCTestCase {

    /// A row `running` pushed → pull in
    /// flight → `succeeded` written locally → pull answer says `running`.
    func testStaleFrameDoesNotRegressARowWithAPendingWrite() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("running")])
        _ = try await engine.drain()

        // The server answers with what it had when the request arrived —
        // BEFORE the local write below.
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "running")], cursor: "1:", more: false
        ))
        let hook = HookOutcome()
        await transport.onPull { _ in
            do {
                try await engine.saveRow(
                    stream: "notes", id: "n1", type: nil, data: ["title": .string("succeeded")]
                )
            } catch {
                await hook.record(error)
            }
        }

        try await engine.pullOnce(shard: "user")
        let hookFailure = await hook.failure
        XCTAssertNil(hookFailure, "the mid-pull write failed; the race this test stages never happened")

        let row = try store.peekSnapshot("notes", "n1")
        XCTAssertEqual(row?.data["title"]?.string, "succeeded",
                       "the pull's older server state overwrote a write the server has not seen yet")
        XCTAssertEqual(try store.peekPending().count, 1, "the newer write is still owed — nothing may drop it")

        // The baseline, asserted where it discriminates: once nothing is owed,
        // the rebase must stop protecting the row and the server's frame wins.
        // On its own this case is satisfied by a rebase that never runs.
        await transport.onPull { _ in }
        _ = try await engine.drain()
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "theirs")], cursor: "2:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"]?.string, "theirs")
    }

    /// The same overwrite without a race: the lane is cold, the pull does not
    /// drain, the pending write is in the journal when the frame lands.
    func testStaleFrameDoesNotRegressAPendingWriteOnAColdLane() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("running"), "rank": .string("a")])
        _ = try await engine.drain()

        await transport.failPushes(true)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("succeeded"), "rank": .string("a")])
        do {
            _ = try await engine.drain()
            XCTFail("the dead push side must surface — its throw is what cools the lane")
        } catch {}
        await transport.failPushes(false)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "running", rank: "server-touched")], cursor: "1:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let row = try store.peekSnapshot("notes", "n1")
        XCTAssertEqual(row?.data["title"]?.string, "succeeded", "my unacked patch must ride on top of the server's row")
        XCTAssertEqual(row?.data["rank"]?.string, "server-touched", "fields I did not touch take the server's value")
        XCTAssertEqual(try store.peekPending().count, 1)
    }

    /// An address alone cannot prove a birth was accepted. A different
    /// incarnation must leave local authoring intact until its result arrives.
    func testAnotherIncarnationDoesNotEraseAnUnprocessedBirth() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.failPushes(true)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("Before")])
        do {
            _ = try await engine.drain()
            XCTFail("the dead push side must surface")
        } catch {}
        await transport.failPushes(false)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "Renamed elsewhere", rank: "server")], cursor: "1:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let row = try store.peekSnapshot("notes", "n1")
        XCTAssertEqual(row?.data["title"]?.string, "Before")
        XCTAssertEqual(try store.peekPending().count, 1)

        await transport.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "IdentityCollision") } }
        try await engine.drain()
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"]?.string, "Renamed elsewhere")
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["rank"]?.string, "server")
    }
}
