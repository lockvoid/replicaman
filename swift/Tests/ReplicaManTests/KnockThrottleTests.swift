import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Amendment B — the doorbell throttle: during a render the signal rate is
/// ~5/s per cook, and coalescing alone still runs back-to-back pulls. A
/// trailing-edge throttle caps doorbell-triggered syncs at ~1 per window;
/// the LAST doorbell always lands, so the final state is caught up.
final class KnockThrottleTests: XCTestCase {

    func testRapidKnocksCostAtMostOnePullPerWindow() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let knocker = ReplicaKnocker(interval: 0.4) {
            do {
                _ = try await engine.pullUntilCaughtUp()
            } catch {}
        }

        // A render-storm burst: far more doorbells than windows.
        for _ in 0..<20 {
            await knocker.knock()
            try await Task.sleep(nanoseconds: 10_000_000)
        }

        // Quiesce.
        await eventually(timeout: 8, "knocker never went quiet") {
            let before = await transport.pullCount
            guard before > 0 else { return false }
            try? await Task.sleep(nanoseconds: 500_000_000)
            let after = await transport.pullCount
            return after == before
        }

        let shards = Fixture.schema().shards.count
        let cycles = await transport.pullCount / shards
        // ~0.2s of burst + trailing runs: one immediate cycle plus at most
        // two throttled trailing ones is the ceiling; without the throttle
        // this is 10+.
        XCTAssertGreaterThanOrEqual(cycles, 1, "the doorbell still pulls")
        XCTAssertLessThanOrEqual(cycles, 3, "one sync per window, not one per doorbell")
    }

    func testTrailingEdgeAlwaysDeliversTheLastDoorbell() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let knocker = ReplicaKnocker(interval: 0.2) {
            do {
                _ = try await engine.pullUntilCaughtUp()
            } catch {}
        }

        // First doorbell syncs immediately and consumes the queued state.
        await knocker.knock()
        await eventually(timeout: 2, "first sync never ran") {
            await transport.pullCount > 0
        }
        let afterFirst = await transport.pullCount

        // A doorbell INSIDE the window must still produce a sync — late,
        // never lost: the world moved after the last pull.
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "fresh")], cursor: "9:", more: false
        ))
        await knocker.knock()

        try await eventually(timeout: 3, "the trailing doorbell was dropped") {
            try store.peekSnapshot("notes", "n1")?.data["title"] == .string("fresh")
        }
        let afterSecond = await transport.pullCount
        XCTAssertGreaterThan(afterSecond, afterFirst)
    }

    /// A doorbell rung BY A TAP jumps the window. STOP writes no row — the
    /// epoch bump on the chat row IS the answer — so the button the person is
    /// staring at cannot flip until this pull lands. Making them wait out a
    /// throttle window designed for render chatter is how "Stop does nothing"
    /// gets reported.
    func testAnImmediateKnockDoesNotWaitOutTheWindow() async throws {
        let syncs = Tally()
        let knocker = ReplicaKnocker(interval: 5.0) { syncs.bump() }

        await knocker.knock()
        await eventually(timeout: 2, "the first sync never ran") { syncs.count == 1 }

        // Inside the 5s window. A polite doorbell would sit here for seconds.
        let started = Date()
        await knocker.knock(immediate: true)
        await eventually(timeout: 2, "the tapped doorbell waited out the window") { syncs.count == 2 }
        XCTAssertLessThan(Date().timeIntervalSince(started), 1.0)

        // The window is restored for the machine chatter that follows. This
        // wait IS the contract — a throttle is a statement about time, and 0.7s
        // inside a 5s window is what "still throttled" means. It is not a
        // settle standing in for a missing seam.
        await knocker.knock()
        try await Task.sleep(nanoseconds: 700_000_000)
        XCTAssertEqual(syncs.count, 2, "an ordinary doorbell is still throttled")
    }
}
