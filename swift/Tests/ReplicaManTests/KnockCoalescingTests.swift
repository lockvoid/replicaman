import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 9 — doorbell coalescing: N concurrent knocks collapse into at
/// most one in-flight sync plus one queued re-run (≤ 2 transport cycles);
/// callers never await.
final class KnockCoalescingTests: XCTestCase {

    func testTenConcurrentKnocksCostAtMostTwoCycles() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.delayPulls(nanos: 50_000_000)

        let knocker = ReplicaKnocker {
            do {
                _ = try await engine.pullUntilCaughtUp()
            } catch {}
        }

        for _ in 0..<10 {
            await knocker.knock()
        }

        // Quiescence, not a settle: the count must stop moving across a window
        // LONGER than the stubbed pull latency, so an in-flight cycle cannot
        // look finished. Bounded, with a named failure.
        await eventually(timeout: 8, "knocker never went quiet") {
            let before = await transport.pullCount
            guard before > 0 else { return false }
            try? await Task.sleep(nanoseconds: 150_000_000)
            let after = await transport.pullCount
            return after == before
        }

        let shards = Fixture.schema().shards.count
        let pulls = await transport.pullCount
        XCTAssertGreaterThanOrEqual(pulls, shards, "a knock really pulls")
        XCTAssertLessThanOrEqual(
            pulls, 2 * shards,
            "ten doorbells collapse to at most one in-flight sync plus one queued re-run"
        )
    }
}
