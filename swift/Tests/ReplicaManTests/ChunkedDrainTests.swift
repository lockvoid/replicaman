import Foundation
import XCTest
@testable import ReplicaMan

/// The drain ships BOUNDED chunks with INCREMENTAL acks. The 500-op single
/// request this replaces looped forever on a real device: the server spent
/// minutes applying it, the client timed out first, no verdict ever landed,
/// and the same 647 ops re-pushed every cold-window — the "idle phone
/// ringing its own doorbell 2.5/s" storm. Small chunks return in seconds
/// and every acked chunk leaves the journal for good, so a mid-drain
/// transport death costs one chunk, never the whole backlog.
final class ChunkedDrainTests: XCTestCase {

    /// Thread-safe pending-count log for the push-time observation hook.
    private final class PendingLog: @unchecked Sendable {
        private let lock = NSLock()
        private var values: [Int] = []
        func append(_ value: Int) {
            lock.lock()
            values.append(value)
            lock.unlock()
        }
        var counts: [Int] {
            lock.lock()
            defer { lock.unlock() }
            return values
        }
    }

    private func fill(_ engine: ReplicaEngine, count: Int) async throws {
        for i in 0..<count {
            try await engine.saveRow(
                stream: "notes", id: String(format: "n%03d", i), type: nil,
                data: ["title": .string("t\(i)")]
            )
        }
    }

    func testDrainShipsBoundedChunksWithIncrementalAcks() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        // The load-bearing assertion is the MID-DRAIN one: pending counts
        // observed at each chunk's push time. Chunk 2 must find chunk 1's
        // 50 entries already GONE from the journal (applied before the next
        // chunk ships), chunk 3 must find 100 gone. An end-state check alone
        // stays green under the old apply-everything-at-the-end drain — a
        // review mutation proved it.
        let observed = PendingLog()
        let hook = HookOutcome()
        await transport.onPush { [store, observed] _ in
            do {
                observed.append(try await store.pool.read { [store] in try store.pending($0).count })
            } catch {
                // A failed read here would otherwise vanish into a sentinel and
                // read as a passing chunk boundary (FLEET_LAW ban #4).
                await hook.record(error)
            }
        }

        try await fill(engine, count: 120)
        _ = try await engine.drain()

        let hookFailure = await hook.failure
        XCTAssertNil(hookFailure, "the mid-push observation itself failed; the counts below prove nothing")
        let sizes = await transport.pushedBatches.map(\.count)
        XCTAssertEqual(sizes, [50, 50, 20], "the whole queue drains as bounded chunks, in order")
        XCTAssertEqual(
            observed.counts, [120, 70, 20],
            "each chunk ships only after the previous chunk's acks left the journal"
        )

        let pending = try await store.pool.read { [store] in try store.pending($0) }
        XCTAssertTrue(pending.isEmpty, "every acked chunk leaves the journal")
    }

    func testMidDrainFailureKeepsOnlyUnackedChunksPending() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await transport.failPushesAfter(calls: 1)
        let engine = Fixture.engine(store: store, transport: transport)

        try await fill(engine, count: 120)

        do {
            _ = try await engine.drain()
            XCTFail("the second chunk's transport death must surface")
        } catch {}

        let pending = try await store.pool.read { [store] in try store.pending($0) }
        XCTAssertEqual(
            pending.count, 70,
            "chunk 1's 50 ops acked INCREMENTALLY and left the journal; only the unsent 70 stay — " +
            "a lost response can no longer strand the whole backlog"
        )
    }
}
