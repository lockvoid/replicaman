import Testing
@testable import ReplicaMan

/// The contract CatalogWarmer leans on: a zero-interval knocker is a pure
/// coalescer — a burst of triggers costs one in-flight sync plus at most one
/// queued re-run, and syncs NEVER overlap. Overlap is not just waste there:
/// parallel warm passes race over the same font file mid-move.
@Suite struct KnockSerialityTests {
    actor Probe {
        var running = 0
        var peak = 0
        var runs = 0

        func enter() {
            running += 1
            peak = max(peak, running)
            runs += 1
        }

        func exit() { running -= 1 }
    }

    @Test func aTriggerBurstNeverOverlapsSyncsAndCoalescesToTwo() async throws {
        let probe = Probe()
        let knocker = ReplicaKnocker {
            await probe.enter()
            try? await Task.sleep(nanoseconds: 50_000_000)
            await probe.exit()
        }

        for _ in 0..<7 { await knocker.knock() }

        // Quiescence, not a settle: wait until a sync has started and then
        // until none is running and the count has stopped moving. Bounded, and
        // it fails by timing out rather than by asserting too early.
        try await until("the burst never started a sync at all") { await probe.runs >= 1 }
        try await until("the knocker never came to rest") {
            let before = await probe.runs
            try await Task.sleep(nanoseconds: 60_000_000)
            let running = await probe.running
            let after = await probe.runs
            return running == 0 && after == before
        }

        #expect(await probe.peak == 1, "syncs must never run concurrently")
        #expect(await probe.runs <= 2, "a burst folds into the in-flight sync plus one re-run")
    }
}
