import Foundation
import GRDB
import Testing
@_spi(SignOutIdentityTransition) @testable import ReplicaMan

/// Retry and backoff graded from the RAW journal and from the engine's own
/// cold stamp — never through `pendingOps()`, which is the accessor the engine
/// wrote with.
///
/// W7a Wave 1 found that every retry assertion in the package read through
/// that accessor, and that the cold window could only be exercised with wall
/// clock sleeps because `isCold` calls `Date()` directly
/// (`ReplicaEngine.swift:1191`) and ignores the engine's injected `clock`
/// (`:55`). Both are avoidable: `isColdForTesting(_:)` (`:1194`) exposes the
/// stamp, and `coldWindow: 0` expresses "no cooling" exactly, so nothing here
/// sleeps.
@Suite struct RawJournalRetryTests {

    private struct RawEntry: Equatable {
        var id: String
        var lane: String
        var parked: String?
        var payload: String
    }

    private func rawJournal(_ store: ReplicaStateStore) throws -> [RawEntry] {
        try store.pool.read { db in
            try Row.fetchAll(db, sql: "SELECT id, lane, reason, payload FROM intents WHERE state <> 'accepted' ORDER BY rowid")
                .map { RawEntry(id: $0["id"], lane: $0["lane"], parked: $0["reason"], payload: $0["payload"]) }
        }
    }

    @Test func aFailedPrefixCoolsBothPrioritiesUntilAnExplicitRetry() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, coldWindow: 60)
        try await engine.saveRow(stream: "notes", id: "import", type: nil, data: ["title": .string("bulk")])
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "message", type: nil, data: ["title": .string("typed")])
        }
        let before = try rawJournal(store)
        await transport.failPushes(true)
        do {
            _ = try await engine.drain(.bulk)
            Issue.record("the failed wire must surface")
        } catch ReplicaError.transport {
            // Expected connection failure; no result was reconciled.
        }
        #expect(await engine.isColdForTesting(.bulk))
        #expect(await engine.isColdForTesting(.interactive))
        await transport.failPushes(false)
        try await engine.drainIfWarm()
        #expect(try rawJournal(store) == before)

        _ = try await engine.drain()
        #expect(await transport.pushedOps().map(\.rowId) == ["import", "message"])
        #expect(try rawJournal(store).isEmpty)
    }

    /// A transport failure is RETRYABLE, never a verdict: the entries keep
    /// their ids, their lanes and their exact BYTES, so the retry is the same
    /// request. Graded raw — a decode through `op()` would hide a payload the
    /// engine had rewritten.
    ///
    /// `coldWindow: 0` is what makes this sleep-free: the stamp is set to
    /// "now", so the very next `isCold` check is already false. That is the
    /// "past the window the drain retries" case stated as a value, not as a
    /// duration to wait out.
    ///
    /// KILL: `ReplicaEngine.swift:1368` — delete `coldUntil[lane ?? .bulk] = nil`.
    /// A lane that has just succeeded stays stamped, so with any real cold
    /// window the next `drainIfWarm` skips a wire that is provably alive.
    @Test func aFailedDrainKeepsEveryByteAndTheSuccessfulRetryIsTheSameRequest() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, coldWindow: 0)

        for index in 0..<3 {
            try await engine.saveRow(
                stream: "notes", id: "n\(index)", type: nil, data: ["title": .string("t\(index)")]
            )
        }
        let before = try rawJournal(store)
        #expect(before.count == 3)

        await transport.failPushes(true)
        do {
            _ = try await engine.drain(.bulk)
            Issue.record("the dead wire never surfaced")
        } catch {
            // Expected: transport death throws and applies nothing.
        }

        #expect(try rawJournal(store) == before,
                "a severed wire rewrote the journal — ids, lanes or bytes moved when nothing was judged")
        #expect(try rawJournal(store).allSatisfy { $0.parked == nil }, "no connection is not a refusal")

        // Zero cold window: the retry is admitted immediately, no sleep.
        #expect(await engine.isColdForTesting(.bulk) == false)
        await transport.failPushes(false)
        try await engine.drainIfWarm()

        let attempts = await transport.events.compactMap { event -> [String]? in
            if case .push(let ids) = event { return ids } else { return nil }
        }
        #expect(attempts.count == 2 && attempts[0] == attempts[1],
                "the retry must re-present exactly the operations that failed, under the same ids")
        let sent = await transport.pushedBatches.flatMap { $0 }
        #expect(sent.map(\.rowId) == ["n0", "n1", "n2"], "the retry carries exactly the entries that failed")
        #expect(try rawJournal(store).isEmpty)
        #expect(await engine.isColdForTesting(.bulk) == false, "a successful drain leaves no stamp behind")
    }

    /// The sign-out flush is the one drain whose failure destroys work: what it
    /// leaves behind, the retirement wipes. Incremental acks bound the loss to
    /// one chunk (`ReplicaEngine.swift:1342-1349`). Graded raw, because this is
    /// the count that decides whether a user's typed message survives.
    ///
    /// KILL: `ReplicaEngine.swift:1346` — move the `applyVerdicts(…)` call out
    /// of the `while start < ops.count` loop to after it. All 120 entries then
    /// stay owed and the assertion of 70 goes red; in production the whole
    /// backlog is re-stranded on every flush.
    @Test func aMidFlushChunkDeathStrandsOnlyTheUnackedChunks() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await transport.failPushesAfter(calls: 1)
        let engine = Fixture.engine(store: store, transport: transport)

        for index in 0..<120 {
            try await engine.saveRow(
                stream: "notes", id: String(format: "n%03d", index),
                type: nil, data: ["title": .string("t\(index)")]
            )
        }

        await engine.seal()
        do {
            _ = try await engine.sealAndDrain(using: transport)
            Issue.record("the second chunk's transport death never surfaced")
        } catch {
            // Expected.
        }

        let owed = try rawJournal(store)
        #expect(owed.count == 70,
                "chunk 1's 50 ops acked incrementally and left the journal; only the unsent 70 may remain")
        #expect(owed.allSatisfy { $0.parked == nil }, "a flush failure parks nothing — the wipe is what threatens it")
        // The survivors are the TAIL: the first 50 are gone, in journal order.
        let survivingRows = try await store.pool.read { db in
            try String.fetchAll(db, sql: """
                SELECT json_extract(payload, '$.row_id') FROM intents
                WHERE state IN ('draft', 'owed', 'frozen') ORDER BY rowid LIMIT 1
                """)
        }
        #expect(survivingRows == ["n050"], "the acked prefix must be the prefix, not an arbitrary 50")
    }
}
