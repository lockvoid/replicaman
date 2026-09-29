import ReplicaManTestProtocol
import Foundation
import GRDB
import Testing
@testable import ReplicaMan

/// The cold boot: a process starts over a store file that already exists and
/// already owes work. W7a Wave 1 found this whole door untested —
/// `openForColdBoot` (`ReplicaEngine.swift:530`) had ZERO references anywhere
/// in `Tests/`, while being the ONLY door production uses on launch.
///
/// It is not `open(owner:)` with a different name. It is `nonisolated` (no
/// actor hop, so a returning user's grid renders on the first frame), it binds
/// only from nothing, it runs its OWN `healCursors` call (`:535`), and — the
/// finding this suite exists to pin — unlike `open(owner:)` (`:520`) it never
/// calls `unseal()`, so it never schedules a push for the journal it just
/// re-opened.
///
/// Scope note: the ENGINE contract is graded here. The app-level wake that
/// makes the contract safe in production — `ReplicaHost.open(owner:)` calling
/// `knock()` (`ReplicaHost.swift:103`), and `BlobManager.markUploadLanded(_:)`
/// calling `ReplicaHost.knock()` (`BlobManager+UploadStaging.swift`,
/// `markUploadLanded:176-179`) — belongs to W5 and is deliberately not
/// duplicated here.
@Suite struct ColdBootDrainTests {

    /// Everything a killed process leaves on disk for the next one.
    private func writeOwedWorld(
        in directory: URL,
        owner: Int,
        rows: [(id: String, data: [String: ReplicaValue])],
        syncGates: [any SyncGate] = []
    ) async throws -> [String] {
        let transport = StubTransport()
        let engine = Fixture.unopenedEngine(in: directory, transport: transport, syncGates: syncGates)
        try await engine.open(owner: owner)
        for row in rows {
            try await engine.saveRow(stream: "notes", id: row.id, type: nil, data: row.data)
        }
        let owed = try await engine.pendingOps().map(\.id)
        // The process ends. The file stays; `close` never retires it.
        try await engine.close()
        return owed
    }

    private func rawPendingIds(_ store: ReplicaStateStore) throws -> [String] {
        try store.pool.read { db in
            try String.fetchAll(
                db, sql: "SELECT id FROM intents WHERE state IN ('draft', 'owed', 'frozen') ORDER BY rowid"
            )
        }
    }

    /// The store's own header states the stake: "losing the journal loses the
    /// user's unsent work" (`ReplicaStateStore.swift:8-9`). Nothing proved it
    /// across a real process boundary — every other durability test in this
    /// package reopens a `ReplicaStateStore` directly, never the engine's cold
    /// door. Read RAW: `SELECT id FROM intents`, never `pendingOps()`.
    ///
    /// KILL: `ReplicaSyncSchema.sql` (from `protocol/storage.sql`)
    /// `CREATE TABLE IF NOT EXISTS intents (` →
    /// `CREATE TEMP TABLE IF NOT EXISTS intents (`.
    /// Every in-process test stays green; the relaunch loses the user's
    /// unsent work and only this test says so.
    @Test func theOwedJournalSurvivesTheProcessAndIsStillOwedAfterAColdBoot() async throws {
        let directory = Fixture.directory()
        let owedBefore = try await writeOwedWorld(in: directory, owner: 1, rows: [
            ("n1", ["title": .string("typed before the kill")]),
            ("n2", ["title": .string("and this one too")]),
        ])
        #expect(owedBefore.count == 2)

        // A NEW engine — a new process, in every way the test can express.
        let reborn = Fixture.unopenedEngine(in: directory, transport: StubTransport())
        try reborn.openForColdBoot(owner: 1)

        let store = try #require(reborn.store, "the cold door bound nothing")
        #expect(try rawPendingIds(store) == owedBefore,
                "the relaunched process no longer owes the work the user did before the kill")
        #expect(try RowStream<TestNote>(engine: reborn).find("n1")?.title == "typed before the kill",
                "…and the returning user's world is readable with no actor hop, on the first frame")
    }

    @Test func coldBootKeepsTheCursorOfAValidEmptyCheckpoint() async throws {
        let directory = Fixture.directory()
        let transport = StubTransport()
        let engine = Fixture.unopenedEngine(in: directory, transport: transport)
        try await engine.open(owner: 1)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [], cursor: "10:", more: false
        ))
        _ = try await engine.pullOnce()
        try await engine.close()
        let reborn = Fixture.unopenedEngine(in: directory, transport: StubTransport())
        try reborn.openForColdBoot(owner: 1)
        #expect(try await reborn.currentCursor() == "10:")
    }

    /// THE FINDING. `open(owner:)` ends in `unseal()` (`ReplicaEngine.swift:520`),
    /// which schedules a push for every lane that owes work (`:147-152`).
    /// `openForColdBoot` does not. So a relaunch over an owed journal is SILENT
    /// until something wakes it — in production that is `ReplicaHost.open`'s
    /// `knock()` (`ReplicaHost.swift:103`), which the cold path does not reach.
    ///
    /// Both halves are graded in ONE test against the SAME budget, so the
    /// negative is proven by contrast with a positive that did fire, never by
    /// elapsed time.
    ///
    /// KILL (either direction):
    /// - add `unseal()` after `ReplicaEngine.swift:537` — the cold half flips;
    /// - delete `unseal()` at `ReplicaEngine.swift:520` — the warm half flips.
    @Test func theColdDoorNeverSelfSchedulesWhileTheWarmDoorAlwaysDoes() async throws {
        let coldDirectory = Fixture.directory()
        _ = try await writeOwedWorld(in: coldDirectory, owner: 1, rows: [
            ("n1", ["title": .string("owed across the kill")]),
        ])
        let warmDirectory = Fixture.directory()
        _ = try await writeOwedWorld(in: warmDirectory, owner: 2, rows: [
            ("n1", ["title": .string("owed across the kill")]),
        ])

        let coldTransport = StubTransport()
        let cold = Fixture.unopenedEngine(
            in: coldDirectory, transport: coldTransport, automaticallyPushWrites: true
        )
        let warmTransport = StubTransport()
        let warm = Fixture.unopenedEngine(
            in: warmDirectory, transport: warmTransport, automaticallyPushWrites: true
        )

        try cold.openForColdBoot(owner: 1)
        try await warm.open(owner: 2)

        // The warm door's own delivery is the budget. When it has fired, a
        // scheduled push has had at least as long to reach the stub on the
        // cold side — `schedulePush` is one `Task.yield()` away (`:1145`).
        try await until("the warm door never scheduled the journal it re-opened") {
            await warmTransport.pushCount == 1
        }
        #expect(await coldTransport.pushCount == 0,
                "the cold door scheduled a push; if that becomes true, ReplicaHost's knock is dead weight")

        // And the contract that makes the silence safe: ONE explicit drain —
        // what the host's doorbell ultimately calls — clears everything owed.
        _ = try await cold.drain()
        #expect(await coldTransport.pushCount == 1)
        let store = try #require(cold.store)
        #expect(try rawPendingIds(store).isEmpty, "the first wake must clear the whole owed journal")
    }

    /// Bytes that landed while the process was gone: the cold boot asks the
    /// holds again — after it binds, never on the first frame's path.
    ///
    /// KILL: `openForColdBoot` — drop `askHoldsAgain()`.
    @Test func aColdBootAsksEveryHoldAgainAfterItBinds() async throws {
        let directory = Fixture.directory()
        _ = try await writeOwedWorld(in: directory, owner: 1, rows: [
            ("n1", ["title": .string("a"), "blob": .string("k1")]),
        ], syncGates: [blobGate(ReleaseLedger())])

        let transport = StubTransport()
        let reborn = Fixture.unopenedEngine(
            in: directory, transport: transport,
            syncGates: [blobGate(ReleaseLedger(released: ["k1"]))]
        )
        try reborn.openForColdBoot(owner: 1)

        try await until("the cold boot never asked its holds again") { try reborn.heldRows().isEmpty }
        _ = try await reborn.drain()
        let sent = await transport.pushedOps()
        #expect(sent.map(\.verb) == [ReplicaOp.Verb.rowCreate])
        #expect(sent.first?.data == ["title": .string("a"), "blob": .string("k1")])
    }

    /// The cold door binds only from NOTHING (`ReplicaEngine.swift:529-531`):
    /// changing owners is a transition and must go through `open(owner:)`,
    /// where in-flight work is quiesced. A cold door that could re-bind would
    /// swap the store under a live pull.
    ///
    /// KILL: `ReplicaBinding.swift:71` `guard bound == nil else {` →
    /// `guard true else {`… i.e. drop the guard so `bindIfUnbound` always
    /// binds. The second owner takes the process and this test goes red.
    @Test func aSecondColdBootCannotStealAnAlreadyBoundProcess() async throws {
        let directory = Fixture.directory()
        _ = try await writeOwedWorld(in: directory, owner: 1, rows: [
            ("n1", ["title": .string("first owner")]),
        ])
        _ = try await writeOwedWorld(in: directory, owner: 2, rows: [
            ("n2", ["title": .string("second owner")]),
        ])

        let engine = Fixture.unopenedEngine(in: directory, transport: StubTransport())
        try engine.openForColdBoot(owner: 1)
        try engine.openForColdBoot(owner: 2)

        #expect(engine.owner == 1, "the cold door re-bound a process that already had an owner")
        #expect(try RowStream<TestNote>(engine: engine).find("n2") == nil,
                "…and served the other identity's rows")
    }
}
