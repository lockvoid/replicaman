import Foundation
import GRDB
import Testing
@testable import ReplicaMan

/// Delivery through the production scheduler: a held row holes the batch
/// without blocking it, costs one judge while it waits, and leaves on its
/// gate's own signal — no write, no knock.
@Suite struct GateWakeTests {

    /// A held row must hole the batch, not block it — on the automatic path
    /// too. The live symptom of getting this wrong is every unrelated row in
    /// the app freezing behind one cook whose bytes are still uploading.
    ///
    /// KILL: `admit` — return true for `.hold`; the held row then rides the
    /// wire.
    @Test func aHeldRowHolesTheAutomaticBatchWithoutBlockingIt() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger(released: ["free"])
        let engine = Fixture.engine(
            store: store, transport: transport,
            automaticallyPushWrites: true,
            syncGates: [gateWholeRowGate(released)]
        )

        try await engine.saveRow(stream: "notes", id: "held", type: nil, data: ["title": .string("waiting")])
        try await engine.saveRow(stream: "notes", id: "free", type: nil, data: ["title": .string("ready")])

        try await until("the free row never reached the wire on the engine's own schedule") {
            await transport.pushedOps().contains { $0.rowId == "free" }
        }
        await engine.seal()

        #expect(await transport.pushedOps().allSatisfy { $0.rowId == "free" }, "the held row escaped onto the wire")
        #expect(try engine.heldRows().map(\.rowId) == ["held"])
        #expect(try store.peekPending().isEmpty)
    }

    /// The landing is the event: a release must not wait for an unrelated
    /// write. The gate's signal alone takes the row to the wire.
    ///
    /// KILL: `ReplicaEngine.init` — drop the task that listens to each gate's
    /// `changes`.
    @Test func aLandingSendsTheRowOnTheEnginesOwnSchedule() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(
            store: store, transport: transport,
            automaticallyPushWrites: true,
            syncGates: [blobGate(released)]
        )
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a"), "blob": .string("k1")])
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(await transport.pushCount == 0)

        released.land("k1")

        try await until("the landing never took the row to the wire") {
            await transport.pushedOps().contains { $0.rowId == "n1" }
        }
        await engine.seal()
        let sent = await transport.pushedOps()
        #expect(sent.map(\.verb) == [ReplicaOp.Verb.rowCreate])
        #expect(sent.first?.data == ["title": .string("a"), "blob": .string("k1")])
        #expect(try store.peekPending().isEmpty)
    }

    /// A row awaiting its bytes is judged once, when written, however long
    /// it waits.
    @Test func aHeldRowIsJudgedOnceWhileItWaits() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let judges = JudgeCount()
        let engine = Fixture.engine(
            store: store, transport: transport,
            coldWindow: 60,
            automaticallyPushWrites: true,
            syncGates: [TestGate("notes") { _ in
                judges.tick()
                return .gate("bytes still uploading")
            }]
        )

        try await engine.saveRow(stream: "notes", id: "cook-1", type: nil, data: ["title": .string("x")])
        try await engine.saveRow(stream: "notes", id: "cook-2", type: nil, data: [:])
        _ = try await engine.drain()
        try await Task.sleep(nanoseconds: 200_000_000)
        await engine.seal()

        #expect(judges.value == 2, "a waiting row was judged again (judges=\(judges.value))")
    }

    @Test func aWriteDuringAnAutomaticPushIsDeliveredWithoutAnotherWake() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(
            store: store, transport: transport, automaticallyPushWrites: true
        )
        await transport.onPush { ops in
            guard ops.contains(where: { $0.rowId == "first" }) else { return }
            do {
                try await engine.saveRow(stream: "notes", id: "second", type: nil, data: [:])
            } catch {
                Issue.record(error)
            }
        }
        try await engine.saveRow(stream: "notes", id: "first", type: nil, data: [:])
        try await until("the write committed during an active push was stranded") {
            await transport.pushedOps().contains { $0.rowId == "second" }
        }
        await engine.seal()
        #expect(await transport.pushedOps().map(\.rowId) == ["first", "second"])
        #expect(try store.peekPending().isEmpty)
    }

}
