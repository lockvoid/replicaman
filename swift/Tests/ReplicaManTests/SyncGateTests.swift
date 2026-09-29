import Foundation
import XCTest
@testable import ReplicaMan

/// The sync gates judge a change when it is WRITTEN: push journals it, a hold
/// keeps the whole row on the device — in `gates`, off the journal — and a
/// discard drops it. A drain asks nobody. Predicates are stubs: the engine's
/// contract needs no bytes, no uploads, no network.
final class SyncGateTests: XCTestCase {

    private func held(_ engine: ReplicaEngine) throws -> [String] {
        try engine.heldRows().map(\.rowId)
    }

    // MARK: - hold at write

    /// KILL: `enqueueOp` — journal the op whatever `admit` answers.
    func testAHeldRowWritesNothingToTheJournalNorDoesItsNextWrite() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [blobGate(ReleaseLedger())])

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a"), "blob": .string("k1")])
        XCTAssertEqual(try store.peekPending().count, 0, "a held birth reached the journal")
        XCTAssertEqual(try held(engine), ["n1"])

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        XCTAssertEqual(try store.peekPending().count, 0, "a held row's next write reached the journal")
        XCTAssertEqual(try held(engine), ["n1"])
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("b"), "the device keeps its write")
    }

    /// Held entries judged again on every drain cost every write a pass over
    /// all of them. A held row is off the journal, so a drain has
    /// nothing to ask.
    ///
    /// KILL: `performDrain` — judge every selected entry again before the push.
    func testADrainAsksNoGate() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let judged = JudgeCount()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [TestGate { change in
            judged.tick()
            return change.rowId.hasPrefix("held") ? .gate("waits") : .push
        }])
        for id in ["held1", "held2", "held3", "free"] {
            try await engine.saveRow(stream: "notes", id: id, type: nil, data: ["title": .string(id)])
        }
        let atWrite = judged.value

        _ = try await engine.drain()
        _ = try await engine.drain()

        XCTAssertEqual(judged.value, atWrite, "a drain asked a gate again")
        let sent = await transport.pushedOps().map(\.rowId)
        XCTAssertEqual(sent, ["free"])
        XCTAssertEqual(try held(engine), ["held1", "held2", "held3"])
    }

    /// A stream the gate is not registered for is never judged.
    func testUnregisteredStreamsFlowUntouched() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(ReleaseLedger())])
        try await engine.saveRow(stream: "assets", id: "a1", type: nil, data: ["blob": .string("k1")])

        _ = try await engine.drain()
        let sent = await transport.pushedOps().map(\.rowId)
        XCTAssertEqual(sent, ["a1"])
        XCTAssertEqual(try held(engine), [])
    }

    /// A held row asked again on a new state that still cannot leave keeps
    /// its place and says why now.
    func testAHeldRowWrittenAgainTellsItsNewReason() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [blobGate(ReleaseLedger())])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])
        let first = try XCTUnwrap(engine.heldRows().first)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k2")])

        let now = try XCTUnwrap(engine.heldRows().first)
        XCTAssertEqual(now.reason, "blob k2 in flight")
        XCTAssertEqual(now.seq, first.seq, "a hold keeps the place it began at")
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    // MARK: - discard

    /// A change the server never needs never joins the journal; the device
    /// keeps its value and the row's later writes go as ever.
    func testADiscardedChangeNeverReachesTheJournalAndTheRowGoesOn() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [TestGate { change in
            change.kind == .patch && change.local.keys.sorted() == ["progress"] ? .discard : .push
        }])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a"), "progress": .number(0)])
        _ = try await engine.drain()

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["progress": .number(0.5)])
        XCTAssertEqual(try store.peekPending().count, 0, "a discarded change is not owed")
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        _ = try await engine.drain()

        let sent = await transport.pushedOps().map { $0.data ?? [:] }
        XCTAssertEqual(sent, [["title": .string("a"), "progress": .number(0)], ["title": .string("b")]],
                       "the progress write never left; the title after it did")
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["progress"], .number(0.5),
                       "the device keeps its own value")
    }

    /// A change never needed is not held for later either: a discard beats
    /// another gate's hold, and the first hold names its gate.
    func testADiscardBeatsAHold() {
        let gates = SyncGates([
            TestGate(nil, id: "every") { _ in .gate("held") },
            TestGate(id: "notes") { change in change.local.keys.sorted() == ["progress"] ? .discard : .push },
        ])
        let progress = SyncChange(stream: "notes", rowId: "n1", kind: .patch, local: ["progress": .number(0.5)])
        let title = SyncChange(stream: "notes", rowId: "n1", kind: .patch, local: ["title": .string("b")])

        XCTAssertEqual(gates.judge(progress), .discard)
        XCTAssertEqual(gates.judge(title), .hold(gate: "every", reason: "held"))
    }

    /// Every later write stands on a row's birth: a gate that discards one
    /// is refused, and the birth leaves.
    ///
    /// KILL: `admit` — drop the `.discard where !knows` case.
    func testADiscardedBirthIsSentAnyway() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [TestGate { _ in .discard }])

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        _ = try await engine.drain()

        let verbs = await transport.pushedOps().map(\.verb)
        XCTAssertEqual(verbs, [ReplicaOp.Verb.rowCreate], "the birth left; the discarded patch did not")
    }
}
