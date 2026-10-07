import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Removed authoring is archived before active rows or documents leave the view.
final class DeleteCascadeTests: XCTestCase {

    func testDocumentStreamDeleteCascadesFoldAndJournal() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SEED".utf8), data: [:])],
            cursor: "5:", more: false
        ))
        _ = try await engine.pullOnce()
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+edit".utf8))
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("stays")])

        // Push side dead: the frozen delta waits for its answer, and the edit
        // after it is still owed when the delete frame arrives — the cascade,
        // not the drain, must clear that one.
        await transport.failPushes(true)
        do {
            _ = try await engine.drain()
            XCTFail("the dead wire must surface")
        } catch ReplicaError.transport {}
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+more".utf8))
        let frozen = try store.peekPending().filter(\.sent).map(\.id)
        XCTAssertEqual(try store.peekPending().count, 3)
        let originalFold = try store.peekDoc("boards", "b1")?.fold
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowDelete(stream: "boards", id: "b1")], cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        XCTAssertNil(try store.peekSnapshot("boards", "b1"), "the projection row is gone")
        XCTAssertNil(try store.peekDoc("boards", "b1"), "the fold is gone")
        let archive = try XCTUnwrap(store.recoveryRecords().first)
        let fold = try XCTUnwrap(store.recoveryParts(id: archive.id).first { $0.kind == "document.fold" })
        XCTAssertEqual(try store.recoveryChunk(id: archive.id, part: fold), originalFold)
        let pending = try store.peekPending()
        XCTAssertEqual(
            try pending.map { try $0.op().rowId }, ["b1", "n1"],
            "every op the dead doc still owed is discharged; the unrelated note entry survives"
        )
        XCTAssertEqual(pending.map(\.id), frozen, "only the frozen ones stay, each waiting for its own verdict")
    }

    func testDocumentCascadeSparesOtherRowsEntries() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        try await parkBoardBirthBesideAnAcceptedNote(engine, transport)
        await editNoteWhileThePullIsOnTheWire(engine, transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "stays"), .rowDelete(stream: "boards", id: "b1")], cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        XCTAssertEqual(try store.peekParked().count, 1, "a removal cannot hide a refusal before it is dismissed")
        let survivors = try store.peekPending().map { try "\($0.op().rowId) \($0.op().verb)" }
        XCTAssertEqual(survivors, ["n1 \(ReplicaOp.Verb.rowPatch)"], "the note edit owed at the cascade survives it")
    }

    /// The board create is refused and parks; the note is accepted.
    private func parkBoardBirthBesideAnAcceptedNote(_ engine: ReplicaEngine, _ transport: StubTransport) async throws {
        try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+edit".utf8))
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("stays")])
        await transport.scriptPush { ops in
            ops.map { op in
                op.verb == ReplicaOp.Verb.rowCreate && op.stream == "boards"
                    ? ReplicaVerdict(id: op.id, outcome: .rejected, reason: "quota")
                    : ReplicaVerdict(id: op.id, outcome: .accepted)
            }
        }
        _ = try await engine.drain()
    }

    /// After the pre-pull drain, so the edit is still owed when the page lands.
    private func editNoteWhileThePullIsOnTheWire(_ engine: ReplicaEngine, _ transport: StubTransport) async {
        await transport.onPull { _ in
            do {
                try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("edited")])
            } catch {
                XCTFail("the note edit failed: \(error)")
            }
        }
    }

    func testRowStreamRemovalArchivesPendingPatch() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "server copy")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("local edit")])
        let owed = try XCTUnwrap(store.peekPending().first)
        await transport.failPushes(true)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowDelete(stream: "notes", id: "n1")], cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try store.peekPending().map(\.payload), [owed.payload],
                       "the patch froze on the dead wire; only its verdict settles it")
        let archive = try XCTUnwrap(store.recoveryRecords().first)
        let intent = try XCTUnwrap(store.recoveryParts(id: archive.id).first { $0.kind == "intent" })
        XCTAssertEqual(try store.recoveryChunk(id: archive.id, part: intent), owed.payload)

        await transport.failPushes(false)
        _ = try await engine.drain()
        XCTAssertTrue(try store.peekPending().isEmpty)
        XCTAssertNil(try store.peekSnapshot("notes", "n1"), "an answer for a removed row does not bring it back")
    }

    /// A doorbell's round started before this device's own delete was
    /// accepted, and carries the tombstone. The accepted intent is the server's
    /// state waiting for a round, not authoring: there is nothing to archive.
    func testAnAcceptedDeleteSeenByARoundStartedBeforeItLeavesNoRecoveryRecord() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [Fixture.note("n1", title: "one")], cursor: "c1", more: false))
        try await engine.pullOnce()

        let entered = Latch()
        let gate = Latch()
        await transport.onPull { _ in
            entered.open()
            await gate.wait()
        }
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [.rowDelete(stream: "notes", id: "n1")], cursor: "c2", more: false))
        let pull = Task { try await engine.pullOnce() }
        await entered.wait()
        try await engine.deleteRow(stream: "notes", id: "n1")
        _ = try await engine.drain()
        gate.open()
        _ = try await pull.value

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try store.recoveryRecords().count, 0)
        XCTAssertFalse(try store.syncStatus().hasUnsettledWork)
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    /// Another device gave the address a new life while this device's patch of
    /// the old one was frozen. The round that shows the new life archives the
    /// old branch and drops what was still editable — never the frozen intent:
    /// its bytes may already be committed, so it waits for its own verdict,
    /// which the replaced life then consumes.
    func testAFrozenIntentOutlivesAnArchiveOfItsAddressUntilItsVerdict() async throws {
        let wire = StubTransport()
        let store = try Fixture.store()
        let device = Fixture.engine(store: store, transport: wire, coldWindow: 3600)
        let other = Fixture.engine(store: try Fixture.store(), transport: wire)
        func attempts() async -> [[String]] {
            await wire.events.compactMap { if case .push(let ids) = $0 { return ids } else { return nil } }
        }

        try await other.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("first life")])
        try await other.drain()
        await wire.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "first life")], cursor: "c1", more: false
        ))
        try await device.pullOnce()

        try await device.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("offline edit")])
        await wire.failPushes(true)
        do {
            try await device.drain()
            XCTFail("the dead wire must surface")
        } catch ReplicaError.transport {}
        await wire.failPushes(false)
        try await device.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("still editable")])
        let failedAttempts = await attempts()
        let failed = try XCTUnwrap(failedAttempts.last)

        try await other.deleteRow(stream: "notes", id: "n1")
        try await other.createRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("second life")])
        try await other.drain()
        await wire.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "second life")], cursor: "c2", more: false
        ))
        try await device.pullOnce()

        func intents() throws -> [String] {
            try store.pool.read { try String.fetchAll($0, sql: "SELECT state FROM intents WHERE row_id = 'n1' ORDER BY rowid") }
        }
        func submissions() throws -> Int {
            try store.pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM submissions") ?? 0 }
        }
        XCTAssertEqual(try intents(), ["frozen"], "the archive dropped the editable patch and left the frozen one")
        XCTAssertEqual(try submissions(), 1)
        XCTAssertEqual(try store.recoveryRecords().map(\.reason), ["Entity left this view or changed lifetime"])
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("second life"))

        try await device.drain()

        let retried = await attempts().last
        XCTAssertEqual(retried, failed, "the frozen operation went out again under its id")
        XCTAssertEqual(try intents(), [], "the verdict for the replaced life consumed the intent")
        XCTAssertEqual(try submissions(), 0)
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("second life"))
    }
}
