import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// Identity is first-class: one store FILE per owner, and no owner means no
/// store at all. A write with nobody to own it has nowhere to land — that is
/// what kills the "a mint wipes the writes made before it" class structurally
/// instead of by gate.
final class OwnerBindingTests: XCTestCase {

    private func directory(_ name: String = #function) -> URL {
        Fixture.directory(name)
    }

    private func engine(in directory: URL, transport: StubTransport = StubTransport()) -> ReplicaEngine {
        Fixture.unopenedEngine(in: directory, transport: transport)
    }

    // MARK: - Closed semantics

    func testClosedEngineRefusesEveryWriteWithNoOwner() async throws {
        let engine = engine(in: directory())

        await assertNoOwner {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("x")])
        }
        await assertNoOwner {
            _ = try await engine.deleteRow(stream: "notes", id: "n1")
        }
        await assertNoOwner {
            _ = try await engine.createDoc(stream: "boards", id: "b1", seed: Data("seed".utf8), peer: 1)
        }
        await assertNoOwner {
            try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("d".utf8))
        }
        await assertNoOwner { try await engine.resetCursors() }
        await assertNoOwner { try await engine.discardOps(ids: ["x"]) }
    }

    func testClosedEngineAnswersEveryReadEmpty() async throws {
        let engine = engine(in: directory())

        XCTAssertNil(engine.owner)
        XCTAssertNil(engine.store)
        XCTAssertNil(engine.database)
        XCTAssertNil(try engine.docFold(stream: "boards", id: "b1"))
        XCTAssertNil(try engine.docPeer(stream: "boards", id: "b1"))
        let pending = try await engine.pendingOps()
        let parked = try await engine.parkedOps()
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(pending.count, 0)
        XCTAssertEqual(parked.count, 0)
        XCTAssertNil(cursor)

        let notes = RowStream<TestNote>(engine: engine)
        XCTAssertEqual(try notes.list(), [])
        XCTAssertNil(try notes.find("n1"))
    }

    func testClosedEngineNeverTouchesTheWire() async throws {
        let transport = StubTransport()
        let engine = engine(in: directory(), transport: transport)

        let verdicts = try await engine.drain()
        let applied = try await engine.pullUntilCaughtUp()
        try await engine.drainIfWarm()
        XCTAssertEqual(verdicts.count, 0)
        XCTAssertEqual(applied, 0)

        let pulls = await transport.pullCount
        let pushes = await transport.pushCount
        XCTAssertEqual(pulls, 0, "a closed engine has no owner to authorize a pull")
        XCTAssertEqual(pushes, 0, "a closed engine holds no journal to push")
    }

    /// The stale-handle hazard: a generated verb surface captured while an
    /// owner was open must refuse once that owner is gone, not write into a
    /// store the process no longer owns.
    func testAHandleCapturedWhileOpenRefusesAfterClose() async throws {
        let engine = engine(in: directory())
        try await engine.open(owner: 1)
        let notes = RowStream<TestNote>(engine: engine)
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "mine")) }

        try await engine.close()

        await assertNoOwner { try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "n2", title: "orphan")) } }
        XCTAssertEqual(try notes.list(), [])
    }

    // MARK: - One file per owner

    func testOpenCreatesTheOwnersFileAndReopenServesTheSameWorld() async throws {
        let directory = directory()
        let engine = Fixture.unopenedEngine(in: directory, transport: StubTransport(), schema: Fixture.schema(boardStamp: .standard))

        try await engine.open(owner: 1)
        XCTAssertEqual(engine.owner, 1)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("kept")])
        XCTAssertTrue(FileManager.default.fileExists(
            atPath: directory.appendingPathComponent("replica-1.sqlite").path
        ))

        try await engine.close()
        XCTAssertNil(engine.owner)

        try await engine.open(owner: 1)
        let notes = RowStream<TestNote>(engine: engine)
        XCTAssertEqual(try notes.find("n1")?.title, "kept", "reopening the same owner reopens the same file")

        // The OPENED owner is the only owner a create can carry — the stamp
        // follows the binding, not a value the caller passed in.
        _ = try await engine.createDoc(
            stream: "boards", id: "b1", seed: Data("seed".utf8), peer: 1,
            data: ["title": .string("board")]
        )
        XCTAssertEqual(try XCTUnwrap(engine.store).peekSnapshot("boards", "b1")?.data["userId"], .number(1))
    }

    func testRetireDeletesTheOwnersThreeFilesAndTheNextOwnerStartsEmpty() async throws {
        let directory = directory()
        let transport = StubTransport()
        let engine = engine(in: directory, transport: transport)

        try await engine.open(owner: 1)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one")], cursor: "10:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("unpushed")])
        let owed = try await engine.pendingOps()
        XCTAssertEqual(owed.count, 1)

        try await engine.retire()

        for suffix in ["", "-wal", "-shm"] {
            XCTAssertFalse(
                FileManager.default.fileExists(
                    atPath: directory.appendingPathComponent("replica-1.sqlite").path + suffix
                ),
                "retire leaves no trace of the outgoing owner (\(suffix.isEmpty ? "db" : suffix))"
            )
        }

        try await engine.open(owner: 2)
        let notes = RowStream<TestNote>(engine: engine)
        let cursorAfterSwitch = try await engine.currentCursor()
        let owedAfterSwitch = try await engine.pendingOps()
        XCTAssertEqual(try notes.list(), [], "the next owner never sees the retired owner's rows")
        XCTAssertNil(cursorAfterSwitch, "a blank cursor makes the next pull re-snapshot")
        XCTAssertEqual(owedAfterSwitch.count, 0, "an unpushed op must never ride the next identity's bearer")
    }

    func testReopenKeepsTheCursorOfAValidEmptyCheckpoint() async throws {
        let directory = directory()
        let transport = StubTransport()
        let engine = engine(in: directory, transport: transport)
        try await engine.open(owner: 1)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [], cursor: "10:", more: false
        ))
        _ = try await engine.pullOnce()
        try await engine.close()
        try await engine.open(owner: 1)
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "10:")
        XCTAssertTrue(try RowStream<TestNote>(engine: engine).list().isEmpty)
    }

    func testOpenKeepsAWarmCursorWhenTheStoreStillHoldsItsWorld() async throws {
        let directory = directory()
        let transport = StubTransport()
        let engine = engine(in: directory, transport: transport)

        try await engine.open(owner: 1)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one")], cursor: "10:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        try await engine.close()
        try await engine.open(owner: 1)

        let kept = try await engine.currentCursor()
        XCTAssertEqual(kept, "10:", "a coherent store keeps its read position")
    }

    // MARK: - The owner is the stamp
    // MARK: - L1: watchers ride the owner, never a dead pool

    func testAWatcherArmedWithoutAnOwnerServesTheOwnerThatArrives() async throws {
        let engine = engine(in: directory())
        let seen = CallbackPictures()

        let watcher = RowStream<TestNote>(engine: engine).watch(includeInitial: true) { rows in seen.record(rows.map(\.id)) }
        defer { watcher.cancel() }

        await eventually("a closed engine must still deliver its empty picture") {
            seen.values == [[]]
        }

        try await engine.open(owner: 1)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("first")])

        await eventually("the watcher never re-armed on the owner that arrived") {
            seen.values.last == ["n1"]
        }
    }

    func testAWatcherStopsServingARetiredOwnerAndPicksUpTheNextOne() async throws {
        let engine = engine(in: directory())
        try await engine.open(owner: 1)
        try await engine.saveRow(stream: "notes", id: "outgoing", type: nil, data: ["title": .string("theirs")])

        let seen = CallbackPictures()
        let watcher = RowStream<TestNote>(engine: engine).watch(includeInitial: true) { rows in seen.record(rows.map(\.id)) }
        defer { watcher.cancel() }
        await eventually { seen.values.last == ["outgoing"] }

        try await engine.retire()
        try await engine.open(owner: 2)
        try await engine.saveRow(stream: "notes", id: "incoming", type: nil, data: ["title": .string("mine")])

        await eventually("the watcher kept serving the retired owner's rows") {
            seen.values.last == ["incoming"]
        }
        let pictures = seen.values
        XCTAssertFalse(
            pictures.dropFirst(pictures.firstIndex(of: ["outgoing"]).map { $0 + 1 } ?? 0).contains { $0.contains("outgoing") },
            "no picture after the retirement may carry the outgoing owner's row"
        )
    }
    @MainActor
    func testAQueuedCallbackCannotDeliverThePreviousOwnersRows() async throws {
        let engine = engine(in: directory())
        try await engine.open(owner: 1)
        let notes = RowStream<TestNote>(engine: engine)
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "outgoing", title: "private")) }
        let read = DispatchSemaphore(value: 0)
        let rebound = DispatchSemaphore(value: 0)
        let seen = CallbackPictures()
        let watcher = ReplicaReads.watch(
            engine.binding, health: engine.health, stream: "notes", predicate: nil as ReplicaPredicate<TestNote.Field>?, includeInitial: true,
            decode: { id, _, data in
                read.signal()
                return TestNote(id: id, title: data["title"]?.string ?? "")
            },
            deliver: { rows in seen.record(rows.map(\.id)) }
        )
        defer { watcher.cancel() }
        XCTAssertEqual(read.wait(timeout: .now() + 3), .success, "the old picture must reach the callback queue")
        let change = Task.detached {
            defer { rebound.signal() }
            try await engine.close()
            try await engine.open(owner: 2)
        }
        XCTAssertEqual(rebound.wait(timeout: .now() + 3), .success, "the next owner must bind before MainActor delivery resumes")
        try await change.value
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "incoming", title: "mine")) }
        await eventually { @Sendable in seen.values.last == ["incoming"] }
        XCTAssertFalse(seen.values.contains(["outgoing"]), "a queued delivery crossed the owner boundary")
    }

    func testTheCallbackWatchClearsItsRowsWhenTheOwnerCloses() async throws {
        let engine = engine(in: directory())
        try await engine.open(owner: 1)
        let notes = RowStream<TestNote>(engine: engine)
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "outgoing", title: "private")) }
        let seen = CallbackPictures()
        let watcher = notes.watch(includeInitial: true) { seen.record($0.map(\.id)) }
        defer { watcher.cancel() }
        await eventually { seen.values.last == ["outgoing"] }

        try await engine.close()

        await eventually("closing the owner must clear the callback's visible rows") { seen.values.last == [] }
    }

    // MARK: - Helpers

    private func assertNoOwner(
        _ operation: () async throws -> Void,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            try await operation()
            XCTFail("an ownerless engine admitted a write", file: file, line: line)
        } catch ReplicaError.noOwner {
            // Expected.
        } catch {
            XCTFail("unexpected closed-engine error: \(error)", file: file, line: line)
        }
    }
}

/// Every picture a watcher delivered, in order — what proves a re-arm
/// happened and that no post-retirement picture carries the old world.
private final class CallbackPictures: @unchecked Sendable {
    private let lock = NSLock()
    private var pictures: [[String]] = []
    var values: [[String]] { lock.withLock { pictures } }
    func record(_ ids: [String]) { lock.withLock { pictures.append(ids.sorted()) } }
}
