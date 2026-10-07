import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// A local write decides on what it reads, so the read and the write are one
/// transaction (`ReplicaEngine.write`): a pull that lands before it is what
/// the decision reads; one that arrives during it waits and applies after the
/// commit. Never between — a create of a row that now exists, an update that
/// writes a pulled field back to its old value, a row that does not decode
/// read as absent, those were the three shapes of "between".
final class WriteTransactionTests: XCTestCase {

    fileprivate func fixture(
        automaticallyPushWrites: Bool = false
    ) -> (ReplicaStateStore, StubTransport, ReplicaEngine) {
        let store = try! Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, automaticallyPushWrites: automaticallyPushWrites)
        return (store, transport, engine)
    }

    private func pull(_ transport: StubTransport, _ engine: ReplicaEngine, _ frames: [ReplicaFrame], cursor: String) async throws {
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: frames, cursor: cursor, more: false))
        try await engine.pullOnce(shard: "user")
    }

    /// The row the caller meant to create had just
    /// been pulled. Decided inside the transaction, it is an update.
    func testACreateDecidedOnAPulledRowBecomesTheUpdate() async throws {
        let (store, transport, engine) = fixture()
        try await pull(transport, engine, [Fixture.note("n1", title: "placeholder")], cursor: "1:")

        try transact(engine) { tx in
            let notes = tx.rows(TestNote.self)
            if try notes.find("n1") == nil {
                try notes.create(TestNote(id: "n1", title: "verdict"))
            } else {
                try notes.update("n1") { $0.title = "verdict" }
            }
        }

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"]?.string, "verdict")
        XCTAssertEqual(try store.peekPending().map { try $0.op().verb }, [ReplicaOp.Verb.rowPatch])
    }

    /// A pull that arrives while the transaction holds the writer waits for
    /// its commit: the transaction's read stays true until its write lands.
    func testAPullArrivingMidTransactionAppliesAfterItsCommit() async throws {
        let (store, transport, engine) = fixture()
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "server")], cursor: "1:", more: false
        ))
        let released = Latch()
        let pull = await pullHeldOnTheWire(transport, engine, until: released)
        try await createInsideTheWriter(engine, releasing: released)
        try await pull.value

        XCTAssertEqual(try store.peekPending().map { try $0.op().verb }, [ReplicaOp.Verb.rowCreate],
                       "the create committed before the pull and is still owed")
        let cursor = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(cursor, "1:")
        let base = try await store.pool.read { try store.baseRow($0, stream: "notes", id: "n1") }
        XCTAssertEqual(base?.data["title"], .string("server"))
    }

    private func pullHeldOnTheWire(
        _ transport: StubTransport, _ engine: ReplicaEngine, until released: Latch
    ) async -> Task<Void, Error> {
        let reached = Latch()
        await transport.onPull { _ in
            reached.open()
            await released.wait()
        }
        let pull = Task { _ = try await engine.pullOnce(shard: "user") }
        await reached.wait()
        return pull
    }

    private func createInsideTheWriter(_ engine: ReplicaEngine, releasing released: Latch) async throws {
        try await onThread {
            try engine.write { tx in
                let notes = tx.rows(TestNote.self)
                XCTAssertNil(try notes.find("n1"), "the pull is on the wire, not in the store")
                released.open()
                // The pull answer is back and wants the writer; it waits.
                Thread.sleep(forTimeInterval: 0.2)
                XCTAssertNil(try notes.find("n1"), "a pull applied inside an open transaction")
                try notes.create(TestNote(id: "n1", title: "mine"))
            }
        }
    }

    /// Only the field the transaction changed travels: a `rank` a pull moved
    /// after an earlier read keeps the pulled value.
    func testAnUpdateOwesOnlyWhatItsEditChanged() async throws {
        let (store, transport, engine) = fixture()
        try await pull(transport, engine, [Fixture.note("n1", title: "draft", rank: "a")], cursor: "1:")
        _ = try RowStream<TestNote>(engine: engine).find("n1")
        try await pull(transport, engine, [Fixture.note("n1", title: "draft", rank: "b")], cursor: "2:")

        try transact(engine) { tx in
            try tx.rows(TestNote.self).update("n1") { $0.title = "final" }
        }

        let row = try store.peekSnapshot("notes", "n1")
        XCTAssertEqual(row?.data["title"]?.string, "final")
        XCTAssertEqual(row?.data["rank"]?.string, "b")
        XCTAssertEqual(try store.peekPending().first?.op().data?.keys.sorted(), ["title"])
    }

    /// A field the model cannot represent is not a field the edit changed:
    /// it stays as the server wrote it.
    func testAnUpdateLeavesAFieldTheModelCannotReadAlone() async throws {
        let (store, transport, engine) = fixture()
        try await pull(transport, engine, [.rowSet(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("draft"), "rank": .number(7)]
        )], cursor: "1:")

        try transact(engine) { tx in
            try tx.rows(TestNote.self).update("n1") { $0.title = "final" }
        }

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["rank"], .number(7))
        XCTAssertEqual(try store.peekPending().first?.op().data?.keys.sorted(), ["title"])
    }

    /// A row the model cannot read is present, not absent.
    func testARowThatDoesNotDecodeIsNotReadAsAbsent() async throws {
        let (_, transport, engine) = fixture()
        try await pull(transport, engine, [.rowSet(
            stream: "notes", id: "n1", type: "Future", data: ["title": .string("unknown shape")]
        )], cursor: "1:")

        XCTAssertThrowsError(try transact(engine) { tx in try tx.rows(TestNote.self).find("n1") }) { error in
            XCTAssertEqual(error as? ReplicaError, .undecodableRow(stream: "notes", id: "n1"))
        }
        XCTAssertThrowsError(try transact(engine) { tx in try tx.rows(TestNote.self).update("n1") { $0.title = "x" } }) { error in
            XCTAssertEqual(error as? ReplicaError, .undecodableRow(stream: "notes", id: "n1"))
        }
    }

    /// Commit or nothing: a body that throws after writing leaves no row and
    /// owes no op.
    func testATransactionThatThrowsLeavesNothing() throws {
        let (store, _, engine) = fixture()
        struct Refused: Error {}

        XCTAssertThrowsError(try transact(engine) { tx in
            try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "half"))
            throw Refused()
        })

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertTrue(try store.peekPending().isEmpty)
    }

    /// A `write` inside a `write` joins it — one commit, and a throw from the
    /// inner body rolls back the outer writes too.
    func testAWriteInsideAWriteJoinsIt() throws {
        let (store, _, engine) = fixture()
        struct Refused: Error {}

        try transact(engine) { tx in
            try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "outer"))
            try transact(engine) { inner in
                XCTAssertNotNil(try inner.rows(TestNote.self).find("n1"), "the joined transaction sees the outer write")
                try inner.rows(TestNote.self).create(TestNote(id: "n2", title: "inner"))
            }
        }
        XCTAssertEqual(try store.peekPending().count, 2)

        XCTAssertThrowsError(try transact(engine) { tx in
            try tx.rows(TestNote.self).create(TestNote(id: "n3", title: "outer"))
            try transact(engine) { _ in throw Refused() }
        })
        XCTAssertNil(try store.peekSnapshot("notes", "n3"))
    }

    /// The explicit verbs keep their refusals inside a transaction.
    func testTheVerbsRefuseWhatTheyRefuseOutside() async throws {
        let (_, transport, engine) = fixture()
        try await pull(transport, engine, [Fixture.note("n1", title: "there")], cursor: "1:")

        XCTAssertThrowsError(try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1")) }) { error in
            XCTAssertEqual(error as? ReplicaError, .rowExists(stream: "notes", id: "n1"))
        }
        XCTAssertThrowsError(try transact(engine) { tx in try tx.rows(TestNote.self).update("n9") { $0.title = "x" } }) { error in
            XCTAssertEqual(error as? ReplicaError, .unknownRow(stream: "notes", id: "n9"))
        }
        XCTAssertNil(try transact(engine) { tx in try tx.readonlyRows(ReadonlyJob.self).find("j1") }, "a readonly stream reads")
        XCTAssertThrowsError(try transact(engine) { tx in try tx.rows(WritableJob.self).create(WritableJob(id: "j1")) }) { error in
            XCTAssertEqual(error as? ReplicaError, .readonlyStream("jobs"))
        }
    }

    /// The writer's lane and draft ride the entry, though the body runs on
    /// GRDB's writer.
    func testTheLaneAndTheDraftAreTheCallers() async throws {
        let (store, _, engine) = fixture()

        try await engine.lane(.interactive) {
            try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "now")) }
        }
        let entry = try XCTUnwrap(store.peekPending().first)
        let lane = try await store.pool.read { try store.lane($0, entryId: entry.id) }
        XCTAssertEqual(lane, .interactive)

        let draft = try await engine.beginDraft {
            try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n2", title: "held")) }
        }
        XCTAssertEqual(try store.peekDrafted().map { try $0.op().rowId }, ["n2"])
        try await engine.commitDraft(draft)
        XCTAssertTrue(try store.peekDrafted().isEmpty)
    }

    /// A write is a fact, not a request: the task that asks for it being
    /// cancelled — a cancel handler cleaning up behind itself — does not stop it.
    func testACancelledTasksWriteStillLands() async throws {
        let (_, _, engine) = fixture()
        let writing = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "kept")) }
        }
        try await writing.value
        XCTAssertEqual(try RowStream<TestNote>(engine: engine).find("n1")?.title, "kept")
    }

    /// A transaction is one commit: one watch delivery, its writes in order.
    func testATransactionLandsAsOneCommitInOrder() async throws {
        let (_, _, engine) = fixture()
        let notes = RowStream<TestNote>(engine: engine)
        let signals = Tally()
        let stream = await engine.watchSignal(stream: "notes", includeInitial: true)
        let listener = Task { for await _ in stream { signals.bump() } }
        defer { listener.cancel() }
        await eventually(timeout: 3, "baseline must arm the observation") { signals.count == 1 }

        try transact(engine) { tx in
            try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "a"))
            try tx.rows(TestNote.self).create(TestNote(id: "n2", title: "solo"))
            try tx.rows(TestNote.self).update("n1") { $0.title = "b" }
        }

        await eventually(timeout: 3, "the commit must reach the watch") { signals.count >= 2 }
        XCTAssertEqual(signals.count, 2, "three writes land as ONE commit — one signal past baseline")
        XCTAssertEqual(try notes.find("n1")?.title, "b", "writes apply in order")
        XCTAssertEqual(try notes.find("n2")?.title, "solo")
    }

    /// A transaction that changes nothing neither signals nor journals.
    func testAnUnchangedTransactionNeitherSignalsNorJournals() async throws {
        let (store, _, engine) = fixture()
        try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "a")) }
        let baseline = try store.peekPending().count
        let signals = Tally()
        let stream = await engine.watchSignal(stream: "notes", includeInitial: true)
        let listener = Task { for await _ in stream { signals.bump() } }
        defer { listener.cancel() }
        await eventually(timeout: 3, "baseline must arm the observation") { signals.count == 1 }

        try transact(engine) { tx in try tx.rows(TestNote.self).update("n1") { $0.title = "a" } }
        try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n3", title: "sentinel")) }

        await eventually(timeout: 3, "the sentinel commit must arrive") { signals.count >= 2 }
        XCTAssertEqual(signals.count, 2, "the unchanged transaction must not produce its own signal")
        XCTAssertEqual(try store.peekPending().count, baseline + 1, "an unchanged write must not journal")
    }

    /// A transaction that fails part-way writes nothing; its retry deletes
    /// every row, and an unsent birth cancels with its delete.
    func testAFailedDeleteRollsTheWholeTransactionBack() async throws {
        let (store, _, engine) = fixture()
        let notes = RowStream<TestNote>(engine: engine)
        try transact(engine) { tx in
            for id in ["n1", "n2", "n3"] { try tx.rows(TestNote.self).create(TestNote(id: id, title: id)) }
        }
        try await store.pool.write { db in
            try db.execute(sql: """
                CREATE TRIGGER refuse_second_delete BEFORE DELETE ON snapshots
                WHEN OLD.stream = 'notes' AND OLD.row_id = 'n2'
                BEGIN SELECT RAISE(ABORT, 'forced delete failure'); END
                """)
        }

        XCTAssertThrowsError(try transact(engine) { tx in try tx.rows(TestNote.self).delete(ids: ["n1", "n2", "n3"]) }) {
            XCTAssertTrue(String(describing: $0).contains("forced delete failure"))
        }
        XCTAssertEqual(try notes.list().map(\.id).sorted(), ["n1", "n2", "n3"])
        XCTAssertEqual(try store.peekPending().count, 3, "rollback must preserve all three pending creates")

        try await store.pool.write { db in try db.execute(sql: "DROP TRIGGER refuse_second_delete") }
        try transact(engine) { tx in try tx.rows(TestNote.self).delete(ids: ["n1", "n1", "n2", "n3", "absent"]) }
        XCTAssertTrue(try notes.list().isEmpty)
        XCTAssertTrue(try store.peekPending().isEmpty, "unsent births and their deletes cancel inside the same transaction")
    }

    /// A seal waits for a transaction already inside the writer, and a
    /// transaction after it is refused.
    func testASealWaitsForAnOpenTransaction() async throws {
        let (store, _, engine) = fixture()
        let entered = Latch()
        let proceed = DispatchSemaphore(value: 0)

        let writing = Task {
            try await onThread {
                try engine.write { tx in
                    entered.open()
                    proceed.wait()
                    try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "in flight"))
                }
            }
        }
        await entered.wait()
        let sealReturned = Latch()
        let sealed = Task {
            await engine.seal()
            sealReturned.open()
        }
        try await Task.sleep(for: .milliseconds(200))
        XCTAssertFalse(sealReturned.opened, "the seal returned while a transaction was still open")
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        proceed.signal()
        try await writing.value
        await sealed.value

        XCTAssertNotNil(try store.peekSnapshot("notes", "n1"), "the seal returned before the open transaction committed")
        XCTAssertThrowsError(try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n2")) }) { error in
            XCTAssertEqual(error as? ReplicaError, .identityTransitionInProgress)
        }
        await engine.unseal()
        XCTAssertNoThrow(try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n2")) })
    }

    /// A delete of a row whose create is already on the wire keeps the birth
    /// ahead of the delete; one whose create never left collapses both.
    func testADeleteReadsTheFlightClaimedUnderTheWriter() async throws {
        let (store, transport, engine) = fixture()
        try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "flying")) }
        let onWire = Latch()
        let land = Latch()
        await transport.onPush { _ in
            onWire.open()
            await land.wait()
        }
        let drain = Task { try await engine.drain() }
        await onWire.wait()

        let queued = try transact(engine) { tx in try tx.rows(TestNote.self).delete("n1") }
        XCTAssertTrue(queued, "the create is on the wire, so the delete is owed after it")
        land.open()
        _ = try await drain.value

        try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n2", title: "grounded")) }
        XCTAssertFalse(try transact(engine) { tx in try tx.rows(TestNote.self).delete("n2") }, "an unsent birth dies with its delete")
        XCTAssertFalse(try store.peekPending().contains { try $0.op().rowId == "n2" })
    }

    /// A committed transaction schedules the engine's own push.
    func testACommittedTransactionIsPushed() async throws {
        let (_, transport, engine) = fixture(automaticallyPushWrites: true)

        try transact(engine) { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "go")) }

        try await eventually("the transaction's op reached the wire") {
            await transport.pushedOps().map(\.rowId) == ["n1"]
        }
    }

    /// A create the server committed while its answer was lost is not an
    /// unsent birth: the delete after it still goes to the server, so the next
    /// pull cannot bring the row back.
    func testADeleteAfterACreateWhoseAnswerWasLostStillReachesTheServer() async throws {
        let store = try Fixture.store()
        let transport = LostAnswerTransport()
        let engine = ReplicaEngine(store: store, owner: Fixture.owner, transport: transport, schema: Fixture.schema(),
                                   codecs: [StubCodec()], coldWindow: 0, automaticallyPushWrites: false)
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "lost-answer", title: "mine")) }
        do {
            _ = try await engine.drain()
            XCTFail("the answer to the create must be lost")
        } catch ReplicaError.transport(_) {}

        let queued = try await engine.write { tx in try tx.rows(TestNote.self).delete("lost-answer") }
        XCTAssertTrue(queued, "a create the server may have committed was collapsed as never sent")
        _ = try await engine.drain()
        try await engine.pullOnce(shard: "user")
        XCTAssertNil(try store.peekSnapshot("notes", "lost-answer"), "the deleted row came back with the next pull")
    }

    /// An earlier attempt whose answer was lost may have committed; a later
    /// attempt that never left the device proves nothing about it. The delete
    /// after both still reaches the server.
    func testAnOfflineRetryNeverForgetsAnEarlierLostAnswer() async throws {
        let store = try Fixture.store()
        let transport = LostThenOfflineTransport()
        let engine = ReplicaEngine(store: store, owner: Fixture.owner, transport: transport, schema: Fixture.schema(),
                                   codecs: [StubCodec()], coldWindow: 0, automaticallyPushWrites: false)
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "lost-then-offline", title: "mine")) }
        do {
            _ = try await engine.drain()
            XCTFail("the answer to the create must be lost")
        } catch ReplicaError.transport(_) {}
        do {
            _ = try await engine.drain()
            XCTFail("the retry must find the device offline")
        } catch let error as URLError {
            XCTAssertEqual(error.code, .notConnectedToInternet)
        }
        XCTAssertTrue(try XCTUnwrap(store.peekPending().first).sent, "the offline retry erased the earlier uncertain commit")

        let queued = try await engine.write { tx in try tx.rows(TestNote.self).delete("lost-then-offline") }
        XCTAssertTrue(queued, "a create the server may have committed was collapsed as never sent")
        _ = try await engine.drain()
        try await engine.pullOnce(shard: "user")
        XCTAssertNil(try store.peekSnapshot("notes", "lost-then-offline"), "the deleted row came back with the next pull")
    }

    /// The server judges a write against its preconditions, so every patch
    /// carries them — changed or not: a completion without its version was
    /// read at the server row's version and replaced a born fact.
    func testAPatchCarriesTheStreamsPreconditionsChangedOrNot() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let schema = ReplicaSchema(streams: [ReplicaStreamSpec(name: "notes", lane: .row, shard: "user", preconditions: ["rank"])])
        let engine = Fixture.engine(store: store, transport: transport, schema: schema)
        try await pull(transport, engine, [Fixture.note("n1", title: "draft", rank: "a")], cursor: "1:")

        try transact(engine) { tx in
            try tx.rows(TestNote.self).update("n1") { $0.title = "final" }
        }

        XCTAssertEqual(try store.peekPending().first?.op().data, ["title": .string("final"), "rank": .string("a")])
    }

    func testPreconditionsAloneOweNoPatch() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let schema = ReplicaSchema(streams: [ReplicaStreamSpec(name: "notes", lane: .row, shard: "user", preconditions: ["rank"])])
        let engine = Fixture.engine(store: store, transport: transport, schema: schema)
        try await pull(transport, engine, [Fixture.note("n1", title: "draft", rank: "a")], cursor: "1:")

        try transact(engine) { tx in
            try tx.rows(TestNote.self).update("n1") { $0.title = "draft" }
        }

        XCTAssertTrue(try store.peekPending().isEmpty, "an unchanged row owes nothing, its preconditions included")
    }

    /// Once a sequence is frozen, even a failed connection cannot erase it:
    /// the writer must deliver a contiguous history including the later delete.
    func testFrozenCreateRetainsItsSequenceAfterAnOfflineDelete() async throws {
        let store = try Fixture.store()
        let transport = OfflineTransport()
        let engine = ReplicaEngine(store: store, owner: Fixture.owner, transport: transport, schema: Fixture.schema(),
                                   codecs: [StubCodec()], coldWindow: 0, automaticallyPushWrites: false)
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "offline", title: "mine")) }
        do {
            _ = try await engine.drain()
            XCTFail("the device is offline")
        } catch let error as URLError {
            XCTAssertEqual(error.code, .notConnectedToInternet)
        }

        let queued = try await engine.write { tx in try tx.rows(TestNote.self).delete("offline") }
        XCTAssertTrue(queued)
        XCTAssertNil(try store.peekSnapshot("notes", "offline"))
        XCTAssertEqual(try store.peekPending().map { try $0.op().verb }, ["row.create", "row.delete"])
    }

    /// The same after the app restarts between the lost answer and the delete:
    /// what the server may have heard is on disk, not in the last process.
    func testALostAnswerIsRememberedAcrossARestart() async throws {
        let first = try Fixture.store()
        let transport = LostAnswerTransport()
        let before = ReplicaEngine(store: first, owner: Fixture.owner, transport: transport, schema: Fixture.schema(),
                                   codecs: [StubCodec()], coldWindow: 0, automaticallyPushWrites: false)
        try await before.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "lost-answer", title: "mine")) }
        do {
            _ = try await before.drain()
            XCTFail("the answer to the create must be lost")
        } catch ReplicaError.transport(_) {}
        await before.seal()
        try first.close()

        let reopened = try ReplicaStateStore(path: first.path.path)
        let after = ReplicaEngine(store: reopened, owner: Fixture.owner, transport: transport, schema: Fixture.schema(),
                                  codecs: [StubCodec()], coldWindow: 0, automaticallyPushWrites: false)
        let queued = try await after.write { tx in try tx.rows(TestNote.self).delete("lost-answer") }
        XCTAssertTrue(queued, "the restart forgot that the create had gone out")
        _ = try await after.drain()
        try await after.pullOnce(shard: "user")
        XCTAssertNil(try reopened.peekSnapshot("notes", "lost-answer"), "the deleted row came back with the next pull")
    }
}

/// The synchronous door, named: inside an async function `engine.write`
/// resolves to the async overload, as GRDB's `write` does.
private func transact<T>(_ engine: ReplicaEngine, _ body: (ReplicaTransaction) throws -> T) throws -> T {
    try engine.write(body)
}

/// Sync code that holds its thread on purpose — a transaction waiting inside
/// the writer — on a thread of its own, outside the cooperative pool.
private func onThread<T: Sendable>(_ body: @escaping @Sendable () throws -> T) async throws -> T {
    try await withCheckedThrowingContinuation { continuation in
        Thread { continuation.resume(with: Result { try body() }) }.start()
    }
}

extension WriteTransactionTests {
    /// From an async caller the same transaction suspends rather than holding
    /// its thread while the writer is busy.
    func testTheAsyncDoorIsTheSameTransaction() async throws {
        let (store, _, engine) = fixture()

        try await engine.write { tx in
            try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "awaited"))
            try tx.rows(TestNote.self).update("n1") { $0.rank = "a" }
        }

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["rank"]?.string, "a")
        XCTAssertEqual(try store.peekPending().map { try $0.op().verb }, [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.rowPatch])

        try await assertAsyncWriteRollsBack(engine)
        XCTAssertNil(try store.peekSnapshot("notes", "n2"))
        XCTAssertFalse(try store.peekPending().contains { try $0.op().rowId == "n2" })
    }

    private func assertAsyncWriteRollsBack(_ engine: ReplicaEngine) async throws {
        struct Refused: Error {}
        do {
            try await engine.write { tx in
                try tx.rows(TestNote.self).create(TestNote(id: "n2"))
                throw Refused()
            }
            XCTFail("the refused write must throw")
        } catch is Refused {}
    }
}

/// The readonly `jobs` stream, read-only and writable-shaped: a transaction
/// reads the one and refuses to write through the other.
private struct ReadonlyJob: ReplicaRowModel, Equatable {
    static let streamName = "jobs"
    var id: String
    init(id: String) { self.id = id }
    init?(id: String, type: String?, data: [String: ReplicaValue]) { self.id = id }
    var typeName: String? { nil }
    func encode() -> [String: ReplicaValue] { [:] }
}

private struct WritableJob: ReplicaWritableRowModel, Equatable {
    static let streamName = "jobs"
    var id: String
    init(id: String) { self.id = id }
    init?(id: String, type: String?, data: [String: ReplicaValue]) { self.id = id }
    var typeName: String? { nil }
    func encode() -> [String: ReplicaValue] { [:] }
}

/// Commits everything it is sent; its answer to the first push is lost on the
/// way back.
private actor LostAnswerTransport: FixtureTransport {
    let protocolFixture = ProtocolFixture()
    private var rows: Set<String> = []
    private var lost = false

    func push(_ ops: [ReplicaOp]) async throws -> [ReplicaVerdict] {
        for op in ops {
            if op.verb == ReplicaOp.Verb.rowCreate { rows.insert(op.rowId) }
            if op.verb == ReplicaOp.Verb.rowDelete { rows.remove(op.rowId) }
        }
        if !lost {
            lost = true
            throw ReplicaError.transport("the answer was lost")
        }
        return ops.map { ReplicaVerdict(id: $0.id, outcome: .accepted) }
    }

    func pull(shard: String, cursor: String?, limit: Int) async throws -> ReplicaPullResponse {
        ReplicaPullResponse(frames: rows.map { Fixture.note($0, title: "committed") }, cursor: "lost:1", more: false)
    }
}

/// No connection: every push fails before it leaves the device.
private actor OfflineTransport: FixtureTransport {
    let protocolFixture = ProtocolFixture()
    func push(_ ops: [ReplicaOp]) async throws -> [ReplicaVerdict] {
        throw URLError(.notConnectedToInternet)
    }

    func pull(shard: String, cursor: String?, limit: Int) async throws -> ReplicaPullResponse {
        throw URLError(.notConnectedToInternet)
    }
}

/// The first push commits and loses its answer; the second finds the device
/// offline; the third lands.
private actor LostThenOfflineTransport: FixtureTransport {
    let protocolFixture = ProtocolFixture()
    private var rows: Set<String> = []
    private var attempts = 0

    func push(_ ops: [ReplicaOp]) async throws -> [ReplicaVerdict] {
        attempts += 1
        if attempts == 2 { throw URLError(.notConnectedToInternet) }
        for op in ops {
            if op.verb == ReplicaOp.Verb.rowCreate { rows.insert(op.rowId) }
            if op.verb == ReplicaOp.Verb.rowDelete { rows.remove(op.rowId) }
        }
        if attempts == 1 { throw ReplicaError.transport("committed, answer lost") }
        return ops.map { ReplicaVerdict(id: $0.id, outcome: .accepted) }
    }

    func pull(shard: String, cursor: String?, limit: Int) async throws -> ReplicaPullResponse {
        ReplicaPullResponse(frames: rows.map { Fixture.note($0, title: "committed") }, cursor: "lost-then-offline:1", more: false)
    }
}
