import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// The transaction's row verbs: `create` ⇒ `row.create` (absent), `update`
/// ⇒ `row.patch` of the changed fields ONLY / nothing (no change), with the
/// client snapshot write in the same transaction. Deletes discharge what the
/// row still owed.
final class SaveDeleteTests: XCTestCase {

    /// `update` speaks to a row that IS there; a missing id is the caller's
    /// mistake, answered loudly and written nowhere — the silent upsert is
    /// how "it was never created" ships as a green test.
    ///
    /// KILL: answer a missing row in `ReplicaTransaction.update` with a fresh
    /// model instead of `unknownRow` — the missing row is minted and the
    /// assertion below sees a row.
    func testUpdateOfAMissingRowThrowsAndWritesNothing() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<TestNote>(engine: engine)

        do {
            try await engine.write { tx in try tx.rows(TestNote.self).update("n9") { $0.title = "ghost" } }
            XCTFail("an update of a missing row went through")
        } catch let error as ReplicaError {
            XCTAssertEqual(error, .unknownRow(stream: "notes", id: "n9"))
        }
        XCTAssertNil(try notes.find("n9"), "the missing row was minted by an update")
        XCTAssertEqual(try store.peekPending().count, 0, "an update of nothing owes the server nothing")
    }

    /// The server would refuse the whole push carrying this id, on every
    /// retry: the id is refused at the door, and nothing is written.
    func testARowIdOutsideTheBusinessKeyIsRefusedAtTheDoor() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        for id in ["", String(repeating: "x", count: 1025), "nul\u{0}key"] {
            do {
                try await engine.saveRow(stream: "notes", id: id, type: nil, data: ["title": .string("bad key")])
                XCTFail("a write with an invalid id went through")
            } catch let error as ReplicaError {
                XCTAssertEqual(error, .invalidRowId(stream: "notes", id: id))
            }
        }
        XCTAssertEqual(try store.allSnapshots(), [])
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    /// An intent over the request limit could never leave and would stop every
    /// intent behind it: it is refused at the door, and nothing is written.
    func testAWriteOverTheRequestLimitIsRefusedAtTheDoor() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let huge = String(repeating: "x", count: ReplicaProtocol.operationBytes)

        do {
            try await engine.saveRow(stream: "notes", id: "huge", type: nil, data: ["title": .string(huge)])
            XCTFail("an oversized write went through")
        } catch ReplicaError.oversizedWrite(let stream, let id, let bytes) {
            XCTAssertEqual([stream, id], ["notes", "huge"])
            XCTAssertGreaterThan(bytes, ReplicaProtocol.operationBytes)
        }
        XCTAssertNil(try store.peekSnapshot("notes", "huge"))
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    /// `create` mints; a present id is the caller's mistake — the row's
    /// truth stands untouched.
    ///
    /// KILL: drop the presence check in `ReplicaTransaction.create` — the
    /// second create patches the title and the assertion sees "second".
    func testCreateOfAPresentRowThrowsAndKeepsTheRow() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<TestNote>(engine: engine)
        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "first", rank: "a")) }

        do {
            try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "second", rank: "a")) }
            XCTFail("a create over a present row went through")
        } catch let error as ReplicaError {
            XCTAssertEqual(error, .rowExists(stream: "notes", id: "n1"))
        }
        XCTAssertEqual(try notes.find("n1")?.title, "first", "the present row was overwritten by a create")
        XCTAssertEqual(try store.peekPending().count, 1, "only the mint owes the server anything")
    }

    func testCreateThenUpdateDiffsIntoCreateThenPatchThenNothing() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let notes = RowStream<TestNote>(engine: engine)

        try await engine.write { tx in try tx.rows(TestNote.self).create(TestNote(id: "n1", title: "draft", rank: "a")) }
        var pending = try store.peekPending()
        XCTAssertEqual(pending.count, 1)
        var op = try XCTUnwrap(pending.first).op()
        XCTAssertEqual(op.verb, ReplicaOp.Verb.rowCreate)
        XCTAssertEqual(op.data, ["title": .string("draft"), "rank": .string("a")], "creation is the only full-row write")
        XCTAssertEqual(try notes.find("n1")?.title, "draft", "the client write is visible immediately")

        try await engine.write { tx in try tx.rows(TestNote.self).update("n1") { $0.title = "final" } }
        pending = try store.peekPending()
        XCTAssertEqual(pending.count, 2)
        op = try XCTUnwrap(pending.last).op()
        XCTAssertEqual(op.verb, ReplicaOp.Verb.rowPatch)
        XCTAssertEqual(op.data, ["title": .string("final")], "updates are always patches — changed fields ONLY")

        try await engine.write { tx in try tx.rows(TestNote.self).update("n1") { $0.title = "final" } }
        XCTAssertEqual(try store.peekPending().count, 2, "an unchanged update owes the server nothing")
    }

    func testGeneratedOptionalFieldCanBeClearedWithNullPatch() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "items", lane: .row, shard: "user"),
        ])
        let engine = Fixture.engine(store: store, transport: transport, schema: schema)
        let items = RowStream<Item>(engine: engine)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowSet(
                stream: "items",
                id: "i1",
                type: "PhotoItem",
                data: [
                    "boardId": .string("b1"),
                    "label": .string("idea"),
                    "rank": .string("a"),
                ]
            )],
            cursor: "1:",
            more: false
        ))
        try await engine.pullOnce(shard: "user")

        try await engine.write { tx in
            try tx.rows(Item.self).update("i1") { item in
                guard case .photoItem(var photo) = item else { return }
                photo.label = nil
                item = .photoItem(photo)
            }
        }

        let pending = try store.peekPending()
        XCTAssertEqual(pending.count, 1)
        let op = try XCTUnwrap(pending.first).op()
        XCTAssertEqual(op.verb, ReplicaOp.Verb.rowPatch)
        XCTAssertEqual(op.data, ["label": .null], "nil is an authored clear, not an omitted diff")

        guard case .photoItem(let written) = try XCTUnwrap(items.find("i1")) else {
            return XCTFail("the generated STI projection must survive its client write")
        }
        XCTAssertNil(written.label, "the client snapshot must clear the old value immediately")
        XCTAssertEqual(try store.peekSnapshot("items", "i1")?.data["label"], .null)
    }

    func testSaveToReadonlyStreamIsRefused() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        do {
            try await engine.saveRow(stream: "jobs", id: "j1", type: nil, data: ["state": .string("hacked")])
            XCTFail("readonly streams take no client writes")
        } catch ReplicaError.readonlyStream(let name) {
            XCTAssertEqual(name, "jobs")
        }
    }

    func testDeleteOfNeverPushedCreateOwesTheServerNothing() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("oops")])
        try await engine.deleteRow(stream: "notes", id: "n1")

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try store.peekPending().count, 0, "the server never heard n1 — nothing to push, nothing to resurrect")
    }

    func testDeleteOfSyncedRowJournalsRowDelete() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "server copy")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        try await engine.deleteRow(stream: "notes", id: "n1")
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        let pending = try store.peekPending()
        XCTAssertEqual(pending.count, 1)
        let op = try XCTUnwrap(pending.first).op()
        XCTAssertEqual(op.verb, ReplicaOp.Verb.rowDelete)
        XCTAssertEqual(op.rowId, "n1")

        // The discriminating half: a row that never existed is ordinary CRUD
        // silence. Asserted HERE, next to the positive, because on its own
        // "pending is empty" is also satisfied by a delete that does nothing
        // at all.
        let deletedNothing = try await engine.deleteRow(stream: "notes", id: "never-existed")
        XCTAssertFalse(deletedNothing)
        XCTAssertEqual(try store.peekPending().count, 1, "an absent row adds no work")
    }

    func testLocalDocDeleteCascadesItsOwnJournal() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+edit".utf8))
        try await engine.deleteRow(stream: "boards", id: "b1")

        XCTAssertNil(try store.peekSnapshot("boards", "b1"))
        XCTAssertNil(try store.peekDoc("boards", "b1"))
        XCTAssertEqual(try store.peekPending().count, 0, "an unborn doc dies silently — create and deltas discarded, no delete op")
    }
}
