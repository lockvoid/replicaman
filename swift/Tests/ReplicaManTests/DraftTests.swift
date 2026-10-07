import Foundation
import XCTest
@testable import ReplicaMan

/// Drafts: a write made inside `beginDraft { }` lands in
/// the store like any other — every reader, watch and document sees it —
/// but its journal entry carries the draft's key and the drain never
/// selects it. `commitDraft` strips the key (the entries go on the wire in
/// their journal order), `discardDraft` drops the rows and the entries so the
/// server never hears of them, and a draft never survives the process: the
/// store sweeps every keyed entry at open. A later write that NAMES a
/// drafted row joins the draft — a chat message for a draft project waits
/// with it and dies with it; nothing is ever sent ahead of its birth.
final class DraftTests: XCTestCase {

    private func pushed(_ transport: StubTransport) async -> [ReplicaOp] {
        await transport.pushedOps()
    }

    // MARK: - Held, visible, silent

    func testDraftWritesAreVisibleLocallyAndNeverDrained() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let draft = try await engine.beginDraft {
            try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7, data: ["name": .string("Draft board")])
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft note")])
        }

        XCTAssertNotNil(try store.peekSnapshot("boards", "b1"), "a draft row is a row to every reader")
        XCTAssertNotNil(try store.peekDoc("boards", "b1"), "the document is held like any other")
        XCTAssertEqual(try store.peekDrafted().count, 2)
        XCTAssertEqual(try store.peekPending().count, 0, "held entries are not pending work")

        _ = try await engine.drain()
        _ = try await engine.drain(.interactive)
        let sent = await pushed(transport)
        XCTAssertEqual(sent, [], "nothing of a draft reaches the wire")
        XCTAssertFalse(draft.key.isEmpty)
    }

    func testAHeldOnlyJournalOwesNoWork() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        _ = try await engine.beginDraft {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft")])
        }

        let owed = try await store.pool.read { db in try store.lanesOwed(db) }
        XCTAssertEqual(owed, [], "a draft must not keep the pusher awake")
    }

    // MARK: - Commit

    func testCommitReleasesTheWholeDraftInJournalOrder() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let draft = try await engine.beginDraft {
            try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft")])
        }
        // A plain write after the draft: it must NOT overtake the draft's birth
        // once the draft is released — order is the journal's, rowid.
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("later")])

        try await engine.commitDraft(draft)
        XCTAssertEqual(try store.peekDrafted().count, 0)
        XCTAssertEqual(try store.peekPending().count, 3)

        _ = try await engine.drain()
        let sent = await pushed(transport)
        XCTAssertEqual(sent.map(\.rowId), ["b1", "n1", "n2"], "released entries keep their queue position")
        XCTAssertEqual(sent.first?.verb, ReplicaOp.Verb.rowCreate)
    }

    func testCommitIsIdempotentAndUnknownKeysAreSilent() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let draft = try await engine.beginDraft {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft")])
        }
        try await engine.commitDraft(draft)
        try await engine.commitDraft(draft)
        try await engine.commitDraft(ReplicaDraft(key: "never-existed"))

        _ = try await engine.drain()
        let sent = await pushed(transport)
        XCTAssertEqual(sent.map(\.rowId), ["n1"], "exactly one create, once")
    }

    // MARK: - Discard

    func testDiscardDropsRowsDocumentsAndEntriesAndSendsNothing() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let draft = try await engine.beginDraft {
            try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
            try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+edit".utf8))
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft")])
        }
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("stays")])

        try await engine.discardDraft(draft)

        XCTAssertNil(try store.peekSnapshot("boards", "b1"))
        XCTAssertNil(try store.peekDoc("boards", "b1"))
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try store.peekDrafted().count, 0)
        XCTAssertEqual(try store.peekPending().map { try $0.op().rowId }, ["n2"], "the unrelated write survives")

        _ = try await engine.drain()
        let sent = await pushed(transport)
        XCTAssertEqual(sent.map(\.rowId), ["n2"], "no delete op either — the server never knew")
    }

    /// A draft can touch rows the server already has: discarding it undoes
    /// those writes through their preimages, the way a refusal would, so the
    /// rows stand as they stood before the draft — the edits before it included.
    func testDiscardRestoresTheServerRowsTheDraftPatchedOrDeleted() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [
            .rowSet(stream: "notes", id: "n1", type: nil, data: ["title": .string("server"), "rank": .string("1")]),
            .rowSet(stream: "notes", id: "n3", type: nil, data: ["title": .string("doomed")]),
        ], cursor: "c1", more: false))
        try await engine.pullOnce()
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["rank": .string("2")])

        let draft = try await engine.beginDraft {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("edited in a draft")])
            try await engine.saveRow(stream: "notes", id: "n4", type: nil, data: ["title": .string("born in the draft")])
            _ = try await engine.deleteRow(stream: "notes", id: "n3")
        }
        try await engine.discardDraft(draft)

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data, ["title": .string("server"), "rank": .string("2")],
                       "the draft's edit is undone; the edit before the draft stands")
        XCTAssertEqual(try store.peekSnapshot("notes", "n3")?.data, ["title": .string("doomed")], "the deleted row is back")
        XCTAssertNil(try store.peekSnapshot("notes", "n4"))
        XCTAssertEqual(try store.peekDrafted().count, 0)
        XCTAssertEqual(try store.peekPending().map { try $0.op().rowId }, ["n1"], "only the write before the draft is owed")
        _ = try await engine.drain()
        let sent = await pushed(transport)
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowPatch])
        XCTAssertEqual(sent.first?.data, ["rank": .string("2")])
    }

    /// Discarding must give a document back, and only a document the draft
    /// bore has nothing to give back: a draft edits no other document, and
    /// deletes none. Nothing is written by a refused write.
    func testADraftEditsAndDeletesOnlyTheDocumentsItCreated() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [
            .docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SEED".utf8), data: ["name": .string("Board")]),
        ], cursor: "c1", more: false))
        try await engine.pullOnce()

        let draft = try await engine.beginDraft {
            try await engine.createDoc(stream: "boards", id: "b2", seed: Data("MINE".utf8), peer: 7)
            try await engine.recordDocDelta(stream: "boards", id: "b2", payload: Data("+ok".utf8))
            do {
                try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+draft".utf8))
                XCTFail("a draft edited a document it did not create")
            } catch ReplicaError.draftBlocked {}
            do {
                _ = try await engine.deleteRow(stream: "boards", id: "b1")
                XCTFail("a draft deleted a document it did not create")
            } catch ReplicaError.draftBlocked {}
        }

        XCTAssertEqual(try store.peekDoc("boards", "b1")?.fold, Data("SEED".utf8), "the pulled document is untouched")
        XCTAssertEqual(try store.peekDoc("boards", "b2")?.fold, Data("MINE+ok".utf8))
        try await engine.discardDraft(draft)
        XCTAssertNil(try store.peekDoc("boards", "b2"))
        XCTAssertNotNil(try store.peekDoc("boards", "b1"))
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    func testDeletingADraftRowCollapsesToNothing() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let draft = try await engine.beginDraft {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft")])
        }
        _ = try await engine.drain()  // a drain in between changes nothing: the birth was never in flight

        let queuedDelete = try await engine.deleteRow(stream: "notes", id: "n1")

        XCTAssertFalse(queuedDelete, "an unborn row dies silently")
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try store.peekDrafted().count, 0)
        try await engine.commitDraft(draft)
        _ = try await engine.drain()
        let sent = await pushed(transport)
        XCTAssertEqual(sent, [], "nothing to release, nothing sent")
    }

    // MARK: - Dependents join the draft

    func testAWriteNamingADraftRowJoinsTheDraft() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let draft = try await engine.beginDraft {
            _ = try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
        }
        // Written OUTSIDE the scope, on the interactive lane, naming the draft
        // board — the chat message for a draft project.
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "m1", type: nil, data: ["board_id": .string("b1"), "title": .string("hello")])
        }

        XCTAssertEqual(try store.peekDrafted().count, 2, "the dependent is held with its parent")
        _ = try await engine.drain(.interactive)
        _ = try await engine.drain()
        let before = await pushed(transport)
        XCTAssertEqual(before, [], "a dependent never ships ahead of its parent's birth")

        try await engine.commitDraft(draft)
        _ = try await engine.drain(.interactive)
        _ = try await engine.drain()
        let after = await pushed(transport)
        XCTAssertEqual(after.map(\.rowId), ["b1", "m1"], "released together, parent first")
    }

    func testDiscardTakesTheDependentsWithIt() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let draft = try await engine.beginDraft {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft")])
        }
        try await engine.saveRow(stream: "notes", id: "m1", type: nil, data: ["note_id": .string("n1")])

        try await engine.discardDraft(draft)

        XCTAssertNil(try store.peekSnapshot("notes", "m1"), "a message to a discarded draft dies with it")
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    // MARK: - Scopes compose

    func testTheBodyValueComesBackWithTheDraft() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let (draft, id) = try await engine.beginDraft { () -> String in
            try await engine.saveRow(stream: "notes", id: "n9", type: nil, data: ["title": .string("x")])
            return "n9"
        }

        XCTAssertEqual(id, "n9")
        XCTAssertFalse(draft.key.isEmpty)
    }

    // MARK: - Process death

    func testADraftNeverSurvivesAReopen() async throws {
        let directory = Fixture.directory()
        let transport = StubTransport()
        let first = Fixture.unopenedEngine(in: directory, transport: transport)
        try await first.open(owner: 1)
        _ = try await first.beginDraft {
            try await first.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
            try await first.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("draft")])
        }
        try await first.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("real")])
        // Release the process lease without committing or discarding the draft.
        try await first.close()

        let second = Fixture.unopenedEngine(in: directory, transport: transport)
        try await second.open(owner: 1)

        let store = try XCTUnwrap(second.store)
        XCTAssertNil(try store.peekSnapshot("boards", "b1"), "swept at open")
        XCTAssertNil(try store.peekDoc("boards", "b1"))
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try store.peekDrafted().count, 0)
        XCTAssertEqual(try store.peekPending().map { try $0.op().rowId }, ["n2"], "the real write is still owed")
        XCTAssertEqual(Set(try store.recoveryRecords().map(\.rowId)), ["b1", "n1"])
    }
}
