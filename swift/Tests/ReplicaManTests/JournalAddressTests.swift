import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// The journal's address (stream, row_id) is a COLUMN, and the "was this row
/// ever born on the server?" question reads it with the verb.
///
/// Both pins here were holes: blanking the verb filter in `entriesAddressing`
/// passed the whole suite, and every caller of it takes a destructive branch
/// on the answer — a pending PATCH counted as a birth silently swallows a
/// delete the server needed to hear, and silently refuses a create.
final class JournalAddressTests: XCTestCase {

    func testDeletingAnUnsentRowSurvivesUnrelatedMalformedJournalBytes() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.saveRow(stream: "notes", id: "unsent", type: nil, data: ["title": .string("draft")])
        try await store.pool.write { db in
            try db.execute(sql: "INSERT INTO intents (id, stream, row_id, state, op, payload, lane, created_at) VALUES ('corrupt', 'notes', 'other', 'owed', 'row.patch', '{not json', 'bulk', 0)")
        }

        _ = try await engine.deleteRow(stream: "notes", id: "unsent")

        let ids = try await store.pool.read { db in try String.fetchAll(db, sql: "SELECT id FROM intents") }
        XCTAssertEqual(ids, ["corrupt"])
    }

    private func acceptAll(_ transport: StubTransport) async {
        await transport.scriptPush { ops in
            ops.map { ReplicaVerdict(id: $0.id, outcome: .accepted) }
        }
    }

    /// A row the server already knows, with a pending patch owed for it.
    /// The patch is not a birth: deleting must still journal `row.delete`.
    func testAPendingPatchIsNotABirthSoTheDeleteStillShips() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await acceptAll(transport)
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("born")])
        _ = try await engine.drain(.bulk)
        let pendingAfterBirth = try store.pendingOps()
        XCTAssertTrue(pendingAfterBirth.isEmpty, "the create settled — the server has heard of n1")

        // A patch, still owed. The row exists server-side.
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("edited")])
        let deleted = try await engine.deleteRow(stream: "notes", id: "n1")

        XCTAssertTrue(deleted, "a known row's delete is real work, not local silence")
        let verbs = try store.pendingOps().map { try $0.op().verb }
        XCTAssertEqual(verbs, [ReplicaOp.Verb.rowPatch, ReplicaOp.Verb.rowDelete],
                       "the row is born server-side: its owed patch keeps its place, and the "
                       + "delete is appended behind it — nothing is discarded as never-born")
    }

    /// Same confusion at the other caller: an owed entry on the same address
    /// that is not a create must not make the document create think the row
    /// is already born.
    func testAPendingDeleteDoesNotBlockADocumentCreate() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await acceptAll(transport)
        let engine = Fixture.engine(store: store, transport: transport)
        try await engine.createDoc(stream: "boards", id: "b", seed: Data("SEED".utf8), peer: 7)
        try await engine.drain()
        _ = try await engine.deleteRow(stream: "boards", id: "b")
        XCTAssertEqual(try store.peekPending().map { try $0.op().verb }, [ReplicaOp.Verb.rowDelete])

        let reborn = try await engine.createDoc(stream: "boards", id: "b", seed: Data("SEED".utf8), peer: 7)
        XCTAssertTrue(reborn, "a pending delete is not a birth")
    }

}
