import Foundation
import XCTest
@testable import ReplicaMan

/// What a gate sees. At a write: the CHANGE — per field, the value the write
/// set and the value it replaced; a delete, a document's birth and its delta
/// are judged like a row. Asked again, a held row: its whole state.
final class SyncChangeTests: XCTestCase {

    private final class Judge: @unchecked Sendable {
        private let lock = NSLock()
        private var seen: [SyncChange] = []

        func record(_ change: SyncChange) {
            lock.withLock { seen.append(change) }
        }

        var changes: [SyncChange] { lock.withLock { seen } }
    }

    func testAGateSeesEachWriteWithTheValuesItReplaced() async throws {
        let store = try Fixture.store()
        let judge = Judge()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [TestGate("notes") { change in
            judge.record(change)
            return .push
        }])

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("c"), "rank": .string("r1")])

        XCTAssertEqual(judge.changes, [
            SyncChange(stream: "notes", rowId: "n1", kind: .create, local: ["title": .string("a")]),
            SyncChange(stream: "notes", rowId: "n1", kind: .patch,
                       local: ["title": .string("b")], previous: ["title": .string("a")]),
            SyncChange(stream: "notes", rowId: "n1", kind: .patch,
                       local: ["title": .string("c"), "rank": .string("r1")], previous: ["title": .string("b")]),
        ], "each write its own change: a create replaced nothing, a field new to the row replaced nothing")
    }

    /// A delete's gate once saw nothing of the row it removed, so a
    /// gate that keeps one kind of row flowing held its delete.
    ///
    /// KILL: `ReplicaEngine.change(_:preimage:)` — drop `previous: data` from
    /// the delete.
    func testADeleteShowsItsGateTheRowItRemoved() async throws {
        let store = try Fixture.store()
        let judge = Judge()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [TestGate("notes") { change in
            judge.record(change)
            return .push
        }])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        _ = try await engine.drain()

        _ = try await engine.deleteRow(stream: "notes", id: "n1")

        XCTAssertEqual(judge.changes.last, SyncChange(
            stream: "notes", rowId: "n1", kind: .delete, previous: ["title": .string("a")]
        ))
    }

    func testADocumentsBirthAndDeltaAreJudged() async throws {
        let store = try Fixture.store()
        let judge = Judge()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [TestGate("boards") { change in
            judge.record(change)
            return .push
        }])

        try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+edit".utf8))

        XCTAssertEqual(judge.changes, [
            SyncChange(stream: "boards", rowId: "b1", kind: .create),
            SyncChange(stream: "boards", rowId: "b1", kind: .document),
        ])
    }

    /// A released row leaves as its state, so its gates judge that state —
    /// every field, as the create or the patch it will leave as.
    func testAHeldRowAskedAgainShowsItsGateItsWholeState() async throws {
        let store = try Fixture.store()
        let judge = Judge()
        let released = ReleaseLedger()
        let blob = blobGate(released)
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [
            TestGate("notes", id: "blob", signal: released.signal) { change in
                judge.record(change)
                return blob.judge(change)
            },
        ])
        try await engine.saveRow(stream: "notes", id: "born", type: nil, data: ["title": .string("a"), "blob": .string("k1")])
        try await engine.saveRow(stream: "notes", id: "known", type: nil, data: ["title": .string("b")])
        _ = try await engine.drain()
        try await engine.saveRow(stream: "notes", id: "known", type: nil, data: ["blob": .string("k1")])

        released.land("k1")
        try await eventually("the landing never reached the holds") { try engine.heldRows().isEmpty }

        let bornState = SyncChange(
            stream: "notes", rowId: "born", kind: .create, local: ["title": .string("a"), "blob": .string("k1")]
        )
        XCTAssertEqual(judge.changes.filter { $0 == bornState }.count, 2, "judged at the write and again at the landing")
        XCTAssertTrue(judge.changes.contains(SyncChange(
            stream: "notes", rowId: "known", kind: .patch, local: ["title": .string("b"), "blob": .string("k1")]
        )))
    }
}
