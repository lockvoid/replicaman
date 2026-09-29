import Foundation
import XCTest
@testable import ReplicaMan

/// Lead's adversarial probe — edges the lane pins do NOT cover.
final class LaneEdgeProbeTests: XCTestCase {

    /// Promotion walks op data for values matching pending row ids. If two
    /// pending rows name each other, does the walk terminate?
    func testPromotionTerminatesOnAReferenceCycle() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        try await engine.saveRow(stream: "notes", id: "A", type: nil, data: ["ref": .string("B")])
        try await engine.saveRow(stream: "notes", id: "B", type: nil, data: ["ref": .string("A")])

        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "C", type: nil, data: ["ref": .string("A")])
        }

        let lanes = try await Task { try store.pool.read { db -> [String: String] in
            var byRow: [String: String] = [:]
            for entry in try store.pending(db) {
                byRow[try entry.op().rowId] = try store.lane(db, entryId: entry.id).rawValue
            }
            return byRow
        } }.value
        XCTAssertEqual(lanes["A"], "interactive")
        XCTAssertEqual(lanes["B"], "interactive", "the cycle is followed once, not forever")
        XCTAssertEqual(lanes["C"], "interactive")
    }

    /// THE SUSPECTED REGRESSION: promotion decodes every pending entry's op.
    /// Before the lane a corrupt/undecodable entry just sat in the journal.
    /// Does one bad row now blow up a NEW user write?
    func testACorruptPendingEntryDoesNotBreakTheUsersNextWrite() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        try await engine.saveRow(stream: "notes", id: "ok", type: nil, data: ["title": .string("t")])
        // Poison one journal payload the way a partial write or a version
        // skew would.
        try await Task { try store.pool.write { db in
            try db.execute(sql: "UPDATE intents SET payload = ? WHERE id = (SELECT id FROM intents LIMIT 1)",
                           arguments: ["{not json at all"])
        } }.value

        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "message", type: nil, data: ["title": .string("typed")])
        }

        let pending = try await Task { try store.pool.read { try store.pending($0).count } }.value
        XCTAssertEqual(pending, 2, "the user's write landed despite the poison entry")
    }

    /// Parked entries (rejected, awaiting the user) must not be dragged onto a
    /// lane or drained.
    func testParkedEntriesAreExcludedFromLanesAndDrains() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "parked", type: nil, data: ["title": .string("x")])
        try await Task { try store.pool.write { db in
            try db.execute(sql: "UPDATE intents SET state = 'refused', reason = 'refused'")
        } }.value
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "live", type: nil, data: ["ref": .string("parked")])
        }
        _ = try await engine.drain(.interactive)

        let sent = await transport.pushedBatches.flatMap { $0.map(\.rowId) }
        XCTAssertEqual(sent, ["live"], "a parked entry is not resurrected by promotion")
    }
    /// A row reference can sit inside an array or a nested document
    /// (`Cook.data` is an arbitrary jsonb doc), not only in a flat string
    /// column. Promotion that only reads top-level strings misses it.
    func testPromotionFindsIdsNestedInsideArraysAndObjects() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        try await engine.saveRow(stream: "notes", id: "in-array", type: nil, data: ["title": .string("a")])
        try await engine.saveRow(stream: "notes", id: "in-object", type: nil, data: ["title": .string("o")])

        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "doc", type: nil, data: [
                "refs": .array([.string("in-array")]),
                "meta": .object(["source": .string("in-object")]),
            ])
        }

        let lanes = try await Task { try store.pool.read { db -> [String: String] in
            var byRow: [String: String] = [:]
            for entry in try store.pending(db) {
                byRow[try entry.op().rowId] = try store.lane(db, entryId: entry.id).rawValue
            }
            return byRow
        } }.value
        XCTAssertEqual(lanes["in-array"], "interactive")
        XCTAssertEqual(lanes["in-object"], "interactive")
    }

    /// Stickiness pulls a row up by its ROW; what THAT row names must come
    /// with it, or the chain breaks one link further down.
    func testAStickyPromotionAlsoWalksWhatThePromotedEntryNames() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        try await engine.saveRow(stream: "notes", id: "dependency", type: nil, data: ["title": .string("q")])
        try await engine.saveRow(stream: "notes", id: "carrier", type: nil, data: ["ref": .string("dependency")])
        // Touch `carrier` from an interactive action: stickiness pulls its
        // queued create up, and `dependency` must follow.
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "carrier", type: nil, data: ["title": .string("edited")])
        }

        let lanes = try await Task { try store.pool.read { db -> [String: String] in
            var byRow: [String: String] = [:]
            for entry in try store.pending(db) {
                byRow[try entry.op().rowId] = try store.lane(db, entryId: entry.id).rawValue
            }
            return byRow
        } }.value
        XCTAssertEqual(lanes["carrier"], "interactive")
        XCTAssertEqual(lanes["dependency"], "interactive",
                       "the promoted entry's own dependency cannot be left behind")
    }
}
