import XCTest
import GRDB
@testable import ReplicaMan

final class ReferenceTests: XCTestCase {
    private let child = "pmck/Element/é/offline"
    private let expected = "derived:eeaecc490e5bd06da25475507ce21c4cefd406e00f06c9f3bdd4ed03e2eb9720"

    private func schema() -> ReplicaSchema {
        ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "parents", lane: .row),
            ReplicaStreamSpec(name: "children", lane: .row, references: [
                ReplicaReferenceSpec(name: "parent", stream: "parents", keySegment: 2, keyPrefix: "pmck/Element/")
            ], lifetimeFrom: "parent")
        ], namespace: "refs")
    }

    func testBirthAndPatchCarryTheParentLifetimeAndCrossLanguageDerivedIdentity() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport(), schema: schema())
        try await engine.createRow(stream: "parents", id: "é", type: nil, data: [:])
        try await store.pool.write { db in
            try store.setIncarnation(db, stream: "parents", id: "é", shard: "user", incarnation: "parent-life")
        }
        try await engine.createRow(stream: "children", id: child, type: nil, data: ["value": .string("first")])
        try await engine.updateRow(stream: "children", id: child, type: nil, data: ["value": .string("second")])
        let operations = try store.peekPending().map { try ReplicaJSON.decoder().decode(ReplicaOp.self, from: $0.payload) }
            .filter { $0.stream == "children" }
        XCTAssertEqual(operations.count, 2)
        for op in operations {
            XCTAssertEqual(op.incarnation, expected)
            XCTAssertEqual(op.references, [ReplicaReference(name: "parent", stream: "parents", id: "é", incarnation: "parent-life")])
        }
    }

    func testARefusalOfAReBornDerivedLifetimeIsKeptWhileItsEarlierBirthRefusalWaits() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "parents", lane: .row),
            ReplicaStreamSpec(name: "children", lane: .row, references: [
                ReplicaReferenceSpec(name: "parent", stream: "parents", keySegment: 2, keyPrefix: "pmck/Element/")
            ], lifetimeFrom: "parent")
        ])
        let engine = Fixture.engine(store: store, transport: transport, schema: schema)
        try await engine.createRow(stream: "parents", id: "é", type: nil, data: [:])
        try await store.pool.write { db in
            try store.setIncarnation(db, stream: "parents", id: "é", shard: "user", incarnation: "parent-life")
        }
        await transport.scriptPush { ops in
            ops.map { op in
                op.stream == "children"
                    ? ReplicaVerdict(id: op.id, outcome: .rejected, reason: "first birth refused")
                    : ReplicaVerdict(id: op.id, outcome: .accepted)
            }
        }
        try await engine.createRow(stream: "children", id: child, type: nil, data: ["value": .string("first")])
        _ = try await engine.drain()

        await transport.scriptPush { ops in
            ops.map { op in
                op.verb == ReplicaOp.Verb.rowPatch
                    ? ReplicaVerdict(id: op.id, outcome: .rejected, reason: "patch refused")
                    : ReplicaVerdict(id: op.id, outcome: .accepted)
            }
        }
        try await engine.createRow(stream: "children", id: child, type: nil, data: ["value": .string("again")])
        _ = try await engine.drain()
        try await engine.updateRow(stream: "children", id: child, type: nil, data: ["value": .string("edited")])
        _ = try await engine.drain()

        let refusals = try await engine.parkedOps().compactMap(\.parked).sorted()
        XCTAssertEqual(refusals, ["first birth refused", "patch refused"])
    }

    func testHeldOrdinaryChildRetainsItsParentLifetimeWhenReleased() async throws {
        let store = try Fixture.store()
        let ledger = ReleaseLedger()
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "parents", lane: .row),
            ReplicaStreamSpec(name: "children", lane: .row, references: [
                ReplicaReferenceSpec(name: "parent", stream: "parents", field: "parentId")
            ])
        ])
        let gate = TestGate("children") { _ in
            ledger.contains("child") ? .push : .gate("upload pending")
        }
        let engine = Fixture.engine(store: store, transport: StubTransport(), schema: schema, syncGates: [gate])
        try await engine.createRow(stream: "parents", id: "parent", type: nil, data: [:])
        try await engine.createRow(stream: "children", id: "child", type: nil,
                                   data: ["parentId": .string("parent"), "title": .string("keep")])
        let holds = try engine.heldRows()
        XCTAssertEqual(holds.count, 1)
        let pending = try store.peekPending()

        // A verified checkpoint may replace a parent while a media upload holds
        // its child. Ordinary child lifetimes must retain the old relationship.
        try await store.pool.write { db in
            try store.setIncarnation(db, stream: "parents", id: "parent", shard: "user", incarnation: "replacement")
        }
        ledger.landQuietly("child")
        do {
            try await engine.refreshSyncGates()
            XCTFail("release must require recovery of the old relationship")
        } catch ReplicaError.storage { /* The explicit refusal under test. */ }

        XCTAssertEqual(try engine.heldRows(), holds)
        XCTAssertEqual(try store.peekPending(), pending)
        XCTAssertEqual(try store.peekSnapshot("children", "child")?.data["title"], .string("keep"))
    }

    func testDeletingHeldAndDeliveredRowsRetainsRequiredFieldReferences() async throws {
        for heldBirth in [true, false] {
            let store = try Fixture.store()
            let ledger = ReleaseLedger()
            let schema = ReplicaSchema(streams: [
                ReplicaStreamSpec(name: "parents", lane: .row),
                ReplicaStreamSpec(name: "children", lane: .row, references: [
                    ReplicaReferenceSpec(name: "parent", stream: "parents", field: "parentId")
                ])
            ])
            let gate = TestGate("children") { change in
                if ledger.contains("release") { return .push }
                return heldBirth || change.kind == .delete ? .gate("waiting") : .push
            }
            let engine = Fixture.engine(store: store, transport: StubTransport(), schema: schema, syncGates: [gate])
            try await engine.createRow(stream: "parents", id: "parent", type: nil, data: [:])
            try await engine.createRow(stream: "children", id: "child", type: nil, data: ["parentId": .string("parent")])
            if !heldBirth { _ = try await engine.drain() }

            _ = try await engine.deleteRow(stream: "children", id: "child")
            XCTAssertNil(try store.peekSnapshot("children", "child"))
            ledger.landQuietly("release")
            try await engine.refreshSyncGates()
            XCTAssertTrue(try engine.heldRows().isEmpty)
            let deletes = try store.peekPending().map { try $0.op() }.filter { $0.stream == "children" }
            if heldBirth {
                XCTAssertTrue(deletes.isEmpty, "an unsent held birth must disappear locally")
            } else {
                XCTAssertEqual(deletes.count, 1)
                XCTAssertEqual(deletes.first?.verb, ReplicaOp.Verb.rowDelete)
                XCTAssertEqual(deletes.first?.references.first?.id, "parent")
            }
        }
    }

    func testAHostReadsAHeldRowItsLifetimeAndTheLifetimesItNames() async throws {
        let store = try Fixture.store()
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "parents", lane: .row),
            ReplicaStreamSpec(name: "children", lane: .row, references: [
                ReplicaReferenceSpec(name: "parent", stream: "parents", field: "parentId")
            ])
        ])
        let gate = TestGate("children") { _ in .gate("upload pending") }
        let engine = Fixture.engine(store: store, transport: StubTransport(), schema: schema, syncGates: [gate])
        try await engine.createRow(stream: "parents", id: "parent", type: nil, data: [:])
        try await engine.createRow(stream: "children", id: "child", type: "Kid",
                                   data: ["parentId": .string("parent"), "title": .string("keep")])
        XCTAssertEqual(try engine.heldRows().map(\.rowId), ["child"])

        let lifetimes = try engine.incarnations(stream: "parents", ids: ["parent", "stranger"])
        let parent = try XCTUnwrap(lifetimes["parent"])
        XCTAssertEqual(Array(lifetimes.keys), ["parent"], "an address the store never saw has no lifetime")
        XCTAssertEqual(try engine.references(stream: "children", id: "child"),
                       [ReplicaReference(name: "parent", stream: "parents", id: "parent", incarnation: parent)])
        XCTAssertEqual(try engine.references(stream: "parents", id: "parent"), [])
        XCTAssertEqual(try engine.snapshotRow(stream: "children", id: "child"),
                       ReplicaStateStore.SnapshotRow(stream: "children", rowId: "child", type: "Kid",
                                                     data: ["parentId": .string("parent"), "title": .string("keep")]))
        XCTAssertNil(try engine.snapshotRow(stream: "children", id: "stranger"))

        try await engine.close()
        XCTAssertNil(try engine.snapshotRow(stream: "children", id: "child"), "a closed engine reads empty")
        XCTAssertEqual(try engine.incarnations(stream: "parents", ids: ["parent"]), [:])
        XCTAssertEqual(try engine.references(stream: "children", id: "child"), [])
    }

    func testMissingParentRollsBackTheChildAndItsJournal() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport(), schema: schema())
        do {
            try await engine.createRow(stream: "children", id: child, type: nil, data: [:])
            XCTFail("the missing parent must prevent the write")
        } catch ReplicaError.storage { /* The explicit refusal under test. */ }
        XCTAssertNil(try store.peekSnapshot("children", child))
        XCTAssertTrue(try store.peekPending().isEmpty)
    }

    func testReplacementParentCannotRetargetExistingChildAuthoring() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport(), schema: schema())
        try await engine.createRow(stream: "parents", id: "é", type: nil, data: [:])
        try await engine.createRow(stream: "children", id: child, type: nil, data: ["value": .string("keep")])
        let pending = try store.peekPending()
        try await store.pool.write { db in
            try store.setIncarnation(db, stream: "parents", id: "é", shard: "user", incarnation: "replacement")
        }
        do {
            try await engine.updateRow(stream: "children", id: child, type: nil, data: ["value": .string("wrong parent")])
            XCTFail("editing an old child after parent replacement needs recovery")
        } catch ReplicaError.storage { /* The explicit refusal under test. */ }
        XCTAssertEqual(try store.peekPending(), pending)
        XCTAssertEqual(try store.peekSnapshot("children", child)?.data["value"], .string("keep"))
    }
}
