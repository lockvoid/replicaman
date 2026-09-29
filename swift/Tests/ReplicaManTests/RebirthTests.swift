import XCTest
@testable import ReplicaMan

final class RebirthTests: XCTestCase {
    func testCancelledFirstBirthLeavesNoInventedPredecessor() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: [:])
        let queued = try await engine.deleteRow(stream: "notes", id: "n")
        XCTAssertFalse(queued)
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: [:])
        let birth = try XCTUnwrap(store.peekPending().first).op()
        XCTAssertNil(birth.replaces)
    }

    func testCancellingRecreationPreservesThePreviousLifetimesDelete() async throws {
        let store = try Fixture.store()
        let wire = StubTransport()
        let engine = Fixture.engine(store: store, transport: wire)
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: ["title": .string("first")])
        let first = try XCTUnwrap(store.peekPending().first).op()
        try await engine.drain()
        try await engine.deleteRow(stream: "notes", id: "n")
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: ["title": .string("cancelled")])
        let queued = try await engine.deleteRow(stream: "notes", id: "n")
        XCTAssertFalse(queued)
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: ["title": .string("third")])

        let operations = try store.peekPending().map { try $0.op() }
        XCTAssertEqual(operations.map(\.verb), [ReplicaOp.Verb.rowDelete, ReplicaOp.Verb.rowCreate])
        XCTAssertEqual(operations.first?.incarnation, first.incarnation)
        XCTAssertEqual(operations.last?.replaces, first.incarnation)
        XCTAssertNotEqual(operations.last?.incarnation, first.incarnation)
        try await engine.drain()
        let delivered = await wire.pushedOps()
        XCTAssertEqual(delivered.map(\.verb), [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.rowDelete, ReplicaOp.Verb.rowCreate])
    }

    func testHeldRecreationKeepsItsPredecessorAcrossStoreReopen() async throws {
        let directory = Fixture.directory()
        let wire = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.unopenedEngine(in: directory, transport: wire, syncGates: [blobGate(released)])
        try await engine.open(owner: 42)
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: [:])
        try await engine.drain()
        let first = await wire.pushedOps().first
        try await engine.deleteRow(stream: "notes", id: "n")
        try await engine.createRow(stream: "notes", id: "n", type: nil, data: ["blob": .string("upload")])
        XCTAssertEqual(try engine.heldRows().count, 1)
        try await engine.close()

        let reopened = Fixture.unopenedEngine(in: directory, transport: wire, syncGates: [blobGate(released)])
        try await reopened.open(owner: 42)
        XCTAssertEqual(try reopened.heldRows().count, 1)
        released.landQuietly("upload")
        try await reopened.refreshSyncGates()
        try await reopened.drain()
        let sent = await wire.pushedOps()
        XCTAssertEqual(sent.last?.verb, ReplicaOp.Verb.rowCreate)
        XCTAssertEqual(sent.last?.replaces, first?.incarnation)
        XCTAssertNotNil(sent.last?.replaces)
        try await reopened.close()
    }
}

