import XCTest
@testable import ReplicaMan

final class AtomicWriteTests: XCTestCase {
    private func submissions(_ store: ReplicaStateStore) throws -> [ReplicaSubmission] {
        try store.pool.read { try store.frozenSubmissions($0) }
    }

    func testGroupCanCreateAParentAndChildAndEditTheParentBeforeFreezing() async throws {
        let store = try Fixture.store()
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "notes", lane: .row, references: [
                ReplicaReferenceSpec(name: "parent", stream: "notes", keySegment: 1, keyPrefix: "child/", optional: true)
            ])
        ])
        let wire = StubTransport()
        let engine = Fixture.engine(store: store, transport: wire, schema: schema)
        try engine.writeAtomically { tx in
            try tx.create(TestNote(id: "parent", title: "First"))
            try tx.create(TestNote(id: "child/parent", title: "Child"))
            try tx.update(TestNote.self, "parent") { $0.title = "Last" }
        }

        let group = try XCTUnwrap(submissions(store).first)
        let operations = try ReplicaJSON.decoder().decode([ReplicaOp].self, from: group.content)
        XCTAssertEqual(operations.map(\.rowId), ["parent", "child/parent", "parent"])
        XCTAssertEqual(operations[0].incarnation, operations[2].incarnation)
        XCTAssertEqual(operations[1].references.first?.incarnation, operations[0].incarnation)
        XCTAssertEqual(operations[0].data?["title"], .string("First"))
        XCTAssertEqual(operations[2].data?["title"], .string("Last"))

        try await engine.drain()
        XCTAssertTrue(try store.peekPending().isEmpty)
        XCTAssertEqual(try store.peekSnapshot("notes", "parent")?.data["title"], .string("Last"))
        XCTAssertEqual(try store.peekSnapshot("notes", "child/parent")?.data["title"], .string("Child"))
    }

    func testGroupLargerThanTransportBatchRemainsOneSubmissionAfterReopen() async throws {
        let directory = Fixture.directory()
        let wire = StubTransport()
        let engine = Fixture.unopenedEngine(in: directory, transport: wire)
        try await engine.open(owner: 42)
        try engine.writeAtomically { tx in
            for index in 0..<60 { try tx.create(TestNote(id: "n-\(index)", title: "group")) }
        }
        let store = try XCTUnwrap(engine.store)
        let saved = try submissions(store)
        XCTAssertEqual(saved.count, 1)
        XCTAssertEqual(saved.first?.entries.count, 60)
        try await engine.close()

        let reopened = Fixture.unopenedEngine(in: directory, transport: wire)
        try await reopened.open(owner: 42)
        let reopenedStore = try XCTUnwrap(reopened.store)
        let retained = try submissions(reopenedStore)
        XCTAssertEqual(retained.first?.content, saved.first?.content)
        XCTAssertEqual(retained.first?.sequence, saved.first?.sequence)
        try await reopened.drain()
        let delivered = await wire.pushedOps()
        XCTAssertEqual(delivered.map(\.rowId), (0..<60).map { "n-\($0)" })
        XCTAssertTrue(try reopenedStore.peekPending().isEmpty)
        try await reopened.close()
    }

    func testOneHeldOrDiscardedMemberRollsBackEveryMember() throws {
        for discard in [false, true] {
            let store = try Fixture.store()
            let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [MemberGate(discard: discard)])
            XCTAssertThrowsError(try engine.writeAtomically { tx in
                try tx.create(TestNote(id: "ready"))
                try tx.create(TestNote(id: "blocked"))
            }) { error in
                guard case ReplicaError.atomicWriteBlocked = error else { return XCTFail("Unexpected error: \(error)") }
            }
            XCTAssertNil(try store.peekSnapshot("notes", "ready"))
            XCTAssertNil(try store.peekSnapshot("notes", "blocked"))
            XCTAssertTrue(try store.peekPending().isEmpty)
            XCTAssertTrue(try engine.heldRows().isEmpty)
            XCTAssertEqual(try store.pool.read { try store.meta($0).nextSequence }, 1)
        }
    }

    func testUnsubmittedDependencyAndOversizedActionLeaveOriginalWorkIntact() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.createRow(stream: "notes", id: "earlier", type: nil, data: ["title": .string("original")])
        let original = try store.peekPending().map(\.payload)
        assertBlocked(engine, "Unsubmitted dependency: notes/earlier") { tx in
            try tx.create(TestNote(id: "new"))
            try tx.update(TestNote.self, "earlier") { $0.title = "overtaken" }
        }
        XCTAssertEqual(try store.peekPending().map(\.payload), original)
        XCTAssertNil(try store.peekSnapshot("notes", "new"))
        assertBlocked(engine, "An atomic write supports at most 100 operations") { tx in
            for index in 0..<101 { try tx.create(TestNote(id: "limit-\(index)")) }
        }
        XCTAssertEqual(try store.peekPending().map(\.payload), original)
        XCTAssertNil(try store.peekSnapshot("notes", "limit-0"))
    }

    private func assertBlocked(
        _ engine: ReplicaEngine, _ reason: String, _ body: (ReplicaTransaction) throws -> Void,
        line: UInt = #line
    ) {
        XCTAssertThrowsError(try engine.writeAtomically(body), line: line) {
            XCTAssertEqual($0 as? ReplicaError, .atomicWriteBlocked(reason), line: line)
        }
    }

    func testRefusalRevertsTheWholeActionAndRetainsBothReasons() async throws {
        let store = try Fixture.store()
        let wire = StubTransport()
        await wire.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "action refused") } }
        let engine = Fixture.engine(store: store, transport: wire)
        try engine.writeAtomically { tx in
            try tx.create(TestNote(id: "a"))
            try tx.create(TestNote(id: "b"))
        }
        try await engine.drain()
        XCTAssertNil(try store.peekSnapshot("notes", "a"))
        XCTAssertNil(try store.peekSnapshot("notes", "b"))
        XCTAssertEqual(try store.parkedOps().map(\.parked), ["action refused", "action refused"])
        XCTAssertEqual(try store.recoveryRecords().map(\.reason), ["action refused", "action refused"])
    }

    func testLaterEditsCannotChangeFrozenGroupBytes() async throws {
        let store = try Fixture.store()
        let wire = StubTransport()
        let engine = Fixture.engine(store: store, transport: wire)
        try engine.writeAtomically { tx in
            try tx.create(TestNote(id: "a", title: "first"))
            try tx.create(TestNote(id: "b", title: "second"))
        }
        let frozen = try submissions(store)
        try await engine.updateRow(stream: "notes", id: "a", type: nil, data: ["title": .string("later")])
        _ = try await engine.deleteRow(stream: "notes", id: "b")
        XCTAssertEqual(try submissions(store).first?.content, frozen.first?.content)
        try await engine.drain()
        let delivered = await wire.pushedOps()
        XCTAssertEqual(delivered.map(\.verb), ["row.create", "row.create", "row.patch", "row.delete"])
        XCTAssertEqual(try store.peekSnapshot("notes", "a")?.data["title"], .string("later"))
        XCTAssertNil(try store.peekSnapshot("notes", "b"))
    }
}

private struct MemberGate: SyncGate {
    let discard: Bool
    let id = "member"
    let stream: String? = "notes"
    func judge(_ change: SyncChange) -> SyncVerdict {
        guard change.rowId == "blocked" else { return .push }
        return discard ? .discard : .gate("not uploaded")
    }
}
