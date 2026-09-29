import Foundation
import XCTest
@testable import ReplicaMan

/// Generated CRUD verbs stop at the database boundary. Delivery is engine
/// behavior: an app caller must never need to ring a second transport bell
/// after `save`, `create`, `delete`, or a document edit.
final class WriteSchedulingTests: XCTestCase {
    func testSaveSchedulesItsOwnPush() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: Fixture.schema(),
            codecs: [StubCodec()]
        )

        try await engine.saveRow(
            stream: "notes",
            id: "n1",
            type: nil,
            data: ["title": .string("ordinary CRUD")]
        )

        try await eventually("save() returned but the engine never settled its write") {
            let attempts = await transport.pushCount
            return try attempts == 1 && store.peekPending().isEmpty
        }
    }

    func testCreateStampsTheBoundOwnerAndClockInsideTheEngine() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let instant = Date(timeIntervalSince1970: 1_700_000_000)
        let engine = ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: Fixture.schema(boardStamp: .standard),
            codecs: [StubCodec()],
            clock: { instant },
            automaticallyPushWrites: false
        )

        try await engine.createDoc(
            stream: "boards",
            id: "b1",
            seed: Data("SEED".utf8),
            peer: 7,
            data: ["name": .string("Stamped")]
        )

        let row = try XCTUnwrap(store.peekSnapshot("boards", "b1"))
        XCTAssertEqual(row.data["name"], .string("Stamped"))
        XCTAssertEqual(row.data["userId"], .number(42), "the bound owner is the only owner a create can carry")
        XCTAssertEqual(row.data["createdAt"], .string("2023-11-14T22:13:20Z"))
        XCTAssertEqual(row.data["updatedAt"], .string("2023-11-14T22:13:20Z"))
    }

    func testDocumentCreateEditAndDeleteEachScheduleDelivery() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: Fixture.schema(),
            codecs: [StubCodec()]
        )

        try await engine.createDoc(
            stream: "boards", id: "b1", seed: Data("S".utf8), peer: 7
        )
        await eventually { await transport.pushCount == 1 }

        try await engine.recordDocDelta(
            stream: "boards", id: "b1", payload: Data("D".utf8)
        )
        await eventually { await transport.pushCount == 2 }

        try await engine.deleteRow(stream: "boards", id: "b1")
        await eventually { await transport.pushCount == 3 }

        let verbs = await transport.pushedBatches.flatMap { $0.map(\.verb) }
        XCTAssertEqual(
            verbs,
            [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.docDelta, ReplicaOp.Verb.rowDelete]
        )
    }

    func testDeleteDuringAnInFlightCreateStillSendsTheDelete() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await transport.delayPushes(nanos: 250_000_000)
        let engine = ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: Fixture.schema(),
            codecs: [StubCodec()]
        )

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil,
            data: ["title": .string("short lived")]
        )
        await eventually { await transport.pushCount == 1 }

        try await engine.deleteRow(stream: "notes", id: "n1")
        await eventually("the in-flight create reached the server without its delete") {
            await transport.pushCount == 2
        }

        let verbs = await transport.pushedBatches.flatMap { $0.map(\.verb) }
        XCTAssertEqual(verbs, [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.rowDelete])
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
    }

    func testRepeatedDocumentCreatePreservesTheFirstBirthAtomically() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        let first = try await engine.createDoc(
            stream: "boards", id: "b1", seed: Data("FIRST".utf8), peer: 7,
            data: ["name": .string("First")]
        )
        let replay = try await engine.createDoc(
            stream: "boards", id: "b1", seed: Data("SECOND".utf8), peer: 8,
            data: ["name": .string("Second")]
        )

        XCTAssertTrue(first)
        XCTAssertFalse(replay)
        XCTAssertEqual(try engine.docFold(stream: "boards", id: "b1"), Data("FIRST".utf8))
        XCTAssertEqual(try store.peekSnapshot("boards", "b1")?.data["name"], .string("First"))
        XCTAssertEqual(try store.peekPending().count, 1)
    }
    func testAutomaticDeliveryHonorsTheColdWindow() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await transport.failPushes(true)
        let engine = ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: Fixture.schema(),
            codecs: [StubCodec()],
            coldWindow: 60
        )

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil,
            data: ["title": .string("first")]
        )
        await eventually { await transport.pushCount == 1 }

        // The STAMP is the mechanism, so assert the stamp rather than waiting
        // out a window: a cold lane refuses the second write's scheduled push
        // at `pushScheduledWrites`' loop condition (`ReplicaEngine.swift:1153`).
        let cold = await engine.isColdForTesting(.bulk)
        XCTAssertTrue(cold, "the failed push must cool the lane it failed on")

        try await engine.saveRow(
            stream: "notes", id: "n2", type: nil,
            data: ["title": .string("second")]
        )
        try await Task.sleep(nanoseconds: 200_000_000)
        let pushCount = await transport.pushCount
        XCTAssertEqual(pushCount, 1, "a write onto a known-cold lane must not re-attempt the wire")
        XCTAssertEqual(try store.peekPending().count, 2)
    }
    func testRejectedInFlightRowBirthDropsItsQueuedDelete() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await transport.delayPushes(nanos: 250_000_000)
        await transport.scriptPush { ops in
            ops.map {
                ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "refused")
            }
        }
        let engine = ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: Fixture.schema(),
            codecs: [StubCodec()]
        )

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil,
            data: ["title": .string("refused birth")]
        )
        await eventually { await transport.pushCount == 1 }
        try await engine.deleteRow(stream: "notes", id: "n1")

        // The park IS the settle point: the verdict transaction that parks the
        // birth is the same one that discards its dependent delete, so once the
        // park is visible there is nothing left in flight to wait for.
        try await eventually("the rejected birth never settled") {
            try store.peekParked().count == 1
        }

        let pushCount = await transport.pushCount
        XCTAssertEqual(pushCount, 1, "the dependent delete must not reach the wire")
        XCTAssertTrue(try store.peekPending().isEmpty)
        let evidence = try XCTUnwrap(store.peekParked().first?.op())
        XCTAssertEqual(evidence.verb, ReplicaOp.Verb.rowCreate)
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
    }
}
