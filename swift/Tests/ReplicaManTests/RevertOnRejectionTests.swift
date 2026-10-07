import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// The rejected-op revert path. A rejection is a VERDICT: the entry
/// parks — but the client write must not survive it, because the
/// server will never send a correcting frame (pull ships only changed
/// rows). Journal entries carry a client-local PRE-IMAGE; the verdict
/// transaction reverts atomically: create ⇒ delete the row, patch ⇒ restore
/// the pre-image fields, delete ⇒ restore the row. Document rejections are
/// rare by design (the server repairs rather than rejects) — they discard
/// the op and force a re-bootstrap instead.
final class RevertOnRejectionTests: XCTestCase {

    func testRejectedDeltaArchivesEditsAuthoredWhileItWasInFlight() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("S".utf8), data: [:])],
            cursor: "5:", more: false
        ))
        _ = try await engine.pullOnce()
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+1".utf8))
        await transport.onPush { _ in
            do {
                try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+2".utf8))
            } catch {
                XCTFail("edit during upload failed: \(error)")
            }
        }
        await rejectAll(transport)
        _ = try await engine.drain()

        XCTAssertTrue(try store.peekPending().isEmpty)
        XCTAssertEqual(try store.peekParked().count, 1)
        XCTAssertEqual(try store.peekDoc("boards", "b1")?.fold, Data("S".utf8))
        XCTAssertEqual(try store.recoveryRecords().count, 1)
        let archive = try XCTUnwrap(store.recoveryRecords().first)
        let fold = try XCTUnwrap(store.recoveryParts(id: archive.id).first { $0.kind == "document.fold" })
        let bytes = try store.recoveryChunk(id: archive.id, part: fold)
        XCTAssertEqual(bytes, Data("S+1+2".utf8))
    }

    private func rejectAll(_ transport: StubTransport) async {
        await transport.scriptPush { ops in
            ops.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "refused (stub)") }
        }
    }

    func testRejectedCreateDeletesTheClientWrittenRow() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])
        XCTAssertNotNil(try store.peekSnapshot("notes", "n1"))

        await rejectAll(transport)
        try await engine.drain()

        XCTAssertNil(try store.peekSnapshot("notes", "n1"), "a refused birth leaves no ghost row")
        XCTAssertEqual(try store.peekParked().count, 1, "the entry parks as evidence")
    }

    func testRejectedRowCreateDropsEveryDependentAddressEntry() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil,
            data: ["title": .string("birth")]
        )
        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil,
            data: ["title": .string("later patch")]
        )

        await rejectAll(transport)
        try await engine.drain()

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertTrue(try store.peekPending().isEmpty)
        let parked = try store.peekParked()
        XCTAssertEqual(parked.count, 1, "only the refused birth remains as evidence")
        let evidence = try XCTUnwrap(parked.first).op()
        XCTAssertEqual(evidence.verb, ReplicaOp.Verb.rowCreate)
    }

    func testRejectedPatchRestoresExactlyThePatchedFields() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        // Server truth arrives first.
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "server", rank: "a")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        // The local patch touches title AND adds a brand-new field.
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: [
            "title": .string("local"), "rank": .string("a"), "mood": .string("bold"),
        ])
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["mood"], .string("bold"))

        await rejectAll(transport)
        try await engine.drain()

        let reverted = try XCTUnwrap(store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(reverted.data["title"], .string("server"), "the patched field returns to its pre-image")
        XCTAssertEqual(reverted.data["rank"], .string("a"), "untouched fields stay")
        XCTAssertNil(reverted.data["mood"], "a field the patch INTRODUCED is removed, not nulled")
    }

    func testRejectedPatchLeavesInterimServerFieldsAlone() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "server", rank: "a")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("local")])

        // While the patch is in flight, the server replaces the row with a
        // fresher copy carrying a field the patch never touched.
        await transport.failPushes(true)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowSet(stream: "notes", id: "n1", type: nil, data: [
                "title": .string("interim server"), "rank": .string("z"), "starred": .bool(true),
            ], revision: 9)],
            cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        await transport.failPushes(false)

        await rejectAll(transport)
        try await engine.drain()

        let row = try XCTUnwrap(store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(row.data["title"], .string("interim server"), "the patched field reverts to the latest server value")
        XCTAssertEqual(row.data["rank"], .string("z"), "interim server fields survive the revert")
        XCTAssertEqual(row.data["starred"], .bool(true))
    }

    func testRejectedDeleteRestoresTheRowByteIdentical() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowSet(stream: "notes", id: "n1", type: "Note", data: ["title": .string("keep me")])],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        let before = try XCTUnwrap(store.peekSnapshot("notes", "n1"))

        try await engine.deleteRow(stream: "notes", id: "n1")
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))

        await rejectAll(transport)
        try await engine.drain()

        let restored = try XCTUnwrap(store.peekSnapshot("notes", "n1"), "a refused delete resurrects the row")
        XCTAssertEqual(restored, before, "byte-identical prior state — type and data alike")
    }

    /// A refused delete of a pulled document brings the row back WITH its
    /// document, from the base the pull left — never through a re-bootstrap.
    func testARefusedDeleteOfAPulledDocumentRestoresItsDocumentFromTheBase() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SNAP".utf8), data: ["name": .string("Board")])],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        try await engine.deleteRow(stream: "boards", id: "b1")
        XCTAssertNil(try store.peekDoc("boards", "b1"))

        await rejectAll(transport)
        try await engine.drain()

        XCTAssertEqual(try store.peekSnapshot("boards", "b1")?.data, ["name": .string("Board")])
        XCTAssertEqual(try store.peekDoc("boards", "b1")?.fold, Data("SNAP".utf8), "the document comes back from the base")
        let cursor = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(cursor, "5:", "no re-bootstrap: the published checkpoint stands")
        XCTAssertEqual(try store.peekParked().count, 1)
    }

    func testRejectedDocCreateRemovesTheWholeClientWrittenDoc() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7, data: ["name": .string("Plans")])
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+edit".utf8))

        await rejectAll(transport)
        try await engine.drain()

        XCTAssertNil(try store.peekSnapshot("boards", "b1"), "a refused doc birth leaves no projection row")
        XCTAssertNil(try store.peekDoc("boards", "b1"), "…and no fold")
        XCTAssertEqual(try store.peekPending().count, 0, "the doc's superseded delta dies with it")
    }

    func testRejectedDocDeltaRestoresBaseAndKeepsRecoverableRefusal() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        // A server-born doc plus an unrelated pending note — the note's op
        // must survive the reset (bootstrap replaces the WORLD, the journal
        // still owes its ops).
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SNAP".utf8), data: [:])],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+edit".utf8))
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("honest")])

        await transport.scriptPush { ops in
            ops.map { op in
                op.verb == ReplicaOp.Verb.docDelta
                    ? ReplicaVerdict(id: op.id, outcome: .rejected, reason: "delta refused")
                    : ReplicaVerdict(id: op.id, outcome: .accepted)
            }
        }
        try await engine.drain()

        XCTAssertEqual(try store.peekParked().count, 1)
        XCTAssertEqual(try store.peekDoc("boards", "b1")?.fold, Data("SNAP".utf8))
        XCTAssertEqual(try store.recoveryRecords().count, 1)
        let cursor = try await engine.currentCursor(shard: "user")
        XCTAssertEqual(cursor, "5:", "a refusal preserves the published checkpoint")
        XCTAssertEqual(
            try store.peekPending().count, 0,
            "the note create was accepted in the same drain; nothing else was thrown away"
        )
    }

    /// The classification the whole offline story rests on: a severed wire is
    /// RETRYABLE, never a verdict. A transport failure that parked would both
    /// strand the op forever (parked entries are never retried) and trip the
    /// rejection revert — undoing work the user can see, because the network blinked.
    func testTransportFailureNeitherParksNorReverts() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])
        try await engine.deleteRow(stream: "notes", id: "n2")
        await transport.failPushes(true)

        do {
            try await engine.drain()
            XCTFail("a dead wire surfaces from the drain")
        } catch {}

        XCTAssertEqual(try store.peekParked().count, 0, "no connection is not a refusal")
        XCTAssertEqual(try store.peekPending().count, 1, "the real write stays owed; deleting an absent row is a no-op")
        XCTAssertNotNil(
            try store.peekSnapshot("notes", "n1"),
            "the client row must survive: only a VERDICT reverts"
        )
        let reverted = await engine.revertedCount
        XCTAssertEqual(reverted, 0)
    }

    func testRejectionHookFiresWithTheVerdictReason() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let heard = Heard()
        await engine.setRejectionHandler { op, reason in
            Task { await heard.record("\(op.verb):\(op.rowId):\(reason)") }
        }

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])
        await rejectAll(transport)
        try await engine.drain()

        await eventually(timeout: 2, "the rejection seam never fired") {
            await heard.entries == ["row.create:n1:refused (stub)"]
        }
        let reverted = await engine.revertedCount
        XCTAssertEqual(reverted, 1, "the debug surface can read how many writes were undone")
    }

    actor Heard {
        private(set) var entries: [String] = []
        func record(_ entry: String) { entries.append(entry) }
    }
}
