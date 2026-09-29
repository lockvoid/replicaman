import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// A held row leaves when its gate lets it — on the gate's signal, on the
/// row's next write, or when the store opens — as its STATE, never as the
/// history of its writes, and never ahead of a held row it names.
final class GateReleaseTests: XCTestCase {

    private func held(_ engine: ReplicaEngine) throws -> [String] {
        try engine.heldRows().map(\.rowId)
    }

    /// The engine hears a gate's signal on its own task: wait until the holds
    /// it asked show the answer.
    private func settled(
        _ engine: ReplicaEngine, holding expected: [String], file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        try await eventually("holds never became \(expected)", file: file, line: line) {
            try engine.heldRows().map(\.rowId) == expected
        }
    }

    /// A gate on one field, released per key by its own ledger.
    private func fieldGate(_ id: String, _ field: String, _ ledger: ReleaseLedger) -> TestGate {
        TestGate(id: id, signal: ledger.signal) { change in
            guard let key = change.local[field]?.string, !ledger.contains(key) else { return .push }
            return .gate("\(field) \(key) in flight")
        }
    }

    // MARK: - the gate's signal

    /// KILL: `release` — walk the holds newest first.
    func testAGateSignalReleasesOnlyItsOwnRowsInTheOrderTheyWereHeld() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let a = ReleaseLedger()
        let b = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [
            fieldGate("a", "a", a), fieldGate("b", "b", b),
        ])
        try await engine.saveRow(stream: "notes", id: "x1", type: nil, data: ["a": .string("ka")])
        try await engine.saveRow(stream: "notes", id: "y1", type: nil, data: ["b": .string("kb")])
        try await engine.saveRow(stream: "notes", id: "x2", type: nil, data: ["a": .string("ka")])

        a.land("ka")
        try await settled(engine, holding: ["y1"])
        _ = try await engine.drain()

        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.map(\.rowId), ["x1", "x2"])
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.rowCreate])
    }

    // MARK: - what leaves

    /// KILL: `admit` — `let knows = true`; the birth then leaves as a patch.
    func testARowTheServerNeverSawLeavesAsOneCreateOfItsLastState() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a"), "blob": .string("k1")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["rank": .string("r")])

        released.land("k1")
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate])
        XCTAssertEqual(sent.first?.data, ["title": .string("b"), "blob": .string("k1"), "rank": .string("r")])
    }

    /// KILL: `journalRelease` — release every row as a create; the server
    /// already has this one.
    func testARowTheServerKnowsLeavesAsOnePatchOfEveryField() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        _ = try await engine.drain()
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        XCTAssertEqual(try held(engine), ["n1"])

        released.land("k1")
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.rowPatch])
        XCTAssertEqual(sent.last?.data, ["title": .string("b"), "blob": .string("k1")])
    }

    /// A pull answering while a row waits must not undo what the device
    /// wrote: the held row keeps the fields a device writes, takes the fields
    /// only the server writes, and leaves as that state — otherwise the ref
    /// vanishes under a pull and the bytes never bind.
    ///
    /// KILL: `apply(.rowSet)` — drop the held-row branch.
    func testAPullLeavesAHeldRowsDeviceFieldsAlone() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "notes", lane: .row, shard: "user", pushed: ["title", "blob"]),
        ])
        let engine = Fixture.engine(store: store, transport: transport, schema: schema, syncGates: [blobGate(released)])
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
            .rowSet(stream: "notes", id: "n1", type: nil, data: ["title": .string("a"), "url": .string("u1")]),
        ], cursor: "5:", more: false))
        try await engine.pullOnce()
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])

        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
            .rowSet(stream: "notes", id: "n1", type: nil, data: ["title": .string("a"), "url": .string("u2")]),
        ], cursor: "6:", more: false))
        try await engine.pullOnce()

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data,
                       ["title": .string("a"), "blob": .string("k1"), "url": .string("u2")])

        released.land("k1")
        try await settled(engine, holding: [])
        _ = try await engine.drain()
        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.last?.data, ["title": .string("a"), "blob": .string("k1")])
    }

    func testARowDeletedWhileHeldLeavesAsADeleteWhenTheServerKnowsIt() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: true)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        _ = try await engine.drain()

        backup.set(false)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        _ = try await engine.deleteRow(stream: "notes", id: "n1")
        XCTAssertEqual(try held(engine), ["n1"], "a delete of a held row stays with its hold")
        XCTAssertEqual(try store.peekPending().count, 0)

        backup.set(true)
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let verbs = await transport.pushedOps().map(\.verb)
        XCTAssertEqual(verbs, [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.rowDelete])
    }

    /// The server refuses a column it does not take from devices ("unknown
    /// column", `stream.rb#decode`): a released row carries only what the
    /// device may send, whatever else its state holds — a pulled address, a
    /// server-owned stamp.
    ///
    /// KILL: `journalRelease` — send `current` unfiltered.
    func testAReleasedRowCarriesOnlyWhatTheDeviceMaySend() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "notes", lane: .row, shard: "user", pushed: ["title", "blob"]),
        ])
        let engine = Fixture.engine(store: store, transport: transport, schema: schema, syncGates: [blobGate(released)])
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
            .rowSet(stream: "notes", id: "n1", type: nil, data: ["title": .string("a"), "url": .string("https://signed")]),
        ], cursor: "5:", more: false))
        try await engine.pullOnce()
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])

        released.land("k1")
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowPatch])
        XCTAssertEqual(sent.first?.data, ["title": .string("a"), "blob": .string("k1")])
    }

    /// KILL: `askAgain` — drop the `current != nil || held.serverKnows` guard;
    /// the hold then outlives its row.
    func testARowDeletedBeforeTheServerHeardOfItLeavesNothing() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: false)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])

        _ = try await engine.deleteRow(stream: "notes", id: "n1")
        XCTAssertEqual(try held(engine), [], "a hold outlived a row the server never heard of")

        backup.set(true)
        _ = try await engine.drain()
        let pushes = await transport.pushCount
        XCTAssertEqual(pushes, 0)
    }

    /// KILL: `journalRelease` — `codec.diff(fold: doc.fold, since: nil)`; the
    /// delta then re-sends the birth the server already acked.
    func testADocumentTheServerKnowsLeavesAsOneDeltaPastWhatItAcked() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: true)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate])
        try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
        _ = try await engine.drain()

        backup.set(false)
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+a".utf8))
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+b".utf8))
        XCTAssertEqual(try held(engine), ["b1"])

        backup.set(true)
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.docDelta])
        XCTAssertEqual(sent.last?.rowId, "b1")
        XCTAssertEqual(sent.last?.payload, Data("+a+b".utf8))
    }

    func testADocumentBornHeldLeavesAsOneCreateOfItsFold() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: false)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate])
        try await engine.createDoc(stream: "boards", id: "b1", seed: Data("SEED".utf8), peer: 7)
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+x".utf8))

        backup.set(true)
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate])
        XCTAssertEqual(sent.first?.seed, Data("SEED+x".utf8))
        XCTAssertEqual(sent.first?.codec, "stub@1")
        XCTAssertEqual(try store.peekDoc("boards", "b1")?.acked, Data("6".utf8), "the server acked the whole fold")
    }

    /// A released birth the server refuses reverts like any other.
    ///
    /// KILL: `journalRelease` — journal the release with a nil preimage.
    func testAReleasedBirthTheServerRefusesLeavesTheDevice() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])
        released.land("k1")
        try await settled(engine, holding: [])

        await transport.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "no") } }
        _ = try await engine.drain()

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try store.peekParked().count, 1)
    }

    func testRejectedHeldPatchRestoresBaselineAndRemovesIntroducedFields() async throws {
        for pullWhileHeld in [false, true] {
            let store = try Fixture.store()
            let transport = StubTransport()
            let released = ReleaseLedger()
            let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
            try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("original")])
            _ = try await engine.drain()
            try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("held"), "blob": .string("key")])
            try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("latest")])
            if pullWhileHeld {
                await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
                    .rowSet(stream: "notes", id: "n", type: nil, data: ["title": .string("remote")]),
                ], cursor: "9:", more: false))
                try await engine.pullOnce()
            }
            await transport.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "denied") } }
            released.land("key")
            try await settled(engine, holding: [])
            _ = try await engine.drain()
            XCTAssertEqual(try store.peekSnapshot("notes", "n")?.data, ["title": .string(pullWhileHeld ? "remote" : "original")])
            XCTAssertEqual(try store.peekParked().count, 1)
        }
    }

    func testRejectedConvertedHoldRestoresBeforeTheEntireQueuedSuffix() async throws {
        let store = try Fixture.store()
        let wire = StubTransport()
        let backup = BackupFlag(on: true)
        let engine = Fixture.engine(store: store, transport: wire, syncGates: [backup.gate])
        try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("original")])
        _ = try await engine.drain()
        try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("one"), "extra": .string("new")])
        try await engine.saveRow(stream: "notes", id: "n", type: nil, data: ["title": .string("two")])
        backup.set(false)
        try await settled(engine, holding: ["n"])
        await wire.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "denied") } }
        backup.set(true)
        try await settled(engine, holding: [])
        _ = try await engine.drain()
        XCTAssertEqual(try store.peekSnapshot("notes", "n")?.data, ["title": .string("original")])
    }

    // MARK: - a write asks again

    /// Bytes landed without a signal reaching the engine; the row's next
    /// write asks its gates and it leaves — as its state with that write.
    ///
    /// KILL: `admit` — return false for a held row without `rejudge`.
    func testAWriteToAHeldRowAsksItAgain() async throws {
        let store = try Fixture.store()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])
        released.landQuietly("k1")

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])

        XCTAssertEqual(try held(engine), [])
        let owed = try store.peekPending().map { try $0.op() }
        XCTAssertEqual(owed.map(\.verb), [ReplicaOp.Verb.rowCreate])
        XCTAssertEqual(owed.first?.data, ["blob": .string("k1"), "title": .string("b")])
    }

    // MARK: - parent before child

    /// A brand kit waits for its logo; a voice added meanwhile names it, and
    /// the server refuses a voice whose kit it does not know.
    ///
    /// KILL: `admit` — drop the `heldParent` branch; the voice then leaves
    /// first.
    func testAWriteNamingAHeldRowWaitsBehindItAndLeavesAfterIt() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "kit", type: nil, data: ["blob": .string("logo")])
        try await engine.saveRow(stream: "notes", id: "voice", type: nil, data: ["kit": .string("kit")])

        let holds = try engine.heldRows()
        XCTAssertEqual(holds.map(\.rowId), ["kit", "voice"])
        XCTAssertEqual(holds.last?.gateId, "row:notes/kit")

        released.land("logo")
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let sent = await transport.pushedOps().map(\.rowId)
        XCTAssertEqual(sent, ["kit", "voice"])
    }

    /// Once its parent leaves, a waiting row answers to its own gates.
    func testARowWaitingBehindAParentIsJudgedByItsOwnGatesWhenTheParentLeaves() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "kit", type: nil, data: ["blob": .string("logo")])
        try await engine.saveRow(stream: "notes", id: "voice", type: nil,
                                 data: ["kit": .string("kit"), "blob": .string("sample")])

        released.land("logo")
        try await settled(engine, holding: ["voice"])
        XCTAssertEqual(try engine.heldRows().first?.gateId, "blob")

        released.land("sample")
        try await settled(engine, holding: [])
        _ = try await engine.drain()
        let sent = await transport.pushedOps().map(\.rowId)
        XCTAssertEqual(sent, ["kit", "voice"])
    }

    /// The parent's create was still owed when backup turned off, while the
    /// child already waited for its bytes: the rows the flip holds go ahead
    /// of every earlier hold, so the child still leaves second.
    ///
    /// KILL: `holdJournal` — insert the moved rows with the next `seq`
    /// (`seq: nil`); the child then leaves ahead of the kit it names.
    func testARowHeldBeforeTheFlipWaitsBehindAParentTheFlipHolds() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let backup = BackupFlag(on: true)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate, blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "kit", type: nil, data: ["title": .string("kit")])
        try await engine.saveRow(stream: "notes", id: "voice", type: nil,
                                 data: ["kit": .string("kit"), "blob": .string("sample")])
        XCTAssertEqual(try held(engine), ["voice"])

        backup.set(false)
        try await settled(engine, holding: ["kit", "voice"])
        released.land("sample")
        try await eventually("the child's landing never reached its hold") {
            try engine.heldRows().last?.gateId == "backup"
        }

        backup.set(true)
        try await settled(engine, holding: [])
        _ = try await engine.drain()
        let sent = await transport.pushedOps().map(\.rowId)
        XCTAssertEqual(sent, ["kit", "voice"])
    }

    /// Cloud Backup off holds the project, and lets the digest that names it
    /// leave — the server works from it, under the user alone. A gate over
    /// every stream is a policy: what it lets go does not wait for what it
    /// holds.
    ///
    /// KILL: `heldParent` — drop the `syncGates.orders(parent.gateId)` filter.
    func testARowAPolicyGateLetsGoDoesNotWaitBehindWhatItHolds() async throws {
        let store = try Fixture.store()
        let backup = BackupFlag(on: false)
        let policy = TestGate(nil, id: "backup", signal: backup.signal) { change in
            backup.isOn || change.stream == "assets" ? .push : .gate("cloud backup off")
        }
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [policy])
        try await engine.saveRow(stream: "notes", id: "project", type: nil, data: ["title": .string("p")])

        try await engine.saveRow(stream: "assets", id: "digest", type: nil, data: ["projectId": .string("project")])

        XCTAssertEqual(try held(engine), ["project"])
        XCTAssertEqual(try store.peekPending().map { try $0.op().rowId }, ["digest"])
    }

    /// A row the server never saw is born only once: asked again, a gate's
    /// discard is refused like at the write.
    ///
    /// KILL: `askAgain` — drop the birth's discard → push mapping.
    func testABirthItsGatesDiscardWhenAskedAgainStillLeaves() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: false)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [
            TestGate(nil, id: "backup", signal: backup.signal) { _ in backup.isOn ? .discard : .gate("cloud backup off") },
        ])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])

        backup.set(true)
        try await settled(engine, holding: [])
        _ = try await engine.drain()

        let verbs = await transport.pushedOps().map(\.verb)
        XCTAssertEqual(verbs, [ReplicaOp.Verb.rowCreate])
    }

    // MARK: - Cloud Backup off

    /// Turning backup off takes what was still owed off the
    /// journal; turning it on sends each row once, in the order it was owed.
    func testTurningBackupOffMovesTheOwedJournalIntoHolds() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: true)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("c")])

        backup.set(false)
        try await settled(engine, holding: ["n1", "n2"])
        XCTAssertEqual(try store.peekPending().count, 0)

        backup.set(true)
        try await settled(engine, holding: [])
        _ = try await engine.drain()
        let sent = await transport.pushedOps()
        XCTAssertEqual(sent.map(\.rowId), ["n1", "n2"])
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.rowCreate])
        XCTAssertEqual(sent.first?.data, ["title": .string("b")])
    }

    /// An entry already on the wire may be committed server-side: it stays.
    ///
    /// KILL: `owed(_:stream:)` — read `state IN ('owed', 'frozen')`; the frozen
    /// entry then moves into a hold while its answer is on the way.
    func testAnEntryOnTheWireStaysWhenBackupTurnsOff() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: true)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("on the wire")])
        let failure = HookOutcome()
        await transport.onPush { _ in
            do {
                try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("owed")])
                backup.set(false)
                try await until("the flip never took the owed entry") { try engine.heldRows().map(\.rowId) == ["n2"] }
            } catch {
                await failure.record(error)
            }
        }

        _ = try await engine.drain()

        let hookFailure = await failure.failure
        XCTAssertNil(hookFailure)
        let sent = await transport.pushedOps().map(\.rowId)
        XCTAssertEqual(sent, ["n1"])
        XCTAssertEqual(try store.peekPending().count, 0, "the entry on the wire was acked, the owed one moved")
        XCTAssertEqual(try held(engine), ["n2"])
    }

    // MARK: - open

    /// Bytes that landed while the process was gone: the open asks again.
    ///
    /// KILL: `open` — drop `askHoldsAgain()`; `gateChanged` — return early when `id` is nil.
    func testOpenAsksEveryHoldAgain() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])
        released.landQuietly("k1")

        try await engine.open(owner: Fixture.owner)

        try await settled(engine, holding: [])
        _ = try await engine.drain()
        let sent = await transport.pushedOps().map(\.rowId)
        XCTAssertEqual(sent, ["n1"])
    }

    /// A gate that fires while the engine is sealed — an identity transition
    /// — is not lost: unsealing asks the holds again.
    ///
    /// KILL: `unseal` — drop `askHoldsAgain()`.
    func testASignalWhileSealedIsHeardOnUnseal() async throws {
        let store = try Fixture.store()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])
        await engine.seal()
        released.land("k1")
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(try held(engine), ["n1"], "a sealed engine let a row go")

        await engine.unseal()

        try await settled(engine, holding: [])
    }

    // MARK: - drafts

    /// A draft's writes meet the gates when it commits.
    func testADraftsHeldRowsMoveIntoHoldsWhenItCommits() async throws {
        let store = try Fixture.store()
        let backup = BackupFlag(on: false)
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [backup.gate])
        let draft = try await engine.beginDraft {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("b")])
        }
        XCTAssertEqual(try store.peekDrafted().count, 2)
        XCTAssertEqual(try held(engine), [], "a draft was judged before it committed")

        try await engine.commitDraft(draft)

        XCTAssertEqual(try held(engine), ["n1"])
        XCTAssertEqual(try store.peekDrafted().count, 0)
        XCTAssertEqual(try store.peekPending().count, 0)
    }

    func testADiscardedDraftLeavesNoHold() async throws {
        let store = try Fixture.store()
        let backup = BackupFlag(on: false)
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [backup.gate])
        let draft = try await engine.beginDraft {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        }

        try await engine.discardDraft(draft)

        XCTAssertEqual(try held(engine), [])
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
    }

    // MARK: - who else reads "owed"

    /// Backup off: the server's world never had these rows, and a reset
    /// must not erase the only copy.
    ///
    /// KILL: `publishRound` — on a reset, re-materialize every snapshot of the
    /// shard, not only its base rows; the held birth is then removed.
    func testAResetKeepsHeldRows() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let backup = BackupFlag(on: false)
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [backup.gate])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("only copy")])

        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [], cursor: "10:", more: false))
        try await engine.pullOnce()

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("only copy"))
        XCTAssertEqual(try held(engine), ["n1"])
    }

    /// The staged-bytes keeping set: a held row is still the device's to send.
    func testPendingRowIdsIncludeHeldRows() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [blobGate(ReleaseLedger())])
        try await engine.saveRow(stream: "notes", id: "held", type: nil, data: ["blob": .string("k1")])
        try await engine.saveRow(stream: "notes", id: "owed", type: nil, data: ["title": .string("a")])

        let owed = try await engine.pendingRowIds(stream: "notes")
        XCTAssertEqual(Set(owed), ["held", "owed"])
    }

    /// A project thrown away before the server heard of it must not come back
    /// when its gate opens.
    func testDiscardedHoldsNeverLeave() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let released = ReleaseLedger()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(released)])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])

        try await engine.discardHolds(stream: "notes", rowIds: ["n1"])
        released.land("k1")
        try await engine.settle()
        _ = try await engine.drain()

        XCTAssertEqual(try held(engine), [])
        let pushes = await transport.pushCount
        XCTAssertEqual(pushes, 0)
    }

    /// A row waiting behind a discarded row could only be refused: it goes
    /// with it.
    ///
    /// KILL: `discardHolds` — drop only the named rows.
    func testDiscardingAHeldRowDiscardsTheRowsWaitingBehindIt() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport(), syncGates: [blobGate(ReleaseLedger())])
        try await engine.saveRow(stream: "notes", id: "kit", type: nil, data: ["blob": .string("logo")])
        try await engine.saveRow(stream: "notes", id: "voice", type: nil, data: ["kit": .string("kit")])
        try await engine.saveRow(stream: "notes", id: "take", type: nil, data: ["voice": .string("voice")])
        XCTAssertEqual(try held(engine), ["kit", "voice", "take"])

        try await engine.discardHolds(stream: "notes", rowIds: ["kit"])

        XCTAssertEqual(try held(engine), [])
    }

    /// A refused birth takes its row off the device — and the hold its later
    /// writes waited in.
    ///
    /// KILL: `revert` — drop the `dropHold` after `discardEntries`.
    func testARefusedBirthTakesItsHoldWithIt() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, syncGates: [blobGate(ReleaseLedger())])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("a")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["blob": .string("k1")])
        XCTAssertEqual(try held(engine), ["n1"])

        await transport.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "no") } }
        _ = try await engine.drain()

        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        XCTAssertEqual(try held(engine), [])
        XCTAssertEqual(try store.peekParked().count, 1)
    }
}
