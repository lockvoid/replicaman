import ReplicaManTestProtocol
import Foundation
import GRDB
import Loro
@testable import ReplicaManLoro
import XCTest
@testable import ReplicaMan

/// The document's own doors, the rows' law over the real codec: `findDoc`
/// is a SYNC read of a state the engine holds (warm = no decode), `updateDoc`
/// moves the held document and journals ONE delta, a pulled delta reaches
/// the held document without a reopen, `watchDoc` delivers both movements
/// through one door, `undoDoc` inverts this peer's last edit.
final class DocumentStateTests: XCTestCase {

    override func setUp() {
        super.setUp()
        BoardState.decodes.reset()
    }

    private func world() throws -> (store: ReplicaStateStore, transport: LoroStubTransport, engine: ReplicaEngine, boards: DocumentStream<Board>) {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)
        return (store, transport, engine, DocumentStream<Board>(engine: engine))
    }

    private func seed(name: String, peer: UInt64 = 7) throws -> Data {
        let author = try LoroFixture.doc(peer: peer)
        try LoroFixture.setMeta(author, "name", name)
        return try author.export(mode: .snapshot)
    }

    func testTheDocumentWatchClearsItsStateWhenTheOwnerCloses() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Private"), peer: 7)
        let seen = DeliveredNames()
        let watcher = w.boards.watchDoc("b1", includeInitial: true) { seen.append($0?.name) }
        defer { watcher.cancel() }
        try await eventually { seen.names == ["Private"] }

        try await w.engine.close()

        try await eventually { seen.names == ["Private", nil] }
    }

    func testEscapedReadHandleCannotAuthorIntoTheHeldDocument() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Saved"), peer: 100)
        let state = try w.boards.findDoc("b1")
        let escaped = try XCTUnwrap(w.boards.readDoc("b1") { $0 })
        try LoroFixture.setMeta(escaped.doc, "name", "Unjournaled")
        XCTAssertEqual(try w.boards.findDoc("b1"), state)
        _ = try await w.boards.updateDoc("b1") { try LoroFixture.setMeta($0.doc, "color", "blue") }
        let stored = try XCTUnwrap(w.store.peekDoc("boards", "b1"))
        XCTAssertEqual(try LoroFixture.meta(fold: stored.fold, "name"), "Saved")
        XCTAssertEqual(try LoroFixture.meta(fold: stored.fold, "color"), "blue")
    }

    func testEscapedEditHandleCannotEnterALaterSavedEdit() async throws {
        // Captured only in the awaited edit; read afterwards on this test task.
        final class Capture: @unchecked Sendable { var document: LoroDocument? }
        let capture = Capture()
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Saved"), peer: 100)
        _ = try await w.boards.updateDoc("b1") {
            capture.document = $0
            try LoroFixture.setMeta($0.doc, "color", "Legitimate")
        }
        let saved = try XCTUnwrap(w.store.peekDoc("boards", "b1"))
        try LoroFixture.setMeta(XCTUnwrap(capture.document).doc, "name", "Unjournaled")
        do {
            _ = try await w.boards.updateDoc("b1") { try LoroFixture.setMeta($0.doc, "color", "Next") }
            XCTFail("Unscoped changes entered a saved edit")
        } catch ReplicaError.codec {
            // The escaped handle invalidates the cache, not the durable fold.
        }
        XCTAssertEqual(try w.store.peekDoc("boards", "b1")?.fold, saved.fold)
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Saved")
    }

    func testReadMutationThenThrowLeavesTheWarmStateUntouched() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Saved"), peer: 100)
        let state = try w.boards.findDoc("b1")
        struct Failure: Error {}
        XCTAssertThrowsError(try w.boards.readDoc("b1") {
            try LoroFixture.setMeta($0.doc, "name", "Unjournaled")
            throw Failure()
        })
        XCTAssertEqual(try w.boards.findDoc("b1"), state)
        XCTAssertEqual(try LoroFixture.meta(fold: try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold, "name"), "Saved")
    }

    func testFindDocReadsTheSeedSynchronously() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 7)

        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Plans")
        XCTAssertNil(try w.boards.findDoc("nope"), "no document, no state")
    }

    /// The warm law: a second read of an unmoved document decodes nothing —
    /// the state is the engine's, minted once per version.
    ///
    /// KILL: mint the state from the fold on every read (no held document)
    /// and the counter climbs with every call.
    func testAWarmReadDecodesNothing() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 7)
        _ = try w.boards.findDoc("b1")
        let after = BoardState.decodes.count

        _ = try w.boards.findDoc("b1")
        _ = try w.boards.findDoc("b1")
        XCTAssertEqual(BoardState.decodes.count, after, "an unmoved document was decoded again")
    }

    func testUpdateDocMovesTheStateAndJournalsOneDelta() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 7)

        let moved = try await w.boards.updateDoc("b1") { document in
            try LoroFixture.setMeta(document.doc, "name", "Renamed")
        }
        XCTAssertTrue(moved)
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Renamed")
        let deltas = try w.store.peekPending().filter { (try? $0.op().verb) == ReplicaOp.Verb.docDelta }
        XCTAssertEqual(deltas.count, 1, "one pending delta per document, superseded")
        XCTAssertEqual(try LoroFixture.meta(fold: try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold, "name"), "Renamed", "the fold moved with the document")

        let still = try await w.boards.updateDoc("b1") { _ in }
        XCTAssertFalse(still, "a body that moved nothing owes nothing")
    }

    func testASealedEditLeavesBothTheHeldDocumentAndItsFoldUntouched() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 100)
        let before = try XCTUnwrap(w.boards.findDoc("b1"))
        let fold = try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold
        let pending = try w.store.peekPending().map(\.id)
        await w.engine.seal()

        do {
            _ = try await w.boards.updateDoc("b1") { document in
                try LoroFixture.setMeta(document.doc, "name", "Refused")
            }
            XCTFail("a sealed engine accepted a document edit")
        } catch ReplicaError.identityTransitionInProgress {
        }

        let after = try XCTUnwrap(w.boards.findDoc("b1"))
        XCTAssertEqual(after.name, before.name)
        XCTAssertEqual(after.version, before.version)
        XCTAssertEqual(after.canUndo, before.canUndo)
        XCTAssertEqual(try w.store.peekDoc("boards", "b1")?.fold, fold)
        XCTAssertEqual(try w.store.peekPending().map(\.id), pending)
        await w.engine.unseal()
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Plans")
    }

    func testAFailedDiskWriteDropsTheUncommittedHeldEdit() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 7)
        _ = try w.boards.findDoc("b1")
        try await w.store.pool.write { db in
            try db.execute(sql: """
                CREATE TRIGGER refuse_fold BEFORE UPDATE OF fold ON docs
                BEGIN SELECT RAISE(ABORT, 'disk write failed'); END
                """)
        }
        do {
            _ = try await w.boards.updateDoc("b1") { document in
                try LoroFixture.setMeta(document.doc, "name", "Unsaved")
            }
            XCTFail("the disk failure was ignored")
        } catch {
            XCTAssertTrue(error is DatabaseError)
        }
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Plans")
        XCTAssertEqual(try LoroFixture.meta(fold: try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold, "name"), "Plans")
        try await w.store.pool.write { db in try db.execute(sql: "DROP TRIGGER refuse_fold") }
        _ = try await w.boards.updateDoc("b1") { document in
            try LoroFixture.setMeta(document.doc, "color", "blue")
        }
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Plans", "an unrelated edit must not commit the refused rename")
        XCTAssertEqual(try LoroFixture.meta(fold: try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold, "name"), "Plans")
    }

    func testRefusedRegistryEntryRollsBackTheWholeEditAndCannotLeakIntoTheNextSave() async throws {
        let w = try world()
        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Before")
        try author.getMap(id: "items").insert(key: "broken", v: "peer value")
        let seed = try author.export(mode: .snapshot)
        try await w.boards.createDoc(id: "b1", seed: seed, peer: 100)
        try await w.engine.drain()
        let before = try XCTUnwrap(w.boards.findDoc("b1"))
        let fold = try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold

        do {
            _ = try await w.boards.updateDoc("b1") { document in
                try document.writeFields("meta", ["name": .string("Unsaved")], base: nil)
                try document.writeRegistry("items", ["broken": ["title": .string("Lost")]], base: nil)
            }
            XCTFail("a refused registry entry must not report a successful save")
        } catch LoroDocumentError.notAMap { }

        XCTAssertEqual(try w.boards.findDoc("b1")?.name, before.name)
        XCTAssertEqual(try w.boards.findDoc("b1")?.version, before.version)
        XCTAssertEqual(try w.store.peekDoc("boards", "b1")?.fold, fold)
        XCTAssertTrue(try w.store.peekPending().isEmpty)
        _ = try await w.boards.updateDoc("b1") { document in
            try document.writeFields("meta", ["color": .string("blue")], base: nil)
        }
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Before")
        let saved = try LoroReplicaCodec().open(fold: XCTUnwrap(w.store.peekDoc("boards", "b1")).fold, peer: 200)
        XCTAssertEqual(saved.value["items"]?["broken"], .string("peer value"))
    }

    /// A peer's delta pulled from the server lands in the held document —
    /// no reopen — so the next read shows it and the next local edit builds
    /// on it.
    ///
    /// KILL: hold documents but never absorb a pulled delta into them; the
    /// held document stays at the seed and the read shows "Plans".
    func testAPulledDeltaReachesTheHeldDocument() async throws {
        let w = try world()
        let peerDoc = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(peerDoc, "name", "Plans")
        let seedBytes = try peerDoc.export(mode: .snapshot)
        try await w.boards.createDoc(id: "b1", seed: seedBytes, peer: 100)
        try await w.engine.drain()
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: try peerDoc.export(mode: .snapshot), data: [:])],
            cursor: "1:", more: false))
        try await w.engine.pullOnce(shard: "user")
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Plans")

        let payload = try LoroFixture.editPayload(peerDoc, "name", "Peer's rename")
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 2, codec: LoroReplicaCodec.codecName, payload: payload)],
            cursor: "2:", more: false
        ))
        _ = try await w.engine.pullOnce(shard: "user")

        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Peer's rename", "the pulled delta never reached the state the read serves")
        // The next local edit stands on the peer's history: its delta must
        // import cleanly into the peer's document (no missing deps).
        _ = try await w.boards.updateDoc("b1") { document in
            try LoroFixture.setMeta(document.doc, "name", "Both")
        }
        let owed = try XCTUnwrap(w.store.peekPending().first { (try? $0.op().verb) == ReplicaOp.Verb.docDelta })
        try peerDoc.import(bytes: try XCTUnwrap(owed.op().payload))
        XCTAssertEqual(LoroFixture.meta(peerDoc, "name"), "Both")
    }

    func testWatchDocDeliversLocalAndPulledMovement() async throws {
        let w = try world()
        let peerDoc = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(peerDoc, "name", "Plans")
        try await w.boards.createDoc(id: "b1", seed: try peerDoc.export(mode: .snapshot), peer: 100)
        try await w.engine.drain()
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: try peerDoc.export(mode: .snapshot), data: [:])],
            cursor: "1:", more: false))
        try await w.engine.pullOnce(shard: "user")

        let delivered = DeliveredNames()
        let watch = w.boards.watchDoc("b1", includeInitial: true) { state in delivered.append(state?.name) }
        defer { watch.cancel() }
        try await eventually { delivered.names == ["Plans"] }

        _ = try await w.boards.updateDoc("b1") { document in
            try LoroFixture.setMeta(document.doc, "name", "Local")
        }
        try await eventually { delivered.names.contains("Local") }

        // The peer edits ON TOP of the local edit, as it would after its own
        // pull — a concurrent edit would only be an LWW coin toss.
        let owed = try XCTUnwrap(w.store.peekPending().first { (try? $0.op().verb) == ReplicaOp.Verb.docDelta })
        let baseline = peerDoc.oplogVv()
        try peerDoc.import(bytes: try XCTUnwrap(owed.op().payload))
        try LoroFixture.setMeta(peerDoc, "name", "Pulled")
        let payload = try peerDoc.export(mode: .updates(from: baseline))
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 2, codec: LoroReplicaCodec.codecName, payload: payload)],
            cursor: "2:", more: false
        ))
        _ = try await w.engine.pullOnce(shard: "user")
        try await eventually { delivered.names.contains("Pulled") }
        XCTAssertEqual(delivered.names, ["Plans", "Local", "Pulled"], "both movements arrive through the one door, each once")
    }

    func testFailedResetPreservesTheHeldDocumentAndItsUndoHistory() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 100)
        _ = try await w.boards.updateDoc("b1") { document in
            try LoroFixture.setMeta(document.doc, "name", "Local")
        }
        _ = try await w.engine.drain()
        let before = try XCTUnwrap(w.boards.findDoc("b1"))
        let fold = try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold
        XCTAssertTrue(before.canUndo)
        struct Fault: Error {}
        await w.engine.setCheckpointFault { throw Fault() }
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: try seed(name: "Replacement"), data: [:])],
            cursor: "2:", more: false
        ))
        do {
            _ = try await w.engine.pullOnce(shard: "user")
            XCTFail("the checkpoint fault must surface")
        } catch is Fault {}
        XCTAssertEqual(try w.boards.findDoc("b1"), before)
        XCTAssertEqual(try w.store.peekDoc("boards", "b1")?.fold, fold)
        _ = try await w.boards.undoDoc("b1")
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Plans")
    }

    /// Pause the writer after SQLite commits, allowing the real document
    /// observation to read before the checkpoint publishes its held copy.
    /// A peer rename must still reach the observer without another write.
    func testWatchDocCannotMissAReadDuringCheckpointPublication() async throws {
        let w = try world()
        let peerDoc = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(peerDoc, "name", "Plans")
        try await w.boards.createDoc(id: "b1", seed: try peerDoc.export(mode: .snapshot), peer: 100)
        try await w.engine.drain()
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: try peerDoc.export(mode: .snapshot), data: [:])],
            cursor: "1:", more: false))
        try await w.engine.pullOnce(shard: "user")
        // A pull drains local births first. Finish that before arming the
        // barrier so it pauses the checkpoint, not the birth acknowledgement.
        _ = try await w.engine.drain()
        XCTAssertTrue(try w.store.peekPending().isEmpty)
        let gate = CheckpointReadGate()
        let delivered = DeliveredNames()
        let watch: ReplicaWatch = ReplicaReads.watchDocument(
            w.engine.binding, health: w.engine.health, stream: "boards", includeInitial: true,
            read: { _ -> BoardState? in
                let state = try w.boards.findDoc("b1")
                gate.didRead()
                return state
            },
            deliver: { state in delivered.append(state?.name) }
        )
        defer { watch.cancel() }
        try await eventually { delivered.names == ["Plans"] }
        try await w.store.pool.write { db in
            db.add(transactionObserver: gate, extent: .observerLifetime)
        }
        gate.arm()
        let payload = try LoroFixture.editPayload(peerDoc, "name", "Pulled")
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 2, codec: LoroReplicaCodec.codecName, payload: payload)],
            cursor: "2:", more: false
        ))
        _ = try await w.engine.pullOnce(shard: "user")
        XCTAssertTrue(gate.didPause, "the checkpoint barrier never ran")
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Pulled")
        try await eventually { delivered.names.contains("Pulled") }
        XCTAssertEqual(delivered.names, ["Plans", "Pulled"])
    }

    func testUndoDocInvertsThisPeersLastEdit() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 100)
        _ = try await w.boards.updateDoc("b1") { document in
            try LoroFixture.setMeta(document.doc, "name", "Renamed")
        }
        XCTAssertEqual(try w.boards.findDoc("b1")?.canUndo, true)

        let undone = try await w.boards.undoDoc("b1")
        XCTAssertTrue(undone)
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Plans")
        XCTAssertEqual(try w.boards.findDoc("b1")?.canRedo, true)
        let redone = try await w.boards.redoDoc("b1")
        XCTAssertTrue(redone)
        XCTAssertEqual(try w.boards.findDoc("b1")?.name, "Renamed")
    }

    /// A session's hold is a declaration, not a touch: pinned before the
    /// document is even held, the document still never leaves the LRU once
    /// it is — the grid's reads of other projects cannot evict the open
    /// editor's document (and its undo history with it).
    ///
    /// KILL: keep pins on the held entry only (`held[key]?.pins += 1`) — a
    /// pin on a not-yet-held key is a no-op and the open reads it later as
    /// unpinned.
    func testAPinDeclaredBeforeTheOpenSurvivesTheLRU() async throws {
        let w = try world()
        w.boards.pinDoc("b0")
        try await w.boards.createDoc(id: "b0", seed: try seed(name: "Pinned"), peer: 7)
        _ = try w.boards.findDoc("b0")

        for index in 1 ... w.engine.liveDocuments.capacity + 1 {
            try await w.boards.createDoc(id: "b\(index)", seed: try seed(name: "Filler \(index)"), peer: 7)
            _ = try w.boards.findDoc("b\(index)")
        }

        XCTAssertEqual(try w.boards.heldDoc("b0")?.name, "Pinned", "the pinned document left the LRU")
        XCTAssertNil(try w.boards.heldDoc("b1"), "the oldest unpinned document should have been evicted")
    }

    /// The peek: a HELD document answers, a closed one stays closed — the
    /// grid asks every listed project this way and opens none of them.
    func testHeldDocAnswersOnlyAHeldDocument() async throws {
        let w = try world()
        try await w.boards.createDoc(id: "b1", seed: try seed(name: "Plans"), peer: 7)

        XCTAssertNil(try w.boards.heldDoc("b1"), "a peek must not open the document")
        XCTAssertEqual(BoardState.decodes.count, 0, "a peek decoded a closed document")

        _ = try w.boards.findDoc("b1")
        XCTAssertEqual(try w.boards.heldDoc("b1")?.name, "Plans")
    }

    private func eventually(timeout: TimeInterval = 2, _ condition: @escaping () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition(), Date() < deadline {
            try await Task.sleep(for: .milliseconds(20))
        }
        XCTAssertTrue(condition(), "the condition never held within \(timeout)s")
    }
}

// MARK: - The test document

private struct Board: ReplicaDocModel {
    typealias State = BoardState
    static let streamName = "boards"
    var id: String
    init?(id: String, data: [String: ReplicaValue]) { self.id = id }
}

private struct BoardState: ReplicaDocState {
    typealias Codec = LoroReplicaCodec
    static let decodes = LockedCount()

    let name: String?
    let version: Data
    let canUndo: Bool
    let canRedo: Bool

    static func state(of document: LoroDocument, version: Data, canUndo: Bool, canRedo: Bool) -> BoardState {
        decodes.increment()
        return BoardState(name: LoroFixture.meta(document.doc, "name"), version: version, canUndo: canUndo, canRedo: canRedo)
    }
}

private final class LockedCount: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    var count: Int { lock.withLock { value } }
    func increment() { lock.withLock { value += 1 } }
    func reset() { lock.withLock { value = 0 } }
}

private final class DeliveredNames: @unchecked Sendable {
    private let lock = NSLock()
    private var stored: [String?] = []
    var names: [String?] { lock.withLock { stored } }
    func append(_ name: String?) { lock.withLock { stored.append(name) } }
}

/// The observation may finish during the commit gap, or correctly wait for
/// publication. The bounded fallback releases the latter without a deadlock;
/// no assertion depends on how long either side takes.
private final class CheckpointReadGate: TransactionObserver, @unchecked Sendable {
    private let lock = NSLock()
    private let read = DispatchSemaphore(value: 0)
    private var armed = false
    private var waiting = false
    private var paused = false
    var didPause: Bool { lock.withLock { paused } }
    func arm() { lock.withLock { armed = true } }
    func didRead() {
        if lock.withLock({ waiting }) { read.signal() }
    }
    func observes(eventsOfKind eventKind: DatabaseEventKind) -> Bool { true }
    func databaseDidChange(with event: DatabaseEvent) {}
    func databaseDidRollback(_ db: Database) {}
    func databaseDidCommit(_ db: Database) {
        let shouldPause = lock.withLock { () -> Bool in
            guard armed else { return false }
            armed = false
            paused = true
            waiting = true
            return true
        }
        guard shouldPause else { return }
        _ = read.wait(timeout: .now() + 2)
        lock.withLock { waiting = false }
    }
}
