import ReplicaManTestProtocol
import Foundation
import GRDB
import Loro
@testable import ReplicaManLoro
import XCTest
@testable import ReplicaMan

/// A document stream's row is the reflection of its document: a field the
/// document owns (`boards.name` ← `meta.name`) reads what the LOCAL document
/// holds after every movement of its fold — birth, edit, undo, a served delta
/// or snapshot — and a server image of the row never writes over it while
/// the document is here. Written in the fold's commit, only when it moved.
final class ReflectionTests: XCTestCase {

    func testCorruptReadPreservesHistoryAndExplicitRebuildArchivesIt() async throws {
        let w = try world()
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: try seed(name: "Plans"), peer: 7)
        try await w.engine.drain()
        let broken = Data("broken history".utf8)
        try await w.store.pool.write { db in
            try db.execute(sql: "UPDATE docs SET fold = ? WHERE stream = 'boards' AND row_id = 'b1'", arguments: [broken])
        }
        XCTAssertThrowsError(try w.engine.documentState(stream: "boards", id: "b1", as: BoardName.self))
        XCTAssertEqual(try w.store.peekDoc("boards", "b1")?.fold, broken)
        XCTAssertTrue(try w.store.peekPending().isEmpty)
        try await w.engine.rebuildDocument(stream: "boards", id: "b1", fold: try seed(name: "Recovered"), peer: 8)
        let recovery = try XCTUnwrap(w.store.recoveryRecords().last)
        XCTAssertEqual(try recoveryBytes(w.store, record: recovery, kind: "document.fold"), broken)
        XCTAssertEqual(try w.engine.documentState(stream: "boards", id: "b1", as: BoardName.self)?.name, "Recovered")
    }

    func testRebuildRequiresFreshPeerAndResyncArchivesTheLocalFold() async throws {
        let w = try world()
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: try seed(name: "Saved"), peer: 7)
        let before = try XCTUnwrap(w.store.peekDoc("boards", "b1"))
        let pending = try w.store.peekPending()
        do {
            try await w.engine.rebuildDocument(stream: "boards", id: "b1", fold: before.fold, peer: before.peer)
            XCTFail("Reusing the authoring peer was accepted")
        } catch ReplicaError.codec {
            // The refused rebuild must preserve both authoring and recovery state.
        }
        XCTAssertEqual(try w.store.peekDoc("boards", "b1")?.fold, before.fold)
        XCTAssertEqual(try w.store.peekPending(), pending)
        XCTAssertTrue(try w.store.recoveryRecords().isEmpty)

        try await w.engine.resyncDocument(stream: "boards", id: "b1")
        let recovery = try XCTUnwrap(w.store.recoveryRecords().last)
        XCTAssertEqual(try recoveryBytes(w.store, record: recovery, kind: "document.fold"), before.fold)
        XCTAssertNil(try w.store.peekDoc("boards", "b1"))
        XCTAssertEqual(try w.store.peekPending(), pending)
    }

    func testInvalidSeedCannotBeAcknowledgedAsSaved() async throws {
        let w = try world()
        do {
            try await w.engine.createDoc(stream: "boards", id: "bad", seed: Data("broken".utf8), peer: 7)
            XCTFail("invalid seed was saved")
        } catch ReplicaError.codec(let message) {
            XCTAssertTrue(message.hasPrefix("import failed"), message)
        }
        XCTAssertNil(try w.store.peekDoc("boards", "bad"))
        XCTAssertTrue(try w.store.peekPending().isEmpty)
    }

    private func world() throws -> (store: ReplicaStateStore, transport: LoroStubTransport, engine: ReplicaEngine) {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        return (store, transport, LoroFixture.engine(store: store, transport: transport))
    }

    private func seed(name: String, peer: UInt64 = 7) throws -> Data {
        let author = try LoroFixture.doc(peer: peer)
        try LoroFixture.setMeta(author, "name", name)
        return try author.export(mode: .snapshot)
    }

    private func edit(_ engine: ReplicaEngine, _ key: String, _ value: String) async throws {
        _ = try await engine.updateDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self) { document in
            try LoroFixture.setMeta(document.doc, key, value)
        }
    }

    private func rowName(_ store: ReplicaStateStore) throws -> ReplicaValue? {
        try store.peekSnapshot("boards", "b1")?.data["name"]
    }

    func testABornDocumentsRowReadsItsSeed() async throws {
        let w = try world()
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: try seed(name: "Plans"), peer: 7,
                                     data: ["name": .string("Caller's"), "color": .string("red")])

        XCTAssertEqual(try rowName(w.store), .string("Plans"), "the seed owns the name, not the caller")
        XCTAssertEqual(try w.store.peekSnapshot("boards", "b1")?.data["color"], .string("red"))
    }

    func testASeedWithoutTheFieldReadsNull() async throws {
        let w = try world()
        let blank = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(blank, "color", "red")
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: try blank.export(mode: .snapshot), peer: 7)

        XCTAssertEqual(try rowName(w.store), .null)
    }

    func testTheRowMovesInTheFoldsCommitAndOnlyWhenItsFieldMoved() async throws {
        let w = try world()
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: try seed(name: "Plans"), peer: 7)
        let commits = CommitTables()
        w.store.pool.add(transactionObserver: commits)

        try await edit(w.engine, "name", "Renamed")
        try await edit(w.engine, "color", "red")

        XCTAssertEqual(try rowName(w.store), .string("Renamed"))
        XCTAssertEqual(commits.tables, [["docs", "snapshots"], ["docs"]],
                       "the rename writes the row with its fold; an edit the row does not read leaves it alone")
    }

    func testUndoingARenameRestoresTheRow() async throws {
        let w = try world()
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: try seed(name: "Plans"), peer: 7)
        try await edit(w.engine, "name", "Renamed")

        let undone = try await w.engine.undoDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self)

        XCTAssertTrue(undone)
        XCTAssertEqual(try rowName(w.store), .string("Plans"))
    }

    func testAServedDeltaMovesTheRow() async throws {
        let w = try world()
        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer)
        try LoroFixture.setMeta(server, "name", "Plans")
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: try server.export(mode: .snapshot), data: ["name": .string("Plans")])],
            cursor: "5:", more: false
        ))
        try await w.engine.pullOnce(shard: "user")

        let delta = try LoroFixture.editPayload(server, "name", "Renamed elsewhere")
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 1, codec: LoroReplicaCodec.codecName, payload: delta)],
            cursor: "6:", more: false
        ))
        try await w.engine.pullOnce(shard: "user")

        XCTAssertEqual(try rowName(w.store), .string("Renamed elsewhere"))
    }

    func testAServedSnapshotsRowReadsTheDocumentItBrings() async throws {
        let w = try world()
        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer)
        try LoroFixture.setMeta(server, "name", "Plans")
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: try server.export(mode: .snapshot), data: ["color": .string("red")])],
            cursor: "5:", more: false
        ))
        try await w.engine.pullOnce(shard: "user")

        XCTAssertEqual(try rowName(w.store), .string("Plans"))
        XCTAssertEqual(try w.store.peekSnapshot("boards", "b1")?.data["color"], .string("red"))
    }

    /// The server's image of the row reflects the server's document; with
    /// this device's rename still owed, the local document reads "Mine".
    func testAServerImageNeverWritesOverWhatTheLocalDocumentReads() async throws {
        let w = try world()
        let seedBytes = try seed(name: "Plans")
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: seedBytes, peer: 7)
        try await w.engine.drain()
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: seedBytes, data: ["name": .string("Plans")])],
            cursor: "1:", more: false))
        try await w.engine.pullOnce(shard: "user")
        try await edit(w.engine, "name", "Mine")
        await w.transport.failPushes(true)

        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowSet(stream: "boards", id: "b1", type: nil, data: ["name": .string("Plans"), "color": .string("red")])],
            cursor: "5:", more: false
        ))
        try await w.engine.pullOnce(shard: "user")

        XCTAssertEqual(try rowName(w.store), .string("Mine"))
        XCTAssertEqual(try w.store.peekSnapshot("boards", "b1")?.data["color"], .string("red"),
                       "the fields the document does not own take the server's image")

        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer, fold: seedBytes)
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                                  snapshot: try server.export(mode: .snapshot), data: ["name": .string("Plans")])],
            cursor: "6:", more: false
        ))
        try await w.engine.pullOnce(shard: "user")

        XCTAssertEqual(try rowName(w.store), .string("Mine"), "the served snapshot merged under the owed rename")
    }

    /// A fold that will not load is replaced by a blank document — one that
    /// would reflect no name into a row that cannot be read without one.
    /// The replacement carries what the row reflected, and not as an edit
    /// the user can undo.
    func testAnExplicitRebuildHasNoSyntheticUndoStep() async throws {
        let w = try world()
        try await w.engine.createDoc(stream: "boards", id: "b1", seed: try seed(name: "Plans"), peer: 7)
        try await w.engine.rebuildDocument(stream: "boards", id: "b1", fold: try seed(name: "Plans"), peer: 8)

        let recovered = try w.engine.documentState(stream: "boards", id: "b1", as: BoardName.self)
        XCTAssertEqual(recovered?.name, "Plans")
        try await eventually { (try? w.store.peekDoc("boards", "b1"))?.peer != 7 }

        try await edit(w.engine, "color", "red")
        let undoneEdit = try await w.engine.undoDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self)
        let undoneCarry = try await w.engine.undoDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self)

        XCTAssertTrue(undoneEdit)
        XCTAssertFalse(undoneCarry, "the carried name is not the user's edit to undo")
        XCTAssertEqual(try rowName(w.store), .string("Plans"))
        XCTAssertEqual(try LoroFixture.meta(fold: try XCTUnwrap(w.store.peekDoc("boards", "b1")).fold, "name"), "Plans")
    }

    /// A document that moves on this device moves its row's clock, as the
    /// server's record touches its own when a delta lands there.
    func testALocalEditStampsTheRowsClock() async throws {
        let store = try LoroFixture.store()
        let clock = ManualClock(Date(timeIntervalSince1970: 1_800_000_000))
        let engine = LoroFixture.engine(store: store, transport: LoroStubTransport(),
                                        schema: LoroFixture.schema(stamp: .standard), clock: { clock.now })
        try await engine.createDoc(stream: "boards", id: "b1", seed: try seed(name: "Plans"), peer: 7)
        let born = try store.peekSnapshot("boards", "b1")?.data["updatedAt"]

        clock.now = Date(timeIntervalSince1970: 1_800_000_060)
        try await edit(engine, "color", "red")

        XCTAssertEqual(born, .string("2027-01-15T08:00:00Z"))
        XCTAssertEqual(try store.peekSnapshot("boards", "b1")?.data["updatedAt"], .string("2027-01-15T08:01:00Z"))
        XCTAssertEqual(try store.peekSnapshot("boards", "b1")?.data["createdAt"], .string("2027-01-15T08:00:00Z"))
    }

    func testARowWhoseDocumentIsNotHereTakesTheServersImage() async throws {
        let w = try world()
        await w.transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowSet(stream: "boards", id: "b1", type: nil, data: ["name": .string("Plans")])],
            cursor: "5:", more: false
        ))
        try await w.engine.pullOnce(shard: "user")

        XCTAssertEqual(try rowName(w.store), .string("Plans"))
    }
}

private extension ReflectionTests {
    func eventually(timeout: TimeInterval = 2, _ condition: @escaping () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition(), Date() < deadline {
            try await Task.sleep(for: .milliseconds(20))
        }
        XCTAssertTrue(condition(), "the condition never held within \(timeout)s")
    }
}

private struct BoardName: ReplicaDocState {
    typealias Codec = LoroReplicaCodec
    let name: String?

    static func state(of document: LoroDocument, version: Data, canUndo: Bool, canRedo: Bool) -> BoardName {
        BoardName(name: LoroFixture.meta(document.doc, "name"))
    }
}

private final class ManualClock: @unchecked Sendable {
    private let lock = NSLock()
    private var instant: Date

    init(_ instant: Date) {
        self.instant = instant
    }

    var now: Date {
        get { lock.withLock { instant } }
        set { lock.withLock { instant = newValue } }
    }
}

/// The tables each commit wrote, of the two a reflection spans.
private final class CommitTables: TransactionObserver, @unchecked Sendable {
    private static let watched: Set<String> = ["docs", "snapshots"]
    private let lock = NSLock()
    private var writing: Set<String> = []
    private var committed: [Set<String>] = []

    var tables: [Set<String>] { lock.withLock { committed } }

    func observes(eventsOfKind eventKind: DatabaseEventKind) -> Bool {
        switch eventKind {
        case .insert(let table), .delete(let table): Self.watched.contains(table)
        case .update(let table, _): Self.watched.contains(table)
        }
    }

    func databaseDidChange(with event: DatabaseEvent) {
        lock.withLock { _ = writing.insert(event.tableName) }
    }

    func databaseDidCommit(_ db: Database) {
        lock.withLock {
            if !writing.isEmpty { committed.append(writing) }
            writing = []
        }
    }

    func databaseDidRollback(_ db: Database) {
        lock.withLock { writing = [] }
    }
}
