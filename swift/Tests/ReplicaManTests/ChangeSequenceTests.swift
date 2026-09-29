import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

final class ChangeSequenceTests: XCTestCase {
    private func storePath(_ name: String) throws -> String {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("replica-man-tests", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent("\(name)-\(UUID().uuidString).sqlite").path
    }

    func testUnsupportedStoreIsRefusedWithoutChangingItsJournal() throws {
        let path = try storePath("legacy")

        do {
            let legacy = try DatabaseQueue(path: path)
            try legacy.write { db in
                try db.execute(sql: """
                    CREATE TABLE journal (
                        id TEXT PRIMARY KEY,
                        op TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        preimage TEXT,
                        parked TEXT,
                        created_at REAL NOT NULL
                    )
                    """)
                try db.execute(
                    sql: "INSERT INTO journal (id, op, payload, created_at) VALUES (?, ?, ?, ?)",
                    arguments: ["legacy-op", "row.create", "{}", 1.0]
                )
            }
        }

        XCTAssertThrowsError(try ReplicaStateStore(path: path)) { error in
            guard case ReplicaError.storage(let message) = error else { return XCTFail("\(error)") }
            XCTAssertEqual(message, "Unsupported earlier store format; open a fresh store")
        }
        let preserved = try DatabaseQueue(path: path)
        XCTAssertEqual(try preserved.read { db in
            try String.fetchOne(db, sql: "SELECT payload FROM journal WHERE id = 'legacy-op'")
        }, "{}")
        XCTAssertEqual(try preserved.read { try Int.fetchOne($0, sql: "PRAGMA user_version") }, 0)
        XCTAssertFalse(try preserved.read { try $0.tableExists("meta") }, "a refused open leaves nothing behind")
    }

    /// The store's format is `PRAGMA user_version`: an earlier stamp is the
    /// earlier layout's refusal, a later one asks for a newer build — and
    /// neither open changes what the refused store holds or its stamp.
    func testAStoreOfAnotherFormatIsRefusedWithoutChangingIt() throws {
        for (version, refusal) in [
            (1, "Unsupported earlier store format; open a fresh store"),
            (2, "Unsupported earlier store format; open a fresh store"),
            (4, "Store format 4 is newer than this build; upgrade required"),
        ] {
            let path = try storePath("format-\(version)")
            do {
                let other = try DatabaseQueue(path: path)
                try other.write { db in
                    try db.execute(sql: "CREATE TABLE replica_session (id INTEGER PRIMARY KEY, writer_id TEXT NOT NULL)")
                    try db.execute(sql: "INSERT INTO replica_session VALUES (1, 'writer')")
                    try db.execute(sql: "PRAGMA user_version = \(version)")
                }
            }

            XCTAssertThrowsError(try ReplicaStateStore(path: path)) { error in
                guard case ReplicaError.storage(let message) = error else { return XCTFail("\(error)") }
                XCTAssertEqual(message, refusal)
            }
            let preserved = try DatabaseQueue(path: path)
            XCTAssertEqual(try preserved.read { try String.fetchOne($0, sql: "SELECT writer_id FROM replica_session") }, "writer")
            XCTAssertEqual(try preserved.read { try Int.fetchOne($0, sql: "PRAGMA user_version") }, version)
            XCTAssertFalse(try preserved.read { try $0.tableExists("meta") }, "a refused open leaves nothing behind")
        }
    }

    /// A newer build's store refuses even when it holds nothing this build
    /// would recognize — the stamp alone decides.
    func testANewerStampIsRefusedEvenOverAnEmptyStore() throws {
        let path = try storePath("newer-empty")
        do {
            let newer = try DatabaseQueue(path: path)
            try newer.write { try $0.execute(sql: "PRAGMA user_version = 4") }
        }

        XCTAssertThrowsError(try ReplicaStateStore(path: path)) { error in
            guard case ReplicaError.storage(let message) = error else { return XCTFail("\(error)") }
            XCTAssertEqual(message, "Store format 4 is newer than this build; upgrade required")
        }
        let preserved = try DatabaseQueue(path: path)
        XCTAssertEqual(try preserved.read { try Int.fetchOne($0, sql: "PRAGMA user_version") }, 4)
        XCTAssertEqual(try preserved.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM sqlite_master") }, 0)
    }

    /// A fresh file gets the whole shared layout and the current stamp; the
    /// next open of it runs the same idempotent DDL and keeps its identity.
    func testAFreshStoreIsStampedWithTheCurrentFormatAndReopens() throws {
        let path = try storePath("fresh")
        let store = try ReplicaStateStore(path: path)
        let identity = try store.pool.read { try store.meta($0).store }
        try store.close()

        let raw = try DatabaseQueue(path: path)
        XCTAssertEqual(try raw.read { try Int.fetchOne($0, sql: "PRAGMA user_version") }, 3)
        let tables = try raw.read { try String.fetchAll($0, sql: "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name") }
        XCTAssertEqual(tables, [
            "base", "checkpoints", "docs", "download_pages", "downloads", "entities", "entity_references",
            "holds", "intents", "meta", "recoveries", "recovery_parts", "snapshots", "stream_meta", "submissions",
        ])
        try raw.close()

        let reopened = try ReplicaStateStore(path: path)
        XCTAssertEqual(try reopened.pool.read { try reopened.meta($0).store }, identity)
        try reopened.close()
    }

    func testCheckpointResetAndRollbackMoveOnlyCommittedStreamSequences() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        try await pullPage(engine, transport, "global", [.rowSet(stream: "assets", id: "a1", type: nil, data: [:])], cursor: "1:")
        try await pullPage(engine, transport, "user", [Fixture.note("n1", title: "one")], cursor: "5:")
        let afterCheckpoint = try sequence(store, stream: "notes")
        XCTAssertEqual(try sequence(store, stream: "assets"), 1)

        try await engine.resetCursors()
        try await pullPage(engine, transport, "user", [], cursor: "6:")
        let afterReset = try sequence(store, stream: "notes")
        XCTAssertGreaterThan(afterReset, afterCheckpoint)
        XCTAssertEqual(try sequence(store, stream: "assets"), 1, "a user-shard reset moved the global shard")
        XCTAssertNotNil(try store.peekSnapshot("assets", "a1"))

        try await assertAFaultedCheckpointMovesNothing(engine, transport, store, notesAt: afterReset)
    }

    private func pullPage(
        _ engine: ReplicaEngine, _ transport: StubTransport, _ shard: String, _ frames: [ReplicaFrame], cursor: String
    ) async throws {
        await transport.queuePull(shard: shard, ReplicaPullResponse(frames: frames, cursor: cursor, more: false))
        try await engine.pullOnce(shard: shard)
    }

    private func assertAFaultedCheckpointMovesNothing(
        _ engine: ReplicaEngine, _ transport: StubTransport, _ store: ReplicaStateStore, notesAt afterReset: Int64
    ) async throws {
        struct Fault: Error {}
        await engine.setCheckpointFault { throw Fault() }
        do {
            try await pullPage(engine, transport, "user", [Fixture.note("n2", title: "rolled back")], cursor: "7:")
            XCTFail("the faulted checkpoint unexpectedly committed")
        } catch is Fault {
        }
        XCTAssertEqual(try sequence(store, stream: "notes"), afterReset)

        await engine.setCheckpointFault(nil)
        try await pullPage(engine, transport, "user", [Fixture.note("n2", title: "restored")], cursor: "7:")
        XCTAssertGreaterThan(try sequence(store, stream: "notes"), afterReset)
    }

    func testLocalDeleteAndRejectedCreateRevertAdvanceTheSequence() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("one")]
        )
        let afterCreate = try sequence(store, stream: "notes")
        _ = try await engine.deleteRow(stream: "notes", id: "n1")
        let afterDelete = try sequence(store, stream: "notes")
        XCTAssertGreaterThan(afterDelete, afterCreate)

        await transport.scriptPush { ops in
            ops.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "refused") }
        }
        try await engine.saveRow(
            stream: "notes", id: "n2", type: nil, data: ["title": .string("two")]
        )
        let beforeRevert = try sequence(store, stream: "notes")
        _ = try await engine.drain()

        XCTAssertNil(try store.peekSnapshot("notes", "n2"))
        XCTAssertGreaterThan(try sequence(store, stream: "notes"), beforeRevert)
    }

    func testDocumentUpsertUpdateAndDeleteAdvanceTheSequence() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [
                .docSnapshot(
                    stream: "boards",
                    id: "b1",
                    codec: "stub@1",
                    snapshot: Data("SNAP".utf8),
                    data: [:]
                ),
            ],
            cursor: "5:",
            more: false
        ))
        try await engine.pullOnce(shard: "user")
        let afterUpsert = try sequence(store, stream: "boards")

        try await engine.recordDocDelta(
            stream: "boards", id: "b1", payload: Data("+delta".utf8)
        )
        let afterUpdate = try sequence(store, stream: "boards")
        XCTAssertGreaterThan(afterUpdate, afterUpsert)

        try await engine.resyncDocument(stream: "boards", id: "b1")
        XCTAssertGreaterThan(try sequence(store, stream: "boards"), afterUpdate)
    }

    private func sequence(_ store: ReplicaStateStore, stream: String) throws -> Int64 {
        try store.pool.read { db in
            try Int64.fetchOne(
                db,
                sql: "SELECT change_seq FROM stream_meta WHERE stream = ?",
                arguments: [stream]
            ) ?? 0
        }
    }
}
