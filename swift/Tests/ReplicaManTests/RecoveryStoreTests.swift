import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

final class RecoveryStoreTests: XCTestCase {
    func testDamagedAuthoringAndLargeFoldsCanBeExportedInBoundedParts() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let fold = Data(repeating: 97, count: 600_001)
        try await engine.createDoc(stream: "boards", id: "b1", seed: fold, peer: 7)
        try await store.pool.write { db in
            try db.execute(sql: "UPDATE snapshots SET data = '{damaged' WHERE row_id = 'b1'")
            try db.execute(sql: "UPDATE intents SET payload = '{damaged' WHERE row_id = 'b1'")
            try store.archiveEntity(db, stream: "boards", id: "b1", reason: "test export")
        }

        let record = try XCTUnwrap(store.recoveryRecords(limit: 1).first)
        var parts: [ReplicaRecoveryPart] = []
        while true {
            let page = try store.recoveryParts(id: record.id, after: parts.last, limit: 2)
            if page.isEmpty { break }
            XCTAssertLessThanOrEqual(page.count, 2)
            parts += page
        }
        let document = try XCTUnwrap(parts.first { $0.kind == "document.fold" })
        var exported = Data()
        while exported.count < document.byteCount {
            let chunk = try store.recoveryChunk(id: record.id, part: document, offset: Int64(exported.count), limit: 65_536)
            XCTAssertFalse(chunk.isEmpty)
            XCTAssertLessThanOrEqual(chunk.count, 65_536)
            exported.append(chunk)
        }
        XCTAssertEqual(exported, fold)
        for kind in ["row.data", "intent"] {
            let part = try XCTUnwrap(parts.first { $0.kind == kind })
            XCTAssertEqual(try store.recoveryChunk(id: record.id, part: part), Data("{damaged".utf8))
        }
        XCTAssertEqual(try store.peekDoc("boards", "b1")?.fold, fold, "archiving does not alter active bytes")
        enum SinkError: Error { case full }
        var attempts = 0
        XCTAssertThrowsError(try store.exportRecovery(id: record.id) { _ in
            attempts += 1
            if attempts == 3 { throw SinkError.full }
        }) { XCTAssertTrue($0 is SinkError) }
        XCTAssertEqual(try store.recoveryRecords().count, 1, "a failed export must retain the archive")

        var lines: [[String: ReplicaValue]] = []
        try store.exportRecovery(id: record.id) { bytes in
            XCTAssertLessThan(bytes.count, 360_000, "export must stream large folds")
            lines.append(try ReplicaJSON.decoder().decode([String: ReplicaValue].self, from: bytes))
        }
        XCTAssertEqual(lines.first?["format"], .string("replicaman-recovery"))
        XCTAssertEqual(lines.last?["type"], .string("complete"))
        XCTAssertEqual(lines.last?["parts"]?.int, parts.count)
        XCTAssertEqual(lines.last?["bytes"]?.string, String(parts.reduce(0) { $0 + $1.byteCount }))
        var kind: String?
        var reconstructed = Data()
        for line in lines {
            if line["type"] == .string("part") { kind = line["kind"]?.string }
            if kind == "document.fold", line["type"] == .string("chunk") {
                XCTAssertEqual(line["offset"]?.string, String(reconstructed.count))
                let chunk = try XCTUnwrap(Data(base64Encoded: try XCTUnwrap(line["content"]?.string)))
                // SHA-256 constants computed independently from the original fixture bytes.
                let digest = chunk.count == 262_144
                    ? "dd3dde87623d9a6b354c68c943d189c89c63652d945e7bbdf0986cae91a49521"
                    : "845671f868efb188716917bfc3b8a3c61c74a8a77195edaae9df445de3ec0a45"
                XCTAssertEqual(line["sha256"]?.string, digest)
                reconstructed.append(chunk)
            }
        }
        XCTAssertEqual(reconstructed, fold)

        try store.removeRecoveryRecord(id: record.id)
        XCTAssertTrue(try store.recoveryRecords().isEmpty)
        XCTAssertTrue(try store.recoveryParts(id: record.id).isEmpty)
    }

    func testArchiveFailureRollsBackEveryCopiedPartAndKeepsTheOriginal() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("keep")])
        let pending = try store.peekPending()
        try await store.pool.write { db in
            try db.execute(sql: """
                CREATE TRIGGER fail_archive BEFORE INSERT ON recovery_parts
                WHEN NEW.kind = 'intent' BEGIN SELECT RAISE(ABORT, 'archive failed'); END
                """)
        }

        do {
            try await store.pool.write { db in
                try store.archiveEntity(db, stream: "notes", id: "n1", reason: "must roll back")
            }
            XCTFail("the injected archive failure must propagate")
        } catch let error as DatabaseError {
            XCTAssertEqual(error.message, "archive failed")
        }
        XCTAssertTrue(try store.recoveryRecords().isEmpty)
        let partCount = try await store.pool.read {
            try Int.fetchOne($0, sql: "SELECT count(*) FROM recovery_parts")
        }
        XCTAssertEqual(partCount, 0)
        XCTAssertEqual(try store.peekPending(), pending)
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("keep"))
    }

    func testCorruptJournalFailurePropagatesOnEveryPullWithoutAdvancingTheCursor() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("keep")])
        try await store.pool.write { try $0.execute(sql: "UPDATE intents SET payload = '{damaged'") }
        for _ in 0..<2 {
            do {
                _ = try await engine.pullOnce(shard: "user")
                XCTFail("corrupt authoring was hidden by the warm-drain path")
            } catch ReplicaError.storage {
                // This is the refusal under test; any other failure propagates.
            }
        }
        let pulls = await transport.pullCount
        XCTAssertEqual(pulls, 0)
        let cursor = try await engine.currentCursor(shard: "user")
        XCTAssertNil(cursor)
        let payload = try await store.pool.read { try String.fetchOne($0, sql: "SELECT payload FROM intents") }
        XCTAssertEqual(payload, "{damaged")
    }
}
