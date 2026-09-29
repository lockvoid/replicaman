import Foundation
import XCTest
@testable import ReplicaMan

final class StorageIntegrityTests: XCTestCase {
    func testCorruptRowCannotBecomeANewWrite() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await store.pool.write { db in
            try db.execute(sql: "INSERT INTO snapshots (stream, row_id, shard, data) VALUES ('notes', 'n1', 'user', '{broken')")
        }
        do {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("replacement")])
            XCTFail("a damaged baseline must fail the write")
        } catch { }
        XCTAssertTrue(try store.peekPending().isEmpty)
        XCTAssertThrowsError(try store.peekSnapshot("notes", "n1"))
    }

    func testConsumerDatabaseRejectsRawWrites() async throws {
        let store = try Fixture.store()
        do {
            try await store.reader.read { try $0.execute(sql: "DELETE FROM snapshots") }
            XCTFail("consumer reads must be read-only")
        } catch { }
    }
}
