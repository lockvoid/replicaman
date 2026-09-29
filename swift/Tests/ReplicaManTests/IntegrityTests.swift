import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

final class IntegrityTests: XCTestCase {
    func testFrozenIndependentHashVectors() throws {
        struct Vector: Decodable {
            struct Entry: Decodable { let stream: String; let id: String; let incarnation: String; let revision: String }
            let rows: [Entry]
            let view_digest: String
            let empty_view_digest: String
            let base_digest: String
        }
        let root = (0..<4).reduce(URL(fileURLWithPath: #filePath)) { url, _ in url.deletingLastPathComponent() }
        let vector = try JSONDecoder().decode(Vector.self, from: Data(contentsOf: root.appendingPathComponent("protocol/fixtures/integrity.json")))
        var hash = ReplicaIntegrityHash("replicaman-view")
        for row in vector.rows {
            for value in [row.stream, row.id, row.incarnation, row.revision] { hash.append(value) }
        }
        XCTAssertEqual(hash.finish(), vector.view_digest)
        XCTAssertEqual(ReplicaIntegrityHash("replicaman-view").finish(), vector.empty_view_digest)
        XCTAssertEqual(ReplicaIntegrityHash.base(stream: "notes", id: "é/🙂", shard: "user", incarnation: "life-e", revision: 2,
            type: nil, data: #"{"n":9007199254740993}"#, codec: "loro@1", fold: Data([0, 1, 255])), vector.base_digest)
    }

    func testStoredCorruptionFailsVerificationWithoutChangingLocalIntent() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.createRow(stream: "notes", id: "offline", type: nil, data: ["title": .string("keep me")])
        let row = ReplicaBaseRow(incarnation: "lifetime", revision: 2, type: nil,
                                data: ["title": .string("server")], codec: "loro@1", fold: Data([0, 1, 2]))
        try await store.pool.write { db in
            try store.saveBase(db, stream: "notes", id: "server", shard: "user", row: row)
            try store.setCursor(db, "checkpoint", shard: "user")
        }
        let healthy = try await store.pool.read { try store.integritySnapshot($0, shard: "user") }
        XCTAssertEqual(healthy.count, 1)
        let before = try store.peekPending().map(\.payload)
        for assignment in ["data = '{}'", "fold = X'FF'", "revision = 3", "incarnation = 'other'", "type = ''", "integrity = NULL"] {
            try await store.pool.write { db in
                try store.saveBase(db, stream: "notes", id: "server", shard: "user", row: row)
                try db.execute(sql: "UPDATE base SET \(assignment)")
            }
            do {
                _ = try await store.pool.read { try store.integritySnapshot($0, shard: "user") }
                XCTFail("Corruption was accepted: \(assignment)")
            } catch ReplicaError.storage(let message) {
                XCTAssertTrue(message.contains("Authoritative row integrity failed"))
            }
            XCTAssertEqual(try store.peekPending().map(\.payload), before)
        }
        try await store.pool.write { try store.saveBase($0, stream: "notes", id: "server", shard: "user", row: row) }
        let restored = try await store.pool.read { try store.integritySnapshot($0, shard: "user") }
        XCTAssertEqual(restored.digest, healthy.digest)
    }

    private func synchronized() async throws -> (ReplicaStateStore, StubTransport, ReplicaEngine) {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "one"), Fixture.note("n2", title: "two"),
        ], cursor: "c1", more: false))
        try await engine.pullOnce()
        return (store, transport, engine)
    }

    func testVerificationAgreesWithTheServerAtItsHeadsWhateverIsUnsent() async throws {
        let (_, _, engine) = try await synchronized()
        try await engine.saveRow(stream: "notes", id: "local", type: nil, data: ["title": .string("unsent")])
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("edited")])

        try await engine.verifyIntegrity()
    }

    func testAMissingAuthoritativeRowDivergesAndChangesNothing() async throws {
        let (store, _, engine) = try await synchronized()
        try await store.pool.write { try $0.execute(sql: "DELETE FROM base WHERE row_id = 'n2'") }

        do {
            try await engine.verifyIntegrity()
            XCTFail("a base missing a server member must not verify")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "ReplicaDiverged")
        }
        XCTAssertEqual(try store.peekSnapshot("notes", "n2")?.data["title"], .string("two"))
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "c1")
    }

    func testVerificationBehindTheServerAsksForAPullAndChangesNothing() async throws {
        let (store, transport, engine) = try await synchronized()
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n3", title: "three")], cursor: "c2", more: true))
        try await engine.pullOnce()

        do {
            try await engine.verifyIntegrity()
            XCTFail("a cursor behind the server's heads cannot be verified")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "CursorBehind")
        }
        let behind = try await engine.currentCursor()
        XCTAssertEqual(behind, "c1")
        XCTAssertNil(try store.peekSnapshot("notes", "n3"))
        let pulls = await transport.pullCount
        XCTAssertEqual(pulls, 2, "verification does not pull on its own")

        await transport.queuePull(shard: "user", .init(frames: [], cursor: "c3", more: false))
        try await engine.pullOnce()
        try await engine.verifyIntegrity()
    }
}
