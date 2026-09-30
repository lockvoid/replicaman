import ReplicaManGeneratedContract
import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// A generated row is usable before a server echo, without sending pull-only
/// columns — the offline-first contract the Kotlin client pins in
/// `LocalRowSnapshotTests.kt`.
final class LocalRowSnapshotTests: XCTestCase {
    private func world() throws -> (ReplicaStateStore, StubTransport, ReplicaEngine, RowStream<Theme>) {
        let sample = SampleReplica.schema
        let schema = ReplicaSchema(streams: sample.specs, indexes: sample.indexes, version: sample.version)
        let store = try Fixture.store(indexes: schema.indexes)
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, schema: schema)
        return (store, transport, engine, SampleReplica(engine: engine).themes)
    }

    private func theme(_ id: String = "local-theme") -> Theme {
        Theme(
            id: id, colors: [ThemeColor(hex: "#1D8A70", id: "accent-green")], createdAt: "2026-09-25T06:57:14Z",
            logoRef: "blob://local-logo", logoUrl: "https://media.example.test/local-logo.png", name: "Local theme",
            updatedAt: "2026-09-25T06:57:14Z", userId: Fixture.owner
        )
    }

    private func renamed(_ theme: Theme, _ name: String) -> Theme {
        var renamed = theme
        renamed.name = name
        return renamed
    }

    private static func forged(_ theme: inout Theme, name: String) {
        theme.name = name
        theme.createdAt = "1900-01-01T00:00:00Z"
        theme.updatedAt = "1900-01-01T00:00:00Z"
        theme.userId = 999
        theme.logoUrl = "https://media.example.test/forged.png"
    }

    /// The row as the server sends it: every column, the server-owned ones included.
    private func echo(_ theme: Theme) -> [String: ReplicaValue] {
        var fields = theme.encode()
        fields["createdAt"] = .string(theme.createdAt)
        fields["updatedAt"] = .string(theme.updatedAt)
        fields["userId"] = .integer(Int64(theme.userId))
        fields["logoUrl"] = theme.logoUrl.map { .string($0) } ?? .null
        return fields
    }

    private func assertWritableBirth(_ op: ReplicaOp, file: StaticString = #filePath, line: UInt = #line) {
        XCTAssertEqual(op.verb, ReplicaOp.Verb.rowCreate, file: file, line: line)
        XCTAssertEqual(op.stream, "themes", file: file, line: line)
        XCTAssertEqual(Set(op.data?.keys ?? [:].keys), ["colors", "description", "heading", "logoRef", "name", "pinned"],
                       "the server rejects pull-only columns, even when the local model needs them to decode",
                       file: file, line: line)
    }

    func testAGeneratedBirthIsReadableBeforeAnyServerEchoAndPushesOnlyWritableColumns() async throws {
        let (store, transport, engine, themes) = try world()
        let born = theme()

        try await engine.write { tx in try tx.themes.create(born) }

        XCTAssertEqual(try themes.find(born.id), born)
        XCTAssertEqual(try themes.list(), [born], "an offline theme must appear immediately")
        XCTAssertEqual(try store.peekSnapshot("themes", born.id)?.data["userId"]?.int, born.userId)

        _ = try await engine.drain()

        let ops = await transport.pushedOps()
        XCTAssertEqual(ops.count, 1)
        assertWritableBirth(try XCTUnwrap(ops.first))
        XCTAssertEqual(try themes.find(born.id), born, "an acknowledgement without an echo retains the local value")
    }

    func testUpdatingAWritableFieldDoesNotReplaceLocalReadOnlyMetadata() async throws {
        let (_, transport, engine, themes) = try world()
        let born = theme()
        try await engine.write { tx in try tx.themes.create(born) }
        _ = try await engine.drain()

        try await engine.write { tx in try tx.themes.update(born.id) { Self.forged(&$0, name: "Renamed") } }

        XCTAssertEqual(try themes.find(born.id), renamed(born, "Renamed"))
        _ = try await engine.drain()
        let ops = await transport.pushedOps()
        XCTAssertEqual(ops.count, 2)
        assertWritableBirth(ops[0])
        XCTAssertEqual(ops[1].verb, ReplicaOp.Verb.rowPatch)
        XCTAssertEqual(ops[1].data, ["name": .string("Renamed")])
    }

    func testABirthAndUpdateKeepFullMetadataInsideTheTransaction() async throws {
        let (_, _, engine, themes) = try world()
        let born = theme()
        let edited = renamed(born, "Edited")

        try await engine.write { tx in
            try tx.themes.create(born)
            XCTAssertEqual(try tx.themes.find(born.id), born)
            try tx.themes.update(born.id) { Self.forged(&$0, name: "Edited") }
            XCTAssertEqual(try tx.themes.find(born.id), edited)
        }

        XCTAssertEqual(try themes.list(), [edited])
    }

    func testTheServerEchoReplacesBirthMetadataWithTheAuthoritativeSnapshot() async throws {
        let (_, transport, engine, themes) = try world()
        let born = theme()
        try await engine.write { tx in try tx.themes.create(born) }
        _ = try await engine.drain()
        var echoed = born
        echoed.createdAt = "2026-09-25T06:57:15Z"
        echoed.updatedAt = "2026-09-25T06:57:15Z"
        echoed.logoUrl = "https://media.example.test/resolved-logo.png"
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowSet(stream: "themes", id: born.id, type: nil, data: echo(echoed))], cursor: "1:", more: false
        ))

        _ = try await engine.pullOnce(shard: "user")

        XCTAssertEqual(try themes.find(born.id), echoed)
        let pending = try await engine.pendingOps()
        XCTAssertTrue(pending.isEmpty, "accepting an echo does not manufacture a client write")
    }

    func testARejectedBirthRemovesTheEntireLocalSnapshotAndItsDependentPatch() async throws {
        let (store, transport, engine, themes) = try world()
        let born = theme()
        try await engine.write { tx in try tx.themes.create(born) }
        try await engine.write { tx in try tx.themes.update(born.id) { $0.name = "Changed before rejection" } }
        XCTAssertNotNil(try themes.find(born.id))
        await transport.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "theme already exists") } }

        _ = try await engine.drain()

        XCTAssertNil(try themes.find(born.id))
        XCTAssertNil(try store.peekSnapshot("themes", born.id))
        let pending = try await engine.pendingOps()
        XCTAssertTrue(pending.isEmpty)
        assertWritableBirth(try XCTUnwrap(try store.peekParked().first).op())
    }
}
