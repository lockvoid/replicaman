import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// A one-row patch must cost one row's decode. During a processing storm the
/// stream cache invalidates per commit; without per-row reuse every cold
/// reload re-decodes EVERY row's JSON, model, and eagerly-materialized
/// payload. The superseded cache entry is a reuse DONOR: rows
/// whose raw snapshot text is unchanged carry their decoded record and model
/// across materializations; only actually-changed rows decode.
final class RowReuseTests: XCTestCase {
    override func setUp() {
        super.setUp()
        ReusableNote.decodeCounter.reset()
    }

    func testOneRowPatchDecodesExactlyOneModel() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<ReusableNote>(engine: engine)
        for id in ["n1", "n2", "n3"] {
            try await engine.saveRow(
                stream: "notes", id: id, type: nil, data: ["title": .string("seed-\(id)")]
            )
        }

        XCTAssertEqual(try notes.list().count, 3)
        let seeded = ReusableNote.decodeCounter.count
        XCTAssertEqual(seeded, 3)

        try await engine.saveRow(
            stream: "notes", id: "n2", type: nil, data: ["title": .string("patched")]
        )

        XCTAssertEqual(
            try notes.list().map(\.title),
            ["seed-n1", "patched", "seed-n3"]
        )
        XCTAssertEqual(
            ReusableNote.decodeCounter.count, seeded + 1,
            "a one-row patch re-decoded the whole stream — unchanged rows must reuse their donor decode"
        )
    }

    func testAProgressShapedStormCostsOneDecodePerCommit() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<ReusableNote>(engine: engine)
        for id in ["n1", "n2", "n3", "n4"] {
            try await engine.saveRow(
                stream: "notes", id: id, type: nil, data: ["progress": .number(0)]
            )
        }
        XCTAssertEqual(try notes.list().count, 4)
        let seeded = ReusableNote.decodeCounter.count

        // The storm: one row marches, every tick commits, a reader follows
        // each commit — the exact shape of cook progress during processing.
        for tick in 1...5 {
            try await engine.saveRow(
                stream: "notes", id: "n1", type: nil,
                data: ["progress": .number(Double(tick) / 10)]
            )
            XCTAssertEqual(try notes.list().count, 4)
        }

        XCTAssertEqual(
            ReusableNote.decodeCounter.count, seeded + 5,
            "five one-row ticks must cost five decodes, not five whole-stream reloads"
        )
    }

    func testDeleteReusesTheSurvivors() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<ReusableNote>(engine: engine)
        for id in ["n1", "n2", "n3"] {
            try await engine.saveRow(
                stream: "notes", id: id, type: nil, data: ["title": .string(id)]
            )
        }
        XCTAssertEqual(try notes.list().count, 3)
        let seeded = ReusableNote.decodeCounter.count

        try await engine.deleteRow(stream: "notes", id: "n2")

        XCTAssertEqual(try notes.list().map(\.id), ["n1", "n3"])
        XCTAssertEqual(
            ReusableNote.decodeCounter.count, seeded,
            "a delete must not re-decode the surviving rows"
        )
    }

    func testATypeFlipDecodesFresh() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let notes = RowStream<ReusableNote>(engine: engine)
        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("one")]
        )
        XCTAssertEqual(try notes.list().first?.title, "one")
        let seeded = ReusableNote.decodeCounter.count

        // Same data text, different STI type, arriving the way type flips
        // really do — a pulled row.set. The donor must NOT be reused: the
        // model's decode switches on `type`.
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.rowSet(stream: "notes", id: "n1", type: "Special", data: ["title": .string("one")])],
            cursor: "2:",
            more: false
        ))
        try await engine.pullOnce(shard: "user")

        XCTAssertEqual(try notes.list().first?.special, true)
        XCTAssertEqual(ReusableNote.decodeCounter.count, seeded + 1)
    }
}

private struct ReusableNote: ReplicaWritableRowModel, Equatable {
    static let streamName = "notes"
    static let decodeCounter = LockedTally()

    var id: String
    var title: String?
    var special = false

    init?(id: String, type: String?, data: [String: ReplicaValue]) {
        Self.decodeCounter.increment()
        self.id = id
        self.title = data["title"]?.string
        self.special = type == "Special"
    }

    var typeName: String? { special ? "Special" : nil }

    func encode() -> [String: ReplicaValue] {
        title.map { ["title": .string($0)] } ?? [:]
    }
}

private final class LockedTally: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    var count: Int { lock.withLock { value } }
    func increment() { lock.withLock { value += 1 } }
    func reset() { lock.withLock { value = 0 } }
}
