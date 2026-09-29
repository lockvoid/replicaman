import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

final class RowReadCacheTests: XCTestCase {
    override func setUp() {
        super.setUp()
        CountingNote.decodeCounter.reset()
        CountingNote.decodeDelay.seconds = 0
    }

    func testWarmWhereAndFindMaterializeOncePerCommittedSequence() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<CountingNote>(engine: engine)

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("one")]
        )

        XCTAssertEqual(try notes.list().map(\.title), ["one"])
        XCTAssertEqual(CountingNote.decodeCounter.count, 1)
        XCTAssertEqual(try notes.list().map(\.title), ["one"])
        XCTAssertEqual(try notes.find("n1")?.title, "one")
        XCTAssertEqual(CountingNote.decodeCounter.count, 1)

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("two")]
        )

        let rows = try notes.list()
        XCTAssertEqual(rows.map(\.title), ["two"])
        XCTAssertEqual(try notes.find("n1"), rows.first)
        XCTAssertEqual(CountingNote.decodeCounter.count, 2)
    }

    /// A find is a POINT read. Under a write cadence (the device pipeline
    /// writes a cook row every few hundred ms) the stream's picture is stale
    /// almost always, and a find that rebuilt it walked every row of the
    /// stream on the caller's thread — main, for the UI's reads.
    ///
    /// KILL: route `ReplicaReads.find` back through `materializedRows` and
    /// the unchanged row's find decodes the CHANGED row on its way.
    func testFindAfterAWriteReadsOneRowInsteadOfRematerializingTheStream() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<CountingNote>(engine: engine)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("one")])
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("two")])
        XCTAssertEqual(try notes.list().map(\.title), ["one", "two"])
        XCTAssertEqual(CountingNote.decodeCounter.count, 2)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("one!")])

        // The unchanged row comes back from its own decoded record; nothing
        // else in the stream is touched.
        XCTAssertEqual(try notes.find("n2")?.title, "two")
        XCTAssertEqual(CountingNote.decodeCounter.count, 2)
        // The changed row pays exactly its own decode.
        XCTAssertEqual(try notes.find("n1")?.title, "one!")
        XCTAssertEqual(CountingNote.decodeCounter.count, 3)
    }

    func testConcurrentColdReadsShareOneMaterialization() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<CountingNote>(engine: engine)

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("one")]
        )
        CountingNote.decodeCounter.reset()
        CountingNote.decodeDelay.seconds = 0.05
        defer { CountingNote.decodeDelay.seconds = 0 }

        let outcomes = LockedOutcomes()
        DispatchQueue.concurrentPerform(iterations: 2) { _ in
            outcomes.append(Result { try notes.list().map(\.title) })
        }

        for outcome in outcomes.values {
            XCTAssertEqual(try outcome.get(), ["one"])
        }
        XCTAssertEqual(CountingNote.decodeCounter.count, 1)
    }

    func testRolledBackCheckpointKeepsTheWarmMaterialization() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let notes = RowStream<CountingNote>(engine: engine)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        XCTAssertEqual(try notes.list().map(\.title), ["one"])
        XCTAssertEqual(CountingNote.decodeCounter.count, 1)

        struct Fault: Error {}
        await engine.setCheckpointFault { throw Fault() }
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "two")], cursor: "6:", more: false
        ))
        do {
            _ = try await engine.pullOnce(shard: "user")
            XCTFail("the injected checkpoint fault must surface")
        } catch {}

        XCTAssertEqual(try notes.list().map(\.title), ["one"])
        XCTAssertEqual(CountingNote.decodeCounter.count, 1)

        await engine.setCheckpointFault(nil)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "two")], cursor: "6:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        XCTAssertEqual(try notes.list().map(\.title), ["two"])
        XCTAssertEqual(CountingNote.decodeCounter.count, 2)
    }

    func testBatchInvalidationResetsAfterRollbackAndPublishesTheLastMutation() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<CountingNote>(engine: engine)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("saved")])
        XCTAssertEqual(try notes.list().map(\.title), ["saved"])

        struct Fault: Error {}
        do {
            try await store.pool.write { db in
                for index in 0..<100 {
                    try store.upsertSnapshot(db, stream: "notes", rowId: "n1", shard: "user",
                        type: nil, data: ["title": .string("rolled back \(index)")])
                }
                throw Fault()
            }
            XCTFail("the transaction must roll back")
        } catch is Fault {
            // Expected injection: the durable row and warm cache must survive.
        }
        XCTAssertEqual(try notes.list().map(\.title), ["saved"])

        try await store.pool.write { db in
            for index in 0..<100 {
                try store.upsertSnapshot(db, stream: "notes", rowId: "n1", shard: "user",
                    type: nil, data: ["title": .string("committed \(index)")])
            }
        }
        XCTAssertEqual(try notes.list().map(\.title), ["committed 99"])
        XCTAssertEqual(try notes.find("n1")?.title, "committed 99")
    }

    func testTypedWatchPrimesSynchronousListAndFind() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let notes = RowStream<CountingNote>(engine: engine)
        let delivered = Tally()

        let watch = notes.watch(includeInitial: true) { _ in delivered.bump() }

        await eventually("the typed watch baseline was not delivered") {
            delivered.count == 1
        }

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        await eventually("the typed watch did not deliver") {
            delivered.count == 2
        }

        XCTAssertEqual(CountingNote.decodeCounter.count, 1)
        XCTAssertEqual(try notes.list().map(\.title), ["one"])
        XCTAssertEqual(try notes.find("n1")?.title, "one")
        XCTAssertEqual(CountingNote.decodeCounter.count, 1)
        watch.cancel()
    }

    func testTypedWatchCannotPairANewSequenceWithTheOldCacheEntry() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let notes = RowStream<CountingNote>(engine: engine)

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("one")]
        )
        XCTAssertEqual(try notes.list().map(\.title), ["one"])
        XCTAssertEqual(CountingNote.decodeCounter.count, 1)

        let pictures = TitlePictures()
        let watch = notes.watch(includeInitial: true) { models in pictures.record(models.map(\.title)) }
        await eventually("the typed-watch baseline was not delivered") {
            pictures.count == 1
        }

        let releaseCommitCallbacks = DispatchSemaphore(value: 0)
        let writer = Task.detached {
            try store.pool.write { db in
                db.afterNextTransaction { _ in
                    _ = releaseCommitCallbacks.wait(timeout: .now() + 2)
                }
                try store.upsertSnapshot(
                    db,
                    stream: "notes",
                    rowId: "n1",
                    shard: "user",
                    type: nil,
                    data: ["title": .string("two")]
                )
            }
        }

        await eventually("the post-commit typed-watch value was not delivered") {
            pictures.count == 2
        }
        releaseCommitCallbacks.signal()
        try await writer.value

        let deliveredAfterCommit = pictures.value(at: 1)
        XCTAssertEqual(deliveredAfterCommit, ["two"])
        XCTAssertEqual(try notes.list().map(\.title), ["two"])
        XCTAssertEqual(try notes.find("n1")?.title, "two")
        XCTAssertEqual(CountingNote.decodeCounter.count, 2)
        watch.cancel()
    }

}

private struct CountingNote: ReplicaWritableRowModel, Equatable {
    static let streamName = "notes"
    static let decodeCounter = LockedCounter()
    static let decodeDelay = LockedDelay()

    var id: String
    var title: String?

    init?(id: String, type: String?, data: [String: ReplicaValue]) {
        Self.decodeCounter.increment()
        Thread.sleep(forTimeInterval: Self.decodeDelay.seconds)
        guard type == nil else { return nil }
        self.id = id
        self.title = data["title"]?.string
    }

    var typeName: String? { nil }

    func encode() -> [String: ReplicaValue] {
        title.map { ["title": .string($0)] } ?? [:]
    }
}

private final class LockedDelay: @unchecked Sendable {
    private let lock = NSLock()
    private var value: TimeInterval = 0

    var seconds: TimeInterval {
        get { lock.withLock { value } }
        set { lock.withLock { value = newValue } }
    }
}

private final class LockedOutcomes: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [Result<[String?], Error>] = []

    var values: [Result<[String?], Error>] {
        lock.withLock { storage }
    }

    func append(_ outcome: Result<[String?], Error>) {
        lock.withLock { storage.append(outcome) }
    }
}

private final class TitlePictures: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [[String?]] = []

    var count: Int { lock.withLock { storage.count } }

    func record(_ value: [String?]) {
        lock.withLock { storage.append(value) }
    }

    func value(at index: Int) -> [String?] {
        lock.withLock { storage[index] }
    }
}

private final class LockedCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    var count: Int {
        lock.withLock { value }
    }

    func increment() {
        lock.withLock { value += 1 }
    }

    func reset() {
        lock.withLock { value = 0 }
    }
}
