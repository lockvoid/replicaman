import Foundation
import XCTest
@_spi(SignOutIdentityTransition) @testable import ReplicaMan

/// Priorities order eligible work. A frozen writer prefix always finishes first.
final class LaneDrainTests: XCTestCase {

    private func lanes(of store: ReplicaStateStore) throws -> [String: String] {
        try store.pool.read { db in
            var byRow: [String: String] = [:]
            for entry in try store.pending(db) {
                let op = try entry.op()
                byRow[op.rowId] = try store.lane(db, entryId: entry.id).rawValue
            }
            return byRow
        }
    }

    // MARK: - The scope

    func testWritesInsideTheScopeClaimTheLaneAndOutsideStayBulk() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        // The helper knows nothing about lanes — that is the reason the scope
        // is a scope: `sendInternal` calls something that writes elements and
        // must not have to thread a parameter down to it.
        @Sendable func helperThatKnowsNothingAboutLanes() async throws {
            try await engine.saveRow(stream: "notes", id: "nested", type: nil, data: ["title": .string("x")])
        }

        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "hot", type: nil, data: ["title": .string("typed")])
            try await helperThatKnowsNothingAboutLanes()
        }
        try await engine.saveRow(stream: "notes", id: "cold", type: nil, data: ["title": .string("imported")])

        let claimed = try lanes(of: store)
        XCTAssertEqual(claimed["hot"], "interactive")
        XCTAssertEqual(claimed["nested"], "interactive", "a nested helper inherits the claimed lane")
        XCTAssertEqual(claimed["cold"], "bulk", "the default is background — a stream says nothing on its own")
    }
    /// Lanes are task-local, and a `Task { }` inherits them — so background
    /// work started from inside a user action (ProcessorMan's `Host.pulse`
    /// spawns its runs that way) would ride the fast lane and flood the very
    /// thing it exists to keep clear. Claiming bulk RESETS the scope.
    func testBulkNestedInsideAnInteractiveActionResetsTheLane() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "message", type: nil, data: ["title": .string("typed")])
            try await engine.lane(.bulk) {
                try await engine.saveRow(stream: "notes", id: "cook", type: nil, data: ["title": .string("progress")])
            }
        }

        let claimed = try lanes(of: store)
        XCTAssertEqual(claimed["message"], "interactive")
        XCTAssertEqual(claimed["cook"], "bulk", "fan-out started inside an action must not inherit its urgency")
    }

    func testTheLaneSurvivesAStoreReopen() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("lane-durability-\(UUID().uuidString).sqlite").path
        let store = try ReplicaStateStore(path: path)
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("t")])
        }

        try await engine.close()
        let reopened = try ReplicaStateStore(path: path)
        let claimed = try await Task { try reopened.pool.read { db -> String in
            let entry = try reopened.pending(db)[0]
            return try reopened.lane(db, entryId: entry.id).rawValue
        } }.value

        XCTAssertEqual(claimed, "interactive", "a queued message is still urgent after the app is killed")
    }

    // MARK: - Frozen prefix ordering

    func testInteractiveWorkWaitsForTheFrozenBulkPrefix() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        for i in 0..<19 {
            try await engine.saveRow(stream: "notes", id: "bulk\(i)", type: nil, data: ["title": .string("i\(i)")])
        }
        let bulkReleased = Gate()
        let bulkArrived = Gate()
        let flags = Flags()
        await transport.onPush { ops in
            if ops.contains(where: { $0.rowId.hasPrefix("bulk") }) {
                flags.set("bulkInFlight", true)
                bulkArrived.release()
                await bulkReleased.wait()
                flags.set("bulkInFlight", false)
            } else if ops.contains(where: { $0.rowId == "message" }) {
                flags.set("overtook", flags.get("bulkInFlight"))
            }
        }
        let bulkDrain = Task { try await engine.drain(.bulk) }
        // The bulk push signals its OWN arrival from inside `push` — the
        // interactive write below is provably racing a flight that is on the
        // wire, not one that a sleep hoped had started.
        await bulkArrived.wait()

        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "message", type: nil, data: ["title": .string("typed")])
        }
        let interactive = Task { try await engine.drain(.interactive) }
        await Task.yield()
        bulkReleased.release()
        _ = try await bulkDrain.value
        _ = try await interactive.value

        XCTAssertFalse(flags.get("overtook"), "a request cannot pass the immutable writer prefix")
        let sent = await transport.pushedBatches.flatMap { $0.map(\.rowId) }
        XCTAssertEqual(sent.last, "message")
        XCTAssertEqual(Set(sent).count, 20)
    }

    /// Promotion can move an entry the OTHER lane is holding on the wire. If
    /// the drain then re-selects it, the same create ships twice and comes
    /// back a collision that reverts the row.
    func testAPromotedEntryAlreadyOnTheWireIsNotSentTwice() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "element", type: nil, data: ["title": .string("clip")])
        let release = Gate()
        let arrived = Gate()
        await transport.onPush { ops in
            guard ops.contains(where: { $0.rowId == "element" }) else { return }
            arrived.release()
            await release.wait()
        }
        let bulkDrain = Task { try await engine.drain(.bulk) }
        await arrived.wait()

        // The user attaches that very element — promotion pulls it up while
        // its push is still open.
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "attachment", type: nil,
                                     data: ["recordId": .string("element")])
        }
        let interactive = Task { try await engine.drain(.interactive) }
        release.release()
        _ = try await bulkDrain.value
        _ = try await interactive.value

        let sent = await transport.pushedBatches.flatMap { $0.map(\.rowId) }
        XCTAssertEqual(sent.filter { $0 == "element" }.count, 1,
                       "an in-flight entry must not be re-sent by the lane that promoted it")
    }

    /// Sign-out flushes the journal through a pinned transport and then wipes.
    /// If that flush only drains one lane, the user's just-typed message is
    /// destroyed by the wipe.
    func testTheSignOutFlushDrainsBothLanes() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "import", type: nil, data: ["title": .string("bulk")])
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "message", type: nil, data: ["title": .string("typed")])
        }

        await engine.seal()
        let pinned = StubTransport()
        _ = try await engine.sealAndDrain(using: pinned)

        let sent = await pinned.pushedBatches.flatMap { $0.map(\.rowId) }
        XCTAssertEqual(Set(sent), ["import", "message"],
                       "everything owed goes out before the wipe, whatever lane it claimed")
    }

    func testOrderIsPreservedWithinALane() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.lane(.interactive) {
            for i in 0..<5 {
                try await engine.saveRow(stream: "notes", id: "n\(i)", type: nil, data: ["title": .string("t\(i)")])
            }
        }
        _ = try await engine.drain(.interactive)

        let sent = await transport.pushedBatches.flatMap { $0.map(\.rowId) }
        XCTAssertEqual(sent, ["n0", "n1", "n2", "n3", "n4"])
    }

    // MARK: - The invariants that keep overtaking safe

    /// DEFECT THIS PREVENTS: a bulk patch passing the interactive create of
    /// the same row — the server refuses an update to a row it has never seen.
    func testALaterBulkWriteJoinsTheLaneItsRowIsAlreadyQueuedOn() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())

        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("created")])
        }
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("edited")])

        XCTAssertEqual(try lanes(of: store)["n1"], "interactive",
                       "an update must never outrun the create of its own row")
    }

    /// DEFECT THIS PREVENTS: attaching a clip
    /// whose element create is still queued in bulk. The attachment names the
    /// element id; if it overtakes, the door raises "unknown element", the
    /// entry parks, and the local row is reverted.
    func testAnInteractiveWriteNamingAPendingBulkRowPromotesThatRow() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        // The import authored this element and has not pushed yet.
        try await engine.saveRow(stream: "notes", id: "element-7", type: nil, data: ["title": .string("clip")])
        // The user attaches it to a message they just typed.
        try await engine.lane(.interactive) {
            try await engine.saveRow(stream: "notes", id: "attachment-1", type: nil,
                                     data: ["recordId": .string("element-7")])
        }

        XCTAssertEqual(try lanes(of: store)["element-7"], "interactive",
                       "what an interactive write depends on is interactive too")

        _ = try await engine.drain(.interactive)
        let sent = await transport.pushedBatches.flatMap { $0.map(\.rowId) }
        XCTAssertEqual(sent, ["element-7", "attachment-1"], "and it still lands first")
    }

    /// Thread-safe flag board for the mid-push observation.
    private final class Flags: @unchecked Sendable {
        private let lock = NSLock()
        private var values: [String: Bool] = [:]
        func set(_ key: String, _ value: Bool) { lock.lock(); values[key] = value; lock.unlock() }
        func get(_ key: String) -> Bool { lock.lock(); defer { lock.unlock() }; return values[key] ?? false }
    }

    /// A hold that many pushes may await (an `XCTestExpectation` may only be
    /// waited on once — and a duplicate push is exactly what one of these
    /// tests is hunting).
    final class Gate: @unchecked Sendable {
        private let lock = NSLock()
        private var opened = false

        func release() { lock.lock(); opened = true; lock.unlock() }

        var isOpen: Bool { lock.lock(); defer { lock.unlock() }; return opened }

        func wait(seconds: TimeInterval = 5) async {
            let deadline = Date().addingTimeInterval(seconds)
            while !isOpen, Date() < deadline {
                try? await Task.sleep(for: .milliseconds(10))
            }
        }
    }
}
