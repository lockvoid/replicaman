import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// Protocol 2 pull rounds: answers with more to come are staged; the answer
/// that completes the round publishes every staged page, the rebased local
/// authoring and the cursor together. A staged round survives the process;
/// a cursor the server no longer knows restarts the shard from a baseline.
final class PullRoundTests: XCTestCase {
    private func requestedCursors(_ transport: StubTransport) async -> [String?] {
        await transport.events.compactMap {
            if case .pull(_, let cursor) = $0 { return cursor } else { return nil }
        }
    }

    private func staging(_ store: ReplicaStateStore) async throws -> (pages: Int, round: ReplicaRound?) {
        try await store.pool.read { db in
            (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM download_pages") ?? 0, try store.round(db, shard: "user"))
        }
    }

    func testAStagedRoundStaysInvisibleUntilItsLastPagePublishesEverything() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n0", title: "zero")], cursor: "c1", more: false))
        try await engine.pullOnce()
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "one")], cursor: "c2", more: true))
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n2", title: "two")], cursor: "c3", more: false))

        let staged = try await engine.pullOnce()

        XCTAssertEqual(staged, 0)
        XCTAssertNil(try store.peekSnapshot("notes", "n1"))
        let stagedCursor = try await engine.currentCursor()
        XCTAssertEqual(stagedCursor, "c1", "the published cursor waits for the round")
        let during = try await staging(store)
        XCTAssertEqual(during.pages, 1)
        XCTAssertEqual(during.round, ReplicaRound(cursor: "c2", reset: false, visible: 0))

        let published = try await engine.pullOnce()

        XCTAssertEqual(published, 2, "the round publishes every staged frame")
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("one"))
        XCTAssertEqual(try store.peekSnapshot("notes", "n2")?.data["title"], .string("two"))
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "c3")
        let after = try await staging(store)
        XCTAssertEqual(after.pages, 0)
        XCTAssertNil(after.round)
        let cursors = await requestedCursors(transport)
        XCTAssertEqual(cursors, [nil, "c1", "c2"])
    }

    /// 10-01: a round staged against a server that then changed its paging
    /// kept resuming with its old cursor, and the server's answer could never
    /// be published — a delta for a document the device had no baseline for,
    /// on every launch. A round that cannot be published is forgotten, and the
    /// shard bootstraps again from nothing.
    func testARoundThatCannotBePublishedIsForgottenAndTheShardBootstrapsAgain() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "one")], cursor: "c1", more: true))
        await transport.queuePull(shard: "user", .init(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 1, codec: "stub@1", payload: Data("+a".utf8))],
            cursor: "c2", more: false))
        await transport.queuePull(shard: "user", .init(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SNAP".utf8), data: ["name": .string("Plans")])],
            cursor: "c3", more: false))

        let published = try await engine.pullUntilCaughtUp(shards: ["user"])

        XCTAssertEqual(published, 1, "the second round publishes the baseline")
        XCTAssertNotNil(try store.peekDoc("boards", "b1"))
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "c3")
        let after = try await staging(store)
        XCTAssertEqual(after.pages, 0)
        XCTAssertNil(after.round)
        let cursors = await requestedCursors(transport)
        XCTAssertEqual(cursors, [nil, "c1", nil], "the poisoned round is dropped, not resumed")
    }

    /// A baseline round that still cannot be published is the server's fault,
    /// and the failure reaches the caller — forgetting it again would spin.
    func testABootstrapThatCannotBePublishedFails() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 1, codec: "stub@1", payload: Data("+a".utf8))],
            cursor: "c1", more: false))

        do {
            _ = try await engine.pullUntilCaughtUp(shards: ["user"])
            XCTFail("a delta with no baseline on a bootstrap must fail")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "InvalidResponse")
        }
        let cursors = await requestedCursors(transport)
        XCTAssertEqual(cursors, [nil])
    }

    /// The server folded a document's history and shipped the fold's own delta
    /// to nobody; the device's base lacks what the next delta stands on. That
    /// history cannot be absorbed and only a baseline replaces the base: the
    /// round is forgotten and the shard baselines again, once, in the same call.
    func testHistoryTheBaseCannotAbsorbForgetsTheRoundAndTheShardBaselinesAgain() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, schema: Fixture.causalSchema, codecs: [CausalCodec()])
        await transport.queuePull(shard: "user", .init(frames: [
            .docSnapshot(stream: "boards", id: "b1", codec: "causal@1", snapshot: CausalCodec.payload(1...3), data: ["name": .string("Board")]),
        ], cursor: "c1", more: false))
        try await engine.pullUntilCaughtUp(shards: ["user"])
        await transport.queuePull(shard: "user", .init(frames: [
            .docDelta(stream: "boards", id: "b1", seq: 1, codec: "causal@1", payload: CausalCodec.payload(5...5)),
            .rowSet(stream: "boards", id: "b1", type: nil, data: ["name": .string("Renamed")]),
            Fixture.note("n2", title: "two"),
        ], cursor: "c2", more: false))
        await transport.queuePull(shard: "user", .init(frames: [
            .docSnapshot(stream: "boards", id: "b1", codec: "causal@1", snapshot: CausalCodec.payload(1...5), data: ["name": .string("Renamed")]),
            Fixture.note("n2", title: "two"),
        ], cursor: "b1", more: false))

        let published = try await engine.pullUntilCaughtUp(shards: ["user"])

        XCTAssertEqual(published, 2, "the baseline publishes")
        XCTAssertEqual(try store.peekDoc("boards", "b1").map { CausalCodec.tokens($0.fold) }, Set(1...5))
        XCTAssertEqual(try store.peekSnapshot("notes", "n2")?.data["title"], .string("two"))
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "b1")
        let cursors = await requestedCursors(transport)
        XCTAssertEqual(cursors, [nil, "c1", nil], "the round is forgotten and the shard baselines")
        XCTAssertEqual(try store.recoveryRecords().count, 0, "nothing was authored, nothing is archived")
    }

    /// A second round the shard cannot publish, in the same call, is the
    /// server's fault: its failure reaches the caller instead of another download.
    func testASecondRoundTheShardCannotPublishReachesTheCaller() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        for index in 0..<3 {
            await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "one")], cursor: "p1-\(index)", more: true))
            await transport.queuePull(shard: "user", .init(frames: [
                .docDelta(stream: "boards", id: "b9", seq: 1, codec: "stub@1", payload: Data("x".utf8)),
            ], cursor: "p2-\(index)", more: false))
        }

        do {
            _ = try await engine.pullUntilCaughtUp(shards: ["user"])
            XCTFail("a baseline that cannot be published twice must fail")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "InvalidResponse")
        }
        let pulls = await transport.pullCount
        XCTAssertEqual(pulls, 4, "one baseline forgotten, the second one's failure thrown")
        XCTAssertNil(try store.peekSnapshot("notes", "n1"), "no partial round published")
    }

    /// A fresh install's first launch: the warm-up and the knocker pull the
    /// user shard at once, and the snapshot is more than one page. Both callers
    /// must finish the round — on 2026-09-30 the second spun on the first's
    /// finished flight, holding the actor at 99% CPU with no request sent.
    func testTwoCallersOnAPageWithMoreToComeBothFinishTheRound() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.delayPulls(nanos: 200_000_000)
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "one")], cursor: "c1", more: true))
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n2", title: "two")], cursor: "c2", more: false))

        let first = Task { try await engine.pullUntilCaughtUp(shards: ["user"]) }
        try await Task.sleep(nanoseconds: 50_000_000)
        let second = Task { try await engine.pullUntilCaughtUp(shards: ["user"]) }
        let finished = try await withThrowingTaskGroup(of: Bool.self) { group in
            group.addTask {
                _ = try await first.value
                _ = try await second.value
                return true
            }
            group.addTask {
                try await Task.sleep(nanoseconds: 5_000_000_000)
                first.cancel()
                second.cancel()
                return false
            }
            let result = try await group.next() ?? false
            group.cancelAll()
            return result
        }

        XCTAssertTrue(finished, "two callers on a staged page never finished the round")
        XCTAssertEqual(try store.peekSnapshot("notes", "n2")?.data["title"], .string("two"))
        let cursors = await requestedCursors(transport)
        XCTAssertEqual(cursors, [nil, "c1"])
    }

    func testAStagedRoundResumesFromItsCursorAfterTheProcessEnds() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "one")], cursor: "c1", more: true))
        try await engine.pullOnce()
        try await engine.close()

        let reopened = try ReplicaStateStore(path: store.path.path)
        let resumed = Fixture.engine(store: reopened, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n2", title: "two")], cursor: "c2", more: false))
        let published = try await resumed.pullOnce()

        XCTAssertEqual(published, 2)
        XCTAssertEqual(try reopened.peekSnapshot("notes", "n1")?.data["title"], .string("one"))
        XCTAssertEqual(try reopened.peekSnapshot("notes", "n2")?.data["title"], .string("two"))
        let cursors = await requestedCursors(transport)
        XCTAssertEqual(cursors, [nil, "c1"], "the reopened round continues, it does not start over")
        try await resumed.close()
    }

    func testAnInvalidCursorDiscardsStagingAndStartsABaselineRound() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [
            Fixture.note("n1", title: "one"), Fixture.note("n2", title: "two"),
        ], cursor: "c1", more: false))
        try await engine.pullOnce()
        await transport.failPushes(true)
        try await engine.saveRow(stream: "notes", id: "n3", type: nil, data: ["title": .string("unsent")])
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "staged")], cursor: "c2", more: true))
        try await engine.pullOnce()
        await transport.protocolFixture.forgetCursors()

        let restarted = try await engine.pullOnce()

        XCTAssertEqual(restarted, 0)
        let staged = try await staging(store)
        XCTAssertEqual(staged.pages, 0, "the refused round's pages are gone")
        XCTAssertEqual(staged.round?.cursor, nil)
        XCTAssertEqual(staged.round?.reset, true)
        let kept = try await engine.currentCursor()
        XCTAssertEqual(kept, "c1", "the published state stands until the baseline publishes")
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("one"))

        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n2", title: "fresh")], cursor: "b1", more: false))
        try await engine.pullOnce()

        XCTAssertNil(try store.peekSnapshot("notes", "n1"), "a baseline replaces the shard's base")
        XCTAssertEqual(try store.peekSnapshot("notes", "n2")?.data["title"], .string("fresh"))
        XCTAssertEqual(try store.peekSnapshot("notes", "n3")?.data["title"], .string("unsent"), "unsent authoring stays")
        XCTAssertEqual(try store.peekPending().count, 1)
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "b1")
        let cursors = await requestedCursors(transport)
        XCTAssertEqual(cursors, [nil, "c1", nil])
    }

    func testAcceptedAuthoringOutlivesARoundThatStartedBeforeItsAcceptance() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "server")], cursor: "c1", more: false))
        try await engine.pullOnce()
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "server")], cursor: "c2", more: true))
        try await engine.pullOnce()

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])
        try await engine.drain()
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n2", title: "two")], cursor: "c3", more: false))
        try await engine.pullOnce()

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("mine"),
                       "the round began before the acceptance; its base cannot retire the overlay")
        let retained = try await store.pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM intents WHERE state = 'accepted'") }
        XCTAssertEqual(retained, 1)

        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n1", title: "mine")], cursor: "c4", more: false))
        try await engine.pullOnce()

        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("mine"))
        let remaining = try await store.pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM intents WHERE state = 'accepted'") }
        XCTAssertEqual(remaining, 0, "a round that began after the acceptance retires it")
    }

    /// A round removes exactly the accepted intents whose sequence its
    /// `visible` covers: one accepted before the round began goes, one
    /// accepted while it was staged stays visible for the next round.
    func testARoundRetiresOnlyTheAcceptedIntentsItsVisibleCovers() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        try await engine.pullOnce()
        func accepted() throws -> [[String]] {
            try store.pool.read { db in
                try Row.fetchAll(db, sql: "SELECT row_id, sequence FROM intents WHERE state = 'accepted' ORDER BY sequence")
                    .map { [$0["row_id"], String($0["sequence"] as Int64)] }
            }
        }

        try await engine.saveRow(stream: "notes", id: "before", type: nil, data: ["title": .string("mine")])
        try await engine.drain()
        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("n0", title: "zero")], cursor: "c1", more: true))
        try await engine.pullOnce()
        let staged = try await staging(store)
        XCTAssertEqual(staged.round?.visible, 1)

        try await engine.saveRow(stream: "notes", id: "during", type: nil, data: ["title": .string("mine too")])
        try await engine.drain()
        XCTAssertEqual(try accepted(), [["before", "1"], ["during", "2"]])

        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("before", title: "mine")], cursor: "c2", more: false))
        try await engine.pullOnce()

        XCTAssertEqual(try accepted(), [["during", "2"]], "the round retired only what it started after")
        XCTAssertEqual(try store.peekSnapshot("notes", "during")?.data["title"], .string("mine too"),
                       "an accepted write the round cannot show stays visible")

        await transport.queuePull(shard: "user", .init(frames: [Fixture.note("during", title: "mine too")], cursor: "c3", more: false))
        try await engine.pullOnce()

        XCTAssertEqual(try accepted(), [])
        XCTAssertEqual(try store.peekSnapshot("notes", "during")?.data["title"], .string("mine too"))
    }

    func testADocumentsDeltasAndItsRowApplyInOrderInOneRound() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", .init(frames: [
            .docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SNAP".utf8), data: ["name": .string("Plans")]),
        ], cursor: "c1", more: false))
        try await engine.pullOnce()
        await transport.queuePull(shard: "user", .init(frames: [
            .docDelta(stream: "boards", id: "b1", seq: 1, codec: "stub@1", payload: Data("+a".utf8)),
            .docDelta(stream: "boards", id: "b1", seq: 2, codec: "stub@1", payload: Data("+b".utf8)),
            .rowSet(stream: "boards", id: "b1", type: nil, data: ["name": .string("Renamed")]),
        ], cursor: "c2", more: false))

        let published = try await engine.pullOnce()

        XCTAssertEqual(published, 3)
        let base = try await store.pool.read { try store.baseRow($0, stream: "boards", id: "b1") }
        XCTAssertEqual(base?.fold, Data("SNAP+a+b".utf8))
        XCTAssertEqual(try store.peekSnapshot("boards", "b1")?.data["name"], .string("Renamed"))
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "c2")
    }
}
