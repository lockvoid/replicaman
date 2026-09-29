import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// Idempotent pull-apply: the server echoes rows a
/// device just pushed — an import wave rang 40+ originless doorbells,
/// each pull blind-upserting identical bytes → commit → observation →
/// the whole main-side derivation chain re-ran on zero information.
/// LAW: a `.rowSet` frame identical to the local snapshot (type + data)
/// applies NOTHING — no upsert, no rebase, no commit, no watch signal.
final class IdempotentApplyTests: XCTestCase {

    private static func note(_ id: String, _ title: String) -> ReplicaFrame {
        .rowSet(stream: "notes", id: id, type: nil, data: ["title": .string(title)])
    }

    private static func pull(_ frames: [ReplicaFrame], cursor: String) -> ReplicaPullResponse {
        ReplicaPullResponse(frames: frames, cursor: cursor, more: false)
    }

    /// KILL: blind `upsertSnapshot` on every frame — the identical pull
    /// bumps the stream's change sequence and every watch downstream
    /// wakes for zero information. The sequence IS the watch signal
    /// source (`watchSignal` tracks it), so asserting it deterministic-
    /// ally beats counting deliveries (GRDB coalesces rapid commits).
    func testPullingIdenticalContentDoesNotBumpTheChangeSequence() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", Self.pull([Self.note("n1", "a"), Self.note("n2", "b")], cursor: "0:1"))
        _ = try await engine.pullOnce(shard: "user")
        let before = try await store.pool.read { try store.changeSequence($0, stream: "notes") }

        // The echo: same rows, same bytes, new cursor.
        await transport.queuePull(shard: "user", Self.pull([Self.note("n1", "a"), Self.note("n2", "b")], cursor: "0:2"))
        _ = try await engine.pullOnce(shard: "user")

        let after = try await store.pool.read { try store.changeSequence($0, stream: "notes") }
        XCTAssertEqual(after, before, "an identical pull must not move the change sequence — echo carries zero information")
    }

    /// KILL: skip the compare on a CHANGED row — the newer server value
    /// never lands.
    func testChangedContentStillAppliesAndSignals() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", Self.pull([Self.note("n1", "a")], cursor: "0:1"))
        _ = try await engine.pullOnce(shard: "user")

        let signals = Tally()
        let stream = await engine.watchSignal(stream: "notes", includeInitial: true)
        let listener = Task { for await _ in stream { signals.bump() } }
        await eventually(timeout: 3, "baseline must arm the observation") { signals.count == 1 }

        await transport.queuePull(shard: "user", Self.pull([Self.note("n1", "a2")], cursor: "0:2"))
        _ = try await engine.pullOnce(shard: "user")

        await eventually(timeout: 3, "the changed row must commit and signal") { signals.count >= 2 }
        let title: String? = try await store.pool.read { db in
            guard let snap = try store.snapshot(db, stream: "notes", rowId: "n1"),
                  case .string(let s)? = snap.data["title"] else { return nil }
            return s
        }
        XCTAssertEqual(title, "a2")
        listener.cancel()
    }

    /// KILL: compare data but not type — a type flip with identical data
    /// is skipped and the row keeps the stale type.
    func testTypeChangeWithIdenticalDataStillApplies() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", Self.pull(
            [.rowSet(stream: "notes", id: "n1", type: "draft", data: ["title": .string("a")])], cursor: "0:1"
        ))
        _ = try await engine.pullOnce(shard: "user")

        await transport.queuePull(shard: "user", Self.pull(
            [.rowSet(stream: "notes", id: "n1", type: "final", data: ["title": .string("a")])], cursor: "0:2"
        ))
        _ = try await engine.pullOnce(shard: "user")

        let type: String? = try await store.pool.read { db in
            try store.snapshot(db, stream: "notes", rowId: "n1")?.type
        }
        XCTAssertEqual(type, "final", "identical data must not shadow a type move")
    }

    /// KILL: apply + rebase the identical echo anyway — the blind upsert
    /// AND the rebase replay each bump the sequence for zero information,
    /// while a row still owing a write must stay journalled and local.
    /// (Pushes fail here so the drain barrier cannot ack the op away —
    /// `drainIfWarm` is best-effort and the pull proceeds.)
    func testIdenticalEchoWithOwedWriteSkipsApplyAndKeepsTheJournal() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", Self.pull([Self.note("n1", "server")], cursor: "0:1"))
        _ = try await engine.pullOnce(shard: "user")
        // A local write the server has not seen: snapshot moves locally,
        // journal owes one op — and stays owed (pushes refused).
        await transport.failPushes(true)
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("local")])
        let owedBefore = try await engine.pendingOps().map(\.payload)
        XCTAssertEqual(owedBefore.count, 1)
        let before = try await store.pool.read { try store.changeSequence($0, stream: "notes") }

        // The echo of the LOCAL snapshot (server caught up) — identical
        // bytes, nothing to apply, nothing to rebase.
        await transport.queuePull(shard: "user", Self.pull([Self.note("n1", "local")], cursor: "0:2"))
        _ = try await engine.pullOnce(shard: "user")

        let after = try await store.pool.read { try store.changeSequence($0, stream: "notes") }
        XCTAssertEqual(after, before, "identical echo over an owed row must not move the sequence")
        let owedAfter = try await engine.pendingOps().map(\.payload)
        XCTAssertEqual(owedAfter, owedBefore, "an identical echo must leave the journal byte-identical")
        let title: String? = try await store.pool.read { db in
            guard let snap = try store.snapshot(db, stream: "notes", rowId: "n1"),
                  case .string(let s)? = snap.data["title"] else { return nil }
            return s
        }
        XCTAssertEqual(title, "local")
    }
}
