import ReplicaManTestProtocol
import Foundation
import XCTest
@testable import ReplicaMan

/// B8 — `watch`: post-checkpoint change signal per stream, backed by GRDB
/// ValueObservation. Signals fire only after COMMIT — a rolled-back
/// checkpoint fires nothing; the typed watch delivers fresh rows.
final class WatchTests: XCTestCase {

    func testSignalCanIncludeTheCommittedBaselineWithoutChangingTheDefault() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        let changeOnly = Tally()
        let withInitial = Tally()

        let changeListener = Task {
            for await _ in engine.watchSignal(stream: "notes") {
                changeOnly.bump()
            }
        }
        let initialListener = Task {
            for await _ in engine.watchSignal(stream: "notes", includeInitial: true) {
                withInitial.bump()
            }
        }

        await eventually(timeout: 3, "the committed baseline was not delivered") {
            withInitial.count == 1
        }
        XCTAssertEqual(changeOnly.count, 0, "a baseline reached a change-only observer")

        // One commit, seen by BOTH — and both are polled before either total is
        // read. The two observations are independent, so the change-only one
        // may report first; asserting a cross-observer total on the strength of
        // one of them is a race (it produced a false red on the first run).
        // A change-only observer that leaked its baseline lands on 2 here.
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("one")])
        await eventually(timeout: 3, "both observers must report the commit before their totals mean anything") {
            withInitial.count >= 2 && changeOnly.count >= 1
        }
        XCTAssertEqual(withInitial.count, 2)
        XCTAssertEqual(changeOnly.count, 1, "a baseline reached a change-only observer")

        changeListener.cancel()
        initialListener.cancel()
    }

    func testSignalFiresAfterCommitAndNotOnRolledBackCheckpoints() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        let signals = Tally()
        // `includeInitial` makes ARMING observable: the baseline yield is the
        // proof the observation is live, which a sleep only ever assumed.
        let stream = engine.watchSignal(stream: "notes", includeInitial: true)
        let listener = Task {
            for await _ in stream {
                signals.bump()
            }
        }
        await eventually(timeout: 3, "the observation never armed") { signals.count == 1 }
        let baseline = signals.count

        // A faulted checkpoint rolls back — it must NOT signal.
        struct Fault: Error {}
        await engine.setCheckpointFault { throw Fault() }
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "half")], cursor: "5:", more: false
        ))
        do {
            try await engine.pullOnce(shard: "user")
            XCTFail("the injected fault must surface")
        } catch {}

        // A committed checkpoint follows. Its signal is delivered in commit
        // order AFTER any signal the rollback might wrongly have produced, so
        // once it lands the total is the whole story: exactly one more.
        await engine.setCheckpointFault(nil)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "landed")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")
        await eventually(timeout: 3, "commit did not signal the stream") {
            signals.count > baseline
        }
        XCTAssertEqual(signals.count, baseline + 1,
                       "a rolled-back checkpoint fired a signal — consumers would read uncommitted state")
        listener.cancel()
    }

    func testTypedWatchDeliversFreshRows() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let notes = RowStream<TestNote>(engine: engine)

        let seen = Seen()
        let deliveries = Tally()
        let watch = notes.watch(includeInitial: true) { models in
            deliveries.bump()
            seen.record(models.map(\.id))
        }
        await eventually("the typed-watch baseline was not delivered") { deliveries.count == 1 }

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "one"), Fixture.note("n2", title: "two")],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        await eventually(timeout: 3, "the typed watch never saw the imported rows") {
            seen.latest == ["n1", "n2"]
        }
        watch.cancel()
    }

    func testSignalDoesNotRingForAnotherStreamsCommit() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        let notesSignals = Tally()
        let boardsSignals = Tally()

        let notesListener = Task {
            for await _ in engine.watchSignal(stream: "notes", includeInitial: true) {
                notesSignals.bump()
            }
        }
        let boardsListener = Task {
            for await _ in engine.watchSignal(stream: "boards", includeInitial: true) {
                boardsSignals.bump()
            }
        }

        await eventually("the signal baselines were not delivered") {
            notesSignals.count == 1 && boardsSignals.count == 1
        }

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "Trip")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        await eventually("the notes commit did not ring its own signal") {
            notesSignals.count == 2
        }
        // A second notes commit: by the time IT is delivered, the boards
        // observer has been re-evaluated twice on the same table and must
        // still have yielded nothing but its baseline.
        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("again")])
        await eventually("the second notes commit did not ring") { notesSignals.count == 3 }
        XCTAssertEqual(boardsSignals.count, 1, "a commit on another stream rang this one's signal")

        notesListener.cancel()
        boardsListener.cancel()
    }

    func testSameWeightContentChangeRingsItsOwnSignal() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "Trip")], cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let signals = Tally()
        let listener = Task {
            for await _ in engine.watchSignal(stream: "notes", includeInitial: true) {
                signals.bump()
            }
        }
        await eventually("the signal baseline was not delivered") {
            signals.count == 1
        }

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("n1", title: "Trap")], cursor: "6:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        await eventually("the same-weight rename did not ring its stream") {
            signals.count == 2
        }
        listener.cancel()
    }

    /// Regression pin for the removed aggregate fingerprint: a peer is a
    /// random Loro u64 kept as an i64 bit-pattern, so summing two peer values
    /// can overflow Int64 and terminate a GRDB observation. The durable stream
    /// sequence must signal without inspecting or aggregating those values.
    func testWatchSurvivesLargePeerDocs() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(
            store: store,
            transport: transport,
            minter: Fixture.sequentialMinter(from: UInt64(Int64.max) - 1)
        )

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [
                .docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("SNAP-1".utf8), data: [:]),
                .docSnapshot(stream: "boards", id: "b2", codec: "stub@1", snapshot: Data("SNAP-2".utf8), data: [:]),
            ],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let first = try XCTUnwrap(store.peekDoc("boards", "b1"))
        let second = try XCTUnwrap(store.peekDoc("boards", "b2"))
        XCTAssertTrue(
            first.peer > UInt64(Int64.max) / 2 && second.peer > UInt64(Int64.max) / 2,
            "precondition: both peers must be large enough that their sum exceeds Int64.max"
        )

        let signals = Tally()
        let listener = Task {
            for await _ in engine.watchSignal(stream: "boards", includeInitial: true) {
                signals.bump()
            }
        }
        await eventually(timeout: 3, "the observation never armed over the large-peer docs") {
            signals.count == 1
        }
        let baseline = signals.count

        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 1, codec: "stub@1", payload: Data("+d1".utf8))],
            cursor: "6:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        await eventually(timeout: 3, "the committed write never signalled — the docs fingerprint overflowed and ended the observation") {
            signals.count > baseline
        }
        listener.cancel()
    }

    final class Seen: @unchecked Sendable {
        private let lock = NSLock()
        private var stored: [String] = []
        var latest: [String] { lock.withLock { stored } }
        func record(_ ids: [String]) { lock.withLock { stored = ids } }
    }
}
