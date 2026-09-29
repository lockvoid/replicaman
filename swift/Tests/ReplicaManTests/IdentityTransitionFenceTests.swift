import ReplicaManTestProtocol
import Foundation
import XCTest
@_spi(SignOutIdentityTransition) @testable import ReplicaMan

/// The identity boundary lives inside ReplicaEngine because generated CRUD
/// schedules transport without passing back through the app host. These tests
/// hold real engine flights at the transport seam: timing sleeps cannot prove
/// that begin-transition waited, or that an automatic push was covered.
final class IdentityTransitionFenceTests: XCTestCase {

    func testTransitionWaitsForAutomaticPushAndRejectsPostWipeWritesUntilResume() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let pushGate = AsyncGate()
        await transport.onPush { _ in await pushGate.arriveAndWait() }
        let engine = ReplicaEngine(
            store: store,
            owner: 42,
            transport: transport,
            schema: Fixture.schema(),
            codecs: [StubCodec()]
        )

        try await engine.saveRow(
            stream: "notes", id: "outgoing", type: nil,
            data: ["title": .string("must finish under outgoing bearer")]
        )
        await pushGate.waitUntilArrived()

        let transitionFinished = AsyncFlag()
        let transition = Task {
            await engine.seal()
            await transitionFinished.set()
        }
        await eventually("engine never closed identity admissions") {
            await engine.sealed
        }

        let automaticPushTransitionFinished = await transitionFinished.value
        XCTAssertFalse(
            automaticPushTransitionFinished,
            "beginIdentityTransition returned while an automatic push was still on the wire"
        )
        await assertSealedRefusesAWrite(engine, store, id: "late-before-wipe")

        await pushGate.open()
        await transition.value
        XCTAssertTrue(try store.peekPending().isEmpty, "the outgoing flight must settle before the boundary opens")
        await assertSealedRefusesAWrite(engine, store, id: "post-wipe")

        await engine.unseal()
        try await engine.saveRow(
            stream: "notes", id: "replacement", type: nil,
            data: ["title": .string("more work under the same owner")]
        )
        try await eventually("automatic delivery did not resume after the seal lifted") {
            let attempts = await transport.pushCount
            return try attempts == 2 && store.peekPending().isEmpty
        }
    }

    func testTransitionWaitsUntilActivePullResponseIsAppliedAndBlocksAnotherPull() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let pullGate = AsyncGate()
        await transport.onPull { _ in await pullGate.arriveAndWait() }
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("outgoing", title: "old identity response")],
            cursor: "1:", more: false
        ))
        let engine = Fixture.engine(store: store, transport: transport)

        let pull = Task { try await engine.pullOnce(shard: "user") }
        await pullGate.waitUntilArrived()

        let transitionFinished = AsyncFlag()
        let transition = Task {
            await engine.seal()
            await transitionFinished.set()
        }
        await eventually { await engine.sealed }
        let pullTransitionFinished = await transitionFinished.value
        XCTAssertFalse(
            pullTransitionFinished,
            "beginIdentityTransition returned before the outgoing pull response settled"
        )

        await pullGate.open()
        let applied = try await pull.value
        XCTAssertEqual(applied, 1)
        await transition.value
        XCTAssertEqual(
            try store.peekSnapshot("notes", "outgoing")?.data["title"],
            .string("old identity response"),
            "the boundary returned before the already-started response committed locally"
        )

        // A NEW pull while sealed answers zero and never touches the wire —
        // the same graceful shape as the closed engine, so lifecycle callers
        // (warm, refresh, knock) stay silent through a transition.
        let pullsBeforeSealedAttempt = await transport.pullCount
        let sealedApplied = try await engine.pullOnce(shard: "user")
        XCTAssertEqual(sealedApplied, 0)
        let pullsAfterSealedAttempt = await transport.pullCount
        XCTAssertEqual(pullsAfterSealedAttempt, pullsBeforeSealedAttempt,
                       "a sealed engine must stay off the wire for pulls")

        await engine.unseal()
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [Fixture.note("incoming", title: "after resume")], cursor: "2:", more: false
        ))
        let appliedAfterResume = try await engine.pullOnce(shard: "user")
        XCTAssertEqual(appliedAfterResume, 1)
        let pullsAfterResume = await transport.pullCount
        XCTAssertEqual(pullsAfterResume, pullsAfterSealedAttempt + 1)
    }

    func testPinnedSourceDrainSealsFirstRejectsConcurrentCRUDAndIsReplaySafe() async throws {
        let store = try Fixture.store()
        let ordinaryTransport = StubTransport()
        let pinnedSourceTransport = StubTransport()
        let pushGate = AsyncGate()
        await pinnedSourceTransport.onPush { _ in await pushGate.arriveAndWait() }
        let engine = Fixture.engine(store: store, transport: ordinaryTransport)

        try await engine.saveRow(
            stream: "notes", id: "outgoing", type: nil,
            data: ["title": .string("must use pinned source wire")]
        )

        let drain = Task {
            try await engine.sealAndDrain(using: pinnedSourceTransport)
        }
        await pushGate.waitUntilArrived()

        let ordinaryPushesDuringDrain = await ordinaryTransport.pushCount
        let pinnedPushesDuringDrain = await pinnedSourceTransport.pushCount
        XCTAssertEqual(
            ordinaryPushesDuringDrain,
            0,
            "the engine's ordinary/live transport must never carry the frozen source journal"
        )
        XCTAssertEqual(pinnedPushesDuringDrain, 1)
        await assertIdentityTransitionRejects {
            try await engine.saveRow(
                stream: "notes", id: "late", type: nil,
                data: ["title": .string("must not arrive after drain snapshot")]
            )
        }
        XCTAssertNil(try store.peekSnapshot("notes", "late"))

        await pushGate.open()
        let verdicts = try await drain.value
        XCTAssertEqual(verdicts.count, 1)
        XCTAssertTrue(try store.peekPending().isEmpty)
        let stayedPaused = await engine.sealed
        XCTAssertTrue(stayedPaused)

        let replayed = try await engine.sealAndDrain(using: pinnedSourceTransport)
        XCTAssertTrue(replayed.isEmpty)
        let pinnedPushesAfterReplay = await pinnedSourceTransport.pushCount
        XCTAssertEqual(
            pinnedPushesAfterReplay,
            1,
            "recovery must not resend an entry whose accepted verdict already committed"
        )
    }

    func testPinnedSourceDrainReleasesSingleFlightAfterCancellationError() async throws {
        let store = try Fixture.store()
        let engine = Fixture.engine(store: store, transport: StubTransport())
        try await engine.saveRow(
            stream: "notes", id: "retry-after-cancel", type: nil,
            data: ["title": .string("still pending")]
        )
        await engine.seal()

        do {
            _ = try await engine.sealAndDrain(using: CancellationTransport())
            XCTFail("cancellation transport unexpectedly accepted the journal")
        } catch is CancellationError {
            // The internal flight's defer must clear `activeDrain` and wire count.
        }
        XCTAssertEqual(try store.peekPending().count, 1)

        let retryTransport = StubTransport()
        let verdicts = try await engine.sealAndDrain(using: retryTransport)
        XCTAssertEqual(verdicts.count, 1)
        XCTAssertTrue(try store.peekPending().isEmpty)
        let retryPushes = await retryTransport.pushCount
        XCTAssertEqual(retryPushes, 1)
    }

    private func assertSealedRefusesAWrite(
        _ engine: ReplicaEngine, _ store: ReplicaStateStore, id: String, line: UInt = #line
    ) async {
        await assertIdentityTransitionRejects({
            try await engine.saveRow(
                stream: "notes", id: id, type: nil, data: ["title": .string("must not enter outgoing journal")]
            )
        }, line: line)
        XCTAssertNil(try store.peekSnapshot("notes", id), line: line)
    }

    private func assertIdentityTransitionRejects(
        _ operation: () async throws -> Void,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            try await operation()
            XCTFail("operation crossed a paused identity boundary", file: file, line: line)
        } catch ReplicaError.identityTransitionInProgress {
            // Expected: deterministic retryable admission refusal.
        } catch {
            XCTFail("unexpected boundary error: \(error)", file: file, line: line)
        }
    }
    /// Pulls WRITE — checkpoint rows and cursors land in the store. A merge
    /// rebinds and moves that store while sealed, so a pull that slips in
    /// mid-seal races the swap exactly the way local writes would. The seal
    /// must keep the engine off the wire for pulls, not just pushes.
    func testASealedEngineStaysOffTheWireForPulls() async throws {
        let transport = StubTransport()
        let engine = Fixture.unopenedEngine(in: Fixture.directory(), transport: transport)
        try await engine.open(owner: 606)
        await engine.seal()

        let applied = try await engine.pullUntilCaughtUp()

        XCTAssertEqual(applied, 0)
        let pulls = await transport.pullCount
        XCTAssertEqual(pulls, 0, "a sealed engine must not touch the wire for pulls")

        await engine.unseal()
        _ = try await engine.pullOnce(shard: "user")
        let after = await transport.pullCount
        XCTAssertEqual(after, 1, "unseal re-admits the wire")
    }

}

private actor AsyncFlag {
    private(set) var value = false
    func set() { value = true }
}

private actor CancellationTransport: FixtureTransport {
    let protocolFixture = ProtocolFixture()
    func pull(shard: String, cursor: String?, limit: Int) async throws -> ReplicaPullResponse {
        throw CancellationError()
    }

    func push(_ ops: [ReplicaOp]) async throws -> [ReplicaVerdict] {
        throw CancellationError()
    }
}

private actor AsyncGate {
    private var arrived = false
    private var isOpen = false
    private var arrivalWaiters: [CheckedContinuation<Void, Never>] = []
    private var openWaiters: [CheckedContinuation<Void, Never>] = []

    func arriveAndWait() async {
        arrived = true
        let waiters = arrivalWaiters
        arrivalWaiters.removeAll()
        for waiter in waiters { waiter.resume() }

        guard !isOpen else { return }
        await withCheckedContinuation { continuation in
            openWaiters.append(continuation)
        }
    }

    func waitUntilArrived() async {
        guard !arrived else { return }
        await withCheckedContinuation { continuation in
            arrivalWaiters.append(continuation)
        }
    }

    func open() {
        isOpen = true
        let waiters = openWaiters
        openWaiters.removeAll()
        for waiter in waiters { waiter.resume() }
    }
}
