import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 8 — the cold window: a transport failure marks the wire cold;
/// `drainIfWarm` skips inside the window (offline must not stack timeouts)
/// and retries after it. Explicit `drain()` never skips — reconnect's
/// deliberate drains always attempt.
final class ColdWindowTests: XCTestCase {

    /// Both halves are stated as VALUES rather than waited out: a 60s window is
    /// "inside", a 0s window is "past". `isCold` compares against `Date()`
    /// (`ReplicaEngine.swift:1191`) and ignores the engine's injected `clock`
    /// (`:55`), so a zero window is the only way to express "the window has
    /// elapsed" without sleeping — see the W7a report's clock proposal.
    func testDrainIfWarmSkipsInsideTheColdWindow() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, coldWindow: 60)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("queued")])
        await transport.failPushes(true)

        try await engine.drainIfWarm()
        var pushes = await transport.pushCount
        XCTAssertEqual(pushes, 1, "the first attempt hits the wire and proves it dead")

        try await engine.drainIfWarm()
        try await engine.drainIfWarm()
        pushes = await transport.pushCount
        XCTAssertEqual(pushes, 1, "inside the window a known-cold wire is not re-attempted")

        let cold = await engine.isColdForTesting(.bulk)
        XCTAssertTrue(cold, "the skip must come from the stamp, not from an empty journal")
        XCTAssertEqual(try store.peekPending().count, 1, "transport failure leaves the entry pending — retryable, never parked")
    }

    func testDrainIfWarmRetriesOnceTheWindowHasElapsed() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        // Zero window: the stamp is set to "now", so the next check is already
        // past it. No sleep can be needed to observe an elapsed zero.
        let engine = Fixture.engine(store: store, transport: transport, coldWindow: 0)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("queued")])
        await transport.failPushes(true)

        try await engine.drainIfWarm()
        var pushes = await transport.pushCount
        XCTAssertEqual(pushes, 1)
        let stillCold = await engine.isColdForTesting(.bulk)
        XCTAssertFalse(stillCold, "a zero window is elapsed the instant it is stamped")

        try await engine.drainIfWarm()
        pushes = await transport.pushCount
        XCTAssertEqual(pushes, 2, "past the window the drain retries")
        XCTAssertEqual(try store.peekPending().count, 1)
    }

    func testExplicitDrainAlwaysAttempts() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport, coldWindow: 60)
        try await failWarmThenExplicitly(engine, transport)

        await transport.failPushes(false)
        try await engine.drain()
        XCTAssertEqual(try store.peekPending().count, 0)
        let cold = await engine.isColdForTesting(.bulk)
        XCTAssertFalse(cold, "success warms the wire again")

        try await engine.saveRow(stream: "notes", id: "n2", type: nil, data: ["title": .string("next")])
        try await engine.drainIfWarm()
        let pushes = await transport.pushCount
        XCTAssertEqual(pushes, 4, "the next drainIfWarm attempts")
    }

    /// A warm drain onto a dead wire stamps the lane cold; the explicit drain
    /// still attempts.
    private func failWarmThenExplicitly(_ engine: ReplicaEngine, _ transport: StubTransport) async throws {
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("queued")])
        await transport.failPushes(true)
        try await engine.drainIfWarm()
        var pushes = await transport.pushCount
        XCTAssertEqual(pushes, 1)
        do {
            try await engine.drain()
            XCTFail("a dead wire surfaces from the explicit drain")
        } catch {}
        pushes = await transport.pushCount
        XCTAssertEqual(pushes, 2, "explicit drains never skip, cold or not")
    }
}
