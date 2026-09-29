import Foundation
import GRDB
import Testing
@testable import ReplicaMan

/// The governor on the drain loop.
///
/// `performDrain` drains to QUIESCENCE — one flight loops select→push until a
/// pass finds nothing owed — because the single-flight join makes a second
/// drain impossible, so the flight itself must not leave work behind. That
/// loop needs a stop: work that keeps arriving while the flight is on the
/// wire would otherwise spin it forever.
@Suite struct DrainGovernorTests {

    /// Every push brings exactly ONE more row — a write landing while its
    /// predecessor is on the wire, the shape of a gate releasing one row per
    /// network round-trip. The write rides `onPush`, so it happens provably
    /// inside the flight: no timing, no sleep.
    ///
    /// KILL: `performDrain` — delete the
    /// `guard passes < Self.maxDrainPasses else { break }`. The first drain
    /// then swallows all 20 rows and `owed after the first drain` goes red.
    @Test func workThatKeepsArrivingIsStoppedByThePassCap() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "r00", type: nil, data: ["title": .string("t0")])
        await transport.onPush { ops in
            guard let shipped = ops.last?.rowId, let index = Int(shipped.dropFirst()), index < 19 else { return }
            do {
                try await engine.saveRow(
                    stream: "notes", id: String(format: "r%02d", index + 1),
                    type: nil, data: ["title": .string("t\(index + 1)")]
                )
            } catch {
                Issue.record(error)
            }
        }

        _ = try await engine.drain()

        func owed() async throws -> [String] {
            try await store.pool.read { db in
                try String.fetchAll(db, sql: """
                    SELECT json_extract(payload, '$.row_id') FROM intents
                    WHERE state IN ('draft', 'owed', 'frozen') ORDER BY rowid
                    """)
            }
        }

        // `maxDrainPasses` is 16, one row per pass.
        #expect(await transport.pushCount == 16, "the flight must stop at the pass cap, not run to empty")
        #expect(try await owed() == ["r16"],
                "what the cap left behind must stay owed — capping may delay work, never drop it")

        // And the remainder is ordinary work for the next drain.
        _ = try await engine.drain()
        #expect(await transport.pushCount == 20)
        #expect(try await owed().isEmpty)
    }

}
