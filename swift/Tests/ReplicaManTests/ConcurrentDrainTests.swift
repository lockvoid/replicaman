import Foundation
import XCTest
@testable import ReplicaMan

/// Two drains racing (the scheduled push meeting an explicit drain) must
/// coalesce onto ONE flight. A double-sent `row.create` reaches the server
/// twice: the first is accepted, the second collides → rejected → the
/// rejection revert deletes the freshly-created row out from under the user.
final class ConcurrentDrainTests: XCTestCase {

    func testConcurrentDrainsPushEachEntryOnce() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        await transport.delayPushes(nanos: 50_000_000)
        let engine = Fixture.engine(store: store, transport: transport)

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("mine")])

        async let first = engine.drain()
        async let second = engine.drain()
        let (firstVerdicts, secondVerdicts) = try await (first, second)

        let pushed = await transport.pushedBatches.flatMap { $0 }.filter { $0.rowId == "n1" }
        XCTAssertEqual(
            pushed.count, 1,
            "the same journal entry went to the wire \(pushed.count) times — concurrent drains must share one flight"
        )
        // The joiner is answered by the flight it joined, not by an empty
        // second selection — otherwise a caller reads "nothing was owed".
        XCTAssertEqual(firstVerdicts, secondVerdicts)
        XCTAssertEqual(firstVerdicts.count, 1)
        XCTAssertTrue(try store.peekPending().isEmpty)
    }
}
