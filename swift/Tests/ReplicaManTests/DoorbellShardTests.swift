import Foundation
import XCTest
@testable import ReplicaMan

/// The doorbell rings for the USER shard only (the server's hook answers no
/// other), so its pull must ask for that shard alone — every ring used to
/// walk every shard, a wasted catalog round-trip per doorbell. Warm-up,
/// reconnect and refresh keep pulling every shard; a catalog edit lands on
/// the next such pull.
final class DoorbellShardTests: XCTestCase {

    func testPullingNamedShardsTouchesOnlyThose() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        _ = try await engine.pullUntilCaughtUp(shards: ["user"])

        let pulled = await transport.events.compactMap { event -> String? in
            if case .pull(let shard, _) = event { return shard } else { return nil }
        }
        XCTAssertEqual(pulled, ["user"])
    }

    func testPullingEveryShardStillWalksAll() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)

        _ = try await engine.pullUntilCaughtUp()

        let pulled = await transport.events.compactMap { event -> String? in
            if case .pull(let shard, _) = event { return shard } else { return nil }
        }
        XCTAssertEqual(Set(pulled), Set(Fixture.schema().shards))
    }
}
