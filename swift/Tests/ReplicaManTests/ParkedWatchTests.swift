import Foundation
import XCTest
@testable import ReplicaMan

/// The refusal ledger as a subscription: a rejected push PARKS the entry and
/// the watcher delivers the committed picture; a discard delivers the picture
/// without it. The consumer never re-queries — the stale-snapshot
/// race (a triggered re-read racing the discard it was meant to observe) is
/// impossible against commit-ordered values.
final class ParkedWatchTests: XCTestCase {

    /// The next yielded picture matching `predicate`, or a timeout failure —
    /// intermediate duplicates and unrelated commits may interleave, so the
    /// assertion is "the committed state ARRIVES", not "it is the n-th value".
    private func next(
        _ stream: AsyncStream<[ReplicaStateStore.JournalRow]>,
        where predicate: @escaping @Sendable ([ReplicaStateStore.JournalRow]) -> Bool
    ) async throws -> [ReplicaStateStore.JournalRow] {
        try await withThrowingTaskGroup(of: [ReplicaStateStore.JournalRow].self) { group in
            group.addTask {
                for await rows in stream where predicate(rows) { return rows }
                throw CancellationError()
            }
            group.addTask {
                try await Task.sleep(for: .seconds(5))
                throw NSError(domain: "ParkedWatch", code: 1,
                    userInfo: [NSLocalizedDescriptionKey: "the expected parked picture never arrived"])
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }

    func testAParkArrivesAndItsDiscardClearsIt() async throws {
        let store = try Fixture.store()
        let transport = StubTransport()
        let engine = Fixture.engine(store: store, transport: transport)
        await transport.scriptPush { ops in
            ops.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "Scenes is invalid") }
        }

        let stream = engine.watchParkedOps()
        _ = try await next(stream) { $0.isEmpty }

        try await engine.saveRow(
            stream: "notes", id: "n1", type: nil, data: ["title": .string("t")])
        _ = try await engine.drain()

        let parked = try await next(stream) { !$0.isEmpty }
        XCTAssertEqual(parked.count, 1)
        XCTAssertEqual(parked[0].parked, "Scenes is invalid")

        try await engine.discardOps(ids: [parked[0].id])
        _ = try await next(stream) { $0.isEmpty }
    }
}
