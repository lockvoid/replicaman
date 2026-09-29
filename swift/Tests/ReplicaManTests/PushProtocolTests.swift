import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// Protocol 2 push: every frozen operation keeps the UUID it was minted with,
/// an atomic action's operations share a group, a lost answer replays the
/// same bytes into the server's stored verdicts, and an answer that does not
/// answer exactly the operations sent acknowledges nothing.
final class PushProtocolTests: XCTestCase {
    private func synchronized() async throws -> (ReplicaStateStore, StubTransport, ReplicaEngine) {
        let store = try Fixture.store()
        let wire = StubTransport()
        let engine = Fixture.engine(store: store, transport: wire)
        try await engine.pullOnce()
        return (store, wire, engine)
    }

    private func frozen(_ store: ReplicaStateStore) throws -> [Data] {
        try store.pool.read { try store.frozenSubmissions($0).map(\.content) }
    }

    private func isUUIDv7(_ text: String) -> Bool {
        let characters = Array(text)
        return UUID(uuidString: text) != nil && text == text.lowercased()
            && characters[14] == "7" && "89ab".contains(characters[19])
    }

    func testEveryOperationCarriesAUUIDv7AndOnlyAnAtomicActionAGroup() async throws {
        let (_, wire, engine) = try await synchronized()
        try await engine.saveRow(stream: "notes", id: "single", type: nil, data: ["title": .string("alone")])
        try engine.writeAtomically { tx in
            try tx.create(TestNote(id: "a", title: "first"))
            try tx.create(TestNote(id: "b", title: "second"))
        }

        try await engine.drain()

        let sent = await wire.pushedOps()
        XCTAssertEqual(sent.map(\.rowId), ["a", "b", "single"], "the atomic action froze first")
        XCTAssertTrue(sent.allSatisfy { isUUIDv7($0.id) }, "\(sent.map(\.id))")
        XCTAssertEqual(Set(sent.map(\.id)).count, 3)
        let group = try XCTUnwrap(sent[0].group)
        XCTAssertTrue(isUUIDv7(group))
        XCTAssertEqual(sent[1].group, group)
        XCTAssertNil(sent[2].group, "a single operation carries no group")
    }

    func testALostAnswerReplaysTheSameBytesIntoTheStoredVerdict() async throws {
        let (store, wire, engine) = try await synchronized()
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("committed")])
        await wire.protocolFixture.losePushReply()

        do {
            try await engine.drain()
            XCTFail("a lost answer must fail the drain")
        } catch ReplicaError.transport {}
        XCTAssertEqual(try store.peekPending().count, 1, "a lost answer acknowledges nothing")
        let retained = try frozen(store)
        XCTAssertEqual(retained.count, 1)

        // The server already committed it: its stored verdict answers the
        // retry, whatever it would decide about the operation now.
        await wire.scriptPush { $0.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "decided again") } }
        try await engine.drain()

        let batches = await wire.pushedBatches
        XCTAssertEqual(batches.count, 2)
        XCTAssertEqual(batches[0], batches[1], "the retry sends the same operations under the same ids")
        XCTAssertTrue(try store.peekPending().isEmpty)
        XCTAssertTrue(try store.peekParked().isEmpty)
        XCTAssertTrue(try frozen(store).isEmpty)
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("committed"))
    }

    func testACopiedStoreReplaysItsFrozenOperationsByteForByte() async throws {
        let (store, wire, engine) = try await synchronized()
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("copied")])
        await wire.failPushes(true)
        do {
            try await engine.drain()
            XCTFail("the refused push must fail the drain")
        } catch ReplicaError.transport {}
        await wire.failPushes(false)
        let path = Fixture.directory().appendingPathComponent("copy.sqlite")
        try FileManager.default.createDirectory(at: path.deletingLastPathComponent(), withIntermediateDirectories: true)
        let destination = try DatabaseQueue(path: path.path)
        try store.pool.backup(to: destination)
        try destination.close()
        let copy = try ReplicaStateStore(path: path.path)
        XCTAssertEqual(try frozen(copy), try frozen(store))

        try await Fixture.engine(store: copy, transport: wire).drain()
        try await engine.drain()

        let batches = await wire.pushedBatches
        XCTAssertEqual(batches.count, 2)
        XCTAssertEqual(batches[0], batches[1], "both copies send the operation under its one id")
        XCTAssertTrue(try copy.peekPending().isEmpty)
        XCTAssertTrue(try store.peekPending().isEmpty)
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("copied"))
    }

    func testAnAnswerThatDoesNotAnswerTheOperationsSentAcknowledgesNothing() async throws {
        for fault in [ProtocolFixture.VerdictFault.missing, .duplicate, .foreign, .mixedGroup] {
            let (store, wire, engine) = try await synchronized()
            if fault == .mixedGroup {
                try engine.writeAtomically { tx in
                    try tx.create(TestNote(id: "a", title: "first"))
                    try tx.create(TestNote(id: "b", title: "second"))
                }
            } else {
                try await engine.saveRow(stream: "notes", id: "a", type: nil, data: ["title": .string("first")])
                try await engine.saveRow(stream: "notes", id: "b", type: nil, data: ["title": .string("second")])
            }
            await wire.protocolFixture.corruptVerdicts(fault)

            do {
                try await engine.drain()
                XCTFail("\(fault): a malformed answer was accepted")
            } catch ReplicaError.protocolFailure(let code, _) {
                XCTAssertEqual(code, "InvalidResponse", "\(fault)")
            }
            XCTAssertEqual(try store.peekPending().count, 2, "\(fault)")
            XCTAssertEqual(try frozen(store).count, fault == .mixedGroup ? 1 : 2, "\(fault)")
            let accepted = try await store.pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM intents WHERE state = 'accepted'") }
            XCTAssertEqual(accepted, 0, "\(fault)")

            try await engine.drain()
            XCTAssertTrue(try store.peekPending().isEmpty, "\(fault)")
            XCTAssertEqual(try store.peekSnapshot("notes", "b")?.data["title"], .string("second"), "\(fault)")
        }
    }

    /// Supersession merges only into an owed delta: edits made while the
    /// document's delta is frozen become one intent of their own, and the
    /// frozen bytes stay exactly what the retry sends.
    func testEditsWhileADeltaIsFrozenBecomeOneIntentOfTheirOwn() async throws {
        let (store, wire, engine) = try await synchronized()
        await wire.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(stream: "boards", id: "b1", codec: "stub@1", snapshot: Data("S".utf8), data: [:])],
            cursor: "c1", more: false
        ))
        try await engine.pullOnce()
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+1".utf8))
        await wire.failPushes(true)
        do {
            try await engine.drain()
            XCTFail("the dead wire must surface")
        } catch ReplicaError.transport {}
        await wire.failPushes(false)
        let sent = try frozen(store)

        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+2".utf8))
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: Data("+3".utf8))

        let pending = try store.peekPending()
        XCTAssertEqual(pending.map(\.sent), [true, false], "the frozen delta, then one editable delta")
        XCTAssertEqual(try pending.map { try $0.op().payload }, [Data("+1".utf8), Data("+1+2+3".utf8)])
        XCTAssertEqual(try frozen(store), sent, "the frozen bytes never change")

        try await engine.drain()
        let pushed = await wire.pushedBatches.map { $0.map(\.payload) }
        XCTAssertEqual(pushed, [[Data("+1".utf8)], [Data("+1+2+3".utf8)]])
        XCTAssertTrue(try store.peekPending().isEmpty)
    }

    func testChangedBytesUnderAClaimedIdSurfaceAsMutationChanged() async throws {
        let (store, wire, engine) = try await synchronized()
        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("sent")])
        await wire.protocolFixture.losePushReply()
        do {
            try await engine.drain()
            XCTFail("a lost answer must fail the drain")
        } catch ReplicaError.transport {}
        try await store.pool.write { db in
            let content = try XCTUnwrap(Data.fetchOne(db, sql: "SELECT content FROM submissions"))
            var operations = try ReplicaJSON.decoder().decode([ReplicaOp].self, from: content)
            operations[0].data = ["title": .string("changed")]
            try db.execute(sql: "UPDATE submissions SET content = ?", arguments: [ReplicaJSON.encoder().encode(operations)])
        }

        do {
            try await engine.drain()
            XCTFail("a claimed id with other bytes must not be acknowledged")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "MutationChanged")
        }
        XCTAssertEqual(try store.peekPending().count, 1)
        XCTAssertEqual(try frozen(store).count, 1)
    }

    func testAPushCarriesAtMostOneHundredOperationsAndNeverSplitsASubmission() async throws {
        let (store, wire, engine) = try await synchronized()
        for action in 0..<3 {
            try engine.writeAtomically { tx in
                for index in 0..<40 { try tx.create(TestNote(id: "a\(action)-\(index)")) }
            }
        }

        try await engine.drain()

        let batches = await wire.pushedBatches
        XCTAssertEqual(batches.map(\.count), [80, 40])
        XCTAssertEqual(batches.flatMap { $0.map(\.rowId) }, (0..<3).flatMap { action in (0..<40).map { "a\(action)-\($0)" } })
        XCTAssertEqual(Set(batches[0].map(\.group)).count, 2)
        XCTAssertTrue(try store.peekPending().isEmpty)
    }

    func testARestoredDatasetFencesPullAndPushAndKeepsEveryLocalByte() async throws {
        let (store, wire, engine) = try await synchronized()
        await wire.protocolFixture.restore(dataset: "restored")

        do {
            try await engine.pullOnce()
            XCTFail("a pull into a restored dataset must stop")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "DatasetChanged")
        }

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["title": .string("only copy")])
        do {
            try await engine.drain()
            XCTFail("a push into a restored dataset must stop")
        } catch ReplicaError.protocolFailure(let code, _) {
            XCTAssertEqual(code, "DatasetChanged")
        }
        XCTAssertEqual(try store.peekPending().count, 1)
        XCTAssertEqual(try frozen(store).count, 1)
        XCTAssertEqual(try store.peekSnapshot("notes", "n1")?.data["title"], .string("only copy"))
        let dataset = try await store.pool.read { try store.meta($0).dataset }
        XCTAssertEqual(dataset, "fixture-dataset", "the store keeps the dataset it synchronized")
    }
}
