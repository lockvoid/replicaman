import ReplicaManTestProtocol
import Foundation
import Loro
@testable import ReplicaManLoro
import XCTest
@testable import ReplicaMan

/// Matrix 5 — the document lifecycle over the REAL codec: create(seed) →
/// journal op; delta supersede folds two local edits into ONE pending op;
/// accepted verdicts advance acked; rejections park with their error;
/// doc.snapshot merge preserves unpushed local ops.
final class DocLifecycleTests: XCTestCase {

    func testInvalidDocumentFrameRollsBackTheWholeCheckpoint() async throws {
        for kind in ["delta", "existing snapshot", "new snapshot", "reset snapshot"] {
            let store = try LoroFixture.store()
            let transport = LoroStubTransport()
            let engine = LoroFixture.engine(store: store, transport: transport)
            let author = try LoroFixture.doc(peer: 7)
            try LoroFixture.setMeta(author, "name", "Before")
            let seed = try author.export(mode: .snapshot)
            await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
                .docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName, snapshot: seed, data: ["name": .string("Before")])
            ], cursor: "5:", more: false))
            try await engine.pullOnce(shard: "user")
            let before = try XCTUnwrap(store.peekDoc("boards", "b1")).fold
            let invalid: ReplicaFrame = kind == "delta"
                ? .docDelta(stream: "boards", id: "b1", seq: 1, codec: LoroReplicaCodec.codecName, payload: Data("broken".utf8))
                : .docSnapshot(stream: "boards", id: kind == "existing snapshot" ? "b1" : "b2", codec: LoroReplicaCodec.codecName, snapshot: Data("broken".utf8), data: ["name": .string("After")])
            await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [
                .rowSet(stream: "notes", id: "partial", type: nil, data: [:]), invalid
            ], cursor: "9:", more: false))
            if kind == "reset snapshot" {
                await transport.protocolFixture.forgetCursors()
                let restarted = try await engine.pullOnce(shard: "user")
                XCTAssertEqual(restarted, 0, "a cursor the server no longer knows starts a baseline round")
            }

            do {
                try await engine.pullOnce(shard: "user")
                XCTFail("A corrupt \(kind) must refuse the checkpoint")
            } catch { }

            let cursor = try await engine.currentCursor()
            XCTAssertEqual(cursor, "5:", kind)
            XCTAssertNil(try store.peekSnapshot("notes", "partial"), kind)
            XCTAssertNil(try store.peekSnapshot("boards", "b2"), kind)
            XCTAssertEqual(try store.peekDoc("boards", "b1")?.fold, before, kind)
        }
    }

    func testCommandRefreshMergesRealLoroHistoryAndPreservesUnsentEdits() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)
        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Initial")
        let seed = try author.export(mode: .snapshot)
        await transport.queuePull(shard: "user", .init(frames: [
            .docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName, snapshot: seed, data: [:])
        ], cursor: "before", more: false))
        _ = try await engine.pullOnce()
        let session = try await engine.commitSession()
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: LoroFixture.editPayload(author, "color", "red"))
        await transport.failPushes(true)
        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer, fold: seed)
        try LoroFixture.setMeta(server, "name", "Remote")
        await transport.queuePull(shard: "user", .init(frames: [
            .docSnapshot(stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                         snapshot: try server.export(mode: .snapshot), data: [:], revision: 9_007_199_254_740_993)
        ], cursor: "after", more: false))
        let hint = try JSONSerialization.data(withJSONObject: [
            "protocol": 2, "namespace": "replicaman", "schema": 1,
            "dataset": "fixture-dataset", "shards": ["user"]
        ]).base64EncodedString()
        try await engine.apply(commit: hint, session: session)
        try await engine.apply(commit: hint, session: session)
        let merged = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(try LoroFixture.meta(fold: merged.fold, "name"), "Remote")
        XCTAssertEqual(try LoroFixture.meta(fold: merged.fold, "color"), "red")
        XCTAssertEqual(try store.peekPending().count, 1)
        let cursor = try await engine.currentCursor()
        XCTAssertEqual(cursor, "after")
    }

    func testCreateSeedJournalsAndAcceptanceAdvancesAcked() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)

        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Plans")
        let seed = try author.export(mode: .snapshot)

        try await engine.createDoc(stream: "boards", id: "b1", seed: seed, peer: 7, data: ["name": .string("Plans")])

        let pending = try store.peekPending()
        XCTAssertEqual(pending.count, 1)
        let op = try XCTUnwrap(pending.first).op
        XCTAssertEqual(try op().verb, ReplicaOp.Verb.rowCreate)
        XCTAssertEqual(try op().codec, LoroReplicaCodec.codecName)
        XCTAssertEqual(try op().seed, seed, "the create carries the seed, not a delta")
        XCTAssertEqual(try store.peekSnapshot("boards", "b1")?.data["name"], .string("Plans"))
        XCTAssertEqual(try store.peekDoc("boards", "b1")?.peer, 7, "the authoring peer is recorded")

        try await engine.drain()
        XCTAssertEqual(try store.peekPending().count, 0)

        // Acked caught up with the seed: the document owes nothing further.
        let doc = try XCTUnwrap(store.peekDoc("boards", "b1"))
        let codec = LoroReplicaCodec()
        XCTAssertTrue(
            codec.isEmptyDiff(try codec.diff(fold: doc.fold, since: doc.acked)),
            "an accepted create advances acked to the seed's version"
        )
    }

    func testTwoLocalEditsSupersedeIntoOnePendingOp() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)

        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Plans")
        let seed = try author.export(mode: .snapshot)
        try await engine.createDoc(stream: "boards", id: "b1", seed: seed, peer: 7)
        try await engine.drain()

        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: LoroFixture.editPayload(author, "color", "red"))
        let first = try XCTUnwrap(store.peekPending().first)
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: LoroFixture.editPayload(author, "mood", "calm"))

        let pending = try store.peekPending()
        XCTAssertEqual(pending.count, 1, "per-document supersede: ONE pending merged doc.delta")
        let entry = try XCTUnwrap(pending.first)
        XCTAssertEqual(entry.id, first.id, "the supersede id is stable")
        XCTAssertEqual(try entry.op().id, entry.id, "the superseding bytes carry the intent's id")
        let op = try entry.op()
        XCTAssertEqual(op.verb, ReplicaOp.Verb.docDelta)

        // The one payload carries BOTH edits: apply it to the server's copy.
        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer, fold: seed)
        _ = try server.import(bytes: try XCTUnwrap(op.payload))
        XCTAssertEqual(LoroFixture.meta(server, "color"), "red")
        XCTAssertEqual(LoroFixture.meta(server, "mood"), "calm")

        // The fold absorbed both too.
        let fold = try XCTUnwrap(store.peekDoc("boards", "b1")).fold
        XCTAssertEqual(try LoroFixture.meta(fold: fold, "color"), "red")
        XCTAssertEqual(try LoroFixture.meta(fold: fold, "mood"), "calm")
    }

    /// A rejected doc.delta is DISCARDED (parking would push
    /// the same refused history forever) and the document is reborn from the
    /// server's truth when the next pull brings it.
    func testRejectedDeltaDiscardsAndRebootstrapsToServerTruth() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)
        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [], cursor: "5:", more: false))
        try await engine.pullOnce(shard: "user")

        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Plans")
        let seed = try author.export(mode: .snapshot)
        try await engine.createDoc(stream: "boards", id: "b1", seed: seed, peer: 7)
        try await engine.drain()

        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: LoroFixture.editPayload(author, "color", "red"))
        await transport.scriptPush { ops in
            ops.map { ReplicaVerdict(id: $0.id, outcome: .rejected, reason: "beyond quota") }
        }
        try await engine.drain()

        XCTAssertEqual(try store.peekPending().count, 0, "the refused delta is gone")
        XCTAssertEqual(try store.peekParked().count, 1, "the refusal remains visible without being retried")
        XCTAssertNil(try store.peekDoc("boards", "b1"), "the refused history leaves the active document")

        // The next pull lands the server's copy of the doc fresh.
        let serverTruth = try LoroFixture.doc(peer: LoroFixture.serverPeer, fold: seed)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(
                stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                snapshot: try serverTruth.export(mode: .snapshot), data: ["name": .string("Plans")]
            )],
            cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let doc = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(try LoroFixture.meta(fold: doc.fold, "name"), "Plans")
        XCTAssertNil(try LoroFixture.meta(fold: doc.fold, "color"), "the refused edit is genuinely undone")
        XCTAssertEqual(doc.peer, 100, "the reborn fold minted a fresh peer")
        try await engine.close()
        let reopened = try ReplicaStateStore(path: store.path.path)
        let recovery = try XCTUnwrap(reopened.recoveryRecords().last)
        XCTAssertEqual(try LoroFixture.meta(fold: recoveryBytes(reopened, record: recovery, kind: "document.fold"), "color"), "red")
        XCTAssertEqual(recovery.reason, "beyond quota")
        let states = try reopened.recoveryParts(id: recovery.id).filter { $0.kind == "intent.metadata" }.map { part in
            try ReplicaJSON.decoder().decode([String: ReplicaValue].self, from: reopened.recoveryChunk(id: recovery.id, part: part))["state"]
        }
        XCTAssertEqual(states.compactMap { $0?.string }.sorted(), ["accepted", "frozen"],
                       "the refused delta, and the birth the server accepted before it")
        try reopened.removeRecoveryRecord(id: recovery.id)
        XCTAssertTrue(try reopened.recoveryRecords().isEmpty)
        try reopened.close()

    }

    func testServerSnapshotMergePreservesUnpushedLocalOps() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)

        // Server-born document arrives on bootstrap.
        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer)
        try LoroFixture.setMeta(server, "name", "Server")
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(
                stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                snapshot: try server.export(mode: .snapshot), data: ["name": .string("Server")]
            )],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        // A local edit authored from the fold, under the store's minted peer.
        let peer = try XCTUnwrap(engine.docPeer(stream: "boards", id: "b1"))
        XCTAssertEqual(peer, 100, "the fold's peer was minted by the engine")
        let fold = try XCTUnwrap(engine.docFold(stream: "boards", id: "b1"))
        let app = try LoroFixture.doc(peer: peer, fold: fold)
        try await engine.recordDocDelta(stream: "boards", id: "b1", payload: LoroFixture.editPayload(app, "color", "red"))

        // Push side dead: the local op is still UNPUSHED when the server's
        // snapshot arrives — the exact shape the merge rule protects.
        await transport.failPushes(true)

        // The server evolves WITHOUT our edit; its fresh snapshot arrives.
        try LoroFixture.setMeta(server, "name", "Server2")
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(
                stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                snapshot: try server.export(mode: .snapshot), data: ["name": .string("Server2")]
            )],
            cursor: "9:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        // MERGE, never a blind replace: both sides survive.
        let merged = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(try LoroFixture.meta(fold: merged.fold, "name"), "Server2")
        XCTAssertEqual(try LoroFixture.meta(fold: merged.fold, "color"), "red", "the unpushed local op survived the snapshot")
        XCTAssertEqual(merged.peer, 100, "no rotation while the fold lives")

        // Still owed: the local op is not acked until its own verdict.
        XCTAssertEqual(try store.peekPending().count, 1)
        let codec = LoroReplicaCodec()
        XCTAssertFalse(codec.isEmptyDiff(try codec.diff(fold: merged.fold, since: merged.acked)))

        await transport.failPushes(false)
        try await engine.drain()
        let drained = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertTrue(
            codec.isEmptyDiff(try codec.diff(fold: drained.fold, since: drained.acked)),
            "the accepted delta advances acked over the local op"
        )
    }

    func testServerDeltaFrameMergesAndAdvancesAcked() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)

        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer)
        try LoroFixture.setMeta(server, "name", "Server")
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(
                stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                snapshot: try server.export(mode: .snapshot), data: [:]
            )],
            cursor: "5:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let delta = try LoroFixture.editPayload(server, "name", "Server2")
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docDelta(stream: "boards", id: "b1", seq: 1, codec: LoroReplicaCodec.codecName, payload: delta)],
            cursor: "6:", more: false
        ))
        try await engine.pullOnce(shard: "user")

        let doc = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(try LoroFixture.meta(fold: doc.fold, "name"), "Server2")
        let codec = LoroReplicaCodec()
        XCTAssertTrue(
            codec.isEmptyDiff(try codec.diff(fold: doc.fold, since: doc.acked)),
            "a served delta is by definition acked — the client owes nothing for it"
        )
    }

    /// The sign-in race:
    /// opening an owner knocks a bootstrap pull without waiting, the person
    /// creates a project, and the server answers the bootstrap before the
    /// birth reaches it. That reset page lacks the document the journal still
    /// owes.
    func testAnOwedBirthSurvivesAResetPageAnsweredBeforeIt() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)
        await transport.failPushes(true)
        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Plans")
        try await engine.createDoc(stream: "boards", id: "b1", seed: try author.export(mode: .snapshot), peer: 7)

        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [], cursor: "5:", more: false))
        try await engine.pullOnce(shard: "user")

        XCTAssertEqual(try store.peekPending().count, 1, "the journal still owes the birth")
        let doc = try XCTUnwrap(store.peekDoc("boards", "b1"), "the reset erased a birth the journal still owes")
        XCTAssertEqual(try LoroFixture.meta(fold: doc.fold, "name"), "Plans")
        _ = try await engine.updateDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self) { document in
            try LoroFixture.setMeta(document.doc, "color", "red")
        }
    }

    /// Cloud Backup as a flag the test flips; flipping it is the gate's signal.
    private final class Backup: SyncGate, @unchecked Sendable {
        let id = "backup"
        let stream: String? = nil
        private let lock = NSLock()
        private var on: Bool
        private let signal = SyncGateSignal()

        init(on: Bool) { self.on = on }

        func judge(_ change: SyncChange) -> SyncVerdict {
            lock.withLock { on } ? .push : .gate("cloud backup off")
        }

        var changes: AsyncStream<Void> { signal.stream }

        func set(_ value: Bool) {
            lock.withLock { on = value }
            signal.fire()
        }
    }

    /// The free plan: Cloud Backup off holds every write on the device, so the
    /// server's world never has these documents. A reset — a device back after
    /// the server's GC horizon, the rebuild lever — must not erase the only copy.
    func testAResetKeepsWhatTheBackupGateHolds() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport, syncGates: [Backup(on: false)])
        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Plans")
        try await engine.createDoc(stream: "boards", id: "b1", seed: try author.export(mode: .snapshot), peer: 7)
        _ = try await engine.updateDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self) { document in
            try LoroFixture.setMeta(document.doc, "color", "red")
        }
        _ = try await engine.drain()
        let held = await transport.pushedBatches.flatMap { $0 }
        XCTAssertTrue(held.isEmpty, "backup off sends nothing")

        await transport.queuePull(shard: "user", ReplicaPullResponse(frames: [], cursor: "5:", more: false))
        try await engine.pullOnce(shard: "user")

        let doc = try XCTUnwrap(store.peekDoc("boards", "b1"), "the reset erased the only copy of a held document")
        XCTAssertEqual(try LoroFixture.meta(fold: doc.fold, "name"), "Plans")
        XCTAssertEqual(try LoroFixture.meta(fold: doc.fold, "color"), "red")
        XCTAssertEqual(doc.peer, 7, "the held document keeps its peer")
        XCTAssertEqual(try store.peekSnapshot("boards", "b1")?.data["name"], .string("Plans"), "its row stays too")
        XCTAssertEqual(try engine.heldRows().map(\.rowId), ["b1"], "the document is still held")
        XCTAssertTrue(try store.peekPending().isEmpty)
    }

    /// Born while backup was off: the document leaves as ONE birth whose seed
    /// is its whole fold — the edits made while held included.
    func testADocumentBornHeldLeavesAsOneBirthOfItsWholeFold() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let backup = Backup(on: false)
        let engine = LoroFixture.engine(store: store, transport: transport, syncGates: [backup])
        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Plans")
        try await engine.createDoc(stream: "boards", id: "b1", seed: try author.export(mode: .snapshot), peer: 7)
        _ = try await engine.updateDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self) { document in
            try LoroFixture.setMeta(document.doc, "color", "red")
        }

        backup.set(true)
        try await waitForNoHolds(engine)
        _ = try await engine.drain()

        let sent = await transport.pushedBatches.flatMap { $0 }
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate])
        let seed = try XCTUnwrap(sent.first?.seed)
        XCTAssertEqual(try LoroFixture.meta(fold: seed, "name"), "Plans")
        XCTAssertEqual(try LoroFixture.meta(fold: seed, "color"), "red")
    }

    /// Known to the server: the document leaves as ONE delta past what the
    /// server acked — never the birth again.
    func testADocumentTheServerKnowsLeavesAsOneDeltaPastWhatItAcked() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let backup = Backup(on: true)
        let engine = LoroFixture.engine(store: store, transport: transport, syncGates: [backup])
        let author = try LoroFixture.doc(peer: 7)
        try LoroFixture.setMeta(author, "name", "Plans")
        let seed = try author.export(mode: .snapshot)
        try await engine.createDoc(stream: "boards", id: "b1", seed: seed, peer: 7)
        _ = try await engine.drain()

        backup.set(false)
        for color in ["red", "blue"] {
            _ = try await engine.updateDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self) { document in
                try LoroFixture.setMeta(document.doc, "color", color)
            }
        }
        XCTAssertEqual(try engine.heldRows().map(\.rowId), ["b1"])

        backup.set(true)
        try await waitForNoHolds(engine)
        _ = try await engine.drain()

        let sent = await transport.pushedBatches.flatMap { $0 }
        XCTAssertEqual(sent.map(\.verb), [ReplicaOp.Verb.rowCreate, ReplicaOp.Verb.docDelta])
        let delta = try XCTUnwrap(sent.last?.payload)
        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer, fold: seed)
        _ = try server.import(bytes: delta)
        XCTAssertEqual(LoroFixture.meta(server, "color"), "blue")
        let bare = try LoroFixture.doc(peer: 999_998)
        _ = try bare.import(bytes: delta)
        XCTAssertNil(LoroFixture.meta(bare, "name"), "the delta carried the birth the server already acked")
    }

    private func waitForNoHolds(_ engine: ReplicaEngine, file: StaticString = #filePath, line: UInt = #line) async throws {
        let deadline = Date().addingTimeInterval(3)
        while Date() < deadline {
            if try engine.heldRows().isEmpty { return }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        XCTFail("the backup signal never released the holds", file: file, line: line)
    }

    /// A server-known document with an edit the server has not taken: the
    /// reset's copy merges under the edit instead of replacing the fold.
    func testAResetMergesTheServersCopyUnderAnUnpushedEdit() async throws {
        let store = try LoroFixture.store()
        let transport = LoroStubTransport()
        let engine = LoroFixture.engine(store: store, transport: transport)
        let server = try LoroFixture.doc(peer: LoroFixture.serverPeer)
        try await queueServerCopy(server, named: "Server", transport, cursor: "5:")
        try await engine.pullOnce(shard: "user")
        try await editUnpushed(engine, transport)

        try await queueServerCopy(server, named: "Server2", transport, cursor: "9:")
        await transport.protocolFixture.forgetCursors()
        let restarted = try await engine.pullOnce(shard: "user")
        XCTAssertEqual(restarted, 0, "a cursor the server no longer knows starts a baseline round")
        try await engine.pullOnce(shard: "user")

        try assertServerCopyMergedUnderTheEdit(store)
    }

    private func queueServerCopy(_ server: LoroDoc, named name: String, _ transport: LoroStubTransport, cursor: String) async throws {
        try LoroFixture.setMeta(server, "name", name)
        await transport.queuePull(shard: "user", ReplicaPullResponse(
            frames: [.docSnapshot(
                stream: "boards", id: "b1", codec: LoroReplicaCodec.codecName,
                snapshot: try server.export(mode: .snapshot), data: ["name": .string(name)]
            )],
            cursor: cursor, more: false
        ))
    }

    private func editUnpushed(_ engine: ReplicaEngine, _ transport: LoroStubTransport) async throws {
        await transport.failPushes(true)
        _ = try await engine.updateDocument(stream: "boards", id: "b1", codec: LoroReplicaCodec.self) { document in
            try LoroFixture.setMeta(document.doc, "color", "red")
        }
    }

    private func assertServerCopyMergedUnderTheEdit(_ store: ReplicaStateStore) throws {
        let doc = try XCTUnwrap(store.peekDoc("boards", "b1"))
        XCTAssertEqual(try LoroFixture.meta(fold: doc.fold, "name"), "Server2")
        XCTAssertEqual(try LoroFixture.meta(fold: doc.fold, "color"), "red", "the reset erased an edit the server never took")
        XCTAssertEqual(doc.peer, 100, "no rotation while the fold lives")
        XCTAssertEqual(try store.peekPending().count, 1, "the edit is still owed")
    }
}
