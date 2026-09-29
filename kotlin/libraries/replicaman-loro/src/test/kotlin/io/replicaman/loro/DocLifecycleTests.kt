package io.replicaman.loro

import kotlinx.coroutines.runBlocking
import org.junit.Test
import io.replicaman.loro.binding.ExportMode
import io.replicaman.DocumentStream
import io.replicaman.ReplicaEngine
import io.replicaman.ReplicaFrame
import io.replicaman.ReplicaNoField
import io.replicaman.ReplicaOp
import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.ReplicaReflection
import io.replicaman.ReplicaSchema
import io.replicaman.ReplicaStreamSpec
import io.replicaman.ReplicaValue
import io.replicaman.ReplicaVerdict
import io.replicaman.SyncChange
import io.replicaman.SyncGate
import io.replicaman.SyncGateSignal
import io.replicaman.SyncVerdict
import io.replicaman.ReplicaStateStore
import io.replicaman.recoveryChunk
import io.replicaman.recoveryParts
import io.replicaman.recoveryRecords
import io.replicaman.removeRecoveryRecord
import io.replicaman.docRow
import io.replicaman.updateDocument
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.assertContentEquals
import kotlin.test.assertFails
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The document lifecycle over the REAL codec: create(seed) →
 * journal op; delta supersede folds two local edits into ONE pending op;
 * accepted verdicts advance acked; rejections discard and re-bootstrap;
 * doc.snapshot merge preserves unpushed local ops.
 */
class DocLifecycleTests : LoroTestCase() {
    @Test
    fun invalidDocumentFrameRollsBackTheWholeCheckpoint() = runBlocking {
        for (kind in listOf("delta", "existing snapshot", "new snapshot", "reset snapshot")) {
            val transport = LoroStubTransport()
            val engine = LoroFixture.engine(transport)
            val author = LoroFixture.doc(peer = 7uL)
            LoroFixture.setMeta(author, "name", "Before")
            val seed = author.export(ExportMode.Snapshot)
            transport.queuePull("user", ReplicaPullResponse(frames = listOf(
                ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, seed, mapOf("name" to ReplicaValue.Str("Before")))
            ), cursor = "5:", more = false))
            engine.pullOnce("user")
            val before = assertNotNull(engine.docRow("boards", "b1")).fold
            val invalid = if (kind == "delta")
                ReplicaFrame.DocDelta("boards", "b1", 1, LoroReplicaCodec.CODEC_NAME, "broken".toByteArray())
            else ReplicaFrame.DocSnapshot("boards", if (kind == "existing snapshot") "b1" else "b2", LoroReplicaCodec.CODEC_NAME, "broken".toByteArray(), mapOf("name" to ReplicaValue.Str("After")))
            if (kind == "reset snapshot") engine.resetCursors()
            transport.queuePull("user", ReplicaPullResponse(frames = listOf(invalid), cursor = "9:", more = false))

            assertFails(kind) { engine.pullOnce("user") }

            assertEquals(if (kind == "reset snapshot") null else "5:", engine.currentCursor(), kind)
            assertNull(DocumentStream(engine, Board).find("b2"), kind)
            assertContentEquals(before, engine.docRow("boards", "b1")?.fold, kind)
        }
    }
    private val codec = LoroReplicaCodec()

    // The current Swift reset cases use the real name reflection in their fixture.
    private fun resetSchema() = ReplicaSchema(listOf(
        ReplicaStreamSpec("boards", ReplicaStreamSpec.Lane.DOCUMENT, shard = "user",
            codec = LoroReplicaCodec.CODEC_NAME,
            reflections = listOf(ReplicaReflection("name", listOf("meta", "name")))),
        ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, shard = "user"),
    ))

    /** Kill: journal the create without its `seed` — the pushed op carries no document and acked never reaches the seed's version. */
    @Test
    fun createSeedJournalsAndAcceptanceAdvancesAcked() = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)

        val author = LoroFixture.doc(peer = 7uL)
        LoroFixture.setMeta(author, "name", "Plans")
        val seed = author.export(ExportMode.Snapshot)

        engine.createDoc("boards", "b1", seed, 7uL, mapOf("name" to ReplicaValue.Str("Plans")))

        val pending = engine.pendingOps()
        assertEquals(1, pending.size)
        val op = assertNotNull(pending.firstOrNull()).op()
        assertEquals(ReplicaOp.Verb.ROW_CREATE, op.verb)
        assertEquals(LoroReplicaCodec.CODEC_NAME, op.codec)
        assertContentEquals(seed, op.seed, "the create carries the seed, not a delta")
        assertEquals("Plans", DocumentStream(engine, Board).find("b1")?.name)
        assertEquals(7uL, engine.docRow("boards", "b1")?.peer, "the authoring peer is recorded")

        engine.drain()
        assertEquals(0, engine.pendingOps().size)

        // Acked caught up with the seed: the document owes nothing further.
        val doc = assertNotNull(engine.docRow("boards", "b1"))
        assertTrue(
            codec.isEmptyDiff(codec.diff(doc.fold, since = doc.acked)),
            "an accepted create advances acked to the seed's version",
        )
    }

    /** Kill: journal each `recordDocDelta` as its own op — two pending entries, two pushes for one document. */
    @Test
    fun twoLocalEditsSupersedeIntoOnePendingOp() = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)

        val author = LoroFixture.doc(peer = 7uL)
        LoroFixture.setMeta(author, "name", "Plans")
        val seed = author.export(ExportMode.Snapshot)
        engine.createDoc("boards", "b1", seed, 7uL)
        engine.drain()

        engine.recordDocDelta("boards", "b1", LoroFixture.editPayload(author, "color", "red"))
        val first = engine.pendingOps().single().id
        engine.recordDocDelta("boards", "b1", LoroFixture.editPayload(author, "mood", "calm"))

        val pending = engine.pendingOps()
        assertEquals(1, pending.size, "per-document supersede: ONE pending merged doc.delta")
        val entry = assertNotNull(pending.firstOrNull())
        assertEquals(first, entry.id, "the supersede id is stable")
        val op = entry.op()
        assertEquals(ReplicaOp.Verb.DOC_DELTA, op.verb)

        // The one payload carries BOTH edits: apply it to the server's copy.
        val server = LoroFixture.doc(peer = LoroFixture.SERVER_PEER, fold = seed)
        server.import(assertNotNull(op.payload))
        assertEquals("red", LoroFixture.meta(server, "color"))
        assertEquals("calm", LoroFixture.meta(server, "mood"))

        // The fold absorbed both too.
        val fold = assertNotNull(engine.docRow("boards", "b1")).fold
        assertEquals("red", LoroFixture.meta(fold, "color"))
        assertEquals("calm", LoroFixture.meta(fold, "mood"))
    }

    /**
     * A rejected doc.delta is DISCARDED (parking would push
     * the same refused history forever) and the shard re-bootstraps — the
     * forced `reset: true` replaces the world with the server's truth.
     *
     * Kill: park a rejected doc.delta like a row verdict — `parkedOps` holds
     * it, the cursor stays, and the refused edit survives in the fold.
     */
    @Test
    fun rejectedDeltaDiscardsAndRebootstrapsToServerTruth() = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)

        val author = LoroFixture.doc(peer = 7uL)
        LoroFixture.setMeta(author, "name", "Plans")
        val seed = author.export(ExportMode.Snapshot)
        engine.createDoc("boards", "b1", seed, 7uL)
        engine.drain()

        engine.recordDocDelta("boards", "b1", LoroFixture.editPayload(author, "color", "red"))
        transport.scriptPush { ops ->
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, reason = "beyond quota") }
        }
        engine.drain()

        assertEquals(0, engine.pendingOps().size, "the refused delta is gone")
        assertEquals(1, engine.parkedOps().size, "the refusal remains visible without automatic retry")
        assertNull(engine.currentCursor("user"), "the shard re-bootstraps: the next pull replaces the world with truth")

        // The re-bootstrap lands the server's copy of the doc fresh.
        val serverTruth = LoroFixture.doc(peer = LoroFixture.SERVER_PEER, fold = seed)
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot(
                        stream = "boards", id = "b1", codec = LoroReplicaCodec.CODEC_NAME,
                        snapshot = serverTruth.export(ExportMode.Snapshot),
                        data = mapOf("name" to ReplicaValue.Str("Plans")),
                    ),
                ),
                cursor = "9:", more = false,
            ),
        )
        engine.pullOnce("user")

        val doc = assertNotNull(engine.docRow("boards", "b1"))
        assertEquals("Plans", LoroFixture.meta(doc.fold, "name"))
        assertNull(LoroFixture.meta(doc.fold, "color"), "the refused edit is genuinely undone")
        assertEquals(100uL, doc.peer, "the reborn fold minted a fresh peer")
        val path = assertNotNull(engine.storePath).path
        engine.close()
        val reopened = ReplicaStateStore(path)
        try {
            val recovery = reopened.recoveryRecords().single()
            assertEquals("red", LoroFixture.meta(recoveryBytes(reopened, recovery, "document.fold"), "color"))
            assertEquals("beyond quota", recovery.reason)
            val states = reopened.recoveryParts(recovery.id).filter { it.kind == "intent.metadata" }.map { part ->
                Json.parseToJsonElement(reopened.recoveryChunk(recovery.id, part).decodeToString())
                    .jsonObject.getValue("state").jsonPrimitive.content
            }
            assertEquals(listOf("accepted", "frozen"), states.sorted(), "the refused delta, and the accepted birth it followed")
            reopened.removeRecoveryRecord(recovery.id)
            assertTrue(reopened.recoveryRecords().isEmpty())
        } finally { reopened.close() }

    }

    /** Kill: replace the fold with the server snapshot instead of merging — the unpushed local op vanishes. */
    @Test
    fun serverSnapshotMergePreservesUnpushedLocalOps() = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)

        // Server-born document arrives on bootstrap.
        val server = LoroFixture.doc(peer = LoroFixture.SERVER_PEER)
        LoroFixture.setMeta(server, "name", "Server")
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot(
                        stream = "boards", id = "b1", codec = LoroReplicaCodec.CODEC_NAME,
                        snapshot = server.export(ExportMode.Snapshot),
                        data = mapOf("name" to ReplicaValue.Str("Server")),
                    ),
                ),
                cursor = "5:", more = false,
            ),
        )
        engine.pullOnce("user")

        // A local edit authored from the fold, under the store's minted peer.
        val peer = assertNotNull(engine.docPeer("boards", "b1"))
        assertEquals(100uL, peer, "the fold's peer was minted by the engine")
        val fold = assertNotNull(engine.docFold("boards", "b1"))
        val app = LoroFixture.doc(peer = peer, fold = fold)
        engine.recordDocDelta("boards", "b1", LoroFixture.editPayload(app, "color", "red"))

        // Push side dead: the local op is still UNPUSHED when the server's
        // snapshot arrives — the exact shape the merge rule protects.
        transport.failPushes(true)

        // The server evolves WITHOUT our edit; its fresh snapshot arrives.
        LoroFixture.setMeta(server, "name", "Server2")
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot(
                        stream = "boards", id = "b1", codec = LoroReplicaCodec.CODEC_NAME,
                        snapshot = server.export(ExportMode.Snapshot),
                        data = mapOf("name" to ReplicaValue.Str("Server2")),
                    ),
                ),
                cursor = "9:", more = false,
            ),
        )
        engine.pullOnce("user")

        // MERGE, never a blind replace: both sides survive.
        val merged = assertNotNull(engine.docRow("boards", "b1"))
        assertEquals("Server2", LoroFixture.meta(merged.fold, "name"))
        assertEquals("red", LoroFixture.meta(merged.fold, "color"), "the unpushed local op survived the snapshot")
        assertEquals(100uL, merged.peer, "no rotation while the fold lives")

        // Still owed: the local op is not acked until its own verdict.
        assertEquals(1, engine.pendingOps().size)
        assertFalse(codec.isEmptyDiff(codec.diff(merged.fold, since = merged.acked)))

        transport.failPushes(false)
        engine.drain()
        val drained = assertNotNull(engine.docRow("boards", "b1"))
        assertTrue(
            codec.isEmptyDiff(codec.diff(drained.fold, since = drained.acked)),
            "the accepted delta advances acked over the local op",
        )
    }

    /** Kill: fold a served delta without advancing acked by its `payloadVersion` — the client owes the server its own frame back. */
    @Test
    fun serverDeltaFrameMergesAndAdvancesAcked() = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)

        val server = LoroFixture.doc(peer = LoroFixture.SERVER_PEER)
        LoroFixture.setMeta(server, "name", "Server")
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot(
                        stream = "boards", id = "b1", codec = LoroReplicaCodec.CODEC_NAME,
                        snapshot = server.export(ExportMode.Snapshot), data = emptyMap(),
                    ),
                ),
                cursor = "5:", more = false,
            ),
        )
        engine.pullOnce("user")

        val delta = LoroFixture.editPayload(server, "name", "Server2")
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocDelta(
                        stream = "boards", id = "b1", seq = 1, codec = LoroReplicaCodec.CODEC_NAME, payload = delta,
                    ),
                ),
                cursor = "6:", more = false,
            ),
        )
        engine.pullOnce("user")

        val doc = assertNotNull(engine.docRow("boards", "b1"))
        assertEquals("Server2", LoroFixture.meta(doc.fold, "name"))
        assertTrue(
            codec.isEmptyDiff(codec.diff(doc.fold, since = doc.acked)),
            "a served delta is by definition acked — the client owes nothing for it",
        )
    }

    /** Current Swift sign-in race: the bootstrap page can precede the device's unsent birth. */
    @Test fun anOwedBirthSurvivesAResetPageAnsweredBeforeIt(): Unit = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport, schema = resetSchema())
        transport.failPushes(true)
        val author = LoroFixture.doc(7uL)
        LoroFixture.setMeta(author, "name", "Plans")
        engine.createDoc("boards", "b1", author.export(ExportMode.Snapshot), 7uL)

        transport.queuePull("user", ReplicaPullResponse(frames = emptyList(), cursor = "5:", more = false))
        engine.pullOnce("user")

        assertEquals(1, engine.pendingOps().size, "the journal still owes the birth")
        val doc = assertNotNull(engine.docRow("boards", "b1"), "the reset erased a birth the journal still owes")
        assertEquals("Plans", LoroFixture.meta(doc.fold, "name"))
        engine.updateDocument("boards", "b1", codec) { LoroFixture.setMeta(it.doc, "color", "red") }
    }

    /** Backup as a flag the test flips; flipping it is the gate's signal. */
    private class Backup(on: Boolean) : SyncGate {
        override val id: String = "backup"
        override val stream: String? = null
        @Volatile private var enabled = on
        private val signal = SyncGateSignal()

        override fun judge(change: SyncChange): SyncVerdict = if (enabled) SyncVerdict.Push else SyncVerdict.Gate("cloud backup off")

        override val changes get() = signal.changes

        fun set(on: Boolean) {
            enabled = on
            signal.fire()
        }
    }

    private suspend fun waitForNoHolds(engine: ReplicaEngine) {
        val deadline = System.currentTimeMillis() + 3_000
        while (System.currentTimeMillis() < deadline) {
            if (engine.heldRows().isEmpty()) return
            Thread.sleep(20)
        }
        kotlin.test.fail("the backup signal never released the holds")
    }

    /** Current Swift free-plan law: a backup gate can hold the only copy indefinitely. */
    @Test fun aResetKeepsWhatTheBackupGateHolds(): Unit = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport, schema = resetSchema(), syncGates = listOf(Backup(on = false)))
        val author = LoroFixture.doc(7uL)
        LoroFixture.setMeta(author, "name", "Plans")
        engine.createDoc("boards", "b1", author.export(ExportMode.Snapshot), 7uL)
        engine.updateDocument("boards", "b1", codec) { LoroFixture.setMeta(it.doc, "color", "red") }
        engine.drain()
        assertTrue(transport.pushedBatches.flatten().isEmpty(), "backup off sends nothing")

        transport.queuePull("user", ReplicaPullResponse(frames = emptyList(), cursor = "5:", more = false))
        engine.pullOnce("user")

        val doc = assertNotNull(engine.docRow("boards", "b1"), "the reset erased the only copy of a held document")
        assertEquals("Plans", LoroFixture.meta(doc.fold, "name"))
        assertEquals("red", LoroFixture.meta(doc.fold, "color"))
        assertEquals(7uL, doc.peer, "the held document keeps its peer")
        assertEquals("Plans", DocumentStream(engine, Board).find("b1")?.name, "its row stays too")
        assertEquals(listOf("b1"), engine.heldRows().map { it.rowId }, "the document is still held")
        assertTrue(engine.pendingOps().isEmpty())
    }

    /** Born while backup was off: the document leaves as ONE birth whose seed is its whole fold. */
    @Test fun aDocumentBornHeldLeavesAsOneBirthOfItsWholeFold(): Unit = runBlocking {
        val transport = LoroStubTransport()
        val backup = Backup(on = false)
        val engine = LoroFixture.engine(transport, schema = resetSchema(), syncGates = listOf(backup))
        val author = LoroFixture.doc(7uL)
        LoroFixture.setMeta(author, "name", "Plans")
        engine.createDoc("boards", "b1", author.export(ExportMode.Snapshot), 7uL)
        engine.updateDocument("boards", "b1", codec) { LoroFixture.setMeta(it.doc, "color", "red") }

        backup.set(true)
        waitForNoHolds(engine)
        engine.drain()

        val sent = transport.pushedBatches.flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), sent.map { it.verb })
        val seed = assertNotNull(sent.first().seed)
        assertEquals("Plans", LoroFixture.meta(seed, "name"))
        assertEquals("red", LoroFixture.meta(seed, "color"))
    }

    /** Known to the server: the document leaves as ONE delta past what the server acked — never the birth again. */
    @Test fun aDocumentTheServerKnowsLeavesAsOneDeltaPastWhatItAcked(): Unit = runBlocking {
        val transport = LoroStubTransport()
        val backup = Backup(on = true)
        val engine = LoroFixture.engine(transport, schema = resetSchema(), syncGates = listOf(backup))
        val author = LoroFixture.doc(7uL)
        LoroFixture.setMeta(author, "name", "Plans")
        val seed = author.export(ExportMode.Snapshot)
        engine.createDoc("boards", "b1", seed, 7uL)
        engine.drain()

        backup.set(false)
        for (color in listOf("red", "blue")) {
            engine.updateDocument("boards", "b1", codec) { LoroFixture.setMeta(it.doc, "color", color) }
        }
        assertEquals(listOf("b1"), engine.heldRows().map { it.rowId })

        backup.set(true)
        waitForNoHolds(engine)
        engine.drain()

        val sent = transport.pushedBatches.flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.DOC_DELTA), sent.map { it.verb })
        val delta = assertNotNull(sent.last().payload)
        val server = LoroFixture.doc(LoroFixture.SERVER_PEER, seed)
        server.import(delta)
        assertEquals("blue", LoroFixture.meta(server, "color"))
        val bare = LoroFixture.doc(999_998uL)
        bare.import(delta)
        assertNull(LoroFixture.meta(bare, "name"), "the delta carried the birth the server already acked")
    }

    /** Current Swift reset snapshot merges beneath the edit the server has not taken. */
    @Test fun aResetMergesTheServersCopyUnderAnUnpushedEdit(): Unit = runBlocking {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport, schema = resetSchema())
        val server = LoroFixture.doc(LoroFixture.SERVER_PEER)
        LoroFixture.setMeta(server, "name", "Server")
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocSnapshot("boards", "b1", codec.name,
                server.export(ExportMode.Snapshot), mapOf("name" to ReplicaValue.Str("Server")))),
            cursor = "5:", more = false))
        engine.pullOnce("user")
        transport.failPushes(true)
        engine.updateDocument("boards", "b1", codec) { LoroFixture.setMeta(it.doc, "color", "red") }

        LoroFixture.setMeta(server, "name", "Server2")
        engine.resetCursors()
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocSnapshot("boards", "b1", codec.name,
                server.export(ExportMode.Snapshot), mapOf("name" to ReplicaValue.Str("Server2")))),
            cursor = "9:", more = false))
        engine.pullOnce("user")

        val doc = assertNotNull(engine.docRow("boards", "b1"))
        assertEquals("Server2", LoroFixture.meta(doc.fold, "name"))
        assertEquals("red", LoroFixture.meta(doc.fold, "color"), "the reset erased an edit the server never took")
        assertEquals(100uL, doc.peer, "no rotation while the fold lives")
        assertEquals(1, engine.pendingOps().size, "the edit is still owed")
    }
}
