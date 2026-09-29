package io.replicaman.loro

import io.replicaman.testing.fixtureWrite

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import io.replicaman.ReplicaError
import io.replicaman.ReplicaValue
import kotlin.test.assertContentEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import io.replicaman.loro.binding.ExportMode
import io.replicaman.DocumentStream
import io.replicaman.ReplicaEngine
import io.replicaman.ReplicaFrame
import io.replicaman.ReplicaNoField
import io.replicaman.ReplicaOp
import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.ReplicaSchema
import io.replicaman.ReplicaStreamSpec
import io.replicaman.recoveryRecords
import io.replicaman.docRow
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The document's own doors, the rows' law over the real codec: `findDoc`
 * is a SYNC read of a state the engine holds (warm = no decode), `updateDoc`
 * moves the held document and journals ONE delta, a pulled delta reaches
 * the held document without a reopen, `watchDoc` delivers both movements
 * through one door, `undoDoc` inverts this peer's last edit.
 */
class DocumentStateTests : LoroTestCase() {
    private val codec = LoroReplicaCodec()

    @Test
    fun aFailedDiskWriteDropsTheUncommittedHeldEdit() = runBlocking {
        val w = world()
        w.boards.createDoc("b1", seed("Plans"), 7uL)
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
        val store = assertNotNull(w.engine.database)
        store.fixtureWrite { db -> db.prepare("""
            CREATE TRIGGER refuse_fold BEFORE UPDATE OF fold ON docs
            BEGIN SELECT RAISE(ABORT, 'disk write failed'); END
        """.trimIndent()).use { it.step() } }

        val failure = assertFails {
            w.boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Unsaved") }
        }
        assertTrue(failure.toString().contains("disk write failed"), failure.toString())
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
        assertEquals("Plans", LoroFixture.meta(assertNotNull(w.engine.docRow("boards", "b1")).fold, "name"))
        store.fixtureWrite { db -> db.prepare("DROP TRIGGER refuse_fold").use { it.step() } }
        w.boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "color", "blue") }
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name,
            "an unrelated edit cannot commit the refused rename")
        val fold = assertNotNull(w.engine.docRow("boards", "b1")).fold
        assertEquals("Plans", LoroFixture.meta(fold, "name"))
        assertEquals("blue", LoroFixture.meta(fold, "color"), "the later edit really committed")
    }

    private class World(
        val transport: LoroStubTransport,
        val engine: ReplicaEngine,
        val boards: DocumentStream<Board, ReplicaNoField>,
    )

    @Before
    fun resetDecodes() {
        BoardState.decodes.set(0)
    }

    private suspend fun world(): World {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)
        return World(transport, engine, DocumentStream(engine, Board))
    }

    private fun seed(name: String, peer: ULong = 7uL): ByteArray {
        val author = LoroFixture.doc(peer = peer)
        LoroFixture.setMeta(author, "name", name)
        return author.export(ExportMode.Snapshot)
    }

    /** Kill: make `findDoc` read the projection row instead of opening the fold — a seed with no `data` reads null. */
    @Test
    fun findDocReadsTheSeedSynchronously() = runBlocking {
        val w = world()
        w.boards.createDoc("b1", seed("Plans"), 7uL)

        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
        assertNull(w.boards.findDoc("nope", BoardState), "no document, no state")
    }

    /**
     * The warm law: a second read of an unmoved document decodes nothing —
     * the state is the engine's, minted once per version.
     *
     * Kill: mint the state from the fold on every read (no held document)
     * and the counter climbs with every call.
     */
    @Test
    fun aWarmReadDecodesNothing() = runBlocking {
        val w = world()
        w.boards.createDoc("b1", seed("Plans"), 7uL)
        w.boards.findDoc("b1", BoardState)
        val after = BoardState.decodes.get()

        w.boards.findDoc("b1", BoardState)
        w.boards.findDoc("b1", BoardState)
        assertEquals(after, BoardState.decodes.get(), "an unmoved document was decoded again")
    }

    /** Kill: journal the delta but leave the fold at the seed — `findDoc` moves, the fold does not, and the next open reverts. */
    @Test
    fun updateDocMovesTheStateAndJournalsOneDelta() = runBlocking {
        val w = world()
        w.boards.createDoc("b1", seed("Plans"), 7uL)

        val moved = w.boards.updateDoc("b1", codec) { document ->
            LoroFixture.setMeta(document.doc, "name", "Renamed")
        }
        assertTrue(moved)
        assertEquals("Renamed", w.boards.findDoc("b1", BoardState)?.name)
        val deltas = w.engine.pendingOps().filter { it.op().verb == ReplicaOp.Verb.DOC_DELTA }
        assertEquals(1, deltas.size, "one pending delta per document, superseded")
        assertEquals(
            "Renamed",
            LoroFixture.meta(assertNotNull(w.engine.docRow("boards", "b1")).fold, "name"),
            "the fold moved with the document",
        )

        val still = w.boards.updateDoc("b1", codec) { }
        assertFalse(still, "a body that moved nothing owes nothing")
    }

    /**
     * A peer's delta pulled from the server lands in the held document —
     * no reopen — so the next read shows it and the next local edit builds
     * on it.
     *
     * Kill: hold documents but never absorb a pulled delta into them; the
     * held document stays at the seed and the read shows "Plans".
     */
    @Test
    fun aPulledDeltaReachesTheHeldDocument() = runBlocking {
        val w = world()
        val peerDoc = LoroFixture.doc(peer = 7uL)
        LoroFixture.setMeta(peerDoc, "name", "Plans")
        val seedBytes = peerDoc.export(ExportMode.Snapshot)
        w.boards.createDoc("b1", seedBytes, 100uL)
        w.engine.drain()
        w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, peerDoc.export(ExportMode.Snapshot), emptyMap())
        ), cursor = "1:", more = false))
        w.engine.pullOnce()

        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)

        val payload = LoroFixture.editPayload(peerDoc, "name", "Peer's rename")
        w.transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocDelta(
                        stream = "boards", id = "b1", seq = 2, codec = LoroReplicaCodec.CODEC_NAME, payload = payload,
                    ),
                ),
                cursor = "2:", more = false,
            ),
        )
        w.engine.pullOnce("user")

        assertEquals(
            "Peer's rename",
            w.boards.findDoc("b1", BoardState)?.name,
            "the pulled delta never reached the state the read serves",
        )
        // The next local edit stands on the peer's history: its delta must
        // import cleanly into the peer's document (no missing deps).
        w.boards.updateDoc("b1", codec) { document ->
            LoroFixture.setMeta(document.doc, "name", "Both")
        }
        val owed = assertNotNull(w.engine.pendingOps().firstOrNull { it.op().verb == ReplicaOp.Verb.DOC_DELTA })
        peerDoc.import(assertNotNull(owed.op().payload))
        assertEquals("Both", LoroFixture.meta(peerDoc, "name"))
    }

    /** Kill: bump the change sequence only on pulled deltas (or only on local edits) — one of the two movements never arrives. */
    @Test
    fun watchDocDeliversLocalAndPulledMovement() = runBlocking {
        val w = world()
        val peerDoc = LoroFixture.doc(peer = 7uL)
        LoroFixture.setMeta(peerDoc, "name", "Plans")
        w.boards.createDoc("b1", peerDoc.export(ExportMode.Snapshot), 100uL)
        w.engine.drain()
        w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, peerDoc.export(ExportMode.Snapshot), emptyMap())
        ), cursor = "1:", more = false))
        w.engine.pullOnce()


        val delivered = Delivered<String?>()
        // `includeInitial`: the baseline delivery is the proof the watch is armed
        // before the edit below — the current iOS test waits for that same baseline.
        val watch = w.boards.watchDoc("b1", BoardState, includeInitial = true) { state -> delivered.add(state?.name) }
        try {
            until("the baseline is delivered") { delivered.values.contains("Plans") }
            w.boards.updateDoc("b1", codec) { document ->
                LoroFixture.setMeta(document.doc, "name", "Local")
            }
            until("the local edit is delivered") { delivered.values.contains("Local") }

            // The peer edits ON TOP of the local edit, as it would after its
            // own pull — a concurrent edit would only be an LWW coin toss.
            val owed = assertNotNull(w.engine.pendingOps().firstOrNull { it.op().verb == ReplicaOp.Verb.DOC_DELTA })
            val baseline = peerDoc.oplogVv()
            peerDoc.import(assertNotNull(owed.op().payload))
            LoroFixture.setMeta(peerDoc, "name", "Pulled")
            val payload = peerDoc.export(ExportMode.Updates(baseline))
            w.transport.queuePull(
                "user",
                ReplicaPullResponse(
                    frames = listOf(
                        ReplicaFrame.DocDelta(
                            stream = "boards", id = "b1", seq = 2, codec = LoroReplicaCodec.CODEC_NAME, payload = payload,
                        ),
                    ),
                    cursor = "2:", more = false,
                ),
            )
            w.engine.pullOnce("user")
            until("the pulled delta is delivered") { delivered.values.contains("Pulled") }
            assertEquals(
                listOf("Plans", "Local", "Pulled"),
                delivered.values,
                "both movements arrive through the one door, each once, after the baseline",
            )
        } finally {
            watch.cancel()
        }
    }

    /** Kill: implement undo as "reopen the fold at the previous version" — `canRedo` is false afterwards and the redo fails. */
    @Test
    fun undoDocInvertsThisPeersLastEdit() = runBlocking {
        val w = world()
        w.boards.createDoc("b1", seed("Plans"), 100uL)
        w.boards.updateDoc("b1", codec) { document ->
            LoroFixture.setMeta(document.doc, "name", "Renamed")
        }
        assertEquals(true, w.boards.findDoc("b1", BoardState)?.canUndo)

        val undone = w.boards.undoDoc("b1", codec)
        assertTrue(undone)
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
        assertEquals(true, w.boards.findDoc("b1", BoardState)?.canRedo)
        val redone = w.boards.redoDoc("b1", codec)
        assertTrue(redone)
        assertEquals("Renamed", w.boards.findDoc("b1", BoardState)?.name)
    }

    /**
     * A session's hold is a declaration, not a touch: pinned before the
     * document is even held, the document still never leaves the LRU once
     * it is — the grid's reads of other projects cannot evict the open
     * editor's document (and its undo history with it).
     *
     * Kill: keep pins on the held entry only (`held[key]?.pins += 1`) — a
     * pin on a not-yet-held key is a no-op and the open reads it later as
     * unpinned.
     */
    @Test
    fun aPinDeclaredBeforeTheOpenSurvivesTheLRU() = runBlocking {
        val w = world()
        w.boards.pinDoc("b0")
        w.boards.createDoc("b0", seed("Pinned"), 7uL)
        w.boards.findDoc("b0", BoardState)

        // `LiveDocuments.capacity` is 32; one past it evicts the oldest unpinned entry.
        for (index in 1..LIVE_DOCUMENTS_CAPACITY + 1) {
            w.boards.createDoc("b$index", seed("Filler $index"), 7uL)
            w.boards.findDoc("b$index", BoardState)
        }

        assertEquals("Pinned", w.boards.heldDoc("b0", BoardState)?.name, "the pinned document left the LRU")
        assertNull(w.boards.heldDoc("b1", BoardState), "the oldest unpinned document should have been evicted")
    }

    /**
     * The peek: a HELD document answers, a closed one stays closed — the
     * grid asks every listed project this way and opens none of them.
     *
     * Kill: route `heldDoc` through `findDoc` — the peek opens the document and the decode counter moves.
     */
    @Test
    fun heldDocAnswersOnlyAHeldDocument() = runBlocking {
        val w = world()
        w.boards.createDoc("b1", seed("Plans"), 7uL)

        assertNull(w.boards.heldDoc("b1", BoardState), "a peek must not open the document")
        assertEquals(0, BoardState.decodes.get(), "a peek decoded a closed document")

        w.boards.findDoc("b1", BoardState)
        assertEquals("Plans", w.boards.heldDoc("b1", BoardState)?.name)
    }

    /** KILL: keep the last held document picture when its owner closes. */
    @Test fun theDocumentWatchClearsItsStateWhenTheOwnerCloses() = runBlocking {
        val w = world(); w.boards.createDoc("b1", seed("Private"), 7uL)
        val seen = Delivered<String?>()
        val watcher = w.boards.watchDoc("b1", BoardState, includeInitial = true) { seen.add(it?.name) }
        try {
            until("initial private document") { seen.values == listOf("Private") }
            w.engine.close()
            until("owner close must clear document") { seen.values == listOf("Private", null) }
        } finally { watcher.cancel() }
    }

    /** KILL: edit the held Loro before checking the sealed write admission — failed edits leak into reads and undo. */
    @Test fun aSealedEditLeavesBothTheHeldDocumentAndItsFoldUntouched() = runBlocking {
        val w = world(); w.boards.createDoc("b1", seed("Plans"), 100uL)
        val before = assertNotNull(w.boards.findDoc("b1", BoardState))
        val fold = assertNotNull(w.engine.docRow("boards", "b1")).fold
        val pending = w.engine.pendingOps().map { it.id }
        w.engine.seal()
        assertFailsWith<ReplicaError.IdentityTransitionInProgress> {
            w.boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Refused") }
        }
        assertEquals(before, w.boards.findDoc("b1", BoardState))
        assertContentEquals(fold, assertNotNull(w.engine.docRow("boards", "b1")).fold)
        assertEquals(pending, w.engine.pendingOps().map { it.id })
        w.engine.unseal()
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
    }

    /** A successful reset cannot discard the undo owner of a still-owed document. */
    @Test fun anOwedResetPreservesTheHeldDocumentAndItsUndoHistory(): Unit = runBlocking {
        val w = world()
        w.transport.failPushes(true)
        w.boards.createDoc("b1", seed("Plans"), 100uL)
        w.boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Local") }
        val before = assertNotNull(w.boards.findDoc("b1", BoardState))
        assertTrue(before.canUndo)
        val fold = assertNotNull(w.engine.docRow("boards", "b1")).fold

        w.transport.queuePull("user", ReplicaPullResponse(frames = emptyList(), cursor = "2:", more = false))
        w.engine.pullOnce("user")

        assertSame(before, w.boards.heldDoc("b1", BoardState), "the surviving document must not be reopened")
        assertContentEquals(fold, assertNotNull(w.engine.docRow("boards", "b1")).fold)
        assertTrue(w.boards.undoDoc("b1", codec), "the same held undo owner must survive the reset")
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
        assertTrue(w.boards.redoDoc("b1", codec))
        assertEquals("Local", w.boards.findDoc("b1", BoardState)?.name)
    }

    /** Global/assets bootstrap must not retire the held undo owner in the unchanged user shard. */
    @Test fun resettingAnotherShardKeepsTheHeldUserDocumentAndUndo(): Unit = runBlocking {
        val transport = LoroStubTransport()
        transport.failPushes(true)
        val schema = ReplicaSchema(listOf(
            ReplicaStreamSpec("boards", ReplicaStreamSpec.Lane.DOCUMENT, shard = "user", codec = LoroReplicaCodec.CODEC_NAME),
            ReplicaStreamSpec("assets", ReplicaStreamSpec.Lane.ROW, shard = "global"),
        ))
        val engine = LoroFixture.engine(transport, schema = schema)
        val boards = DocumentStream(engine, Board)
        boards.createDoc("b1", seed("Plans"), 100uL)
        boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Local") }
        val before = assertNotNull(boards.findDoc("b1", BoardState))
        assertTrue(before.canUndo)
        val fold = assertNotNull(engine.docRow("boards", "b1")).fold
        val pending = engine.pendingOps().map { it.id }

        transport.queuePull("global", ReplicaPullResponse(frames = listOf(ReplicaFrame.RowSet("assets", "a1", null, mapOf("kind" to ReplicaValue.Str("font")))),
            cursor = "1:", more = false))
        engine.pullOnce("global")

        assertSame(before, boards.heldDoc("b1", BoardState), "an unrelated reset must not reopen the user's document")
        assertContentEquals(fold, assertNotNull(engine.docRow("boards", "b1")).fold)
        assertEquals(pending, engine.pendingOps().map { it.id })
        assertEquals("1:", engine.currentCursor("global"))
        assertNull(engine.currentCursor("user"))
        assertTrue(boards.undoDoc("b1", codec))
        assertEquals("Plans", boards.findDoc("b1", BoardState)?.name)
        assertTrue(boards.redoDoc("b1", codec))
        assertEquals("Local", boards.findDoc("b1", BoardState)?.name)
    }

    @Test fun aServerDeletionArchivesTheOwedHeldDocumentOnResetAndTail(): Unit = runBlocking {
        for (reset in listOf(true, false)) {
            val w = world()
            w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(
                ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, seed("Plans"), emptyMap())
            ), cursor = "1:", more = false))
            w.engine.pullOnce()
            w.transport.failPushes(true)
            w.boards.createDoc("b0", seed("Cools the wire"), 8uL)
            assertFailsWith<ReplicaError.Transport> { w.engine.drain() }
            w.boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Local") }
            assertNotNull(w.boards.heldDoc("b1", BoardState))
            if (reset) w.engine.resetCursors()
            val frames = if (reset) emptyList() else listOf(ReplicaFrame.RowDelete("boards", "b1", revision = 11))
            w.transport.queuePull("user", ReplicaPullResponse(frames = frames, cursor = "11:", more = false))
            w.engine.pullOnce()
            assertNull(w.engine.docRow("boards", "b1"))
            assertEquals(listOf("b0"), w.engine.pendingOps().map { it.op().rowId })
            assertNull(w.boards.heldDoc("b1", BoardState))
            assertNull(w.boards.findDoc("b1", BoardState))
            val store = assertNotNull(w.engine.store)
            val archive = store.recoveryRecords().single()
            assertEquals("Local", LoroFixture.meta(recoveryBytes(store, archive, "document.fold"), "name"))
        }
    }

    /** Invalidating a held document is also transactional: a refused publication keeps its undo owner. */
    @Test fun aRolledBackBaselineKeepsTheOwedHeldDocument(): Unit = runBlocking {
        val w = world()
        w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, seed("Plans"), emptyMap())
        ), cursor = "1:", more = false))
        w.engine.pullOnce()
        w.transport.failPushes(true)
        w.boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Local") }
        val before = assertNotNull(w.boards.findDoc("b1", BoardState))
        val fold = assertNotNull(w.engine.docRow("boards", "b1")).fold
        val pending = w.engine.pendingOps().map { it.id }
        val store = assertNotNull(w.engine.store)
        w.engine.resetCursors()
        store.fixtureWrite { db -> db.prepare("CREATE TRIGGER refuse_reset_delete BEFORE INSERT ON checkpoints BEGIN SELECT RAISE(ABORT,'reset delete disk failure'); END").use { it.step() } }
        try {
            w.transport.queuePull("user", ReplicaPullResponse(frames = emptyList(), cursor = "10:", more = false))
            val error = assertFails { w.engine.pullOnce("user") }
            assertTrue(error.message.orEmpty().contains("reset delete disk failure"), "unexpected publication refusal: $error")
            assertSame(before, w.boards.heldDoc("b1", BoardState))
            assertContentEquals(fold, assertNotNull(w.engine.docRow("boards", "b1")).fold)
            assertEquals(pending, w.engine.pendingOps().map { it.id })
            assertNull(w.engine.currentCursor("user"))
        } finally {
            store.fixtureWrite { db -> db.prepare("DROP TRIGGER refuse_reset_delete").use { it.step() } }
        }
        assertTrue(w.boards.undoDoc("b1", codec))
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
    }

    /** KILL: clear held documents before a reset transaction commits — a real SQLite failure destroys undo history. */
    @Test fun failedResetPreservesTheHeldDocumentAndItsUndoHistory() = runBlocking {
        val w = world(); w.boards.createDoc("b1", seed("Plans"), 100uL)
        w.boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Local") }; w.engine.drain()
        val before = assertNotNull(w.boards.findDoc("b1", BoardState)); assertTrue(before.canUndo)
        val fold = assertNotNull(w.engine.docRow("boards", "b1")).fold
        val store = assertNotNull(w.engine.store)
        store.fixtureWrite { db -> db.prepare("CREATE TRIGGER fail_reset BEFORE INSERT ON docs BEGIN SELECT RAISE(ABORT,'reset disk failure'); END").use { it.step() } }
        w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, seed("Replacement"), emptyMap())), cursor = "2:", more = false))
        val error = assertFails { w.engine.pullOnce("user") }
        assertTrue(error.message.orEmpty().contains("reset disk failure"), "expected actual SQLite trigger failure, got $error")
        assertEquals(before, w.boards.findDoc("b1", BoardState))
        assertContentEquals(fold, assertNotNull(w.engine.docRow("boards", "b1")).fold)
        store.fixtureWrite { db -> db.prepare("DROP TRIGGER fail_reset").use { it.step() } }
        w.boards.undoDoc("b1", codec)
        assertEquals("Plans", w.boards.findDoc("b1", BoardState)?.name)
    }

    /** KILL: publish SQLite then release the held-document lock before absorption — a checkpoint-gap read serves the old version. */
    @Test fun watchDocCannotMissAReadDuringCheckpointPublication() = runBlocking {
        val w = world(); val peer = LoroFixture.doc(7uL)
        LoroFixture.setMeta(peer, "name", "Plans")
        w.boards.createDoc("b1", peer.export(ExportMode.Snapshot), 100uL)
        w.engine.drain()
        w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, peer.export(ExportMode.Snapshot), emptyMap())
        ), cursor = "1:", more = false))
        w.engine.pullOnce()

        val delivered = Delivered<String?>(); val watch = w.boards.watchDoc("b1", BoardState, includeInitial = true) { delivered.add(it?.name) }
        val store = assertNotNull(w.engine.store)
        val paused = java.util.concurrent.atomic.AtomicBoolean()
        var racingRead: java.util.concurrent.CompletableFuture<String?>? = null
        val observer = CoroutineScope(Dispatchers.Unconfined).launch(start = CoroutineStart.UNDISPATCHED) {
            store.commits.collect {
                val cursor = store.read { db -> db.prepare("SELECT cursor FROM checkpoints WHERE shard='user'").use { if (it.step()) it.getText(0) else null } }
                if (cursor == "2:" && paused.compareAndSet(false, true)) {
                    val entered = java.util.concurrent.CountDownLatch(1)
                    racingRead = java.util.concurrent.CompletableFuture.supplyAsync { entered.countDown(); w.boards.findDoc("b1", BoardState)?.name }
                    check(entered.await(2, java.util.concurrent.TimeUnit.SECONDS))
                    try { racingRead!!.get(250, java.util.concurrent.TimeUnit.MILLISECONDS) }
                    catch (_: java.util.concurrent.TimeoutException) { /* correct reader waits for held publication */ }
                }
            }
        }
        try {
            until("initial held document delivery") { delivered.values == listOf("Plans") }
            val delta = LoroFixture.editPayload(peer, "name", "Pulled")
            w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocDelta("boards", "b1", 2, LoroReplicaCodec.CODEC_NAME, delta)), cursor = "2:", more = false))
            w.engine.pullOnce("user")
            assertTrue(paused.get(), "SQLite commit observer never paused the checkpoint")
            assertEquals("Pulled", assertNotNull(racingRead).get(3, java.util.concurrent.TimeUnit.SECONDS))
            until("the checkpoint must deliver without another write") { delivered.values == listOf("Plans", "Pulled") }
        } finally { observer.cancel(); watch.cancel() }
    }

    private companion object {
        const val LIVE_DOCUMENTS_CAPACITY = 32
    }
}
