package io.replicaman.loro

import io.replicaman.testing.fixtureWrite

import kotlinx.coroutines.runBlocking
import org.junit.Test
import io.replicaman.loro.binding.ExportMode
import io.replicaman.DocumentStream
import io.replicaman.ReplicaEngine
import io.replicaman.ReplicaError
import io.replicaman.ReplicaOp
import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.docRow
import io.replicaman.recoveryRecords
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Recovery through the real SQLite store, held-document door and native Loro codec. */
class DocumentRecoveryTests : LoroTestCase() {
    private val codec = LoroReplicaCodec()
    private val corrupt = "not a loro snapshot".toByteArray()

    private suspend fun broken(engine: ReplicaEngine, id: String = "b1") {
        val original = LoroFixture.doc(7uL)
        LoroFixture.setMeta(original, "old", "durable")
        engine.createDoc("boards", id, original.export(ExportMode.Snapshot), 7uL)
        engine.drain()
        val store = assertNotNull(engine.store)
        store.fixtureWrite { db ->
            db.prepare("UPDATE docs SET fold = ? WHERE stream = 'boards' AND row_id = ?").use {
                it.bindBlob(1, corrupt); it.bindText(2, id); it.step()
            }
        }
    }

    @Test
    fun corruptReadsPreserveEveryByteUntilAnExplicitRebuild(): Unit = runBlocking {
        val engine = LoroFixture.engine(LoroStubTransport()); broken(engine)
        val boards = DocumentStream(engine, Board)
        val before = assertNotNull(engine.docRow("boards", "b1"))
        assertFailsWith<ReplicaError.Codec> { boards.findDoc("b1", BoardState) }
        assertEquals(before, engine.docRow("boards", "b1"))
        assertNull(boards.heldDoc("b1", BoardState))
        val replacement = LoroFixture.doc(100uL)
        LoroFixture.setMeta(replacement, "name", "Recovered")
        engine.rebuildDocument("boards", "b1", replacement.export(ExportMode.Snapshot), 100uL)
        val store = assertNotNull(engine.store)
        assertContentEquals(corrupt, recoveryBytes(store, store.recoveryRecords().last(), "document.fold"))
        assertEquals("Recovered", boards.findDoc("b1", BoardState)?.name)
        engine.close(); engine.open(LoroFixture.OWNER)
        assertEquals("Recovered", boards.findDoc("b1", BoardState)?.name)
    }

    @Test
    fun failedExplicitRebuildRollsBackTheArchiveFoldAndCursor() = runBlocking<Unit> {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)
        val original = LoroFixture.doc(7uL).export(ExportMode.Snapshot)
        transport.queuePull("user", ReplicaPullResponse(
            frames = listOf(io.replicaman.ReplicaFrame.DocSnapshot(
                "boards", "b1", LoroReplicaCodec.CODEC_NAME, original, emptyMap())),
            cursor = "12:", more = false,
        ))
        engine.pullOnce("user")
        val store = assertNotNull(engine.store)
        store.fixtureWrite { db ->
            db.prepare("UPDATE docs SET fold = ? WHERE row_id = 'b1'").use {
                it.bindBlob(1, corrupt)
                it.step()
            }
            db.prepare("CREATE TRIGGER fail_recovery BEFORE INSERT ON docs BEGIN SELECT RAISE(ABORT,'recovery disk failure'); END").use { it.step() }
        }
        val before = engine.docRow("boards", "b1")
        val recovered = LoroFixture.doc(100uL).export(ExportMode.Snapshot)
        try {
            val failure = assertFails { engine.rebuildDocument("boards", "b1", recovered, 200uL) }
            assertContains(assertNotNull(failure.message), "recovery disk failure")
            assertEquals(before, engine.docRow("boards", "b1"))
            assertEquals("12:", engine.currentCursor("user"))
            assertTrue(store.recoveryRecords().isEmpty(), "the recovery record shares the failed transaction")
        } finally {
            store.fixtureWrite { db -> db.prepare("DROP TRIGGER fail_recovery").use { it.step() } }
        }
    }

    @Test
    fun rebuildRequiresFreshPeerAndResyncArchivesLocalFold(): Unit = runBlocking {
        val engine = LoroFixture.engine(LoroStubTransport())
        val seed = LoroFixture.doc(7uL)
        LoroFixture.setMeta(seed, "name", "Saved")
        engine.createDoc("boards", "b1", seed.export(ExportMode.Snapshot), 7uL)
        val before = assertNotNull(engine.docRow("boards", "b1"))
        val pending = engine.pendingOps()
        val store = assertNotNull(engine.store)
        assertFailsWith<ReplicaError.Codec> {
            engine.rebuildDocument("boards", "b1", before.fold, before.peer)
        }
        assertEquals(before, engine.docRow("boards", "b1"))
        assertEquals(pending, engine.pendingOps())
        assertTrue(store.recoveryRecords().isEmpty())

        engine.resyncDocument("boards", "b1")
        assertContentEquals(before.fold, recoveryBytes(store, store.recoveryRecords().last(), "document.fold"))
        assertNull(engine.docRow("boards", "b1"))
        assertEquals(pending, engine.pendingOps())
    }

    @Test
    fun invalidSeedsAndRebuildsCannotBecomeSavedDocuments(): Unit = runBlocking {
        val engine=LoroFixture.engine(LoroStubTransport())
        assertFailsWith<ReplicaError.Codec> { engine.createDoc("boards","bad",corrupt,7uL) }
        assertNull(engine.docRow("boards","bad")); assertTrue(engine.pendingOps().isEmpty())
        broken(engine)
        assertFailsWith<ReplicaError.Codec> { engine.rebuildDocument("boards","b1",corrupt,9uL) }
        assertEquals(7uL,engine.docRow("boards","b1")?.peer)
    }

    /** KILL: detach repair from the captured local session — old A work repairs a same-id document in B. */
    @Test
    fun anOutgoingProducerCannotRepairTheNewOwnersDocument(): Unit = runBlocking {
        val engine = LoroFixture.engine(LoroStubTransport())
        engine.open(4_294_967_201L)
        broken(engine)
        engine.open(4_294_967_202L)
        broken(engine)
        engine.open(4_294_967_201L)
        val admission = engine.captureLocalSession()
        val boards = DocumentStream(engine, Board)
        engine.withLocalSession(admission) {
            engine.open(4_294_967_202L)
            assertFailsWith<ReplicaError.StaleLocalSession> { boards.findDoc("b1", BoardState) }
        }
        assertNull(boards.heldDoc("b1", BoardState))
        assertContentEquals(corrupt, engine.docRow("boards", "b1")?.fold)
        engine.open(4_294_967_201L)
        assertContentEquals(corrupt, engine.docRow("boards", "b1")?.fold)
    }

    /** KILL: compare only owner numbers — A → B → A admits a repair from A's retired binding. */
    @Test
    fun returningToTheSameOwnerDoesNotReviveAnOldRecoveryAdmission(): Unit = runBlocking {
        val engine = LoroFixture.engine(LoroStubTransport())
        engine.open(4_294_967_201L)
        broken(engine)
        val admission = engine.captureLocalSession()
        val boards = DocumentStream(engine, Board)
        engine.withLocalSession(admission) {
            engine.open(4_294_967_202L)
            engine.open(4_294_967_201L)
            assertFailsWith<ReplicaError.StaleLocalSession> { boards.findDoc("b1", BoardState) }
        }
        assertNull(boards.heldDoc("b1", BoardState))
        assertContentEquals(corrupt, engine.docRow("boards", "b1")?.fold)
        assertFailsWith<ReplicaError.Codec> { boards.findDoc("b1", BoardState) }
    }
}
