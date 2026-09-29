package io.replicaman.loro

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.*
import io.replicaman.testing.fixtureWrite
import io.replicaman.loro.binding.ExportMode
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.util.Base64
import kotlin.test.*

/** Command hints publish through the same pull rounds as ordinary synchronization. */
class CommitDocumentTests : LoroTestCase() {
    private val hint = Base64.getEncoder().encodeToString(
        """{"protocol":2,"namespace":"replicaman","schema":1,"dataset":"fixture-dataset","shards":["user"]}""".toByteArray()
    )

    private suspend fun publish(engine: ReplicaEngine, transport: LoroStubTransport, fold: ByteArray, cursor: String) {
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", LoroReplicaCodec.CODEC_NAME, fold, emptyMap())
        ), cursor = cursor, more = false))
        engine.apply(hint, engine.commitSession())
    }

    @Test fun projectionMovementDoesNotSuppressUnseenDocumentHistory() = runBlocking<Unit> {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)
        val server = LoroFixture.doc(7uL)
        LoroFixture.setMeta(server, "name", "first")
        publish(engine, transport, server.export(ExportMode.Snapshot), "1:")
        val boards = DocumentStream(engine, Board)
        assertEquals("first", boards.findDoc("b1", BoardState)?.name)
        val delta = LoroFixture.editPayload(server, "name", "second")
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocDelta("boards", "b1", 1, LoroReplicaCodec.CODEC_NAME, delta),
            ReplicaFrame.RowSet("boards", "b1", null, mapOf("status" to ReplicaValue.Str("ready")), 30),
        ), cursor = "2:", more = false))
        engine.pullOnce()
        assertEquals("second", boards.findDoc("b1", BoardState)?.name)
        LoroFixture.setMeta(server, "name", "third")
        publish(engine, transport, server.export(ExportMode.Snapshot), "3:")
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocDelta("boards", "b1", 2, LoroReplicaCodec.CODEC_NAME, delta),
            ReplicaFrame.RowSet("boards", "b1", null, mapOf("status" to ReplicaValue.Str("ready")), 40),
        ), cursor = "4:", more = false))
        engine.pullOnce()
        assertEquals("third", boards.findDoc("b1", BoardState)?.name)
    }

    @Test fun commandRefreshRemovesBothTheRowAndHeldDocument() = runBlocking<Unit> {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)
        val server = LoroFixture.doc(7uL)
        LoroFixture.setMeta(server, "name", "old")
        publish(engine, transport, server.export(ExportMode.Snapshot), "1:")
        val boards = DocumentStream(engine, Board)
        assertNotNull(boards.findDoc("b1", BoardState))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.RowDelete("boards", "b1", 30)
        ), cursor = "2:", more = false))
        engine.apply(hint, engine.commitSession())
        assertNull(engine.docRow("boards", "b1"))
        assertNull(boards.findDoc("b1", BoardState))
        assertEquals("2:", engine.currentCursor())
    }

    @Test fun failedPublicationKeepsHeldStateAndCursorAndCanRetry() = runBlocking<Unit> {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)
        val server = LoroFixture.doc(7uL)
        LoroFixture.setMeta(server, "name", "before")
        publish(engine, transport, server.export(ExportMode.Snapshot), "1:")
        val boards = DocumentStream(engine, Board)
        assertEquals("before", boards.findDoc("b1", BoardState)?.name)
        val store = assertNotNull(engine.store)
        store.fixtureWrite { db ->
            db.prepare("CREATE TRIGGER reject_fold BEFORE UPDATE ON docs BEGIN SELECT RAISE(ABORT,'injected disk failure'); END").use { it.step() }
        }
        LoroFixture.setMeta(server, "name", "after")
        val next = server.export(ExportMode.Snapshot)
        val failure = assertFails { publish(engine, transport, next, "2:") }
        assertTrue(failure.message.orEmpty().contains("injected disk failure"))
        assertEquals("before", boards.findDoc("b1", BoardState)?.name)
        assertEquals("before", LoroFixture.meta(assertNotNull(engine.docRow("boards", "b1")).fold, "name"))
        assertEquals("1:", engine.currentCursor())
        store.fixtureWrite { db -> db.prepare("DROP TRIGGER reject_fold").use { it.step() } }
        publish(engine, transport, next, "2:")
        assertEquals("after", boards.findDoc("b1", BoardState)?.name)
        assertEquals("2:", engine.currentCursor())
    }

    @Test fun refreshMergesUnsentHistoryAndUndoKeepsTheOtherPeer() = runBlocking<Unit> {
        val transport = LoroStubTransport()
        val engine = LoroFixture.engine(transport)
        val codec = LoroReplicaCodec()
        val server = LoroFixture.doc(7uL)
        LoroFixture.setMeta(server, "name", "before")
        publish(engine, transport, server.export(ExportMode.Snapshot), "1:")
        transport.failPushes(true)
        val boards = DocumentStream(engine, Board)
        boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "local", "unsent") }
        LoroFixture.setMeta(server, "name", "remote")
        publish(engine, transport, server.export(ExportMode.Snapshot), "2:")
        val fold = assertNotNull(engine.docRow("boards", "b1")).fold
        assertEquals("unsent", LoroFixture.meta(fold, "local"))
        assertEquals("remote", LoroFixture.meta(fold, "name"))
        assertTrue(boards.undoDoc("b1", codec))
        assertEquals("remote", boards.findDoc("b1", BoardState)?.name)
        assertNull(LoroFixture.meta(assertNotNull(engine.docRow("boards", "b1")).fold, "local"))
        for (entry in engine.pendingOps()) {
            server.import(assertNotNull(entry.op().payload))
        }
        assertEquals("remote", LoroFixture.meta(server, "name"))
        assertNull(LoroFixture.meta(server, "local"))
    }
}
