package io.replicaman.loro

import kotlinx.coroutines.runBlocking
import org.junit.Test
import io.replicaman.loro.binding.ExportMode
import io.replicaman.loro.binding.LoroDoc
import io.replicaman.loro.binding.LoroException
import io.replicaman.loro.binding.Frontiers
import io.replicaman.*
import kotlin.test.*

/** Read actual Loro history through the SQLite-backed engine's synchronous door. */
class DocumentReadTests : LoroTestCase() {
    private val codec = LoroReplicaCodec()

    private suspend fun world(): Pair<ReplicaEngine, DocumentStream<Board, ReplicaNoField>> {
        val engine = LoroFixture.engine(LoroStubTransport())
        val boards = DocumentStream(engine, Board)
        val seed = LoroFixture.doc(7uL)
        try {
            LoroFixture.setMeta(seed, "name", "Saved")
            boards.createDoc("b1", seed.export(ExportMode.Snapshot), 100uL)
        } finally {
            seed.close()
        }
        return engine to boards
    }

    @Test fun escapedEditHandleCannotEnterALaterSavedEdit(): Unit = runBlocking {
        val (engine, boards) = world()
        var escaped: LoroDocument? = null
        boards.updateDoc("b1", codec) {
            escaped = it
            LoroFixture.setMeta(it.doc, "color", "Legitimate")
        }
        val saved = assertNotNull(engine.docRow("boards", "b1"))
        LoroFixture.setMeta(assertNotNull(escaped).doc, "name", "Unjournaled")
        assertFailsWith<ReplicaError.Codec> {
            boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "color", "Next") }
        }
        assertEquals(saved, engine.docRow("boards", "b1"))
        assertEquals("Saved", boards.findDoc("b1", BoardState)?.name)
    }

    @Test fun anEscapedReadHandleCannotAuthorIntoTheHeldDocument(): Unit = runBlocking {
        val (engine, boards) = world()
        val state = assertNotNull(boards.findDoc("b1", BoardState))
        val escaped = assertNotNull(boards.readDoc("b1", codec) { it })
        try {
            LoroFixture.setMeta(escaped.doc, "name", "Unjournaled")
            assertSame(state, boards.findDoc("b1", BoardState))
            boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "color", "blue") }
            val stored = assertNotNull(engine.docRow("boards", "b1"))
            assertEquals("Saved", LoroFixture.meta(stored.fold, "name"))
            assertEquals("blue", LoroFixture.meta(stored.fold, "color"))
        } finally {
            escaped.undo.close()
            escaped.doc.close()
        }
    }

    @Test fun malformedHistoryAddressIsAnErrorRatherThanAnAbsentVersion(): Unit = runBlocking {
        val (_, boards) = world()
        assertFailsWith<LoroException> {
            boards.readDoc("b1", codec) { it.fork(byteArrayOf(0xff.toByte())) }
        }
    }

    @Test fun unexpectedNativeHistoryFailurePropagates() {
        val native = object : LoroDoc() {
            override fun forkAt(frontiers: Frontiers): LoroDoc =
                throw LoroException.LockException("injected native lock failure")
        }
        native.use {
            val document = LoroDocument(native)
            try {
                LoroFixture.setMeta(native, "name", "saved")
                val empty = Frontiers().use { it.encode() }
                for (read in listOf<() -> Any?>(
                    { document.fork(empty) },
                    { document.firstMatch(listOf(empty)) },
                    { document.differingRoots(empty) }
                )) {
                    val failure = assertFailsWith<LoroException.LockException> { read() }
                    assertEquals("injected native lock failure", failure.message)
                }
            } finally {
                document.undo.close()
            }
        }
    }

    @Test fun historyReadsWorkWhileWritesAreSealedWithoutMovingStateOrJournal(): Unit = runBlocking {
        val (engine, boards) = world()
        val address = assertNotNull(boards.readDoc("b1", codec) { it.frontiers })
        boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Current") }
        val before = assertNotNull(engine.docRow("boards", "b1"))
        val state = assertNotNull(boards.findDoc("b1", BoardState))
        val pending = engine.pendingOps().map { it.id }
        engine.seal()
        try {
            assertEquals("Saved", boards.readDoc("b1", codec) { document ->
                document.fork(address)?.use { LoroFixture.meta(it, "name") }
            })
            assertEquals("Current", boards.readDoc("b1", codec) { LoroFixture.meta(it.doc, "name") })
            assertSame(state, boards.findDoc("b1", BoardState), "a read keeps the warm state")
            assertEquals(before, engine.docRow("boards", "b1"), "history browsing cannot change durable bytes")
            assertEquals(pending, engine.pendingOps().map { it.id })
            assertFailsWith<ReplicaError.IdentityTransitionInProgress> {
                boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Refused") }
            }
        } finally {
            engine.unseal()
        }
    }

    @Test fun anAccidentalReadMutationIsRefusedAndCannotLeakIntoLaterEdits(): Unit = runBlocking {
        val (engine, boards) = world()
        val before = assertNotNull(engine.docRow("boards", "b1"))
        val pending = engine.pendingOps().map { it.id }
        assertFailsWith<ReplicaError.Codec> {
            boards.readDoc("b1", codec) { LoroFixture.setMeta(it.doc, "name", "Unjournaled") }
        }
        assertNull(boards.heldDoc("b1", BoardState), "the mutated held copy must be evicted")
        assertEquals(before, engine.docRow("boards", "b1"))
        assertEquals(pending, engine.pendingOps().map { it.id })
        assertEquals("Saved", boards.findDoc("b1", BoardState)?.name)
        boards.updateDoc("b1", codec) { LoroFixture.setMeta(it.doc, "color", "blue") }
        val fold = assertNotNull(engine.docRow("boards", "b1")).fold
        assertEquals("Saved", LoroFixture.meta(fold, "name"))
        assertEquals("blue", LoroFixture.meta(fold, "color"))
    }

    @Test fun aReadThatMutatesThenThrowsAlsoDiscardsItsUncommittedCopy(): Unit = runBlocking {
        val (engine, boards) = world()
        val before = assertNotNull(engine.docRow("boards", "b1"))
        val failure = IllegalStateException("reader failed after editing")
        val thrown = assertFailsWith<IllegalStateException> {
            boards.readDoc("b1", codec) {
                LoroFixture.setMeta(it.doc, "name", "Unjournaled")
                throw failure
            }
        }
        assertSame(failure, thrown)
        assertNull(boards.heldDoc("b1", BoardState))
        assertEquals(before, engine.docRow("boards", "b1"))
        assertEquals("Saved", boards.readDoc("b1", codec) { LoroFixture.meta(it.doc, "name") })
    }

    @Test fun missingOrClosedDocumentsReturnNullWithoutInvokingTheReader(): Unit = runBlocking {
        val (engine, boards) = world()
        var calls = 0
        assertNull(boards.readDoc("missing", codec) { calls++; "read" })
        engine.close()
        assertNull(boards.readDoc("b1", codec) { calls++; "read" })
        assertEquals(0, calls)
    }

    @Test fun anOldReadersAdmissionCannotReadAnotherOwnerOrAReopenedWorld(): Unit = runBlocking {
        val (engine, boards) = world()
        val admitted = engine.captureLocalSession()
        var calls = 0
        engine.withLocalSession(admitted) {
            engine.open(LoroFixture.OWNER + 1)
            assertFailsWith<ReplicaError.StaleLocalSession> {
                boards.readDoc("b1", codec) { calls++; "read" }
            }
            engine.open(LoroFixture.OWNER)
            assertFailsWith<ReplicaError.StaleLocalSession> {
                boards.readDoc("b1", codec) { calls++; "read" }
            }
        }
        assertEquals(0, calls)
        assertEquals("Saved", boards.readDoc("b1", codec) { LoroFixture.meta(it.doc, "name") })
    }
}
