package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.Recorder
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.TestNote
import io.replicaman.support.eventually
import io.replicaman.support.peekSnapshot
import io.replicaman.support.recordInto
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * Identity is first-class: one store FILE per owner, and no owner means no
 * store at all. A write with nobody to own it has nowhere to land — that is
 * what kills the "a mint wipes the writes made before it" class structurally
 * instead of by gate.
 */
class OwnerBindingTests : ReplicaTestCase() {

    private fun engine(directory: File, transport: StubTransport = StubTransport()): ReplicaEngine =
        Fixture.unopenedEngine(directory, transport)

    // MARK: - Closed semantics

    /** KILL: fall back to a temp store when the binding is empty — a write lands nowhere real. */
    @Test
    fun closedEngineRefusesEveryWriteWithNoOwner() = runTest {
        val engine = engine(Fixture.directory())

        assertNoOwner { engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("x"))) }
        assertNoOwner { engine.deleteRow("notes", "n1") }
        assertNoOwner { engine.createDoc("boards", "b1", "seed".toByteArray(), 1uL) }
        assertNoOwner { engine.recordDocDelta("boards", "b1", "d".toByteArray()) }
        assertNoOwner { engine.resetCursors() }
        assertNoOwner { engine.discardOps(listOf("x")) }
    }

    /** KILL: throw instead of answering empty on a closed read — every screen crashes at launch. */
    @Test
    fun closedEngineAnswersEveryReadEmpty() = runTest {
        val engine = engine(Fixture.directory())

        assertNull(engine.owner)
        assertNull(engine.store)
        assertNull(engine.database)
        assertNull(engine.docFold("boards", "b1"))
        assertNull(engine.docPeer("boards", "b1"))
        assertEquals(0, engine.pendingOps().size)
        assertEquals(0, engine.parkedOps().size)
        assertNull(engine.currentCursor())

        val notes = RowStream(engine, TestNote)
        assertEquals(emptyList(), notes.where())
        assertEquals(emptyList(), notes.list())
        assertNull(notes.find("n1"))
    }

    /** KILL: let a closed engine pull — the request has no owner to authorize it. */
    @Test
    fun closedEngineNeverTouchesTheWire() = runTest {
        val transport = StubTransport()
        val engine = engine(Fixture.directory(), transport)

        assertEquals(0, engine.drain().size)
        assertEquals(0, engine.pullUntilCaughtUp())
        engine.drainIfWarm()

        assertEquals(0, transport.pullCount(), "a closed engine has no owner to authorize a pull")
        assertEquals(0, transport.pushCount(), "a closed engine holds no journal to push")
    }

    /**
     * The stale-handle hazard: a generated verb surface captured while an
     * owner was open must refuse once that owner is gone.
     *
     * KILL: cache the store inside `RowStream` at construction.
     */
    @Test
    fun aHandleCapturedWhileOpenRefusesAfterClose() = runTest {
        val engine = engine(Fixture.directory())
        engine.open(1)
        val notes = RowStream(engine, TestNote)
        engine.write { it.rows(TestNote).create(TestNote("n1", "mine")) }

        engine.close()

        assertNoOwner { engine.write { it.rows(TestNote).create(TestNote("n2", "orphan")) } }
        assertEquals(emptyList(), notes.where())
        assertEquals(emptyList(), notes.list())
    }

    // MARK: - One file per owner

    /** KILL: name the file from a constant — two owners share one world. */
    @Test
    fun openCreatesTheOwnersFileAndReopenServesTheSameWorld() = runTest {
        val directory = Fixture.directory()
        val engine = Fixture.unopenedEngine(directory, StubTransport(), schema = Fixture.schema(ReplicaStamp.standard))

        engine.open(1)
        assertEquals(1L, engine.owner)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("kept")))
        assertTrue(File(directory, "replica-1.sqlite").exists())

        engine.close()
        assertNull(engine.owner)

        engine.open(1)
        val notes = RowStream(engine, TestNote)
        assertEquals("kept", notes.find("n1")?.title, "reopening the same owner reopens the same file")

        // The OPENED owner is the only owner a create can carry — the stamp
        // follows the binding, not a value the caller passed in.
        engine.createDoc(
            "boards", "b1", "seed".toByteArray(), 1uL,
            mapOf("title" to ReplicaValue.Str("board"))
        )
        assertEquals(
            ReplicaValue.Num(1.0),
            engine.store!!.peekSnapshot("boards", "b1")?.data?.get("userId")
        )
    }

    /** KILL: leave the `-wal`/`-shm` sidecars behind on retire — the next owner inherits pages. */
    @Test
    fun retireDeletesTheOwnersThreeFilesAndTheNextOwnerStartsEmpty() = runTest {
        val directory = Fixture.directory()
        val transport = StubTransport()
        val engine = engine(directory, transport)

        engine.open(1)
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "one")), cursor = "10:", more = false
            )
        )
        engine.pullOnce("user")
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("unpushed")))
        assertEquals(1, engine.pendingOps().size)

        engine.retire()

        for (suffix in listOf("", "-wal", "-shm")) {
            assertFalse(
                File(directory, "replica-1.sqlite$suffix").exists(),
                "retire leaves no trace of the outgoing owner (${if (suffix.isEmpty()) "db" else suffix})"
            )
        }

        engine.open(2)
        val notes = RowStream(engine, TestNote)
        assertEquals(emptyList(), notes.where(), "the next owner never sees the retired owner's rows")
        assertEquals(emptyList(), notes.list(), "the current read door cannot serve the retired world")
        assertNull(engine.currentCursor(), "a blank cursor makes the next pull re-snapshot")
        assertEquals(0, engine.pendingOps().size, "an unpushed op must never ride the next identity's bearer")
    }

    /**
     * The coherence check: an EMPTY store holding a warm cursor can never
     * heal — every tail pull serves nothing.
     *
     * KILL: delete `healCursors(store)` from `bindStore`.
     */
    @Test
    fun reopenKeepsTheCursorOfAnEmptyCheckpoint() = runTest {
        val directory = Fixture.directory()
        val transport = StubTransport()
        val engine = engine(directory, transport)

        engine.open(1)
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = emptyList(), cursor = "10:", more = false
            )
        )
        engine.pullOnce("user")
        assertEquals("10:", engine.currentCursor())

        engine.close()
        engine.open(1)

        assertEquals("10:", engine.currentCursor())
    }

    /** KILL: blank the cursor at every open — a coherent store re-downloads the world. */
    @Test
    fun openKeepsAWarmCursorWhenTheStoreStillHoldsItsWorld() = runTest {
        val directory = Fixture.directory()
        val transport = StubTransport()
        val engine = engine(directory, transport)

        engine.open(1)
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "one")), cursor = "10:", more = false
            )
        )
        engine.pullOnce("user")

        engine.close()
        engine.open(1)

        assertEquals("10:", engine.currentCursor(), "a coherent store keeps its read position")
    }

    // MARK: - watchers ride the owner, never a dead store

    /** KILL: end the watch when there is no owner — an app that arms before sign-in never updates. */
    @Test
    fun aWatcherArmedWithoutAnOwnerServesTheOwnerThatArrives() = runTest {
        val engine = engine(Fixture.directory())
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val pictures = Recorder<List<TestNote>>()
        RowStream(engine, TestNote).watch().recordInto(scope, pictures)
        val callbacks = Recorder<List<TestNote>>()
        val watch = RowStream(engine, TestNote).watch(includeInitial = true) { callbacks.record(it) }
        try {
            eventually(5.seconds, "a closed engine must still deliver its empty picture through both doors") {
                pictures.values.any { it.isEmpty() } && callbacks.values == listOf(emptyList<TestNote>())
            }

            engine.open(1)
            engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("first")))

            eventually(5.seconds, "the watcher never re-armed on the owner that arrived") {
                pictures.last?.map { it.id } == listOf("n1") && callbacks.last?.map { it.id } == listOf("n1")
            }
        } finally { watch.cancel(); scope.cancel() }
    }

    /** KILL: keep the observation on the retired store — the outgoing owner's rows keep serving. */
    @Test
    fun aWatcherStopsServingARetiredOwnerAndPicksUpTheNextOne() = runTest {
        val engine = engine(Fixture.directory())
        engine.open(1)
        engine.saveRow("notes", "outgoing", null, mapOf("title" to ReplicaValue.Str("theirs")))

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val pictures = Recorder<List<TestNote>>()
        RowStream(engine, TestNote).watch().recordInto(scope, pictures)
        val callbacks = Recorder<List<TestNote>>()
        val watch = RowStream(engine, TestNote).watch(includeInitial = true) { callbacks.record(it) }
        try {
            eventually(5.seconds, "the first owner's picture never arrived") {
                pictures.last?.map { it.id } == listOf("outgoing") && callbacks.last?.map { it.id } == listOf("outgoing")
            }
            val afterFirst = pictures.count
            val afterFirstCallback = callbacks.count

            engine.retire()
            engine.open(2)
            engine.saveRow("notes", "incoming", null, mapOf("title" to ReplicaValue.Str("mine")))

            eventually(5.seconds, "the watcher kept serving the retired owner's rows") {
                pictures.last?.map { it.id } == listOf("incoming") && callbacks.last?.map { it.id } == listOf("incoming")
            }
            for (afterRetirement in listOf(pictures.values.drop(afterFirst), callbacks.values.drop(afterFirstCallback))) {
                assertTrue(
                    afterRetirement.none { picture -> picture.any { it.id == "outgoing" } },
                    "no picture after the retirement may carry the outgoing owner's row"
                )
            }
            assertNotNull(engine.store)
        } finally { watch.cancel(); scope.cancel() }
    }

    /** KILL: drop the generation check between decoded picture and callback — the old owner is delivered after reopen. */
    @Test fun aQueuedCallbackCannotDeliverThePreviousOwnersRows() = runTest {
        val engine = engine(Fixture.directory())
        engine.open(1)
        val notes = RowStream(engine, TestNote)
        engine.write { it.rows(TestNote).create(TestNote("outgoing", "private")) }
        val read = java.util.concurrent.CountDownLatch(1)
        val rebound = java.util.concurrent.CountDownLatch(1)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val seen = Recorder<List<TestNote>>()
        val watch = ReplicaReads.watch<TestNote, ReplicaNoField>(scope, engine.binding, "notes", TestNote::class, null, emptyList(), null, true,
            decode = { id, _, data ->
                if (id == "outgoing") { read.countDown(); check(rebound.await(3, java.util.concurrent.TimeUnit.SECONDS)) }
                TestNote.from(id, null, data)
            }, deliver = { seen.record(it) })
        try {
            assertTrue(read.await(3, java.util.concurrent.TimeUnit.SECONDS), "old picture never reached delivery boundary")
            engine.close(); engine.open(2); rebound.countDown()
            engine.write { it.rows(TestNote).create(TestNote("incoming", "mine")) }
            eventually(5.seconds, "new owner's callback never arrived") { seen.last?.map { it.id } == listOf("incoming") }
            assertTrue(seen.values.none { rows -> rows.any { it.id == "outgoing" } })
        } finally { rebound.countDown(); watch.cancel(); scope.cancel() }
    }

    /** KILL: wait indefinitely on an unbound store without publishing empty — a closed owner's rows remain visible. */
    @Test fun theCallbackWatchClearsItsRowsWhenTheOwnerCloses() = runTest {
        val engine = engine(Fixture.directory()); engine.open(1)
        val notes = RowStream(engine, TestNote); engine.write { it.rows(TestNote).create(TestNote("outgoing", "private")) }
        val seen = Recorder<List<TestNote>>()
        val watch = notes.watch(includeInitial = true) { seen.record(it) }
        try {
            eventually(5.seconds, "initial callback missing") { seen.last?.map { it.id } == listOf("outgoing") }
            engine.close()
            eventually(5.seconds, "close must clear callback rows") { seen.last == emptyList<TestNote>() }
        } finally { watch.cancel() }
    }

    // MARK: - Helpers

    private suspend fun assertNoOwner(operation: suspend () -> Unit) {
        try {
            operation()
            fail("an ownerless engine admitted a write")
        } catch (_: ReplicaError.NoOwner) {
            // Expected.
        } catch (error: Throwable) {
            fail("unexpected closed-engine error: $error")
        }
    }
}
