package io.replicaman

import kotlinx.coroutines.Job
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.eventually
import io.replicaman.support.peekParked
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Generated CRUD verbs stop at the database boundary. Delivery is engine
 * behavior: an app caller must never need to ring a second transport bell
 * after `save`, `create`, `delete`, or a document edit.
 */
class WriteSchedulingTests : ReplicaTestCase() {

    /** KILL: drop `schedulePush()` from the end of `writeRow` — save() never delivers. */
    @Test
    fun saveSchedulesItsOwnPush() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(
            store = store, transport = transport, automaticallyPushWrites = true
        )

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("ordinary CRUD")))

        eventually(5.seconds, "save() returned but the engine never pushed its write") {
            transport.pushCount() == 1 && store.peekPending().isEmpty()
        }
    }

    /** KILL: stamp from a caller-supplied user id — a create can carry an owner nobody holds. */
    @Test
    fun createStampsTheBoundOwnerAndClockInsideTheEngine() = runTest {
        val store = Fixture.store()
        val instant = Instant.ofEpochSecond(1_700_000_000)
        val engine = Fixture.engine(store = store, transport = StubTransport(), clock = { instant }, schema = Fixture.schema(ReplicaStamp.standard))
        val claimed = mapOf(
            "name" to ReplicaValue.Str("Stamped"),
            "userId" to ReplicaValue.Num(999.0),
            "createdAt" to ReplicaValue.Str("1900-01-01T00:00:00Z"),
        )
        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL, claimed)

        val row = assertNotNull(store.peekSnapshot("boards", "b1"))
        assertEquals(ReplicaValue.Str("Stamped"), row.data["name"])
        assertEquals(ReplicaValue.Num(42.0), row.data["userId"], "the bound owner is the only owner a create can carry")
        assertEquals(ReplicaValue.Str("2023-11-14T22:13:20Z"), row.data["createdAt"])
        assertEquals(ReplicaValue.Str("2023-11-14T22:13:20Z"), row.data["updatedAt"])
    }

    /** KILL: drop `schedulePush()` from `recordDocDelta` — a doc edit never leaves the device. */
    @Test
    fun documentCreateEditAndDeleteEachScheduleDelivery() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(
            store = store, transport = transport, automaticallyPushWrites = true
        )

        engine.createDoc("boards", "b1", "S".toByteArray(), 7uL)
        eventually(5.seconds, "the doc create never delivered") { transport.pushCount() == 1 }

        engine.recordDocDelta("boards", "b1", "D".toByteArray())
        eventually(5.seconds, "the doc delta never delivered") { transport.pushCount() == 2 }

        engine.deleteRow("boards", "b1")
        eventually(5.seconds, "the doc delete never delivered") { transport.pushCount() == 3 }

        assertEquals(
            listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.DOC_DELTA, ReplicaOp.Verb.ROW_DELETE),
            transport.pushedBatches().flatten().map { it.verb }
        )
    }

    /** KILL: discard the delete when ANY birth exists — the in-flight create is never undone. */
    @Test
    fun deleteDuringAnInFlightCreateStillSendsTheDelete() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        transport.delayPushes(250.milliseconds)
        val engine = Fixture.engine(
            store = store, transport = transport, automaticallyPushWrites = true
        )

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("short lived")))
        eventually(5.seconds, "the create never reached the wire") { transport.pushCount() == 1 }

        engine.deleteRow("notes", "n1")
        eventually(5.seconds, "the in-flight create reached the server without its delete") {
            transport.pushCount() == 2
        }

        assertEquals(
            listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_DELETE),
            transport.pushedBatches().flatten().map { it.verb }
        )
        assertNull(store.peekSnapshot("notes", "n1"))
    }

    /** KILL: let the second `createDoc` overwrite the fold — the first birth's seed is lost. */
    @Test
    fun repeatedDocumentCreatePreservesTheFirstBirthAtomically() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        val first = engine.createDoc(
            "boards", "b1", "FIRST".toByteArray(), 7uL, mapOf("name" to ReplicaValue.Str("First"))
        )
        val replay = engine.createDoc(
            "boards", "b1", "SECOND".toByteArray(), 8uL, mapOf("name" to ReplicaValue.Str("Second"))
        )

        assertTrue(first)
        assertFalse(replay)
        assertTrue(engine.docFold("boards", "b1")!!.contentEquals("FIRST".toByteArray()))
        assertEquals(ReplicaValue.Str("First"), store.peekSnapshot("boards", "b1")?.data?.get("name"))
        assertEquals(1, store.peekPending().size)
    }

    /** KILL: skip the `isCold` check in `pushScheduledWrites` — an offline device re-attempts per write. */
    @Test
    fun automaticDeliveryHonorsTheColdWindow() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        transport.failPushes(true)
        val engine = Fixture.engine(
            store = store, transport = transport, coldWindow = 60.seconds,
            automaticallyPushWrites = true
        )

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("first")))
        engine.awaitScheduledPush(ReplicaLane.BULK)
        assertEquals(1, transport.pushCount(), "the first write never reached the wire")
        assertTrue(engine.isColdForTesting(ReplicaLane.BULK), "the failed push must cool the lane it failed on")

        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("second")))
        engine.awaitScheduledPush(ReplicaLane.BULK)
        assertEquals(1, transport.pushCount(), "a write onto a known-cold lane must not re-attempt the wire")
        assertEquals(2, store.peekPending().size)
    }

    /** The engine keeps its scheduled deliveries private; the map is read on the engine's own dispatcher. */
    private suspend fun ReplicaEngine.awaitScheduledPush(lane: ReplicaLane) {
        val field = ReplicaEngine::class.java.getDeclaredField("scheduledPushes").apply { isAccessible = true }
        @Suppress("UNCHECKED_CAST")
        val job = withContext(engineContext) { (field.get(this@awaitScheduledPush) as Map<ReplicaLane, Job>)[lane] }
        job?.join()
    }

    /** KILL: keep the queued delete after the birth is refused — the wire hears a delete for nothing. */
    @Test
    fun rejectedInFlightRowBirthDropsItsQueuedDelete() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        transport.delayPushes(250.milliseconds)
        transport.scriptPush { ops ->
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "refused") }
        }
        val engine = Fixture.engine(
            store = store, transport = transport, automaticallyPushWrites = true
        )

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("refused birth")))
        eventually(5.seconds, "the birth never reached the wire") { transport.pushCount() == 1 }
        engine.deleteRow("notes", "n1")

        // The park IS the settle point: the verdict transaction that parks the
        // birth is the same one that discards its dependent delete.
        eventually(5.seconds, "the rejected birth never settled") { store.peekParked().size == 1 }

        assertEquals(1, transport.pushCount(), "the dependent delete must not reach the wire")
        assertTrue(store.peekPending().isEmpty())
        assertEquals(ReplicaOp.Verb.ROW_CREATE, store.peekParked().first().op().verb)
        assertNull(store.peekSnapshot("notes", "n1"))
    }
}
