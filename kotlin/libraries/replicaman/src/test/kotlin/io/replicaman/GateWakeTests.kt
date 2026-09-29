package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Delivery through the production scheduler: a held row holes the batch
 * without blocking it, costs one judge while it waits, and leaves on its
 * gate's own signal — no write, no knock.
 *
 * Every test here writes and then WAITS — it never calls `drain()` to make
 * its own assertion true.
 */
class GateWakeTests : ReplicaTestCase() {

    /**
     * A held row must hole the batch, not block it — on the automatic path
     * too. The live symptom of getting this wrong is every unrelated row in
     * the app freezing behind one cook whose bytes are still uploading.
     *
     * KILL: `admit` — return true for a hold; the held row then rides the wire.
     */
    @Test fun aHeldRowHolesTheAutomaticBatchWithoutBlockingIt(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val released = ReleaseLedger(listOf("free"))
        val engine = Fixture.engine(store = store, transport = transport, automaticallyPushWrites = true,
            syncGates = listOf(gateWholeRowGate(released)))

        engine.saveRow("notes", "held", null, mapOf("title" to ReplicaValue.Str("waiting")))
        engine.saveRow("notes", "free", null, mapOf("title" to ReplicaValue.Str("ready")))

        until("the free row never reached the wire on the engine's own schedule") {
            transport.pushedBatches().flatten().any { it.rowId == "free" }
        }
        engine.seal()

        assertTrue(transport.pushedBatches().flatten().all { it.rowId == "free" }, "the held row escaped onto the wire")
        assertEquals(listOf("held"), engine.heldRows().map { it.rowId })
        assertTrue(store.peekPending().isEmpty())
    }

    /**
     * The landing is the event: a release never waits for an unrelated
     * write. The gate's signal alone takes the row to the wire.
     *
     * KILL: `ReplicaEngine` init — drop the coroutine that collects each
     * gate's `changes`.
     */
    @Test fun aLandingSendsTheRowOnTheEnginesOwnSchedule(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.engine(store = store, transport = transport, automaticallyPushWrites = true,
            syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("a"), "blob" to ReplicaValue.Str("k1")))
        realDelay(50)
        assertEquals(0, transport.pushCount())

        released.land("k1")

        until("the landing never took the row to the wire") {
            transport.pushedBatches().flatten().any { it.rowId == "n1" }
        }
        engine.seal()
        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), sent.map { it.verb })
        assertEquals(mapOf("title" to ReplicaValue.Str("a"), "blob" to ReplicaValue.Str("k1")), sent.first().data)
        assertTrue(store.peekPending().isEmpty())
    }

    /** A row awaiting its bytes is judged once, when written, however long it waits. */
    @Test fun aHeldRowIsJudgedOnceWhileItWaits(): Unit = runTest {
        val transport = StubTransport()
        val judges = JudgeCount()
        val engine = Fixture.engine(store = Fixture.store(), transport = transport, coldWindow = 60.seconds,
            automaticallyPushWrites = true,
            syncGates = listOf(TestGate("notes") {
                judges.tick()
                SyncVerdict.Gate("bytes still uploading")
            }))

        engine.saveRow("notes", "cook-1", null, mapOf("title" to ReplicaValue.Str("x")))
        engine.saveRow("notes", "cook-2", null, emptyMap())
        engine.drain()
        realDelay(200)
        engine.seal()

        assertEquals(2, judges.value, "a waiting row was judged again (judges=${judges.value})")
    }

    /** KILL: clear the automatic push wake that arrives while a push is in flight; second never leaves its journal. */
    @Test fun aWriteDuringAnAutomaticPushIsDeliveredWithoutAnotherWake(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport, automaticallyPushWrites = true)
        val failures = Recorder<Throwable>()
        transport.onPush { ops ->
            if (ops.any { it.rowId == "first" }) {
                try { engine.saveRow("notes", "second", null, emptyMap()) }
                catch (error: Throwable) { failures.record(error) }
            }
        }
        engine.saveRow("notes", "first", null, emptyMap())
        until("the write committed during an active push was stranded") { transport.pushedBatches().flatten().any { it.rowId == "second" } }
        engine.seal()
        assertTrue(failures.values.isEmpty(), "mid-flight write failed: ${failures.values}")
        assertEquals(listOf("first", "second"), transport.pushedBatches().flatten().map { it.rowId })
        assertTrue(store.peekPending().isEmpty())
    }
}
