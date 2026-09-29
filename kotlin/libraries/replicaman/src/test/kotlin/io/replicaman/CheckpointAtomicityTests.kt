package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.assertFails
import kotlin.test.assertTrue
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekSnapshot
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertFailsWith

/**
 * One pull batch applies ATOMICALLY with its cursor advance. A
 * crash mid-batch leaves the previous checkpoint intact: store unchanged,
 * cursor unmoved, and the same batch re-serves cleanly afterwards.
 */
class CheckpointAtomicityTests : ReplicaTestCase() {

    @Test
    fun aCursorReadFailureDoesNotLeakWireAdmission() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        store.write { db -> db.prepare("DROP TABLE checkpoints").use { it.step() } }
        assertFails { engine.pullOnce("user") }
        withTimeout(2_000) { engine.seal() }
        assertTrue(engine.isSealed)
    }

    private class Fault : Exception("injected")

    /**
     * KILL: split `applyCheckpoint` so the frames commit in one transaction
     * and the cursor in a second one after the fault
     * (`store.write { frames }; fault?.invoke(); store.write { setCursor }`) —
     * the faulted batch then leaves `n1 = "poisoned"` behind. Verified: that
     * mutation turns this test red; simply MOVING the `setCursor` call after
     * the fault does not, because the fault throws first.
     */
    @Test
    fun faultBeforeCommitLeavesPreviousCheckpointIntact() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")

        engine.setCheckpointFault { throw Fault() }
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "poisoned"), Fixture.note("n2", "half")),
                cursor = "9:", more = false
            )
        )

        assertFailsWith<Fault>("the injected storage fault must surface") {
            engine.pullOnce("user")
        }

        assertEquals(
            ReplicaValue.Str("one"),
            store.peekSnapshot("notes", "n1")?.data?.get("title"),
            "a faulted batch must not leave partial writes"
        )
        assertNull(store.peekSnapshot("notes", "n2"))
        assertEquals("5:", engine.currentCursor("user"), "the cursor must not advance past an unapplied batch")

        // The next pull re-serves from the intact checkpoint and lands.
        engine.setCheckpointFault(null)
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "poisoned"), Fixture.note("n2", "half")),
                cursor = "9:", more = false
            )
        )
        engine.pullOnce("user")
        assertEquals(
            ReplicaValue.Str("poisoned"),
            store.peekSnapshot("notes", "n1")?.data?.get("title")
        )
        assertEquals(
            ReplicaValue.Str("half"),
            store.peekSnapshot("notes", "n2")?.data?.get("title")
        )
        assertEquals("9:", engine.currentCursor("user"))
    }
}
