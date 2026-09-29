package io.replicaman

import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekPending
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

/**
 * Two drains racing (the scheduled push meeting an explicit drain) must
 * coalesce onto ONE flight. A double-sent `row.create` reaches the server
 * twice: the first is accepted, the second collides → rejected → the
 * rejection revert deletes the freshly-created row out from under the user.
 */
class ConcurrentDrainTests : ReplicaTestCase() {

    /**
     * KILL: drop the `activeDrains[lane]` join in `drain(lane)` and start a
     * second flight — the same entry reaches the wire twice.
     */
    @Test
    fun concurrentDrainsPushEachEntryOnce() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        transport.delayPushes(50.milliseconds)
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))

        val first = async { engine.drain() }
        val second = async { engine.drain() }
        val firstVerdicts = first.await()
        val secondVerdicts = second.await()

        val pushed = transport.pushedBatches().flatten().filter { it.rowId == "n1" }
        assertEquals(
            1, pushed.size,
            "the same journal entry went to the wire ${pushed.size} times — " +
                "concurrent drains must share one flight"
        )
        // The joiner is answered by the flight it joined, not by an empty
        // second selection — otherwise a caller reads "nothing was owed".
        assertEquals(firstVerdicts, secondVerdicts)
        assertEquals(1, firstVerdicts.size)
        assertTrue(store.peekPending().isEmpty())
    }
}
