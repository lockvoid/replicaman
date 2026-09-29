package io.replicaman

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.realDelay
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/** Joined delivery callers receive the same failure; a later explicit retry owns a new request. */
class DrainJoinAfterReconnectTests : ReplicaTestCase() {

    @Test
    fun joinedCallersSeeTheFailureAndExplicitRetryDeliversTheSavedWork() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport, coldWindow = 60.seconds)

        val onWire = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val pushes = AtomicInteger(0)
        transport.onPush {
            // Dead wire for the first push: it hangs until released, then
            // fails the way a connect timeout does. Healthy for every push after.
            if (pushes.incrementAndGet() == 1) {
                onWire.complete(Unit)
                release.await()
                throw ReplicaError.Transport("connect timed out")
            }
        }

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("written offline")))

        val callers = CoroutineScope(Dispatchers.Default)
        val doomed = callers.async { runCatching { engine.drain() } }
        onWire.await()
        val reconnect = callers.async { runCatching { engine.drain() } }
        realDelay(100)
        release.complete(Unit)

        val doomedResult = doomed.await()
        val reconnectResult = reconnect.await()

        assertTrue(doomedResult.isFailure, "the flight that left on the dead wire reports the dead wire")
        assertTrue(reconnectResult.exceptionOrNull() is ReplicaError.Transport)
        assertEquals(1, store.pendingOps().size)
        engine.drain()
        engine.seal()
        assertEquals(
            listOf("n1"), transport.pushedBatches().flatten().map { it.rowId },
            "the reconnect drain delivered nothing — it joined the flight that left while the wire was dead"
        )
        assertEquals(2, pushes.get(), "the reconnect ran a flight of its own")
    }
}
