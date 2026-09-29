package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekPending
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * The cold window: a transport failure marks the wire cold;
 * `drainIfWarm` skips inside the window (offline must not stack timeouts)
 * and retries after it. Explicit `drain()` never skips — reconnect's
 * deliberate drains always attempt.
 */
class ColdWindowTests : ReplicaTestCase() {

    /**
     * Both halves are stated as VALUES rather than waited out: a 60s window is
     * "inside", a 0s window is "past". Nothing here sleeps.
     *
     * KILL: delete the `coldUntil[lane] = markNow() + coldWindow` stamp in the
     * drain's failure path — an offline device re-attempts on every barrier.
     */
    @Test
    fun drainIfWarmSkipsInsideTheColdWindow() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport, coldWindow = 60.seconds)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("queued")))
        transport.failPushes(true)

        engine.drainIfWarm()
        assertEquals(1, transport.pushCount(), "the first attempt hits the wire and proves it dead")

        engine.drainIfWarm()
        engine.drainIfWarm()
        assertEquals(1, transport.pushCount(), "inside the window a known-cold wire is not re-attempted")

        assertTrue(engine.isColdForTesting(ReplicaLane.BULK), "the skip must come from the stamp, not from an empty journal")
        assertEquals(1, store.peekPending().size, "transport failure leaves the entry pending — retryable, never parked")
    }

    /** KILL: keep the stamp forever (never compare against now) — the lane never warms. */
    @Test
    fun drainIfWarmRetriesOnceTheWindowHasElapsed() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        // Zero window: the stamp is set to "now", so the next check is already
        // past it. No sleep can be needed to observe an elapsed zero.
        val engine = Fixture.engine(store = store, transport = transport, coldWindow = Duration.ZERO)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("queued")))
        transport.failPushes(true)

        engine.drainIfWarm()
        assertEquals(1, transport.pushCount())
        assertFalse(engine.isColdForTesting(ReplicaLane.BULK), "a zero window is elapsed the instant it is stamped")

        engine.drainIfWarm()
        assertEquals(2, transport.pushCount(), "past the window the drain retries")
        assertEquals(1, store.peekPending().size)
    }

    /**
     * KILL: make explicit `drain()` consult `isCold` — reconnect can never recover.
     * KILL: delete the success path's `coldUntil.clear()` — a wire that just answered stays cold.
     */
    @Test
    fun explicitDrainAlwaysAttempts() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport, coldWindow = 60.seconds)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("queued")))
        transport.failPushes(true)
        engine.drainIfWarm()
        assertEquals(1, transport.pushCount())
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        assertEquals(2, transport.pushCount(), "explicit drains never skip, cold or not")

        transport.failPushes(false)
        engine.drain()
        assertEquals(0, store.peekPending().size)
        assertFalse(engine.isColdForTesting(ReplicaLane.BULK), "a successful drain leaves no stamp behind")
    }
}
