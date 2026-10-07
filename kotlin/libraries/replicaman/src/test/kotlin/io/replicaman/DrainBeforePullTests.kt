package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Drain-before-pull: a pending create followed by an immediate
 * pull reaches the wire as push FIRST, pull second — so the echo is in the
 * answer and a response can never clobber writes it never saw.
 */
class DrainBeforePullTests : ReplicaTestCase() {

    /** KILL: delete the `drainIfWarm()` call at the head of `pullOnce`. */
    @Test
    fun pushIsObservedBeforePull() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        engine.pullOnce("user")

        val events = transport.events()
        assertTrue(events.size >= 2, "expected a push and a pull on the wire, saw $events")
        val push = events[0] as? StubTransport.Event.Push
            ?: fail("the FIRST wire event must be the drain, saw $events")
        assertEquals(1, push.ids.size)
        val pull = events[1] as? StubTransport.Event.Pull
            ?: fail("the pull follows the drain, saw $events")
        assertEquals("user", pull.shard)
    }

    /**
     * Push and pull are independent: a push the server refuses — one bad operation fails the
     * whole request, on every retry — reaches health and never keeps the shard from receiving.
     */
    @Test
    fun aPushTheServerRefusesDoesNotStopThePull() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        engine.pullOnce("user")
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        transport.refusePushes(error = ReplicaError.Protocol("operation data must be an object", "HTTP 400"))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n2", "from the server")), cursor = "c2", more = false))

        val published = engine.pullUntilCaughtUp(listOf("user"))

        assertEquals(1, published)
        assertEquals("from the server", store.peekSnapshot("notes", "n2")?.data?.get("title")?.string)
        assertEquals(1, store.peekPending().size, "the refused submission stays frozen for its own retry")
        assertEquals("push before pull", engine.health.failure.value?.operation)
    }

    /**
     * An explicit warm drain hands the server's refusal to its caller: a host barrier names its
     * own operation. The engine takes only a dead wire.
     */
    @Test
    fun anExplicitWarmDrainHandsTheServersRefusalToItsCaller() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        engine.pullOnce("user")
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        transport.refusePushes(error = ReplicaError.Protocol("unauthenticated", "HTTP 401"))

        assertFailsWith<ReplicaError.Protocol> { engine.drainIfWarm() }

        assertNull(engine.health.failure.value, "the refusal is the caller's to report")
        assertEquals(1, store.peekPending().size)
        assertFalse(engine.isColdForTesting(ReplicaLane.BULK), "a refusal is not a dead wire")
    }

    /** KILL: push unconditionally before every pull — a read costs two requests. */
    @Test
    fun emptyJournalPullsWithoutAPush() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.pullOnce("user")

        assertEquals(0, transport.pushCount(), "a pure-read pull costs one request, not two")
        assertEquals(1, transport.pullCount())
    }
}
