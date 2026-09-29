package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import kotlin.test.assertEquals
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
