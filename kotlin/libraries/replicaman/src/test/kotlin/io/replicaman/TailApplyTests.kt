package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekSnapshot
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Tail apply: `row.set` REPLACES (no merge, no
 * insert-or-update branching), `row.delete` removes, and ordering within a
 * batch is preserved.
 */
class TailApplyTests : ReplicaTestCase() {

    /** KILL: merge the frame's data into the existing row — the stale `rank` survives. */
    @Test
    fun rowSetReplacesRowDeleteRemovesOrderPreserved() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    Fixture.note("n1", "first", "a"),
                    // Replacement drops fields the new copy doesn't carry — that is
                    // what "unconditional replace" means.
                    ReplicaFrame.RowSet("notes", "n1", null, mapOf("title" to ReplicaValue.Str("second"))),
                    Fixture.note("n2", "doomed"),
                    ReplicaFrame.RowDelete("notes", "n2"),
                    Fixture.note("n3", "last"),
                ),
                cursor = "7:", more = false
            )
        )
        engine.pullOnce("user")

        val n1 = store.peekSnapshot("notes", "n1")
        assertNotNull(n1)
        assertEquals(ReplicaValue.Str("second"), n1.data["title"])
        assertNull(n1.data["rank"], "row.set replaced the whole copy; the stale field is gone")
        assertNull(store.peekSnapshot("notes", "n2"), "set-then-delete within one batch lands deleted")
        assertEquals(ReplicaValue.Str("last"), store.peekSnapshot("notes", "n3")?.data?.get("title"))
    }

    /** KILL: pass `null` instead of the held cursor in `pullPage` — page 2 re-serves page 1. */
    @Test
    fun pullUntilCaughtUpFollowsMoreAndThreadsTheCursor() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one")), cursor = "5:x", more = true)
        )
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n2", "two")), cursor = "9:", more = false)
        )

        val applied = engine.pullUntilCaughtUp()
        assertEquals(2, applied)

        val userPulls = transport.events()
            .filterIsInstance<StubTransport.Event.Pull>()
            .filter { it.shard == "user" }
            .map { it.cursor }
        assertEquals(listOf(null, "5:x"), userPulls, "the second page pulls FROM the first page's cursor")
        assertEquals("9:", engine.currentCursor("user"))
        assertNotNull(store.peekSnapshot("notes", "n2"))
    }
}
