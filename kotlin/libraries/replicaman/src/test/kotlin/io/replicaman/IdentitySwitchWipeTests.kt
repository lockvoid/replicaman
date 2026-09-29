package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.allSnapshots
import io.replicaman.support.peekPending
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * A foreign identity takes the device: the outgoing user's ENTIRE local
 * world goes — snapshots on every shard, cursors AND the journal. An
 * unpushed op that survived would ride the next identity's bearer: the
 * write-side mirror of the E1.9 cross-identity read leak.
 *
 * The wipe is the FILE going away, so there is no wiping pass that could
 * miss a table, and no store for a late write to land in.
 */
class IdentitySwitchWipeTests : ReplicaTestCase() {

    /** KILL: make `retire()` call `releaseBinding(retiring = false)` — the file survives. */
    @Test
    fun retirementTakesEveryShardTheCursorsAndTheJournalWithTheFile() = runTest {
        val directory = Fixture.directory()
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(directory, transport)
        engine.open(1)

        // A signed-in world on both shards…
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "one")), cursor = "10:", more = false
            )
        )
        engine.pullOnce("user")
        transport.queuePull(
            "global",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.RowSet("assets", "a1", null, mapOf("kind" to ReplicaValue.Str("font")))
                ),
                cursor = "10:", more = false
            )
        )
        engine.pullOnce("global")

        // …plus an unpushed local op the dead wire cannot discharge.
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("local")))
        transport.failPushes(true)
        assertEquals(1, engine.pendingOps().size)

        engine.retire()
        engine.open(2)

        val store = engine.store
        assertNotNull(store)
        assertEquals(0, store.allSnapshots().size, "no snapshot survives a foreign switch — either shard")
        assertEquals(
            0, store.peekPending().size,
            "an unpushed op must NEVER ride the next identity's bearer"
        )
        assertNull(engine.currentCursor("user"), "a blank cursor makes the next pull re-snapshot the new identity's world")
        assertNull(engine.currentCursor("global"))
        assertFalse(
            File(directory, "replica-1.sqlite").exists(),
            "the outgoing owner's file is the wipe"
        )
    }
}
