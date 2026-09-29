package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.allSnapshots
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import kotlin.test.assertEquals

/**
 * Bootstrap with `reset` replaces the server's part of the world;
 * what the journal still owes is the device's and stays with the journal.
 */
class BootstrapResetTests : ReplicaTestCase() {

    /** KILL: on a reset, delete the shard's snapshots the round did not deliver — n3's row goes with them. */
    @Test
    fun resetReplacesTheShardWorldAndJournalSurvives() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        // World v1: two notes on the user shard, one asset on global.
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "one"), Fixture.note("n2", "two")),
                cursor = "10:", more = false
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

        // Unsent local work: a third note, journaled — and the wire's push
        // side goes dead, so the drain barrier can't discharge it first
        // (the offline shape this matrix item exists for).
        engine.saveRow("notes", "n3", null, mapOf("title" to ReplicaValue.Str("local")))
        assertEquals(1, store.peekPending().size)
        transport.failPushes(true)

        engine.resetCursors()

        // Forced resnapshot (GC horizon / rebuild): the server's part of the
        // user shard is REPLACED — not merged.
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n4", "fresh")), cursor = "20:", more = false
            )
        )
        engine.pullOnce("user")

        val userRows = store.allSnapshots().filter { it.stream == "notes" }
        assertEquals(listOf("n3", "n4"), userRows.map { it.rowId }.sorted(),
            "reset wipes the server's n1/n2 and keeps n3, which the server has never had")
        assertEquals(ReplicaValue.Str("local"), store.peekSnapshot("notes", "n3")?.data?.get("title"))

        assertEquals(
            ReplicaValue.Str("font"),
            store.peekSnapshot("assets", "a1")?.data?.get("kind"),
            "another shard's rows are untouched by this shard's reset"
        )

        val pending = store.peekPending()
        assertEquals(1, pending.size, "the journal SURVIVES a reset — it still owes n3")
        assertEquals("n3", pending.first().op().rowId)

        assertEquals("20:", engine.currentCursor("user"), "the reset's cursor is the new checkpoint")
    }
}
