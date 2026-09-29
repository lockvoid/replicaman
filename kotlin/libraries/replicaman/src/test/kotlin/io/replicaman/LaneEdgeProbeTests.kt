package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import kotlin.test.assertEquals

/** Lead's adversarial probe — edges the lane pins do NOT cover. */
class LaneEdgeProbeTests : ReplicaTestCase() {

    private fun lanes(store: ReplicaStateStore): Map<String, String> = store.read { db ->
        val byRow = mutableMapOf<String, String>()
        for (entry in store.pending(db)) {
            byRow[entry.op().rowId] = store.lane(db, entry.id).rawValue
        }
        byRow
    }

    /**
     * Promotion walks op data for values matching pending row ids. If two
     * pending rows name each other, does the walk terminate?
     *
     * KILL: drop the `visited` set from `promoteDependencies` — the cycle
     * spins the write forever.
     */
    @Test
    fun promotionTerminatesOnAReferenceCycle() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        engine.saveRow("notes", "A", null, mapOf("ref" to ReplicaValue.Str("B")))
        engine.saveRow("notes", "B", null, mapOf("ref" to ReplicaValue.Str("A")))

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "C", null, mapOf("ref" to ReplicaValue.Str("A")))
        }

        val claimed = lanes(store)
        assertEquals("interactive", claimed["A"])
        assertEquals("interactive", claimed["B"], "the cycle is followed once, not forever")
        assertEquals("interactive", claimed["C"])
    }

    /**
     * THE SUSPECTED REGRESSION: promotion decodes every pending entry's op.
     * Before the lane a corrupt/undecodable entry just sat in the journal.
     *
     * KILL: let `promoteDependencies` rethrow a decode failure — one bad row
     * blows up the user's next write.
     */
    @Test
    fun aCorruptPendingEntryDoesNotBreakTheUsersNextWrite() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        engine.saveRow("notes", "ok", null, mapOf("title" to ReplicaValue.Str("t")))
        // Poison one journal payload the way a partial write or a version
        // skew would.
        store.write { db ->
            db.exec(
                "UPDATE intents SET payload = ? WHERE id = (SELECT id FROM intents LIMIT 1)",
                listOf("{not json at all")
            )
        }

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "message", null, mapOf("title" to ReplicaValue.Str("typed")))
        }

        assertEquals(2, store.read { store.pending(it) }.size, "the user's write landed despite the poison entry")
    }

    /**
     * Parked entries (rejected, awaiting the user) must not be dragged onto a
     * lane or drained.
     *
     * KILL: take refused intents in `pendingBulkEntries` — a refused entry
     * is resurrected onto the wire by an unrelated interactive write.
     */
    @Test
    fun parkedEntriesAreExcludedFromLanesAndDrains() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "parked", null, mapOf("title" to ReplicaValue.Str("x")))
        store.write { db -> db.exec("UPDATE intents SET state = 'refused', reason = 'refused'") }
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "live", null, mapOf("ref" to ReplicaValue.Str("parked")))
        }
        engine.drain(ReplicaLane.INTERACTIVE)

        assertEquals(
            listOf("live"), transport.pushedBatches().flatten().map { it.rowId },
            "a parked entry is not resurrected by promotion"
        )
    }

    /**
     * A row reference can sit inside an array or a nested document
     * (`Cook.data` is an arbitrary jsonb doc), not only in a flat string
     * column.
     *
     * KILL: make `namedIds` read only top-level strings — the nested
     * dependency is left on the bulk lane and the server refuses the write.
     */
    @Test
    fun promotionFindsIdsNestedInsideArraysAndObjects() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        engine.saveRow("notes", "in-array", null, mapOf("title" to ReplicaValue.Str("a")))
        engine.saveRow("notes", "in-object", null, mapOf("title" to ReplicaValue.Str("o")))

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow(
                "notes", "doc", null,
                mapOf(
                    "refs" to ReplicaValue.Arr(listOf(ReplicaValue.Str("in-array"))),
                    "meta" to ReplicaValue.Obj(mapOf("source" to ReplicaValue.Str("in-object")))
                )
            )
        }

        val claimed = lanes(store)
        assertEquals("interactive", claimed["in-array"])
        assertEquals("interactive", claimed["in-object"])
    }

    /**
     * Stickiness pulls a row up by its ROW; what THAT row names must come
     * with it, or the chain breaks one link further down.
     *
     * KILL: drop `alsoWalk` from `enqueueOp` — `dependency` stays bulk.
     */
    @Test
    fun aStickyPromotionAlsoWalksWhatThePromotedEntryNames() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        engine.saveRow("notes", "dependency", null, mapOf("title" to ReplicaValue.Str("q")))
        engine.saveRow("notes", "carrier", null, mapOf("ref" to ReplicaValue.Str("dependency")))
        // Touch `carrier` from an interactive action: stickiness pulls its
        // queued create up, and `dependency` must follow.
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "carrier", null, mapOf("title" to ReplicaValue.Str("edited")))
        }

        val claimed = lanes(store)
        assertEquals("interactive", claimed["carrier"])
        assertEquals(
            "interactive", claimed["dependency"],
            "the promoted entry's own dependency cannot be left behind"
        )
    }
}
