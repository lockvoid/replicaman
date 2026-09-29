package io.replicaman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.realDelay
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * One FIFO is wrong when it carries two kinds of write. A chat message the
 * user had just typed sat behind ~19 bulk import rows and reached the server
 * seconds after the tap — nothing was slow, it was waiting its turn.
 *
 * A LANE is claimed by an ACTION, not by a stream. Writes inside
 * `lane(INTERACTIVE)` ride together, in order, on their own drain — which
 * runs CONCURRENTLY with bulk, so they overtake the backlog.
 *
 * Overtaking is only safe if a write can never pass something it depends on.
 * Two engine invariants make that automatic rather than remembered:
 *   • row stickiness — later ops for a row join that row's pending lane
 *   • causal promotion — an interactive op naming a pending row promotes it
 */
class LaneDrainTests : ReplicaTestCase() {

    private fun lanes(store: ReplicaStateStore): Map<String, String> = store.read { db ->
        val byRow = mutableMapOf<String, String>()
        for (entry in store.pending(db)) {
            byRow[entry.op().rowId] = store.lane(db, entry.id).rawValue
        }
        byRow
    }

    // MARK: - The scope

    /** KILL: read the lane from a parameter instead of the scope — the nested helper claims bulk. */
    @Test
    fun writesInsideTheScopeClaimTheLaneAndOutsideStayBulk() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        // The helper knows nothing about lanes — that is the reason the scope
        // is a scope.
        suspend fun helperThatKnowsNothingAboutLanes() {
            engine.saveRow("notes", "nested", null, mapOf("title" to ReplicaValue.Str("x")))
        }

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "hot", null, mapOf("title" to ReplicaValue.Str("typed")))
            helperThatKnowsNothingAboutLanes()
        }
        engine.saveRow("notes", "cold", null, mapOf("title" to ReplicaValue.Str("imported")))

        val claimed = lanes(store)
        assertEquals("interactive", claimed["hot"])
        assertEquals("interactive", claimed["nested"], "a nested helper inherits the claimed lane")
        assertEquals("bulk", claimed["cold"], "the default is background — a stream says nothing on its own")
    }

    /** KILL: make `lane()` only ever RAISE the claim — fan-out inherits the action's urgency. */
    @Test
    fun bulkNestedInsideAnInteractiveActionResetsTheLane() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "message", null, mapOf("title" to ReplicaValue.Str("typed")))
            engine.lane(ReplicaLane.BULK) {
                engine.saveRow("notes", "cook", null, mapOf("title" to ReplicaValue.Str("progress")))
            }
        }

        val claimed = lanes(store)
        assertEquals("interactive", claimed["message"])
        assertEquals("bulk", claimed["cook"], "fan-out started inside an action must not inherit its urgency")
    }

    /** KILL: keep the lane in a runtime map instead of the column — a kill loses the claim. */
    @Test
    fun theLaneSurvivesAStoreReopen() = runTest {
        val path = Fixture.path("lane-durability")
        val store = Fixture.storeAt(path)
        val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("t")))
        }
        store.close()

        val reopened = Fixture.storeAt(path)
        val claimed = reopened.read { db ->
            reopened.lane(db, reopened.pending(db)[0].id).rawValue
        }

        assertEquals("interactive", claimed, "a queued message is still urgent after the app is killed")
    }

    // MARK: - The overtake (the whole point)

    /**
     * The feature itself: the interactive push must be ON THE WIRE while the
     * bulk one is still held. An "it eventually shipped alone" assertion is
     * NOT this — that version stays green with the lanes fully serialised.
     *
     * KILL: key `activeDrains` on the engine instead of the lane — the
     * interactive drain joins the bulk flight and `overtook` is false.
     */
    @Test
    fun interactiveWorkWaitsForTheFrozenBulkPrefix() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        for (i in 0 until 19) {
            engine.saveRow("notes", "bulk$i", null, mapOf("title" to ReplicaValue.Str("i$i")))
        }
        val bulkReleased = Gate()
        val bulkArrived = Gate()
        val flags = Flags()
        transport.onPush { ops ->
            when {
                ops.any { it.rowId.startsWith("bulk") } -> {
                    flags.set("bulkInFlight", true)
                    bulkArrived.release()
                    bulkReleased.wait()
                    flags.set("bulkInFlight", false)
                }

                ops.any { it.rowId == "message" } -> flags.set("overtook", flags.get("bulkInFlight"))
            }
        }
        val bulkDrain = scope.async { runCatching { engine.drain(ReplicaLane.BULK) } }
        // The bulk push signals its OWN arrival from inside `push` — the
        // interactive write below is provably racing a flight that is on the
        // wire, not one that a sleep hoped had started.
        bulkArrived.wait()

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "message", null, mapOf("title" to ReplicaValue.Str("typed")))
        }
        val interactive = scope.async { engine.drain(ReplicaLane.INTERACTIVE) }
        bulkReleased.release()
        bulkDrain.await().getOrThrow()
        interactive.await()
        assertFalse(flags.get("overtook"), "a request cannot pass the frozen submissions ahead of it")
        val sent = transport.pushedBatches().flatten().map { it.rowId }
        assertEquals("message", sent.last())
        assertEquals(20, sent.toSet().size)
        scope.cancel()
    }

    /**
     * Promotion can move an entry the OTHER lane is holding on the wire. If
     * the drain then re-selects it, the same create ships twice.
     *
     * KILL: drop the `inFlightEntryIds` filter from the drain's selection.
     */
    @Test
    fun aPromotedEntryAlreadyOnTheWireIsNotSentTwice() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        engine.saveRow("notes", "element", null, mapOf("title" to ReplicaValue.Str("clip")))
        val release = Gate()
        val arrived = Gate()
        transport.onPush { ops ->
            if (ops.none { it.rowId == "element" }) return@onPush
            arrived.release()
            release.wait()
        }
        val bulkDrain = scope.async { runCatching { engine.drain(ReplicaLane.BULK) } }
        arrived.wait()

        // The user attaches that very element — promotion pulls it up while
        // its push is still open.
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "attachment", null, mapOf("recordId" to ReplicaValue.Str("element")))
        }
        val interactive = scope.async { engine.drain(ReplicaLane.INTERACTIVE) }
        release.release()
        bulkDrain.await().getOrThrow()
        interactive.await()

        val sent = transport.pushedBatches().flatten().map { it.rowId }
        assertEquals(
            1, sent.count { it == "element" },
            "an in-flight entry must not be re-sent by the lane that promoted it"
        )
        scope.cancel()
    }

    /**
     * Sign-out flushes the journal through a pinned transport and then wipes.
     *
     * KILL: pass `lane = BULK` instead of `null` to the sealed flush — the
     * user's just-typed message is destroyed by the wipe.
     */
    @Test
    fun theSignOutFlushDrainsBothLanes() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "import", null, mapOf("title" to ReplicaValue.Str("bulk")))
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "message", null, mapOf("title" to ReplicaValue.Str("typed")))
        }

        engine.seal()
        val pinned = StubTransport()
        engine.sealAndDrain(pinned)

        assertEquals(
            setOf("import", "message"), pinned.pushedBatches().flatten().map { it.rowId }.toSet(),
            "everything owed goes out before the wipe, whatever lane it claimed"
        )
    }

    /** KILL: select pending entries without `ORDER BY rowid` — order inside a lane is lost. */
    @Test
    fun orderIsPreservedWithinALane() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.lane(ReplicaLane.INTERACTIVE) {
            for (i in 0 until 5) {
                engine.saveRow("notes", "n$i", null, mapOf("title" to ReplicaValue.Str("t$i")))
            }
        }
        engine.drain(ReplicaLane.INTERACTIVE)

        assertEquals(
            listOf("n0", "n1", "n2", "n3", "n4"),
            transport.pushedBatches().flatten().map { it.rowId }
        )
    }

    // MARK: - The invariants that keep overtaking safe

    /**
     * DEFECT THIS PREVENTS: a bulk patch passing the interactive create of
     * the same row — the server refuses an update to a row it has never seen.
     *
     * KILL: drop the stickiness lookup in `enqueueOp` (always use the
     * requested lane).
     */
    @Test
    fun aLaterBulkWriteJoinsTheLaneItsRowIsAlreadyQueuedOn() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("created")))
        }
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("edited")))

        assertEquals(
            "interactive", lanes(store)["n1"],
            "an update must never outrun the create of its own row"
        )
    }

    /**
     * DEFECT THIS PREVENTS: attaching a clip
     * whose element create is still queued in bulk.
     *
     * KILL: drop `promoteDependencies` — `element-7` stays bulk and the
     * attachment reaches the server first.
     */
    @Test
    fun anInteractiveWriteNamingAPendingBulkRowPromotesThatRow() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        // The import authored this element and has not pushed yet.
        engine.saveRow("notes", "element-7", null, mapOf("title" to ReplicaValue.Str("clip")))
        // The user attaches it to a message they just typed.
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "attachment-1", null, mapOf("recordId" to ReplicaValue.Str("element-7")))
        }

        assertEquals(
            "interactive", lanes(store)["element-7"],
            "what an interactive write depends on is interactive too"
        )

        engine.drain(ReplicaLane.INTERACTIVE)
        assertEquals(
            listOf("element-7", "attachment-1"),
            transport.pushedBatches().flatten().map { it.rowId },
            "and it still lands first"
        )
    }

    /** Thread-safe flag board for the mid-push observation. */
    private class Flags {
        private val lock = ReentrantLock()
        private val values = mutableMapOf<String, Boolean>()

        fun set(key: String, value: Boolean) = lock.withLock { values[key] = value; Unit }

        fun get(key: String): Boolean = lock.withLock { values[key] ?: false }
    }

    /**
     * A hold that many pushes may await — a duplicate push is exactly what
     * one of these tests is hunting, so a one-shot latch would not do.
     */
    private class Gate {
        private val lock = ReentrantLock()
        private var opened = false

        fun release() = lock.withLock { opened = true; Unit }

        val isOpen: Boolean get() = lock.withLock { opened }

        suspend fun wait(seconds: Long = 5) {
            val deadline = System.currentTimeMillis() + seconds * 1000
            while (!isOpen && System.currentTimeMillis() < deadline) realDelay(10)
            assertTrue(isOpen, "the held transport gate did not open")
        }
    }
}
