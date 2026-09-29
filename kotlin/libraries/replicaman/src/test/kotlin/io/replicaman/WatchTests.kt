package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.Recorder
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.TestNote
import io.replicaman.support.eventually
import io.replicaman.support.peekDoc
import io.replicaman.support.recordInto
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * B8 — `watch`: post-checkpoint change signal per stream, backed by the
 * store's committed sequence. Signals fire only after COMMIT — a
 * rolled-back checkpoint fires nothing; the typed watch delivers fresh rows.
 */
class WatchTests : ReplicaTestCase() {

    private class Fault : Exception("injected")

    /** KILL: yield the baseline to every observer — a change-only consumer double-counts. */
    @Test
    fun signalCanIncludeTheCommittedBaselineWithoutChangingTheDefault() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val changeOnly = Recorder<Unit>()
        val withInitial = Recorder<Unit>()

        engine.watchSignal("notes").recordInto(scope, changeOnly)
        engine.watchSignal("notes", includeInitial = true).recordInto(scope, withInitial)

        eventually(3.seconds, "the committed baseline was not delivered") { withInitial.count == 1 }
        assertEquals(0, changeOnly.count, "a baseline reached a change-only observer")

        // One commit, seen by BOTH — and both are polled before either total is
        // read. The two observations are independent, so the change-only one
        // may report first; asserting a cross-observer total on the strength of
        // one of them is a race.
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one")))
        eventually(3.seconds, "both observers must report the commit before their totals mean anything") {
            withInitial.count >= 2 && changeOnly.count >= 1
        }
        assertEquals(2, withInitial.count)
        assertEquals(1, changeOnly.count, "a baseline reached a change-only observer")
        scope.cancel()
    }

    /**
     * KILL: not the store-level one this line used to claim. Raising the tick
     * from inside the write transaction leaves this GREEN and is caught by
     * `StoreContractTests.aFailedCommitRollsBackAndTheStoreKeepsWriting`
     * instead (verified: that mutation reddens exactly those two tests over
     * the full suite). What this grades is one layer up — the engine must not
     * signal for a checkpoint it rolled back.
     */
    @Test
    fun signalFiresAfterCommitAndNotOnRolledBackCheckpoints() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        val signals = Recorder<Unit>()
        // `includeInitial` makes ARMING observable: the baseline yield is the
        // proof the observation is live, which a sleep only ever assumed.
        engine.watchSignal("notes", includeInitial = true).recordInto(scope, signals)
        eventually(3.seconds, "the observation never armed") { signals.count == 1 }
        val baseline = signals.count

        // A faulted checkpoint rolls back — it must NOT signal.
        engine.setCheckpointFault { throw Fault() }
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "half")), cursor = "5:", more = false)
        )
        assertFailsWith<Fault> { engine.pullOnce("user") }

        // A committed checkpoint follows. Its signal is delivered in commit
        // order AFTER any signal the rollback might wrongly have produced.
        engine.setCheckpointFault(null)
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "landed")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")
        eventually(3.seconds, "commit did not signal the stream") { signals.count > baseline }
        assertEquals(
            baseline + 1, signals.count,
            "a rolled-back checkpoint fired a signal — consumers would read uncommitted state"
        )
        scope.cancel()
    }

    /** KILL: deliver the picture from the cache without the committed sequence — the rows are stale. */
    @Test
    fun typedWatchDeliversFreshRows() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val notes = RowStream(engine, TestNote)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        val pictures = Recorder<List<TestNote>>()
        notes.watch().recordInto(scope, pictures)
        eventually(3.seconds, "the typed-watch baseline was not delivered") { pictures.count == 1 }
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one"), Fixture.note("n2", "two")), cursor = "5:", more = false))
        engine.pullOnce("user")

        eventually(3.seconds, "the typed watch never saw the imported rows") {
            pictures.last?.map { it.id } == listOf("n1", "n2")
        }
        assertEquals(listOf("one", "two"), pictures.last!!.map { it.title })
        scope.cancel()
    }

    /** KILL: track a global commit counter instead of the stream's sequence — every stream wakes. */
    @Test
    fun signalDoesNotRingForAnotherStreamsCommit() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val notesSignals = Recorder<Unit>()
        val boardsSignals = Recorder<Unit>()

        engine.watchSignal("notes", includeInitial = true).recordInto(scope, notesSignals)
        engine.watchSignal("boards", includeInitial = true).recordInto(scope, boardsSignals)

        eventually(5.seconds, "the signal baselines were not delivered") {
            notesSignals.count == 1 && boardsSignals.count == 1
        }

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "Trip")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")

        eventually(5.seconds, "the notes commit did not ring its own signal") { notesSignals.count == 2 }
        // A second notes commit: by the time IT is delivered, the boards
        // observer has been re-evaluated twice on the same table and must
        // still have yielded nothing but its baseline.
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("again")))
        eventually(5.seconds, "the second notes commit did not ring") { notesSignals.count == 3 }
        assertEquals(1, boardsSignals.count, "a commit on another stream rang this one's signal")
        scope.cancel()
    }

    /** KILL: de-duplicate on the row COUNT — a same-weight rename goes unseen. */
    @Test
    fun sameWeightContentChangeRingsItsOwnSignal() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "Trip")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")

        val signals = Recorder<Unit>()
        engine.watchSignal("notes", includeInitial = true).recordInto(scope, signals)
        eventually(3.seconds, "the signal baseline was not delivered") { signals.count == 1 }

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "Trap")), cursor = "6:", more = false)
        )
        engine.pullOnce("user")

        eventually(3.seconds, "the same-weight rename did not ring its stream") { signals.count == 2 }
        scope.cancel()
    }

    private suspend fun pullAnAsset(transport: StubTransport, engine: ReplicaEngine) {
        transport.queuePull("global", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.RowSet("assets", "a1", null, mapOf("kind" to ReplicaValue.Str("font")))
        ), cursor = "1:", more = false))
        engine.pullOnce("global")
    }

    /** KILL: the same global-counter mistake on the typed watch. */
    @Test
    fun typedWatchDoesNotDeliverForAnotherStreamsCommit() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val notes = RowStream(engine, TestNote)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val deliveries = Recorder<List<TestNote>>()

        notes.watch().recordInto(scope, deliveries)
        eventually(3.seconds, "the typed-watch baseline was not delivered") { deliveries.count == 1 }

        pullAnAsset(transport, engine)

        // The global-shard commit happened FIRST. A notes commit follows; once
        // its delivery lands, anything the assets commit wrongly produced has
        // already been delivered too, so the total is exact.
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        eventually(3.seconds, "the typed watch never reported its own stream's commit") {
            deliveries.last?.map { it.id } == listOf("n1")
        }
        assertEquals(2, deliveries.count, "another stream's commit was delivered to this typed watch")
        scope.cancel()
    }

    /**
     * Regression pin for the removed aggregate fingerprint: a peer is a
     * random Loro u64 kept as an i64 bit-pattern, so summing two peer values
     * can overflow and terminate an observation. The durable stream
     * sequence must signal without inspecting or aggregating those values.
     *
     * KILL: track `SELECT SUM(peer) FROM docs` instead of the sequence.
     */
    @Test
    fun watchSurvivesLargePeerDocs() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(
            store = store, transport = transport,
            minter = Fixture.sequentialMinter(Long.MAX_VALUE.toULong() - 1uL)
        )
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot("boards", "b1", "stub@1", "SNAP-1".toByteArray(), emptyMap()),
                    ReplicaFrame.DocSnapshot("boards", "b2", "stub@1", "SNAP-2".toByteArray(), emptyMap()),
                ),
                cursor = "5:", more = false
            )
        )
        engine.pullOnce("user")

        val first = store.peekDoc("boards", "b1")
        val second = store.peekDoc("boards", "b2")
        assertNotNull(first)
        assertNotNull(second)
        assertTrue(
            first.peer > Long.MAX_VALUE.toULong() / 2uL && second.peer > Long.MAX_VALUE.toULong() / 2uL,
            "precondition: both peers must be large enough that their sum exceeds Long.MAX_VALUE"
        )

        val signals = Recorder<Unit>()
        engine.watchSignal("boards", includeInitial = true).recordInto(scope, signals)
        eventually(3.seconds, "the observation never armed over the large-peer docs") { signals.count == 1 }
        val baseline = signals.count

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(ReplicaFrame.DocDelta("boards", "b1", 1, "stub@1", "+d1".toByteArray())),
                cursor = "6:", more = false
            )
        )
        engine.pullOnce("user")

        eventually(3.seconds, "the committed write never signalled — a docs fingerprint overflowed") {
            signals.count > baseline
        }
        scope.cancel()
    }
}
