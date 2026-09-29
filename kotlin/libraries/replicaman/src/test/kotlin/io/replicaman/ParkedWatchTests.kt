package io.replicaman

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
import io.replicaman.support.eventually
import io.replicaman.support.recordInto
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

/**
 * The refusal ledger as a subscription: a rejected push PARKS the entry and
 * the watcher delivers the committed picture; a discard delivers the picture
 * without it. The consumer never re-queries — the stale-snapshot
 * race (a triggered re-read racing the discard it was meant to observe) is
 * impossible against commit-ordered values.
 */
class ParkedWatchTests : ReplicaTestCase() {

    /**
     * KILL: yield only on CHANGE (drop the baseline) in `watchParkedOps` — a
     * consumer that owns its dictionary by assignment never learns the
     * committed starting state.
     */
    @Test
    fun aParkArrivesAndItsDiscardClearsIt() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.scriptPush { ops ->
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "Scenes is invalid") }
        }

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val pictures = Recorder<List<ReplicaStateStore.JournalRow>>()
        engine.watchParkedOps().recordInto(scope, pictures)
        eventually(5.seconds, "the empty baseline never arrived") {
            pictures.values.any { it.isEmpty() }
        }

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("t")))
        engine.drain()

        eventually(5.seconds, "the expected parked picture never arrived") {
            pictures.values.any { it.size == 1 && it[0].parked == "Scenes is invalid" }
        }
        val parked = pictures.values.first { it.isNotEmpty() }
        assertEquals(1, parked.size)
        assertEquals("Scenes is invalid", parked[0].parked)

        engine.discardOps(listOf(parked[0].id))
        eventually(5.seconds, "the discard's committed picture never arrived") {
            pictures.last?.isEmpty() == true
        }
        scope.cancel()
    }
}
