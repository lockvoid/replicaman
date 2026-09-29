package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.HookOutcome
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith

/**
 * The drain ships BOUNDED chunks with INCREMENTAL acks. The 500-op single
 * request this replaces looped forever on a real device: the server spent
 * minutes applying it, the client timed out first, no verdict ever landed,
 * and the same 647 ops re-pushed every cold-window — the "idle phone
 * ringing its own doorbell 2.5/s" storm. Small chunks return in seconds
 * and every acked chunk leaves the journal for good, so a mid-drain
 * transport death costs one chunk, never the whole backlog.
 */
class ChunkedDrainTests : ReplicaTestCase() {

    /** Thread-safe pending-count log for the push-time observation hook. */
    private class PendingLog {
        private val lock = ReentrantLock()
        private val values = mutableListOf<Int>()

        fun append(value: Int) = lock.withLock { values.add(value); Unit }

        val counts: List<Int> get() = lock.withLock { values.toList() }
    }

    private suspend fun fill(engine: ReplicaEngine, count: Int) {
        for (index in 0 until count) {
            engine.saveRow(
                "notes", String.format("n%03d", index), null,
                mapOf("title" to ReplicaValue.Str("t$index"))
            )
        }
    }

    /**
     * KILL: move the `applyVerdicts(...)` call out of the chunk loop to
     * after it — the observed counts stay [120, 120, 120] and in production
     * the whole backlog is re-stranded on every failure.
     */
    @Test
    fun drainShipsBoundedChunksWithIncrementalAcks() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        // The load-bearing assertion is the MID-DRAIN one: pending counts
        // observed at each chunk's push time. Chunk 2 must find chunk 1's
        // 50 entries already GONE from the journal, chunk 3 must find 100
        // gone. An end-state check alone stays green under the old
        // apply-everything-at-the-end drain.
        val observed = PendingLog()
        val hook = HookOutcome()
        transport.onPush {
            try {
                observed.append(store.read { store.pending(it).size })
            } catch (error: Throwable) {
                // A failed read here would otherwise vanish into a sentinel and
                // read as a passing chunk boundary.
                hook.record(error)
            }
        }

        fill(engine, 120)
        engine.drain()

        assertNull(hook.failure, "the mid-push observation itself failed; the counts below prove nothing")
        assertEquals(
            listOf(50, 50, 20), transport.pushedBatches().map { it.size },
            "the whole queue drains as bounded chunks, in order"
        )
        assertEquals(
            listOf(120, 70, 20), observed.counts,
            "each chunk ships only after the previous chunk's acks left the journal"
        )

        assertTrue(store.read { store.pending(it) }.isEmpty(), "every acked chunk leaves the journal")
    }

    /** KILL: the same move — a lost response re-strands the whole backlog. */
    @Test
    fun midDrainFailureKeepsOnlyUnackedChunksPending() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        transport.failPushesAfter(1)
        val engine = Fixture.engine(store = store, transport = transport)

        fill(engine, 120)

        assertFailsWith<ReplicaError.Transport> {
            engine.drain()
        }

        assertEquals(
            70, store.read { store.pending(it) }.size,
            "chunk 1's 50 ops acked INCREMENTALLY and left the journal; only the unsent 70 stay — " +
                "a lost response can no longer strand the whole backlog"
        )
    }
}
