package io.replicaman

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.realDelay
import io.replicaman.support.until
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The contract a catalog warmer leans on: a zero-interval nudger is a pure
 * coalescer — a burst of triggers costs one in-flight sync plus at most one
 * queued re-run, and syncs NEVER overlap. Overlap is not just waste there:
 * parallel warm passes race over the same font file mid-move.
 */
class NudgeSerialityTests : ReplicaTestCase() {

    private class Probe {
        private val lock = ReentrantLock()
        private var running = 0
        var peak = 0
            private set
        var runs = 0
            private set

        fun enter() = lock.withLock {
            running += 1
            peak = maxOf(peak, running)
            runs += 1
        }

        fun exit() = lock.withLock { running -= 1; Unit }

        val isRunning: Boolean get() = lock.withLock { running > 0 }
    }

    /** KILL: launch a fresh `run()` per nudge — `peak` climbs above one. */
    @Test
    fun aTriggerBurstNeverOverlapsSyncsAndCoalescesToTwo() = runTest {
        val probe = Probe()
        val nudger = ReplicaNudger {
            probe.enter()
            realDelay(50)
            probe.exit()
        }

        repeat(7) { nudger.nudge() }

        // Quiescence, not a settle: wait until a sync has started and then
        // until none is running and the count has stopped moving.
        until("the burst never started a sync at all") { probe.runs >= 1 }
        until("the nudger never came to rest") {
            val before = probe.runs
            realDelay(60)
            !probe.isRunning && probe.runs == before
        }

        assertEquals(1, probe.peak, "syncs must never run concurrently")
        assertTrue(probe.runs >= 1)
        assertTrue(probe.runs <= 2, "a burst folds into the in-flight sync plus one re-run; saw ${probe.runs}")
    }

    /** KILL: clear `inFlight` after the loop instead of in `finally` — every later doorbell only queues behind a sync that is gone. */
    @Test
    fun aCancelledSyncStillAdmitsTheNextDoorbell() = runTest {
        val probe = Probe()
        val nudger = ReplicaNudger {
            probe.enter()
            probe.exit()
            if (probe.runs == 1) throw CancellationException("the first sync was cancelled")
        }

        nudger.nudge()
        until("the first sync never ran") { probe.runs == 1 }
        until("a doorbell after the cancelled sync never started another") {
            nudger.nudge()
            probe.runs >= 2
        }
    }
}
