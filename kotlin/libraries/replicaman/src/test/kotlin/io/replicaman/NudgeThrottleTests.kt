package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Recorder
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.Tally
import io.replicaman.support.eventually
import io.replicaman.support.peekSnapshot
import io.replicaman.support.realDelay
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * Amendment B — the doorbell throttle: during a render the signal rate is
 * ~5/s per cook, and coalescing alone still runs back-to-back pulls. A
 * trailing-edge throttle caps doorbell-triggered syncs at ~1 per window;
 * the LAST doorbell always lands, so the final state is caught up.
 */
class NudgeThrottleTests : ReplicaTestCase() {

    /** KILL: drop the trailing-edge wait — every doorbell in the burst pulls. */
    @Test
    fun rapidNudgesCostAtMostOnePullPerWindow() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        val failures = Recorder<Throwable>()
        val starts = Recorder<TimeSource.Monotonic.ValueTimeMark>()
        val nudger = ReplicaNudger(400.milliseconds) {
            starts.record(TimeSource.Monotonic.markNow())
            try {
                engine.pullUntilCaughtUp()
            } catch (error: Throwable) {
                failures.record(error)
            }
        }

        // A render-storm burst: far more doorbells than windows.
        repeat(20) {
            nudger.nudge()
            realDelay(10)
        }

        eventually(8.seconds, "nudger never went quiet") {
            val before = transport.pullCount()
            if (before == 0) return@eventually false
            realDelay(500)
            transport.pullCount() == before
        }

        assertTrue(failures.values.isEmpty(), "sync failed: ${failures.values}")
        val gaps = starts.values.zipWithNext { earlier, later -> later - earlier }
        assertTrue(gaps.isNotEmpty(), "the burst collapsed into a single sync; the trailing edge never ran")
        gaps.forEach { assertTrue(it >= 350.milliseconds, "one sync per window, not one per doorbell; saw a $it gap") }
    }

    /** KILL: drop `queued` — a doorbell inside the window is lost, not late. */
    @Test
    fun trailingEdgeAlwaysDeliversTheLastDoorbell() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        val failures = Recorder<Throwable>()
        val nudger = ReplicaNudger(200.milliseconds) {
            try {
                engine.pullUntilCaughtUp()
            } catch (error: Throwable) {
                failures.record(error)
            }
        }

        // First doorbell syncs immediately and consumes the queued state.
        nudger.nudge()
        eventually(2.seconds, "first sync never ran") { transport.pullCount() > 0 }
        val afterFirst = transport.pullCount()

        // A doorbell INSIDE the window must still produce a sync — late,
        // never lost: the world moved after the last pull.
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "fresh")), cursor = "9:", more = false)
        )
        nudger.nudge()

        eventually(3.seconds, "the trailing doorbell was dropped") {
            store.peekSnapshot("notes", "n1")?.data?.get("title") == ReplicaValue.Str("fresh")
        }
        assertTrue(transport.pullCount() > afterFirst)
        assertTrue(failures.values.isEmpty(), "sync failed: ${failures.values}")
    }

    /**
     * A doorbell rung BY A TAP jumps the window. STOP writes no row — the
     * epoch bump on the chat row IS the answer — so the button the person is
     * staring at cannot flip until this pull lands.
     *
     * KILL: ignore `immediate` — the tapped doorbell waits out the window.
     */
    @Test
    fun anImmediateNudgeDoesNotWaitOutTheWindow() = runTest {
        val syncs = Tally()
        val nudger = ReplicaNudger(5.seconds) { syncs.bump() }

        nudger.nudge()
        eventually(2.seconds, "the first sync never ran") { syncs.count == 1 }

        // Inside the 5s window. A polite doorbell would sit here for seconds.
        val started = System.currentTimeMillis()
        nudger.nudge(immediate = true)
        eventually(2.seconds, "the tapped doorbell waited out the window") { syncs.count == 2 }
        assertTrue(System.currentTimeMillis() - started < 1000)

        // The window is restored for the machine chatter that follows. This
        // wait IS the contract — a throttle is a statement about time, and
        // 0.7s inside a 5s window is what "still throttled" means.
        nudger.nudge()
        realDelay(700)
        assertEquals(2, syncs.count, "an ordinary doorbell is still throttled")
    }
}
