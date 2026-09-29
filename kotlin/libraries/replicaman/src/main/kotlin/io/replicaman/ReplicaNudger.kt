package io.replicaman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * The doorbell's landing pad, ported verbatim from Syncer v1. Every "server
 * state moved" signal — a realtime event, app foregrounding, a drained
 * create — calls `nudge()`; the nudger coalesces them into AT MOST one
 * in-flight sync (drain + pull-until-caught-up) plus one queued re-run.
 * Data never rides the signal itself; the pull is always the carrier, so a
 * lost doorbell costs latency, never correctness. Callers never await the sync.
 *
 * The iOS actor becomes one `limitedParallelism(1)` dispatcher: state is
 * touched only inside it, and every suspension releases it — actor
 * reentrancy, which the trailing-edge wait depends on.
 */
public class ReplicaNudger(
    /**
     * The trailing-edge throttle window: doorbell storms (a render signals
     * ~5/s per cook) collapse to at most one sync per window, and the LAST
     * doorbell always lands. 0 = pure coalescer.
     */
    private val interval: Duration = Duration.ZERO,
    private val sync: suspend () -> Unit,
) {
    @Suppress("OPT_IN_USAGE")
    private val context = Dispatchers.Default.limitedParallelism(1)
    private val scope = CoroutineScope(SupervisorJob() + context)

    private var inFlight = false
    private var queued = false
    private var lastStart: TimeSource.Monotonic.ValueTimeMark? = null
    private var skipWindow = false

    /**
     * `immediate` skips the throttle window for this run. The window exists to
     * absorb machine chatter; a doorbell rung BY a tap is the opposite — the
     * person is watching the affordance that the pulled row will flip, and a
     * second of politeness reads as a dead button.
     */
    public suspend fun nudge(immediate: Boolean = false) {
        withContext(context) {
            if (immediate) skipWindow = true
            if (inFlight) {
                queued = true
                return@withContext
            }
            inFlight = true
            scope.launch { run() }
        }
    }

    private suspend fun run() {
        try {
            do {
                // Trailing edge: never start a sync inside the window of the
                // previous one; doorbells arriving during the wait fold into
                // this run.
                val start = lastStart
                if (interval > Duration.ZERO && !skipWindow && start != null) {
                    val wait = interval - start.elapsedNow()
                    if (wait > Duration.ZERO) delay(wait)
                }
                skipWindow = false
                queued = false
                lastStart = TimeSource.Monotonic.markNow()
                sync()
            } while (queued)
        } finally {
            inFlight = false
        }
    }

    public companion object {
        public val NO_WINDOW: Duration = 0.seconds
    }
}
