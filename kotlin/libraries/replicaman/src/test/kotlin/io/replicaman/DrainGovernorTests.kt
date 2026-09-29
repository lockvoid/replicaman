package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.HookOutcome
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The governor on the drain loop.
 *
 * `performDrain` drains to QUIESCENCE — one flight loops select→push until a
 * pass finds nothing owed — because the single-flight join makes a second
 * drain impossible, so the flight itself must not leave work behind. That
 * loop needs a stop: work that keeps arriving while the flight is on the wire
 * would otherwise spin it forever.
 */
class DrainGovernorTests : ReplicaTestCase() {

    /**
     * Every push brings exactly ONE more row — a write landing while its
     * predecessor is on the wire, the shape of a gate releasing one row per
     * network round-trip. The write rides `onPush`, so it happens provably
     * inside the flight: no timing, no sleep.
     *
     * KILL: delete the `if (passes >= MAX_DRAIN_PASSES) break` in
     * `performDrain`. The first drain then swallows all 20 rows and `owed
     * after the first drain` goes red.
     */
    @Test
    fun workThatKeepsArrivingIsStoppedByThePassCap() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val failure = HookOutcome()

        engine.saveRow("notes", "r00", null, mapOf("title" to ReplicaValue.Str("t0")))
        transport.onPush { ops ->
            val index = ops.lastOrNull()?.rowId?.drop(1)?.toIntOrNull() ?: return@onPush
            if (index >= 19) return@onPush
            try {
                engine.saveRow("notes", String.format("r%02d", index + 1), null, mapOf("title" to ReplicaValue.Str("t${index + 1}")))
            } catch (error: Throwable) {
                failure.record(error)
            }
        }

        engine.drain()

        fun owed(): List<String> = store.read { db ->
            db.queryStrings(
                "SELECT json_extract(payload, '${'$'}.row_id') FROM intents WHERE state IN ('draft', 'owed', 'frozen') ORDER BY rowid"
            )
        }

        // `MAX_DRAIN_PASSES` is 16, one row per pass.
        assertNull(failure.failure)
        assertEquals(16, transport.pushCount(), "the flight must stop at the pass cap, not run to empty")
        assertEquals(listOf("r16"), owed(), "what the cap left behind must stay owed — capping may delay work, never drop it")

        // And the remainder is ordinary work for the next drain.
        engine.drain()
        assertEquals(20, transport.pushCount())
        assertTrue(owed().isEmpty())
    }
}
