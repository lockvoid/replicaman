package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Retry and backoff graded from the RAW journal and from the engine's own
 * cold stamp — never through `pendingOps()`, which is the accessor the
 * engine wrote with.
 *
 * `coldWindow = 0` is what makes these sleep-free: the stamp is set to
 * "now", so the very next `isCold` check is already false. That is "past the
 * window the drain retries" stated as a value, not as a duration to wait out.
 */
class RawJournalRetryTests : ReplicaTestCase() {

    private data class RawEntry(
        val id: String,
        val lane: String,
        val parked: String?,
        val payload: String,
    )

    private fun rawJournal(store: ReplicaStateStore): List<RawEntry> = store.read { db ->
        db.query("SELECT id, lane, reason, payload FROM intents WHERE state <> 'accepted' ORDER BY rowid") { statement ->
            RawEntry(
                id = statement.getText(0),
                lane = statement.getText(1),
                parked = statement.textOrNull(2),
                payload = statement.getText(3)
            )
        }
    }

    @Test fun failedPrefixCoolsBothPrioritiesUntilExplicitRetry() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport, coldWindow = 60.seconds)
        engine.saveRow("notes", "import", null, mapOf("title" to ReplicaValue.Str("bulk")))
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "message", null, mapOf("title" to ReplicaValue.Str("typed")))
        }
        val before = rawJournal(store)
        transport.failPushes(true)
        assertFailsWith<ReplicaError.Transport> { engine.drain(ReplicaLane.BULK) }
        assertTrue(engine.isColdForTesting(ReplicaLane.BULK))
        assertTrue(engine.isColdForTesting(ReplicaLane.INTERACTIVE))
        transport.failPushes(false)
        engine.drainIfWarm()
        assertEquals(before, rawJournal(store))
        engine.drain()
        assertEquals(listOf("import", "message"), transport.pushedBatches().flatten().map { it.rowId })
        assertTrue(rawJournal(store).isEmpty())
    }

    /**
     * A transport failure is RETRYABLE, never a verdict: the entries keep
     * their ids, their lanes and their exact BYTES, so the retry is the same
     * request. Graded raw — a decode through `op()` would hide a payload the
     * engine had rewritten.
     */
    @Test
    fun aFailedDrainKeepsEveryByteAndTheSuccessfulRetryIsTheSameRequest() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport, coldWindow = Duration.ZERO)

        for (index in 0 until 3) {
            engine.saveRow("notes", "n$index", null, mapOf("title" to ReplicaValue.Str("t$index")))
        }
        val before = rawJournal(store)
        assertEquals(3, before.size)

        transport.failPushes(true)
        assertFailsWith<ReplicaError.Transport> { engine.drain(ReplicaLane.BULK) }

        assertEquals(
            before, rawJournal(store),
            "a severed wire rewrote the journal — ids, lanes or bytes moved when nothing was judged"
        )
        assertTrue(before.all { it.parked == null }, "no connection is not a refusal")

        // Zero cold window: the retry is admitted immediately, no sleep.
        assertFalse(engine.isColdForTesting(ReplicaLane.BULK))
        transport.failPushes(false)
        engine.drainIfWarm()

        val requests = transport.protocolFixture.requests(ReplicaEndpoint.PUSH).map { it["ops"] }
        assertEquals(2, requests.size)
        assertEquals(requests[0], requests[1], "the retry must re-present exactly the operations that failed")
        assertEquals(listOf("n0", "n1", "n2"), transport.pushedBatches().flatten().map { it.rowId })
        assertTrue(rawJournal(store).isEmpty())
    }

    /**
     * The sign-out flush is the one drain whose failure destroys work: what it
     * leaves behind, the retirement wipes. Incremental acks bound the loss to
     * one chunk. Graded raw, because this is the count that decides whether a
     * user's typed message survives.
     *
     * KILL: move the `applyVerdicts(…)` call out of the chunk loop. All 120
     * entries then stay owed and the assertion of 70 goes red.
     */
    @Test
    fun aMidFlushChunkDeathStrandsOnlyTheUnackedChunks() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        transport.failPushesAfter(1)
        val engine = Fixture.engine(store = store, transport = transport)

        for (index in 0 until 120) {
            engine.saveRow(
                "notes", String.format("n%03d", index), null,
                mapOf("title" to ReplicaValue.Str("t$index"))
            )
        }

        engine.seal()
        assertFailsWith<ReplicaError.Transport> { engine.sealAndDrain(transport) }

        val owed = rawJournal(store)
        assertEquals(
            70, owed.size,
            "chunk 1's 50 ops acked incrementally and left the journal; only the unsent 70 may remain"
        )
        assertTrue(owed.all { it.parked == null }, "a flush failure parks nothing — the wipe is what threatens it")
        assertTrue(owed.first().id.isNotEmpty())
        // The survivors are the TAIL: the first 50 are gone, in journal order.
        val survivingRows = store.read { db ->
            db.queryStrings(
                "SELECT json_extract(payload, '${'$'}.row_id') FROM intents WHERE state IN ('draft', 'owed', 'frozen') ORDER BY rowid LIMIT 1"
            )
        }
        assertEquals(listOf("n050"), survivingRows, "the acked prefix must be the prefix, not an arbitrary 50")
    }
}
