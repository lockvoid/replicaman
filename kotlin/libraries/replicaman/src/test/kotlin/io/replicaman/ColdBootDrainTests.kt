package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReleaseLedger
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.TestNote
import io.replicaman.support.blobGate
import io.replicaman.support.until
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The cold boot: a process starts over a store file that already exists and
 * already owes work. `openForColdBoot` is not `open(owner)` with a different
 * name: it takes no dispatcher hop (a returning user's grid renders on the
 * first frame), it binds only from nothing, it runs its OWN cursor heal,
 * and — the finding this suite exists to pin — unlike `open(owner)` it never
 * calls `unseal()`, so it never schedules a push for the journal it just
 * re-opened.
 *
 * Scope note: the ENGINE contract is graded here. The app-level wake that
 * makes the contract safe in production (`ReplicaHost.open` calling `nudge`)
 * belongs to the app lane and is deliberately not duplicated.
 */
class ColdBootDrainTests : ReplicaTestCase() {

    /** Everything a killed process leaves on disk for the next one. */
    private suspend fun writeOwedWorld(
        directory: File,
        owner: Long,
        rows: List<Pair<String, Map<String, ReplicaValue>>>,
        syncGates: List<SyncGate> = emptyList(),
    ): List<String> {
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(directory, transport, syncGates = syncGates)
        engine.open(owner)
        for ((id, data) in rows) engine.saveRow("notes", id, null, data)
        val owed = engine.pendingOps().map { it.id }
        // The process ends. The file stays; `close` never retires it.
        engine.close()
        return owed
    }

    private fun rawPendingIds(store: ReplicaStateStore): List<String> = store.read { db ->
        db.queryStrings("SELECT id FROM intents WHERE state IN ('draft', 'owed', 'frozen') ORDER BY rowid")
    }

    /**
     * The store's own header states the stake: losing the journal loses the
     * user's unsent work. Read RAW: `SELECT id FROM intents`, never
     * `pendingOps()`.
     *
     * KILL: `CREATE TABLE IF NOT EXISTS intents` → `CREATE TEMP TABLE …`.
     * Every in-process test stays green; the relaunch loses the user's
     * unsent work and only this test says so.
     */
    @Test
    fun theOwedJournalSurvivesTheProcessAndIsStillOwedAfterAColdBoot() = runTest {
        val directory = Fixture.directory()
        val owedBefore = writeOwedWorld(
            directory, 1,
            listOf(
                "n1" to mapOf("title" to ReplicaValue.Str("typed before the kill")),
                "n2" to mapOf("title" to ReplicaValue.Str("and this one too")),
            )
        )
        assertEquals(2, owedBefore.size)

        // A NEW engine — a new process, in every way the test can express.
        val reborn = Fixture.unopenedEngine(directory, StubTransport())
        reborn.openForColdBoot(1)

        val store = reborn.store
        assertNotNull(store, "the cold door bound nothing")
        assertEquals(
            owedBefore, rawPendingIds(store),
            "the relaunched process no longer owes the work the user did before the kill"
        )
        assertEquals(
            "typed before the kill", RowStream(reborn, TestNote).find("n1")?.title,
            "…and the returning user's world is readable with no dispatcher hop, on the first frame"
        )
    }

    /**
     * The cold door runs its OWN coherence heal — a separate call site from
     * the warm `bindStore` one. An empty store holding a warm cursor can
     * never heal: the cursor claims coverage the store does not hold.
     *
     * KILL: delete `healCursors(store)` from `openForColdBoot`. The warm-path
     * test stays green; only this one goes red. That asymmetry is the point.
     */
    @Test
    fun coldBootKeepsTheCursorOfAnEmptyCheckpoint() = runTest {
        val directory = Fixture.directory()
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(directory, transport)
        engine.open(1)
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = emptyList(), cursor = "10:", more = false
            )
        )
        engine.pullOnce("user")
        assertEquals("10:", engine.currentCursor())

        // The shape a half-finished wipe or a failed migration leaves behind.
        engine.close()

        val reborn = Fixture.unopenedEngine(directory, StubTransport())
        reborn.openForColdBoot(1)

        assertEquals("10:", reborn.currentCursor())
    }

    /**
     * THE FINDING. `open(owner)` ends in `unseal()`, which schedules a push
     * for every lane that owes work. `openForColdBoot` does not. So a relaunch
     * over an owed journal is SILENT until something wakes it — in production
     * that is the host's `nudge()`, which the cold path does not reach.
     *
     * Both halves are graded in ONE test against the SAME budget, so the
     * negative is proven by contrast with a positive that did fire, never by
     * elapsed time.
     *
     * KILL (either direction): add `unsealLocked()` to `openForColdBoot` — the
     * cold half flips; delete `unsealLocked()` from `open` — the warm half does.
     */
    @Test
    fun theColdDoorNeverSelfSchedulesWhileTheWarmDoorAlwaysDoes() = runTest {
        val coldDirectory = Fixture.directory()
        writeOwedWorld(coldDirectory, 1, listOf("n1" to mapOf("title" to ReplicaValue.Str("owed across the kill"))))
        val warmDirectory = Fixture.directory()
        writeOwedWorld(warmDirectory, 2, listOf("n1" to mapOf("title" to ReplicaValue.Str("owed across the kill"))))

        val coldTransport = StubTransport()
        val cold = Fixture.unopenedEngine(
            coldDirectory, coldTransport, automaticallyPushWrites = true
        )
        val warmTransport = StubTransport()
        val warm = Fixture.unopenedEngine(
            warmDirectory, warmTransport, automaticallyPushWrites = true
        )

        cold.openForColdBoot(1)
        warm.open(2)

        // The warm door's own delivery is the budget. When it has fired, a
        // scheduled push has had at least as long to reach the stub on the
        // cold side — `schedulePush` is one `yield()` away.
        until("the warm door never scheduled the journal it re-opened") {
            warmTransport.pushCount() == 1
        }
        assertEquals(
            0, coldTransport.pushCount(),
            "the cold door scheduled a push; if that becomes true, the host's nudge is dead weight"
        )

        // And the contract that makes the silence safe: ONE explicit drain —
        // what the host's doorbell ultimately calls — clears everything owed.
        cold.drain()
        assertEquals(1, coldTransport.pushCount())
        assertTrue(rawPendingIds(cold.store!!).isEmpty(), "the first wake must clear the whole owed journal")
    }

    /**
     * Bytes that landed while the process was gone: the cold boot asks the
     * holds again — after it binds, never on the first frame's path.
     *
     * KILL: `openForColdBoot` — drop `askHoldsAgain()`.
     */
    @Test
    fun aColdBootAsksEveryHoldAgainAfterItBinds() = runTest {
        val directory = Fixture.directory()
        writeOwedWorld(
            directory, 1,
            listOf("n1" to mapOf("title" to ReplicaValue.Str("a"), "blob" to ReplicaValue.Str("k1"))),
            syncGates = listOf(blobGate(ReleaseLedger()))
        )

        val transport = StubTransport()
        val reborn = Fixture.unopenedEngine(directory, transport, syncGates = listOf(blobGate(ReleaseLedger(listOf("k1")))))
        reborn.openForColdBoot(1)

        until("the cold boot never asked its holds again") { reborn.heldRows().isEmpty() }
        reborn.drain()
        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), sent.map { it.verb })
        assertEquals(mapOf("title" to ReplicaValue.Str("a"), "blob" to ReplicaValue.Str("k1")), sent.first().data)
    }

    /**
     * The cold door binds only from NOTHING: changing owners is a transition
     * and must go through `open(owner)`, where in-flight work is quiesced.
     *
     * KILL: drop the `bound == null` guard in `ReplicaBinding.bindIfUnbound`.
     * The second owner takes the process and this test goes red.
     */
    @Test
    fun aSecondColdBootCannotStealAnAlreadyBoundProcess() = runTest {
        val directory = Fixture.directory()
        writeOwedWorld(directory, 1, listOf("n1" to mapOf("title" to ReplicaValue.Str("first owner"))))
        writeOwedWorld(directory, 2, listOf("n2" to mapOf("title" to ReplicaValue.Str("second owner"))))

        val engine = Fixture.unopenedEngine(directory, StubTransport())
        engine.openForColdBoot(1)
        engine.openForColdBoot(2)

        assertEquals(1L, engine.owner, "the cold door re-bound a process that already had an owner")
        assertNull(
            RowStream(engine, TestNote).find("n2"),
            "…and served the other identity's rows"
        )
    }
}
