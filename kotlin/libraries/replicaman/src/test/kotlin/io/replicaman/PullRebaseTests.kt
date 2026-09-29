package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.testing.fixtureReviseRow
import io.replicaman.testing.fixtureSeedRow
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.HookOutcome
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

/**
 * A pulled row snapshot must never regress a row that still owes journal
 * ops: local = server + my unacked ops. Both shapes of the same hole:
 *
 *   1. the race — a local write lands WHILE a pull is on the wire; the
 *      answer carries the server's older state and used to overwrite the
 *      newer local row;
 *   2. the cold lane — `drainIfWarm` skips a cold bulk lane, the pull
 *      proceeds with pending bulk ops in the journal, same overwrite.
 */
class PullRebaseTests : ReplicaTestCase() {

    /**
     * The device-suite fixture publishes through the same materialization a
     * round uses: the server's fields land and an owed local edit stays on top.
     * KILL: save the revised base without materializing the row again.
     */
    @Test
    fun aFixtureRevisionLandsUnderTheLocalIntentItMeets() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport(), automaticallyPushWrites = false)
        store.fixtureSeedRow(Fixture.schema(), "notes", "n1",
            mapOf("title" to ReplicaValue.Str("seeded"), "rank" to ReplicaValue.Str("a")))

        engine.fixtureReviseRow("notes", "n1", mapOf("title" to ReplicaValue.Str("revised"), "rank" to ReplicaValue.Str("a")))
        assertEquals(ReplicaValue.Str("revised"), store.peekSnapshot("notes", "n1")?.data?.get("title"))

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("local")))
        engine.fixtureReviseRow("notes", "n1", mapOf("title" to ReplicaValue.Str("server"), "rank" to ReplicaValue.Str("b")))
        val row = store.peekSnapshot("notes", "n1")
        assertEquals(ReplicaValue.Str("local"), row?.data?.get("title"), "the owed edit stays on top")
        assertEquals(ReplicaValue.Str("b"), row?.data?.get("rank"), "the server's other field lands")
        assertEquals(1, store.peekPending().size)
    }

    /**
     * The shape: cook row `running` pushed → pull in
     * flight → `succeeded` written locally → pull answer says `running`.
     *
     * KILL: delete the `rebaseOwedWrites` call after `upsertSnapshot`.
     */
    @Test
    fun staleFrameDoesNotRegressARowWithAPendingWrite() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("running")))
        engine.drain()

        // The server answers with what it had when the request arrived —
        // BEFORE the local write below.
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "running")), cursor = "1:", more = false)
        )
        val hook = HookOutcome()
        transport.onPull {
            try {
                engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("succeeded")))
            } catch (error: Throwable) {
                hook.record(error)
            }
        }

        engine.pullOnce("user")
        assertNull(hook.failure, "the mid-pull write failed; the race this test stages never happened")

        assertEquals(
            "succeeded", store.peekSnapshot("notes", "n1")?.data?.get("title")?.string,
            "the pull's older server state overwrote a write the server has not seen yet"
        )
        assertEquals(1, store.peekPending().size, "the newer write is still owed — nothing may drop it")

        // The baseline, asserted where it discriminates: once nothing is owed,
        // the rebase must stop protecting the row and the server's frame wins.
        transport.onPull { }
        engine.drain()
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "theirs")), cursor = "2:", more = false)
        )
        engine.pullOnce("user")
        assertEquals("theirs", store.peekSnapshot("notes", "n1")?.data?.get("title")?.string)
    }

    /**
     * The same overwrite without a race: the lane is cold, the pull does not
     * drain, the pending write is in the journal when the frame lands.
     *
     * KILL: replay the owed patch's FULL row instead of its changed fields —
     * `rank` would revert to the client's copy.
     */
    @Test
    fun staleFrameDoesNotRegressAPendingWriteOnAColdLane() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow(
            "notes", "n1", null,
            mapOf("title" to ReplicaValue.Str("running"), "rank" to ReplicaValue.Str("a"))
        )
        engine.drain()

        transport.failPushes(true)
        engine.saveRow(
            "notes", "n1", null,
            mapOf("title" to ReplicaValue.Str("succeeded"), "rank" to ReplicaValue.Str("a"))
        )
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        transport.failPushes(false)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "running", "server-touched")), cursor = "1:", more = false
            )
        )
        engine.pullOnce("user")

        val row = store.peekSnapshot("notes", "n1")
        assertEquals("succeeded", row?.data?.get("title")?.string, "my unacked patch must ride on top of the server's row")
        assertEquals("server-touched", row?.data?.get("rank")?.string, "fields I did not touch take the server's value")
        assertEquals(1, store.peekPending().size)
    }

    /**
     * An owed CREATE is not replayed over the frame: ids are client-minted,
     * so the server having the row means the create landed and the frame is
     * the fuller truth.
     *
     * KILL: add `ROW_CREATE` to the `rebaseOwedWrites` switch — the pulled
     * row is masked by the client's create until the ack.
     */
    @Test
    fun anotherIncarnationDoesNotEraseAnUnprocessedBirth() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.failPushes(true)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("Before")))
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        transport.failPushes(false)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("n1", "Renamed elsewhere", "server")), cursor = "1:", more = false
            )
        )
        engine.pullOnce("user")

        val row = store.peekSnapshot("notes", "n1")
        assertEquals("Before", row?.data?.get("title")?.string)
        assertEquals(1, store.peekPending().size)
        transport.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "IdentityCollision") } }
        engine.drain()
        assertEquals("Renamed elsewhere", store.peekSnapshot("notes", "n1")?.data?.get("title")?.string)
        assertEquals("server", store.peekSnapshot("notes", "n1")?.data?.get("rank")?.string)
    }
}
