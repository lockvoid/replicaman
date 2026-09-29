package io.replicaman

import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubCodec
import io.replicaman.support.StubTransport
import io.replicaman.support.allSnapshots
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import io.replicaman.testing.ProtocolFixture
import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Protocol 2 pull rounds: answers with `more` are staged, the answer that ends the
 * round publishes everything at once, staging survives the process, and a
 * refused cursor starts a baseline.
 */
class PullRoundTests : ReplicaTestCase() {
    private fun staged(store: ReplicaStateStore): Pair<String?, Int> = store.read { db ->
        db.queryString("SELECT cursor FROM downloads WHERE shard = 'user'") to
            (db.queryLong("SELECT COUNT(*) FROM download_pages WHERE shard = 'user'") ?: 0).toInt()
    }

    private fun titles(store: ReplicaStateStore): Map<String, String?> =
        store.allSnapshots().filter { it.stream == "notes" }.associate { it.rowId to it.data["title"]?.string }

    private suspend fun pullCursors(transport: StubTransport): List<String?> =
        transport.events().filterIsInstance<StubTransport.Event.Pull>().map { it.cursor }

    /** KILL: keep sending `dataset: null` after the first answer named it. */
    @Test fun theFirstPullNamesNoDatasetAndLaterRequestsCarryTheOneItLearned() = runBlocking<Unit> {
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(Fixture.directory(), transport)
        engine.open(Fixture.OWNER)

        engine.pullOnce()
        engine.pullOnce()

        assertEquals(listOf(ReplicaValue.Null, ReplicaValue.Str(ProtocolFixture.DATASET)),
            transport.protocolFixture.requests(ReplicaEndpoint.PULL).map { it["dataset"] })
    }

    /** KILL: handle `DatasetChanged` like a refused cursor — the store drops its cursor and rebuilds onto a restored history. */
    @Test fun anotherDatasetStopsSynchronizationAndPreservesLocalBytes() = runBlocking<Unit> {
        val path = Fixture.path()
        val first = Fixture.engine(store = Fixture.storeAt(path), transport = StubTransport().apply {
            queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "server")), "c1", more = false))
        })
        first.pullOnce()
        first.close()
        val store = Fixture.storeAt(path)
        val engine = Fixture.engine(store = store, transport = StubTransport(dataset = "restored"))

        assertEquals("DatasetChanged", assertFailsWith<ReplicaError.Protocol> { engine.pullUntilCaughtUp() }.code)
        assertEquals(mapOf("n1" to "server"), titles(store))
        assertEquals("c1", engine.currentCursor())

        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("unsent")))
        val before = titles(store) to store.peekPending().map { it.id to String(it.payload) }
        assertEquals("DatasetChanged", assertFailsWith<ReplicaError.Protocol> { engine.drain() }.code)
        assertEquals(before, titles(store) to store.peekPending().map { it.id to String(it.payload) })
        assertEquals("c1", engine.currentCursor())
    }

    /** KILL: take the dataset from an answer without comparing it — a pull from another history publishes. */
    @Test fun anAnswerFromAnotherDatasetPublishesNothing() = runBlocking<Unit> {
        val store = Fixture.store()
        val wire = StubTransport(dataset = "restored")
        val transport = object : ReplicaTransport {
            override suspend fun exchange(endpoint: ReplicaEndpoint, body: ByteArray): ByteArray {
                val request = (ReplicaProtocol.decode(body) as ReplicaValue.Obj).fields
                return wire.exchange(endpoint, ReplicaJSON.encodeToBytes(ReplicaValue.Obj(request + ("dataset" to ReplicaValue.Str("restored")))))
            }
        }
        wire.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "foreign")), "c1", more = false))
        val engine = ReplicaEngine(store = store, owner = Fixture.OWNER, transport = transport, schema = Fixture.schema(),
            codecs = listOf(StubCodec()), automaticallyPushWrites = false)

        assertEquals("DatasetChanged", assertFailsWith<ReplicaError.Protocol> { engine.pullOnce() }.code)
        assertNull(store.peekSnapshot("notes", "n1"))
        assertNull(engine.currentCursor())
    }

    /** KILL: publish every answer as it arrives — a half round becomes visible. */
    @Test fun aRoundStagesEveryAnswerAndPublishesOnlyWithTheLast() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "one")), "c1", more = true))
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n2", "two")), "c2", more = true))
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n3", "three")), "c3", more = false))

        assertEquals(0, engine.pullOnce())
        assertEquals(0, engine.pullOnce())
        assertTrue(store.allSnapshots().isEmpty(), "a staged answer is not visible")
        assertNull(engine.currentCursor())
        assertEquals("c2" to 2, staged(store))

        assertEquals(3, engine.pullOnce())
        assertEquals(mapOf("n1" to "one", "n2" to "two", "n3" to "three"), titles(store))
        assertEquals("c3", engine.currentCursor())
        assertEquals(null to 0, staged(store))
        assertEquals(listOf(null, "c1", "c2"), pullCursors(transport))
    }

    /** KILL: keep the round in memory — process death throws away what was already downloaded. */
    @Test fun aStagedRoundResumesInTheNextProcessFromItsLastCursor() = runBlocking<Unit> {
        val path = Fixture.path()
        val transport = StubTransport()
        val first = Fixture.engine(store = Fixture.storeAt(path), transport = transport)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "one")), "c1", more = true))
        first.pullOnce()
        first.close()

        val store = Fixture.storeAt(path)
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n2", "two")), "c2", more = false))
        assertEquals(2, engine.pullOnce())

        assertEquals(listOf(null, "c1"), pullCursors(transport))
        assertEquals(mapOf("n1" to "one", "n2" to "two"), titles(store))
        assertEquals("c2", engine.currentCursor())
    }

    /** KILL: retry a refused cursor — or continue the staged round past it. */
    @Test fun aRefusedCursorDiscardsStagingAndStartsABaseline() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "one"), Fixture.note("n2", "two")), "c1", more = false))
        engine.pullOnce()
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n3", "staged")), "c2", more = true))
        engine.pullOnce()
        transport.refuseNextPull("user", "CursorInvalid")

        assertEquals(0, engine.pullOnce())
        assertEquals(null to 0, staged(store))
        assertNull(engine.currentCursor())
        assertEquals(mapOf("n1" to "one", "n2" to "two"), titles(store), "a refused cursor changes no published row")

        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n2", "two")), "c9", more = false))
        engine.pullUntilCaughtUp(listOf("user"))

        assertEquals(listOf(null, "c1", "c2", null), pullCursors(transport))
        assertEquals(mapOf("n2" to "two"), titles(store), "the baseline replaced the shard's base")
        assertEquals("c9", engine.currentCursor())
    }

    /** KILL: admit an answer whose round another request moved meanwhile — two overlapping requests publish as one round. */
    @Test fun anAnswerIsDiscardedWhenAnotherRequestMovedItsRound() = runBlocking<Unit> {
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(Fixture.directory(), transport)
        engine.open(Fixture.OWNER)
        val store = requireNotNull(engine.store)
        val held = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val calls = AtomicInteger()
        transport.onPull {
            if (calls.getAndIncrement() == 0) {
                held.complete(Unit)
                withTimeout(3_000) { release.await() }
            }
        }
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "learned")), "c1", more = true))
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n2", "overlapping")), "c2", more = false))
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n3", "continued")), "c3", more = false))

        val pull = async { engine.pullOnce() }
        withTimeout(3_000) { held.await() }
        engine.saveRow("notes", "n0", null, mapOf("title" to ReplicaValue.Str("offline")))
        engine.drain()
        release.complete(Unit)

        assertEquals(0, withTimeout(3_000) { pull.await() })
        assertEquals("c1" to 1, staged(store))
        assertNull(store.peekSnapshot("notes", "n2"))
        assertEquals(2, engine.pullOnce())
        assertEquals(mapOf("n0" to "offline", "n1" to "learned", "n3" to "continued"), titles(store))
        assertEquals(listOf(null, null, "c1"), pullCursors(transport))
    }

    /** KILL: remove every overlay of the shard at publication — a round older than the acceptance erases it. */
    @Test fun anAcceptedWriteOutlivesARoundThatStartedBeforeItsAcceptance() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "server")), "c1", more = false))
        engine.pullOnce()
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "older")), "c2", more = true))
        engine.pullOnce()
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        engine.drain()
        assertTrue(store.peekPending().isEmpty())

        transport.queuePull("user", ReplicaPullResponse(emptyList(), "c3", more = false))
        engine.pullOnce()
        assertEquals(mapOf("n1" to "mine"), titles(store))
        assertEquals(1, store.syncStatus().acceptedOperations, "the round began before the acceptance")

        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "mine")), "c4", more = false))
        engine.pullOnce()
        assertEquals(mapOf("n1" to "mine"), titles(store))
        assertEquals(0, store.syncStatus().acceptedOperations, "a round that began after the acceptance covers it")
    }

    /** KILL: drop `sequence <= visible` from the publication's removal — a round erases an acceptance it cannot show. */
    @Test fun aRoundRemovesExactlyTheAcceptedIntentsItsVisibleCovers() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        fun accepted() = store.read { db ->
            db.query("SELECT id, sequence FROM intents WHERE state = 'accepted' ORDER BY sequence") { it.getText(0) to it.getLong(1) }
        }
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("first")))
        val first = store.peekPending().single().id
        engine.drain()
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "first")), "c1", more = true))
        engine.pullOnce()
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("second")))
        val second = store.peekPending().single().id
        engine.drain()
        assertEquals(listOf(first to 1L, second to 2L), accepted(), "each intent is accepted in place, keeping its sequence")

        transport.queuePull("user", ReplicaPullResponse(emptyList(), "c2", more = false))
        engine.pullOnce()
        assertEquals(listOf(second to 2L), accepted(), "the round began after the first acceptance, before the second")
        assertEquals(mapOf("n1" to "first", "n2" to "second"), titles(store))

        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n2", "second")), "c3", more = false))
        engine.pullOnce()
        assertEquals(emptyList(), accepted())
        assertEquals(mapOf("n1" to "first", "n2" to "second"), titles(store))
    }
}
