package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.support.*
import java.util.Base64
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import kotlin.test.*

/** Domain commands refresh through the same pull rounds as ordinary sync. */
class CommitTests : ReplicaTestCase() {
    private fun hint(dataset: String = "fixture-dataset", shards: List<String> = listOf("user")): String =
        Base64.getEncoder().encodeToString(ReplicaJSON.encodeToBytes(ReplicaValue.Obj(mapOf(
            "protocol" to ReplicaValue.Integer(2), "namespace" to ReplicaValue.Str("replicaman"),
            "schema" to ReplicaValue.Integer(1), "dataset" to ReplicaValue.Str(dataset),
            "shards" to ReplicaValue.Arr(shards.map(ReplicaValue::Str))
        ))))

    @Test fun commandRefreshPublishesTheCheckpointAndItsCursorTogether() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val session = engine.commitSession()
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "committed")), cursor = "after-command", more = false))

        engine.apply(hint(), session)

        assertEquals(ReplicaValue.Str("committed"), store.peekSnapshot("notes", "n1")?.data?.get("title"))
        assertTrue(store.peekPending().isEmpty())
        assertEquals("after-command", engine.currentCursor())
    }

    @Test fun commandRefreshPreservesOfflineAuthoring() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "before", "a")), cursor = "before", more = false))
        engine.pullOnce()
        val session = engine.commitSession()
        transport.failPushes(true)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("offline")))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "remote", "b")), cursor = "after", more = false))

        engine.apply(hint(), session)

        assertEquals(mapOf("title" to ReplicaValue.Str("offline"), "rank" to ReplicaValue.Str("b")), store.peekSnapshot("notes", "n1")?.data)
        assertEquals(1, store.peekPending().size)
        assertNotNull(engine.health.failure.value)
    }

    /** A staged round continues to the current heads, so it publishes what the command committed after it began. */
    @Test fun commandRefreshContinuesAPartialRoundToTheCurrentHeads() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(frames = emptyList(), cursor = "baseline", more = false))
        engine.pullOnce()
        val session = engine.commitSession()
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "old")), cursor = "page-one", more = true))
        engine.pullOnce()
        assertNull(store.peekSnapshot("notes", "n1"))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "new"), Fixture.note("n2", "new")), cursor = "new-cut", more = false))

        engine.apply(hint(), session)

        assertEquals(StubTransport.Event.Pull("user", "page-one"), transport.events().last())
        assertEquals(ReplicaValue.Str("new"), store.peekSnapshot("notes", "n1")?.data?.get("title"))
        assertEquals(ReplicaValue.Str("new"), store.peekSnapshot("notes", "n2")?.data?.get("title"))
        assertEquals("new-cut", engine.currentCursor())
    }

    /** KILL: drop `session.engine != commitIdentity` from `apply`. */
    @Test fun commandSessionCannotCrossEngines() = runBlocking<Unit> {
        val session = Fixture.engine(store = Fixture.store(), transport = StubTransport()).commitSession()
        val targetStore = Fixture.store()
        val target = Fixture.engine(store = targetStore, transport = StubTransport())

        assertFailsWith<ReplicaError.StaleCommit> { target.apply(hint(), session) }
        assertTrue(targetStore.allSnapshots().isEmpty())
    }

    /** KILL: drop `session.binding != binding.snapshot().second` from `apply`. */
    @Test fun commandSessionCannotCrossOwners() = runBlocking<Unit> {
        val engine = Fixture.unopenedEngine(Fixture.directory(), StubTransport())
        engine.open(42)
        val session = engine.commitSession()
        engine.close()
        engine.open(99)

        assertFailsWith<ReplicaError.StaleCommit> { engine.apply(hint(), session) }
    }

    @Test fun invalidDatasetAndUnknownShardsCannotChangePublishedState() = runBlocking<Unit> {
        for (invalid in listOf(hint(dataset = "restored"), hint(shards = listOf("private")), hint(shards = listOf("user", "user")))) {
            val store = Fixture.store()
            val engine = Fixture.engine(store = store, transport = StubTransport())
            assertFails { engine.apply(invalid, engine.commitSession()) }
            assertTrue(store.allSnapshots().isEmpty())
            assertNull(engine.currentCursor())
        }
    }

    @Test fun resetDuringBootstrapRefusesTheOldResponseEvenWithNoCursor() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val reached = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "stale")), cursor = "old-bootstrap", more = false))
        transport.onPull {
            reached.complete(Unit)
            withTimeout(3_000) { release.await() }
        }
        val pull = async { engine.pullOnce() }
        withTimeout(3_000) { reached.await() }
        engine.resetCursors()
        release.complete(Unit)
        withTimeout(3_000) { pull.await() }

        assertNull(store.peekSnapshot("notes", "n1"))
        assertNull(engine.currentCursor())
        transport.onPull {}
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "fresh")), cursor = "fresh-bootstrap", more = false))
        engine.pullOnce()
        assertEquals(ReplicaValue.Str("fresh"), store.peekSnapshot("notes", "n1")?.data?.get("title"))
    }
}
