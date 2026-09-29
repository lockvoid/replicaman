package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.support.*
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.*

class SyncStatusTests : ReplicaTestCase() {
    @Test fun deliveryStagesStayUnsettledUntilCheckpointVisibility() = runBlocking {
        val store = Fixture.store()
        val wire = StubTransport()
        val engine = Fixture.engine(store = store, transport = wire)
        assertFalse(store.syncStatus().hasUnsettledWork)
        engine.createRow("notes", "n", null, mapOf("title" to ReplicaValue.Str("saved")))
        assertEquals(1, store.syncStatus().queuedOperations)
        assertNotNull(store.syncStatus().oldestIntentAt)

        store.write { store.freezeSubmissions(it, null, ReplicaEngine.MAX_OPS_PER_PUSH) }
        assertEquals(1, store.syncStatus().submittedGroups)
        engine.drain()
        assertTrue(store.peekPending().isEmpty())
        val accepted = store.syncStatus()
        assertEquals(0, accepted.submittedGroups)
        assertEquals(1, accepted.acceptedOperations)
        assertTrue(accepted.hasUnsettledWork)
        assertTrue(store.containsUnsettledOperation { it.rowId == "n" })

        wire.queuePull("user", ReplicaPullResponse(
            frames = listOf(Fixture.note("n", "saved")), cursor = "1:", more = false))
        engine.pullOnce("user")
        assertFalse(store.syncStatus().hasUnsettledWork)
        assertFalse(store.containsUnsettledOperation { it.rowId == "n" })
    }

    @Test fun corruptIntentCannotBeMistakenForNoWork() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.createRow("notes", "n", null, emptyMap())
        store.write { it.exec("UPDATE intents SET payload = '{broken'") }
        assertTrue(store.syncStatus().hasUnsettledWork)
        assertFailsWith<ReplicaError> { store.containsUnsettledOperation { false } }
        assertEquals(1, store.peekPending().size)
    }
}
