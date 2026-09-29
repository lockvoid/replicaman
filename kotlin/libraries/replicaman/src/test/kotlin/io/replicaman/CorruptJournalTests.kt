package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertFails
import kotlin.test.assertEquals
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.*

class CorruptJournalTests : ReplicaTestCase() {
    @Test fun unreadableJournalCannotProveLocalBlobsUnused() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.saveRow("notes", "n", null, mapOf("blob" to ReplicaValue.Str("local:keep")))
        store.write { db -> db.prepare("UPDATE intents SET payload = '{broken'").use { it.step() } }
        assertFails { engine.pendingFieldStrings("notes", listOf("blob")) }
        assertFails { engine.pendingRowIds("notes") }
        assertEquals(1, store.peekPending().size)
    }
    @Test fun unreadableRollbackImageRefusesVerdictAndRetainsPendingWrite() = runTest {
        val store = Fixture.store()
        val wire = StubTransport()
        val engine = Fixture.engine(store = store, transport = wire)
        engine.saveRow("notes", "n", null, mapOf("title" to ReplicaValue.Str("original")))
        engine.drain()
        engine.saveRow("notes", "n", null, mapOf("title" to ReplicaValue.Str("pending")))
        store.write { db -> db.prepare("UPDATE intents SET preimage = '{broken' WHERE state <> 'accepted'").use { it.step() } }
        wire.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "denied") } }
        assertFails { engine.drain() }
        assertEquals(1, store.peekPending().size)
        assertEquals(0, store.peekParked().size)
        assertEquals(ReplicaValue.Str("pending"), store.peekSnapshot("notes", "n")?.data?.get("title"))
    }
}
