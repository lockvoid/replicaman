package io.replicaman

import io.replicaman.support.*
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.*

class RebirthTests : ReplicaTestCase() {
    @Test fun cancelledFirstBirthLeavesNoInventedPredecessor() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.createRow("notes", "n", null, emptyMap())
        assertFalse(engine.deleteRow("notes", "n"))
        engine.createRow("notes", "n", null, emptyMap())
        assertNull(store.peekPending().single().op().replaces)
    }

    @Test fun cancellingRecreationPreservesThePreviousLifetimesDelete() = runBlocking {
        val store = Fixture.store()
        val wire = StubTransport()
        val engine = Fixture.engine(store = store, transport = wire)
        engine.createRow("notes", "n", null, mapOf("title" to ReplicaValue.Str("first")))
        val first = store.peekPending().single().op()
        engine.drain()
        engine.deleteRow("notes", "n")
        engine.createRow("notes", "n", null, mapOf("title" to ReplicaValue.Str("cancelled")))
        assertFalse(engine.deleteRow("notes", "n"))
        engine.createRow("notes", "n", null, mapOf("title" to ReplicaValue.Str("third")))

        val operations = store.peekPending().map { it.op() }
        assertEquals(listOf(ReplicaOp.Verb.ROW_DELETE, ReplicaOp.Verb.ROW_CREATE), operations.map { it.verb })
        assertEquals(first.incarnation, operations.first().incarnation)
        assertEquals(first.incarnation, operations.last().replaces)
        assertNotEquals(first.incarnation, operations.last().incarnation)
        engine.drain()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_DELETE, ReplicaOp.Verb.ROW_CREATE),
            wire.pushedBatches().flatten().map { it.verb })
    }

    @Test fun heldRecreationKeepsItsPredecessorAcrossStoreReopen() = runBlocking {
        val directory = Fixture.directory()
        val wire = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.unopenedEngine(directory, wire, syncGates = listOf(blobGate(released)))
        engine.open(42)
        engine.createRow("notes", "n", null, emptyMap())
        engine.drain()
        val first = wire.pushedBatches().flatten().single()
        engine.deleteRow("notes", "n")
        engine.createRow("notes", "n", null, mapOf("blob" to ReplicaValue.Str("upload")))
        assertEquals(1, engine.heldRows().size)
        engine.close()

        val reopened = Fixture.unopenedEngine(directory, wire, syncGates = listOf(blobGate(released)))
        reopened.open(42)
        assertEquals(1, reopened.heldRows().size)
        released.landQuietly("upload")
        reopened.refreshSyncGates()
        reopened.drain()
        val birth = wire.pushedBatches().flatten().last()
        assertEquals(ReplicaOp.Verb.ROW_CREATE, birth.verb)
        assertEquals(first.incarnation, birth.replaces)
        assertNotNull(birth.replaces)
        reopened.close()
    }
}
