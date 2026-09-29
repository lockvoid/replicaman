package io.replicaman

import io.replicaman.support.*
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.*

class ReferenceTests : ReplicaTestCase() {
    private val child = "pmck/Element/é/offline"
    private val expected = "derived:eeaecc490e5bd06da25475507ce21c4cefd406e00f06c9f3bdd4ed03e2eb9720"

    private fun schema() = ReplicaSchema(listOf(
        ReplicaStreamSpec("parents", ReplicaStreamSpec.Lane.ROW),
        ReplicaStreamSpec("children", ReplicaStreamSpec.Lane.ROW, references = listOf(
            ReplicaReferenceSpec("parent", "parents", keySegment = 2, keyPrefix = "pmck/Element/")
        ), lifetimeFrom = "parent")
    ), namespace = "refs")

    @Test fun birthAndPatchCarryTheParentLifetimeAndCrossLanguageIdentity() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema())
        engine.createRow("parents", "é", null, emptyMap())
        store.write { store.setIncarnation(it, "parents", "é", "user", "parent-life") }
        engine.createRow("children", child, null, mapOf("value" to ReplicaValue.Str("first")))
        engine.updateRow("children", child, null, mapOf("value" to ReplicaValue.Str("second")))
        val operations = store.peekPending().map { it.op() }.filter { it.stream == "children" }
        assertEquals(2, operations.size)
        for (op in operations) {
            assertEquals(expected, op.incarnation)
            assertEquals(listOf(ReplicaReference("parent", "parents", "é", "parent-life")), op.references)
        }
    }

    @Test fun aRefusalOfARebornDerivedLifetimeIsKeptWhileItsEarlierBirthRefusalWaits() = runBlocking {
        val store = Fixture.store()
        val transport = StubTransport()
        val schema = ReplicaSchema(listOf(
            ReplicaStreamSpec("parents", ReplicaStreamSpec.Lane.ROW),
            ReplicaStreamSpec("children", ReplicaStreamSpec.Lane.ROW, references = listOf(
                ReplicaReferenceSpec("parent", "parents", keySegment = 2, keyPrefix = "pmck/Element/")
            ), lifetimeFrom = "parent")
        ))
        val engine = Fixture.engine(store = store, transport = transport, schema = schema)
        engine.createRow("parents", "é", null, emptyMap())
        store.write { store.setIncarnation(it, "parents", "é", "user", "parent-life") }
        transport.scriptPush { ops ->
            ops.map {
                if (it.stream == "children") ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "first birth refused")
                else ReplicaVerdict(it.id, ReplicaVerdict.Outcome.ACCEPTED)
            }
        }
        engine.createRow("children", child, null, mapOf("value" to ReplicaValue.Str("first")))
        engine.drain()

        transport.scriptPush { ops ->
            ops.map {
                if (it.verb == ReplicaOp.Verb.ROW_PATCH) ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "patch refused")
                else ReplicaVerdict(it.id, ReplicaVerdict.Outcome.ACCEPTED)
            }
        }
        engine.createRow("children", child, null, mapOf("value" to ReplicaValue.Str("again")))
        engine.drain()
        engine.updateRow("children", child, null, mapOf("value" to ReplicaValue.Str("edited")))
        engine.drain()

        assertEquals(listOf("first birth refused", "patch refused"), engine.parkedOps().mapNotNull { it.parked }.sorted())
    }

    @Test fun heldOrdinaryChildRetainsItsParentLifetimeWhenReleased() = runBlocking {
        val store = Fixture.store()
        val ledger = ReleaseLedger()
        val schema = ReplicaSchema(listOf(
            ReplicaStreamSpec("parents", ReplicaStreamSpec.Lane.ROW),
            ReplicaStreamSpec("children", ReplicaStreamSpec.Lane.ROW, references = listOf(
                ReplicaReferenceSpec("parent", "parents", field = "parentId")
            ))
        ))
        val gate = TestGate("children") {
            if (ledger.contains("child")) SyncVerdict.Push else SyncVerdict.Gate("upload pending")
        }
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema, syncGates = listOf(gate))
        engine.createRow("parents", "parent", null, emptyMap())
        engine.createRow("children", "child", null,
            mapOf("parentId" to ReplicaValue.Str("parent"), "title" to ReplicaValue.Str("keep")))
        val holds = engine.heldRows()
        assertEquals(1, holds.size)
        val pending = store.peekPending()

        // Checkpoints can replace a parent while media upload holds its child.
        store.write { store.setIncarnation(it, "parents", "parent", "user", "replacement") }
        ledger.landQuietly("child")
        assertFailsWith<ReplicaError.Storage> { engine.refreshSyncGates() }

        assertEquals(holds, engine.heldRows())
        assertEquals(pending, store.peekPending())
        assertEquals(ReplicaValue.Str("keep"), store.peekSnapshot("children", "child")?.data?.get("title"))
    }

    @Test fun heldChildBelongsToItsParentsOwnerOnlyWhileThatLifetimeExists() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema(),
            syncGates = listOf(TestGate("children") { SyncVerdict.Gate("upload") }))
        engine.createRow("parents", "é", null, mapOf("projectId" to ReplicaValue.Str("project")))
        engine.createRow("children", child, null, emptyMap())
        assertEquals(mapOf("children" to listOf(child)), store.heldRowIds("project", listOf("projects"), "projectId"))
        assertTrue(store.heldRowIds("unrelated", listOf("projects"), "projectId").isEmpty())

        store.write { store.setIncarnation(it, "parents", "é", "user", "replacement") }
        assertTrue(store.heldRowIds("project", listOf("projects"), "projectId").isEmpty(),
            "A new parent must not claim or discard an older lifetime's held child")
    }

    @Test fun rowLifetimesNameTheCurrentLifetimeOfEveryRowAFieldSelects() = runBlocking<Unit> {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema())
        engine.createRow("parents", "é", null, mapOf("projectId" to ReplicaValue.Str("project")))
        engine.createRow("parents", "other", null, mapOf("projectId" to ReplicaValue.Str("elsewhere")))
        store.write { store.setIncarnation(it, "parents", "é", "user", "first-life") }
        assertEquals(mapOf("é" to "first-life"), store.rowLifetimes("parents", "projectId", "project"))
        assertTrue(store.rowLifetimes("parents", "projectId", "nobody").isEmpty())

        store.write { store.setIncarnation(it, "parents", "é", "user", "replacement") }
        assertEquals(mapOf("é" to "replacement"), store.rowLifetimes("parents", "projectId", "project"))

        store.write { it.exec("DELETE FROM entities WHERE stream = 'parents' AND row_id = 'é'") }
        assertFailsWith<ReplicaError.Storage> { store.rowLifetimes("parents", "projectId", "project") }
    }

    @Test fun deletingHeldAndDeliveredRowsRetainsRequiredFieldReferences() = runBlocking {
        for (heldBirth in listOf(true, false)) {
            val store = Fixture.store()
            val ledger = ReleaseLedger()
            val schema = ReplicaSchema(listOf(
                ReplicaStreamSpec("parents", ReplicaStreamSpec.Lane.ROW),
                ReplicaStreamSpec("children", ReplicaStreamSpec.Lane.ROW, references = listOf(
                    ReplicaReferenceSpec("parent", "parents", field = "parentId")
                ))
            ))
            val gate = TestGate("children") { change ->
                if (ledger.contains("release")) SyncVerdict.Push
                else if (heldBirth || change.kind == SyncChange.Kind.DELETE) SyncVerdict.Gate("waiting")
                else SyncVerdict.Push
            }
            val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema, syncGates = listOf(gate))
            engine.createRow("parents", "parent", null, emptyMap())
            engine.createRow("children", "child", null, mapOf("parentId" to ReplicaValue.Str("parent")))
            if (!heldBirth) engine.drain()

            engine.deleteRow("children", "child")
            assertNull(store.peekSnapshot("children", "child"))
            ledger.landQuietly("release")
            engine.refreshSyncGates()
            assertTrue(engine.heldRows().isEmpty())
            val deletes = store.peekPending().map { it.op() }.filter { it.stream == "children" }
            if (heldBirth) assertTrue(deletes.isEmpty())
            else {
                assertEquals(1, deletes.size)
                assertEquals(ReplicaOp.Verb.ROW_DELETE, deletes.single().verb)
                assertEquals("parent", deletes.single().references.single().id)
            }
        }
    }

    @Test fun missingParentRollsBackTheChildAndItsJournal() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema())
        assertFailsWith<ReplicaError.Storage> { engine.createRow("children", child, null, emptyMap()) }
        assertNull(store.peekSnapshot("children", child))
        assertTrue(store.peekPending().isEmpty())
    }

    @Test fun replacementParentCannotRetargetExistingChildAuthoring() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema())
        engine.createRow("parents", "é", null, emptyMap())
        engine.createRow("children", child, null, mapOf("value" to ReplicaValue.Str("keep")))
        val pending = store.peekPending()
        store.write { store.setIncarnation(it, "parents", "é", "user", "replacement") }
        assertFailsWith<ReplicaError.Storage> {
            engine.updateRow("children", child, null, mapOf("value" to ReplicaValue.Str("wrong parent")))
        }
        assertEquals(pending, store.peekPending())
        assertEquals(ReplicaValue.Str("keep"), store.peekSnapshot("children", child)?.data?.get("value"))
    }
}
