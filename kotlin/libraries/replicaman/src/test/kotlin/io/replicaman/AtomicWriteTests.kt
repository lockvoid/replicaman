package io.replicaman

import io.replicaman.support.*
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.*

class AtomicWriteTests : ReplicaTestCase() {
    @Test fun groupCanCreateAParentAndChildAndEditTheParentBeforeFreezing() = runBlocking {
        val store = Fixture.store()
        val schema = ReplicaSchema(listOf(
            ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, references = listOf(
                ReplicaReferenceSpec("parent", "notes", keySegment = 1, keyPrefix = "child/", optional = true)
            )),
        ))
        val wire = StubTransport()
        val engine = Fixture.engine(store, transport = wire, schema = schema)
        engine.writeAtomically { tx ->
            tx.create(TestNote, TestNote("parent", "First"))
            tx.create(TestNote, TestNote("child/parent", "Child"))
            tx.update(TestNote, "parent") { it.copy(title = "Last") }
        }
        val operations = store.read { store.frozenSubmissions(it) }.single().operations.map(ReplicaOp::fromValue)
        assertEquals(1, operations.map { it.group }.toSet().size)
        assertNotNull(operations.first().group)
        assertEquals(3, operations.map { it.id }.toSet().size)
        assertEquals(listOf("parent", "child/parent", "parent"), operations.map { it.rowId })
        assertEquals(operations[0].incarnation, operations[2].incarnation)
        assertEquals(operations[0].incarnation, operations[1].references.single().incarnation)
        assertEquals(ReplicaValue.Str("First"), operations[0].data?.get("title"))
        assertEquals(ReplicaValue.Str("Last"), operations[2].data?.get("title"))

        engine.drain()
        assertTrue(store.peekPending().isEmpty())
        assertEquals(ReplicaValue.Str("Last"), store.peekSnapshot("notes", "parent")?.data?.get("title"))
        assertEquals(ReplicaValue.Str("Child"), store.peekSnapshot("notes", "child/parent")?.data?.get("title"))
    }

    @Test fun groupLargerThanTransportBatchRemainsOneSubmissionAfterReopen() = runBlocking {
        val directory = Fixture.directory()
        val wire = StubTransport()
        val engine = Fixture.unopenedEngine(directory, wire)
        engine.open(42)
        engine.writeAtomically { tx ->
            repeat(60) { tx.create(TestNote, TestNote("n-$it", "group")) }
        }
        val store = assertNotNull(engine.store)
        val saved = store.read { store.frozenSubmissions(it) }.single()
        assertEquals(60, saved.entries.size)
        engine.close()

        val reopened = Fixture.unopenedEngine(directory, wire)
        reopened.open(42)
        val reopenedStore = assertNotNull(reopened.store)
        val retained = reopenedStore.read { reopenedStore.frozenSubmissions(it) }.single()
        assertEquals(saved.operations, retained.operations)
        assertEquals(saved.sequence, retained.sequence)
        reopened.drain()
        assertEquals((0 until 60).map { "n-$it" }, wire.pushedBatches().flatten().map { it.rowId })
        assertTrue(reopenedStore.peekPending().isEmpty())
        reopened.close()
    }

    @Test fun oneHeldOrDiscardedMemberRollsBackEveryMember() {
        for (discard in listOf(false, true)) {
            val store = Fixture.store()
            val gate = object : SyncGate {
                override val id = "member"
                override val stream = "notes"
                override fun judge(change: SyncChange): SyncVerdict = when {
                    change.rowId != "blocked" -> SyncVerdict.Push
                    discard -> SyncVerdict.Discard
                    else -> SyncVerdict.Gate("not uploaded")
                }
            }
            val engine = Fixture.engine(store, transport = StubTransport(), syncGates = listOf(gate))
            assertFailsWith<ReplicaError.AtomicWriteBlocked> {
                engine.writeAtomically { tx ->
                    tx.create(TestNote, TestNote("ready"))
                    tx.create(TestNote, TestNote("blocked"))
                }
            }
            assertNull(store.peekSnapshot("notes", "ready"))
            assertNull(store.peekSnapshot("notes", "blocked"))
            assertTrue(store.peekPending().isEmpty())
            assertTrue(engine.heldRows().isEmpty())
            assertEquals(1L, store.read { store.meta(it).nextSequence })
        }
    }

    @Test fun unsubmittedDependencyAndOversizedActionLeaveOriginalWorkIntact() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store, transport = StubTransport())
        engine.createRow("notes", "earlier", null, mapOf("title" to ReplicaValue.Str("original")))
        val original = store.peekPending().single().payload
        assertFailsWith<ReplicaError.AtomicWriteBlocked> {
            engine.writeAtomically { tx ->
                tx.create(TestNote, TestNote("new"))
                tx.update(TestNote, "earlier") { it.copy(title = "overtaken") }
            }
        }
        assertContentEquals(original, store.peekPending().single().payload)
        assertNull(store.peekSnapshot("notes", "new"))
        assertFailsWith<ReplicaError.AtomicWriteBlocked> {
            engine.writeAtomically { tx -> repeat(101) { tx.create(TestNote, TestNote("limit-$it")) } }
        }
        assertContentEquals(original, store.peekPending().single().payload)
        assertNull(store.peekSnapshot("notes", "limit-0"))
    }

    @Test fun refusalRevertsWholeActionAndRetainsBothReasons() = runBlocking {
        val store = Fixture.store()
        val wire = StubTransport()
        wire.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "action refused") } }
        val engine = Fixture.engine(store, transport = wire)
        engine.writeAtomically { tx ->
            tx.create(TestNote, TestNote("a"))
            tx.create(TestNote, TestNote("b"))
        }
        engine.drain()
        assertNull(store.peekSnapshot("notes", "a"))
        assertNull(store.peekSnapshot("notes", "b"))
        assertEquals(listOf("action refused", "action refused"), store.parkedOps().map { it.parked })
        assertEquals(listOf("action refused", "action refused"), store.recoveryRecords().map { it.reason })
    }

    @Test fun laterEditsCannotChangeFrozenGroupBytes() = runBlocking {
        val store = Fixture.store()
        val wire = StubTransport()
        val engine = Fixture.engine(store, transport = wire)
        engine.writeAtomically { tx ->
            tx.create(TestNote, TestNote("a", "first"))
            tx.create(TestNote, TestNote("b", "second"))
        }
        val frozen = store.read { store.frozenSubmissions(it) }.single()
        engine.updateRow("notes", "a", null, mapOf("title" to ReplicaValue.Str("later")))
        engine.deleteRow("notes", "b")
        assertEquals(frozen.operations, store.read { store.frozenSubmissions(it) }.single().operations)
        engine.drain()
        assertEquals(listOf("row.create", "row.create", "row.patch", "row.delete"), wire.pushedBatches().flatten().map { it.verb })
        assertEquals(ReplicaValue.Str("later"), store.peekSnapshot("notes", "a")?.data?.get("title"))
        assertNull(store.peekSnapshot("notes", "b"))
    }
}
