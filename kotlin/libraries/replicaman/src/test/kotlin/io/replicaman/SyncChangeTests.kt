package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * What a gate sees. At a write: the CHANGE — per field, the value the write
 * set and the value it replaced; a delete, a document's birth and its delta
 * are judged like a row. Asked again, a held row: its whole state.
 */
class SyncChangeTests : ReplicaTestCase() {

    private fun str(value: String) = ReplicaValue.Str(value)

    @Test fun aGateSeesEachWriteWithTheValuesItReplaced(): Unit = runTest {
        val seen = CopyOnWriteArrayList<SyncChange>()
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport(), syncGates = listOf(TestGate("notes") {
            seen.add(it); SyncVerdict.Push
        }))

        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("c"), "rank" to str("r1")))

        assertEquals(listOf(
            SyncChange("notes", "n1", SyncChange.Kind.CREATE, mapOf("title" to str("a"))),
            SyncChange("notes", "n1", SyncChange.Kind.PATCH, mapOf("title" to str("b")), mapOf("title" to str("a"))),
            SyncChange("notes", "n1", SyncChange.Kind.PATCH, mapOf("title" to str("c"), "rank" to str("r1")), mapOf("title" to str("b"))),
        ), seen.toList(), "each write its own change: a create replaced nothing, a field new to the row replaced nothing")
    }

    /**
     * A delete's gate sees the row it removes: otherwise a gate that keeps
     * one kind of row flowing holds its delete.
     *
     * KILL: `change` — drop `previous` from the delete.
     */
    @Test fun aDeleteShowsItsGateTheRowItRemoved(): Unit = runTest {
        val seen = CopyOnWriteArrayList<SyncChange>()
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport(), syncGates = listOf(TestGate("notes") {
            seen.add(it); SyncVerdict.Push
        }))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))
        engine.drain()

        engine.deleteRow("notes", "n1")

        assertEquals(SyncChange("notes", "n1", SyncChange.Kind.DELETE, previous = mapOf("title" to str("a"))), seen.last())
    }

    @Test fun aDocumentsBirthAndDeltaAreJudged(): Unit = runTest {
        val seen = CopyOnWriteArrayList<SyncChange>()
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport(), syncGates = listOf(TestGate("boards") {
            seen.add(it); SyncVerdict.Push
        }))

        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
        engine.recordDocDelta("boards", "b1", "+edit".toByteArray())

        assertEquals(listOf(
            SyncChange("boards", "b1", SyncChange.Kind.CREATE),
            SyncChange("boards", "b1", SyncChange.Kind.DOCUMENT),
        ), seen.toList())
    }

    /**
     * A released row leaves as its state, so its gates judge that state —
     * every field, as the create or the patch it will leave as.
     */
    @Test fun aHeldRowAskedAgainShowsItsGateItsWholeState(): Unit = runTest {
        val seen = CopyOnWriteArrayList<SyncChange>()
        val released = ReleaseLedger()
        val blob = blobGate(released)
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport(), syncGates = listOf(
            TestGate("notes", id = "blob", signal = released.signal) { seen.add(it); blob.judge(it) },
        ))
        engine.saveRow("notes", "born", null, mapOf("title" to str("a"), "blob" to str("k1")))
        engine.saveRow("notes", "known", null, mapOf("title" to str("b")))
        engine.drain()
        engine.saveRow("notes", "known", null, mapOf("blob" to str("k1")))

        released.land("k1")
        eventually(message = "the landing never reached the holds") { engine.heldRows().isEmpty() }

        assertTrue(seen.contains(SyncChange("notes", "born", SyncChange.Kind.CREATE, mapOf("title" to str("a"), "blob" to str("k1")))))
        assertTrue(seen.contains(SyncChange("notes", "known", SyncChange.Kind.PATCH, mapOf("title" to str("b"), "blob" to str("k1")))))
    }
}
