package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The sync gates judge a change when it is WRITTEN: push journals it, a hold
 * keeps the whole row on the device — in `gates`, off the journal — and a
 * discard drops it. A drain asks nobody. Predicates are stubs: the engine's
 * contract needs no bytes, no uploads, no network.
 */
class SyncGateTests : ReplicaTestCase() {

    private fun held(engine: ReplicaEngine): List<String> = engine.heldRows().map { it.rowId }

    // MARK: - hold at write

    /** KILL: `enqueueOp` — journal the op whatever `admit` answers. */
    @Test fun aHeldRowWritesNothingToTheJournalNorDoesItsNextWrite(): Unit = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store, transport = StubTransport(), syncGates = listOf(blobGate(ReleaseLedger())))

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("a"), "blob" to ReplicaValue.Str("k1")))
        assertEquals(0, store.peekPending().size, "a held birth reached the journal")
        assertEquals(listOf("n1"), held(engine))

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("b")))
        assertEquals(0, store.peekPending().size, "a held row's next write reached the journal")
        assertEquals(listOf("n1"), held(engine))
        assertEquals(ReplicaValue.Str("b"), store.peekSnapshot("notes", "n1")?.data?.get("title"), "the device keeps its write")
    }

    /**
     * A held row is off the journal, so a drain has nothing to ask: judging
     * every held entry again on every drain makes each write pay for all.
     *
     * KILL: `performDrain` — judge every selected entry again before the push.
     */
    @Test fun aDrainAsksNoGate(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val judged = JudgeCount()
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(TestGate { change ->
            judged.tick()
            if (change.rowId.startsWith("held")) SyncVerdict.Gate("waits") else SyncVerdict.Push
        }))
        for (id in listOf("held1", "held2", "held3", "free")) {
            engine.saveRow("notes", id, null, mapOf("title" to ReplicaValue.Str(id)))
        }
        val atWrite = judged.value

        engine.drain()
        engine.drain()

        assertEquals(atWrite, judged.value, "a drain asked a gate again")
        assertEquals(listOf("free"), transport.pushedBatches().flatten().map { it.rowId })
        assertEquals(listOf("held1", "held2", "held3"), held(engine))
    }

    @Test fun unregisteredStreamsAndCleanRowsFlowUntouched(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(blobGate(ReleaseLedger())))

        // The clear verb ("") never holds; a different stream is never judged.
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("a"), "blob" to ReplicaValue.Str("")))
        engine.saveRow("assets", "a1", null, mapOf("blob" to ReplicaValue.Str("k1")))

        engine.drain()
        assertEquals(setOf("n1", "a1"), transport.pushedBatches().flatten().map { it.rowId }.toSet())
        assertEquals(emptyList(), held(engine))
    }

    /** A held row asked again on a new state that still cannot leave keeps its place and says why now. */
    @Test fun aHeldRowWrittenAgainTellsItsNewReason(): Unit = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store, transport = StubTransport(), syncGates = listOf(blobGate(ReleaseLedger())))
        engine.saveRow("notes", "n1", null, mapOf("blob" to ReplicaValue.Str("k1")))
        val first = engine.heldRows().first()

        engine.saveRow("notes", "n1", null, mapOf("blob" to ReplicaValue.Str("k2")))

        val now = engine.heldRows().first()
        assertEquals("blob k2 in flight", now.reason)
        assertEquals(first.seq, now.seq, "a hold keeps the place it began at")
        assertEquals(0, store.peekPending().size)
    }

    // MARK: - discard

    /**
     * A change the server never needs never joins the journal; the device
     * keeps its value and the row's later writes go as ever.
     */
    @Test fun aDiscardedChangeNeverReachesTheJournalAndTheRowGoesOn(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(TestGate { change ->
            if (change.kind == SyncChange.Kind.PATCH && change.local.keys == setOf("progress")) SyncVerdict.Discard else SyncVerdict.Push
        }))
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("a"), "progress" to ReplicaValue.Num(0.0)))
        engine.drain()

        engine.saveRow("notes", "n1", null, mapOf("progress" to ReplicaValue.Num(0.5)))
        assertEquals(0, store.peekPending().size, "a discarded change is not owed")
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("b")))
        engine.drain()

        assertEquals(
            listOf(mapOf("title" to ReplicaValue.Str("a"), "progress" to ReplicaValue.Num(0.0)), mapOf("title" to ReplicaValue.Str("b"))),
            transport.pushedBatches().flatten().map { it.data },
            "the progress write never left; the title after it did"
        )
        assertEquals(ReplicaValue.Num(0.5), store.peekSnapshot("notes", "n1")?.data?.get("progress"), "the device keeps its own value")
    }

    /**
     * A change never needed is not held for later either: a discard beats
     * another gate's hold, and the first hold names its gate.
     */
    @Test fun aDiscardBeatsAHold() {
        val gates = SyncGates(listOf(
            TestGate(null, id = "every") { SyncVerdict.Gate("held") },
            TestGate(id = "notes") { change -> if (change.local.keys == setOf("progress")) SyncVerdict.Discard else SyncVerdict.Push },
        ))
        val progress = SyncChange("notes", "n1", SyncChange.Kind.PATCH, mapOf("progress" to ReplicaValue.Num(0.5)))
        val title = SyncChange("notes", "n1", SyncChange.Kind.PATCH, mapOf("title" to ReplicaValue.Str("b")))

        assertEquals(SyncGates.Outcome.Discard, gates.judge(progress))
        assertEquals(SyncGates.Outcome.Hold("every", "held"), gates.judge(title))
    }

    /**
     * Every later write stands on a row's birth: a gate that discards one is
     * refused, and the birth leaves.
     *
     * KILL: `admit` — return false for every discard.
     */
    @Test fun aDiscardedBirthIsSentAnyway(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(TestGate { SyncVerdict.Discard }))

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("a")))
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("b")))
        engine.drain()

        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), transport.pushedBatches().flatten().map { it.verb },
            "the birth left; the discarded patch did not")
        assertTrue(store.peekPending().isEmpty())
    }
}
