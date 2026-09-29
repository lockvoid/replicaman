package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.*

/** Conditional cook writes name their original run, not the server's current run. */
class PatchPreconditionTests : ReplicaTestCase() {
    private class World {
        val store = Fixture.store()
        val transport = StubTransport()
        val schema = ReplicaSchema(listOf(ReplicaStreamSpec(
            name = "cooks", lane = ReplicaStreamSpec.Lane.ROW,
            preconditions = listOf("status", "version"),
        )))
        val engine = Fixture.engine(store, transport = transport, schema = schema)
        val original = mapOf(
            "status" to ReplicaValue.Str("running"), "version" to ReplicaValue.Num(7.0),
            "progress" to ReplicaValue.Num(0.2), "data" to ReplicaValue.Obj(mapOf("words" to ReplicaValue.Str("kept"))),
        )
        suspend fun pull(data: Map<String, ReplicaValue> = original) {
            transport.queuePull("user", ReplicaPullResponse(
                frames = listOf(ReplicaFrame.RowSet("cooks", "take", null, data)), cursor = "1:", more = false,
            ))
            engine.pullOnce("user")
        }
        suspend fun patch(data: Map<String, ReplicaValue>) = engine.updateRow("cooks", "take", null, data)
    }

    @Test fun aPartialPatchCarriesTheStoredRunAndLeavesUnmentionedPayloadAlone() = runTest {
        val w = World(); w.pull()
        w.patch(mapOf("progress" to ReplicaValue.Num(0.6)))
        val entry = w.store.peekPending().single()
        val expected = mapOf("progress" to ReplicaValue.Num(0.6),
            "status" to ReplicaValue.Str("running"), "version" to ReplicaValue.Num(7.0))
        assertEquals(expected, entry.op().data)
        val prior = assertIs<ReplicaPreimage.Fields>(ReplicaPreimage.decode(assertNotNull(entry.preimage)))
        assertEquals(w.original.filterKeys { it in expected }, prior.values)
        assertTrue(prior.missing.isEmpty())
        assertEquals(w.original["data"], w.store.peekSnapshot("cooks", "take")?.data?.get("data"))
        w.transport.scriptPush { ops ->
            assertEquals(expected, ops.single().data, "the wire must carry the accepted local run")
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.ACCEPTED) }
        }
        w.engine.drain()
        assertTrue(w.store.peekPending().isEmpty())
        assertEquals(expected, w.transport.pushedBatches().single().single().data)
    }

    @Test fun movedPreconditionsWinAndARefusalRestoresTheirPreimage() = runTest {
        val w = World(); w.pull()
        val changed = mapOf("status" to ReplicaValue.Str("preparing"), "version" to ReplicaValue.Num(8.0),
            "progress" to ReplicaValue.Num(0.0))
        w.patch(changed)
        assertEquals(changed, w.store.peekPending().single().op().data)
        w.transport.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "run refused") } }
        w.engine.drain()
        assertEquals(w.original, w.store.peekSnapshot("cooks", "take")?.data)
        assertEquals(1, w.store.peekParked().size)
        assertTrue(w.store.peekPending().isEmpty())
    }

    @Test fun aNoopDoesNotJournalUnchangedPreconditions() = runTest {
        val w = World(); w.pull()
        w.patch(mapOf("progress" to ReplicaValue.Num(0.2)))
        w.patch(w.original)
        assertTrue(w.store.peekPending().isEmpty())
        assertEquals(w.original, w.store.peekSnapshot("cooks", "take")?.data)
    }

    @Test fun aMissingStoredPreconditionIsNotInventedAndExplicitNullIsPreserved() = runTest {
        val w = World(); w.pull(w.original - "version" + ("status" to ReplicaValue.Null))
        w.patch(mapOf("progress" to ReplicaValue.Num(0.4)))
        val data = assertNotNull(w.store.peekPending().single().op().data)
        assertEquals(mapOf("progress" to ReplicaValue.Num(0.4), "status" to ReplicaValue.Null), data)
        assertFalse(data.containsKey("version"), "a malformed row cannot borrow a fabricated current run")
        assertFalse(assertNotNull(w.store.peekSnapshot("cooks", "take")).data.containsKey("version"))
    }
}
