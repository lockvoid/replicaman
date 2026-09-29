package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.Recorder
import io.replicaman.support.StubTransport
import io.replicaman.support.eventually
import io.replicaman.support.peekDoc
import io.replicaman.support.peekParked
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * The rejected-op revert path. A rejection is a VERDICT: the entry
 * parks — but the client write must not survive it, because the
 * server will never send a correcting frame (pull ships only changed
 * rows). Journal entries carry a client-local PRE-IMAGE; the verdict
 * transaction reverts atomically: create ⇒ delete the row, patch ⇒ restore
 * the pre-image fields, delete ⇒ restore the row. Document rejections are
 * rare by design (the server repairs rather than rejects) — they discard
 * the op and force a re-bootstrap instead.
 */
class RevertOnRejectionTests : ReplicaTestCase() {

    @Test fun rejectedDeltaArchivesEditsAuthoredDuringUpload() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", "stub@1", "S".toByteArray(), emptyMap())
        ), cursor = "5:", more = false))
        engine.pullOnce()
        engine.recordDocDelta("boards", "b1", "+1".toByteArray())
        transport.onPush { engine.recordDocDelta("boards", "b1", "+2".toByteArray()) }
        rejectAll(transport)
        engine.drain()
        assertTrue(store.peekPending().isEmpty())
        assertEquals(1, store.peekParked().size)
        assertEquals("S", assertNotNull(store.peekDoc("boards", "b1")).fold.decodeToString())
        val archive = store.recoveryRecords().single()
        val part = store.recoveryParts(archive.id).single { it.kind == "document.fold" }
        val fold = store.recoveryChunk(archive.id, part).decodeToString()
        assertTrue(fold.contains("+1") && fold.contains("+2"))
    }

    private suspend fun rejectAll(transport: StubTransport) {
        transport.scriptPush { ops ->
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "refused (stub)") }
        }
    }

    /** KILL: park without reverting — a refused birth leaves a ghost row the server denies. */
    @Test
    fun rejectedCreateDeletesTheClientWrittenRow() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        assertNotNull(store.peekSnapshot("notes", "n1"))

        rejectAll(transport)
        engine.drain()

        assertNull(store.peekSnapshot("notes", "n1"), "a refused birth leaves no ghost row")
        assertEquals(1, store.peekParked().size, "the entry parks as evidence")
    }

    /** KILL: drop the `discardEntries(except = op.id)` from the create revert. */
    @Test
    fun rejectedRowCreateDropsEveryDependentAddressEntry() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("birth")))
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("later patch")))

        rejectAll(transport)
        engine.drain()

        assertNull(store.peekSnapshot("notes", "n1"))
        assertTrue(store.peekPending().isEmpty())
        val parked = store.peekParked()
        assertEquals(1, parked.size, "only the refused birth remains as evidence")
        assertEquals(ReplicaOp.Verb.ROW_CREATE, parked.first().op().verb)
    }

    /** KILL: restore the whole preimage row instead of its named fields — `mood` survives. */
    @Test
    fun rejectedPatchRestoresExactlyThePatchedFields() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        // Server truth arrives first.
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "server", "a")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")

        // The local patch touches title AND adds a brand-new field.
        engine.saveRow(
            "notes", "n1", null,
            mapOf(
                "title" to ReplicaValue.Str("local"),
                "rank" to ReplicaValue.Str("a"),
                "mood" to ReplicaValue.Str("bold")
            )
        )
        assertEquals(ReplicaValue.Str("bold"), store.peekSnapshot("notes", "n1")?.data?.get("mood"))

        rejectAll(transport)
        engine.drain()

        val reverted = store.peekSnapshot("notes", "n1")
        assertNotNull(reverted)
        assertEquals(ReplicaValue.Str("server"), reverted.data["title"], "the patched field returns to its pre-image")
        assertEquals(ReplicaValue.Str("a"), reverted.data["rank"], "untouched fields stay")
        assertNull(reverted.data["mood"], "a field the patch INTRODUCED is removed, not nulled")
    }

    /** KILL: revert by replacing the row with the preimage — interim server fields die. */
    @Test
    fun rejectedPatchLeavesInterimServerFieldsAlone() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "server", "a")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("local")))

        // While the patch is in flight, the server replaces the row with a
        // fresher copy carrying a field the patch never touched.
        transport.failPushes(true)
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.RowSet(
                        "notes", "n1", null,
                        mapOf(
                            "title" to ReplicaValue.Str("interim server"),
                            "rank" to ReplicaValue.Str("z"),
                            "starred" to ReplicaValue.Bool(true)
                        ), revision = 9
                    )
                ),
                cursor = "9:", more = false
            )
        )
        engine.pullOnce("user")
        transport.failPushes(false)

        rejectAll(transport)
        engine.drain()

        val row = store.peekSnapshot("notes", "n1")
        assertNotNull(row)
        assertEquals(ReplicaValue.Str("interim server"), row.data["title"], "the freshest authoritative title is the rollback image")
        assertEquals(ReplicaValue.Str("z"), row.data["rank"], "interim server fields survive the revert")
        assertEquals(ReplicaValue.Bool(true), row.data["starred"])
    }

    /** KILL: record no preimage for a delete — a refused delete cannot resurrect the row. */
    @Test
    fun rejectedDeleteRestoresTheRowByteIdentical() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.RowSet("notes", "n1", "Note", mapOf("title" to ReplicaValue.Str("keep me")))
                ),
                cursor = "5:", more = false
            )
        )
        engine.pullOnce("user")
        val before = store.peekSnapshot("notes", "n1")
        assertNotNull(before)

        engine.deleteRow("notes", "n1")
        assertNull(store.peekSnapshot("notes", "n1"))

        rejectAll(transport)
        engine.drain()

        val restored = store.peekSnapshot("notes", "n1")
        assertNotNull(restored, "a refused delete resurrects the row")
        assertEquals(before, restored, "byte-identical prior state — type and data alike")
    }

    /** KILL: skip `deleteDoc` in the create revert — the fold outlives its refused birth. */
    @Test
    fun rejectedDocCreateRemovesTheWholeClientWrittenDoc() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL, mapOf("name" to ReplicaValue.Str("Plans")))
        engine.recordDocDelta("boards", "b1", "+edit".toByteArray())

        rejectAll(transport)
        engine.drain()

        assertNull(store.peekSnapshot("boards", "b1"), "a refused doc birth leaves no projection row")
        assertNull(store.peekDoc("boards", "b1"), "…and no fold")
        assertEquals(0, store.peekPending().size, "the doc's superseded delta dies with it")
    }

    private suspend fun pullServerBornBoard(transport: StubTransport, engine: ReplicaEngine) {
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", "stub@1", "SNAP".toByteArray(), emptyMap())
        ), cursor = "5:", more = false))
        engine.pullOnce("user")
    }

    private suspend fun rejectDeltasAcceptTheRest(transport: StubTransport) {
        transport.scriptPush { ops ->
            ops.map { op ->
                if (op.verb == ReplicaOp.Verb.DOC_DELTA) ReplicaVerdict(op.id, ReplicaVerdict.Outcome.REJECTED, "delta refused")
                else ReplicaVerdict(op.id, ReplicaVerdict.Outcome.ACCEPTED)
            }
        }
    }

    /** KILL: skip `materializeBase` after the verdicts — the refused delta takes its document with it. */
    @Test
    fun rejectedDocDeltaRestoresBaseAndKeepsRecovery() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        pullServerBornBoard(transport, engine)
        engine.recordDocDelta("boards", "b1", "+edit".toByteArray())
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("honest")))
        rejectDeltasAcceptTheRest(transport)
        engine.drain()

        assertEquals("SNAP", assertNotNull(store.peekDoc("boards", "b1")).fold.decodeToString())
        assertEquals(1, store.peekParked().size)
        assertEquals(1, store.recoveryRecords().size)
        assertEquals("5:", engine.currentCursor("user"))
        assertEquals(0, store.peekPending().size, "the note create was accepted in the same drain")
    }

    /**
     * The classification the whole offline story rests on: a severed wire is
     * RETRYABLE, never a verdict.
     *
     * KILL: park on a transport throw — the op strands forever and the
     * rejection revert undoes work the user can see because the network blinked.
     */
    @Test
    fun transportFailureNeitherParksNorReverts() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        engine.deleteRow("notes", "n2")
        transport.failPushes(true)

        assertFailsWith<ReplicaError.Transport> { engine.drain() }

        assertEquals(0, store.peekParked().size, "no connection is not a refusal")
        assertEquals(1, store.peekPending().size, "the real write stays owed; deleting an absent row is a no-op")
        assertNotNull(store.peekSnapshot("notes", "n1"), "the client row must survive: only a VERDICT reverts")
        assertEquals(0, engine.revertedCount)
    }

    /** KILL: fire `onRejected` inside the verdict transaction — the app reads uncommitted state. */
    @Test
    fun rejectionHookFiresWithTheVerdictReason() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        val heard = Recorder<String>()
        engine.setRejectionHandler { op, reason -> heard.record("${op.verb}:${op.rowId}:$reason") }

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("mine")))
        rejectAll(transport)
        engine.drain()

        eventually(2.seconds, "the rejection seam never fired") {
            heard.values == listOf("row.create:n1:refused (stub)")
        }
        assertEquals(1, engine.revertedCount, "the debug surface can read how many writes were undone")
    }
}
