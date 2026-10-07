package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.testing.fixtureSeedRow
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekDoc
import io.replicaman.support.peekParked
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import kotlin.test.assertEquals
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertNull

/**
 * `row.delete` cascade is manifest-driven: a document stream's
 * delete evicts snapshot + fold + every owed journal entry (parked
 * included); a row stream's delete drops the snapshot row alone.
 */
class DeleteCascadeTests : ReplicaTestCase() {

    /**
     * A doorbell's round started before this device's own delete was accepted, and carries the
     * tombstone. The accepted intent is the server's state waiting for a round, not authoring:
     * there is nothing to archive.
     */
    @Test
    fun anAcceptedDeleteSeenByARoundStartedBeforeItLeavesNoRecoveryRecord() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one")), cursor = "c1", more = false))
        engine.pullOnce()

        val entered = CompletableDeferred<Unit>()
        val gate = CompletableDeferred<Unit>()
        transport.onPull { entered.complete(Unit); gate.await() }
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.RowDelete("notes", "n1")), cursor = "c2", more = false))
        val pull = async { engine.pullOnce() }
        entered.await()
        engine.deleteRow("notes", "n1")
        engine.drain()
        gate.complete(Unit)
        pull.await()

        assertNull(store.peekSnapshot("notes", "n1"))
        assertEquals(0, store.recoveryRecords().size)
        assertFalse(store.syncStatus().hasUnsettledWork)
        assertEquals(0, store.peekPending().size)
    }

    /** KILL: drop the `discardEntries` call from the document branch — the dead doc keeps owing. */
    @Test
    fun documentStreamDeleteCascadesFoldAndJournal() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", "stub@1", "SEED".toByteArray(), emptyMap())
        ), cursor = "5:", more = false))
        engine.pullOnce()
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("stays")))

        // Push side dead and the wire cold: the doc's delta is still owed when
        // the delete frame arrives — the cascade, not the drain, must clear it.
        transport.failPushes(true)
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        engine.recordDocDelta("boards", "b1", "+edit".toByteArray())
        val originalFold = assertNotNull(store.peekDoc("boards", "b1")).fold
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(ReplicaFrame.RowDelete("boards", "b1")), cursor = "9:", more = false
            )
        )
        engine.pullOnce("user")

        assertNull(store.peekSnapshot("boards", "b1"), "the projection row is gone")
        assertNull(store.peekDoc("boards", "b1"), "the fold is gone")
        val archive = store.recoveryRecords().single()
        val part = store.recoveryParts(archive.id).single { it.kind == "document.fold" }
        assertContentEquals(originalFold, store.recoveryChunk(archive.id, part))
        assertEquals(
            listOf("n1"), store.peekPending().map { it.op().rowId },
            "every op the dead doc owed is discharged; the unrelated note entry survives"
        )
    }

    private suspend fun refuseTheBoardBirth(transport: StubTransport, engine: ReplicaEngine) {
        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
        engine.recordDocDelta("boards", "b1", "+edit".toByteArray())
        transport.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "quota") } }
        engine.drain()
    }

    private suspend fun oweANoteOnAColdWire(transport: StubTransport, engine: ReplicaEngine) {
        transport.failPushes(true)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("stays")))
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
    }

    /** KILL: discard `refused` intents in `removeActiveEntity` — the removal hides the refusal before dismissal. */
    @Test
    fun documentRemovalKeepsTheRefusalAndOtherRowsEntries() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        refuseTheBoardBirth(transport, engine)
        assertEquals(1, store.peekParked().size)
        oweANoteOnAColdWire(transport, engine)

        transport.queuePull("user", ReplicaPullResponse(
            frames = listOf(ReplicaFrame.RowDelete("boards", "b1")), cursor = "9:", more = false
        ))
        engine.pullOnce("user")

        assertEquals(1, store.peekParked().size, "a removal cannot hide a refusal before dismissal")
        assertEquals(listOf("n1"), store.peekPending().map { it.op().rowId }, "the removal spares other rows' entries")
    }

    /** KILL: cascade the journal on the ROW lane too — a pulled delete eats owed patches. */
    @Test
    fun rowRemovalArchivesPendingPatch() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "server copy")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")
        // A pending patch for the same row — row-lane deletes do NOT touch
        // the journal (the spec's 6B: snapshot row only). Push side dead and
        // the wire cold, so the drain barrier can't discharge it first.
        transport.failPushes(true)
        engine.saveRow("notes", "n0", null, mapOf("title" to ReplicaValue.Str("cools the wire")))
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("local edit")))
        val owed = store.peekPending().single { it.op().rowId == "n1" }.payload

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(ReplicaFrame.RowDelete("notes", "n1")), cursor = "9:", more = false)
        )
        engine.pullOnce("user")

        assertNull(store.peekSnapshot("notes", "n1"))
        assertEquals(listOf("n0"), store.peekPending().map { it.op().rowId })
        val archive = store.recoveryRecords().single()
        val part = store.recoveryParts(archive.id).single { it.kind == "intent" }
        assertContentEquals(owed, store.recoveryChunk(archive.id, part))
    }

    /**
     * A frozen write is the server's to answer: the removal of its address
     * archives it and leaves it, byte for byte, until its own verdict — which
     * finds a replaced lifetime and consumes it.
     *
     * KILL: delete the address's frozen intents with its owed ones in
     * `removeActiveEntity` — the submission loses its intent and never leaves.
     */
    @Test
    fun aFrozenIntentSurvivesTheArchiveOfItsAddressUntilItsVerdict() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        store.fixtureSeedRow(Fixture.schema(), "notes", "n1", mapOf("title" to ReplicaValue.Str("seeded")))
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("local edit")))
        transport.failPushes(true)
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        val frozen = store.peekPending().single()
        assertTrue(frozen.sent)

        // The server's n1 is another lifetime: the round archives the address.
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "replacement")), cursor = "9:", more = false)
        )
        engine.pullOnce("user")

        assertEquals(ReplicaValue.Str("replacement"), store.peekSnapshot("notes", "n1")?.data?.get("title"))
        assertEquals(listOf(frozen), store.peekPending(), "the archive left the frozen intent as it was")
        assertEquals(1, store.syncStatus().submittedGroups)
        val archive = store.recoveryRecords().single()
        val parts = store.recoveryParts(archive.id)
        assertContentEquals(frozen.payload, store.recoveryChunk(archive.id, parts.single { it.kind == "intent" }))
        assertEquals(1, parts.count { it.kind == "submission" })

        transport.failPushes(false)
        engine.drain()

        val pushed = transport.protocolFixture.requests(ReplicaEndpoint.PUSH).map { it["ops"] }
        assertEquals(2, pushed.size)
        assertEquals(pushed.first(), pushed.last(), "the retry sends the frozen bytes")
        assertTrue(store.peekPending().isEmpty(), "its verdict consumed it")
        assertEquals(0, store.syncStatus().acceptedOperations, "a verdict for a replaced lifetime leaves nothing behind")
        assertEquals(0, store.syncStatus().submittedGroups)
        assertEquals(ReplicaValue.Str("replacement"), store.peekSnapshot("notes", "n1")?.data?.get("title"))
    }
}
