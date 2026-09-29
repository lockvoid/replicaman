package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.Recorder
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.eventually
import io.replicaman.support.recordInto
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

/**
 * Idempotent pull-apply: the server echoes rows a device just pushed —
 * an import wave rings originless doorbells, and a pull that
 * blind-upserts identical bytes → commit → observation re-runs the
 * whole main-side derivation chain on zero information.
 * LAW: a `RowSet` frame identical to the local snapshot (type + data)
 * applies NOTHING — no upsert, no rebase, no commit, no watch signal.
 */
class IdempotentApplyTests : ReplicaTestCase() {

    private fun note(id: String, title: String): ReplicaFrame =
        ReplicaFrame.RowSet("notes", id, null, mapOf("title" to ReplicaValue.Str(title)))

    private fun pull(frames: List<ReplicaFrame>, cursor: String) =
        ReplicaPullResponse(frames = frames, cursor = cursor, more = false)

    /**
     * KILL: blind `upsertSnapshot` on every frame — the identical pull
     * bumps the stream's change sequence and every watch downstream
     * wakes for zero information. The sequence IS the watch signal
     * source, so asserting it deterministically beats counting deliveries.
     */
    @Test
    fun pullingIdenticalContentDoesNotBumpTheChangeSequence() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull("user", pull(listOf(note("n1", "a"), note("n2", "b")), "0:1"))
        engine.pullOnce("user")
        val before = store.read { store.changeSequence(it, "notes") }

        // The echo: same rows, same bytes, new cursor.
        transport.queuePull("user", pull(listOf(note("n1", "a"), note("n2", "b")), "0:2"))
        engine.pullOnce("user")

        val after = store.read { store.changeSequence(it, "notes") }
        assertEquals(before, after, "an identical pull must not move the change sequence — echo carries zero information")
    }

    /** KILL: skip the compare on a CHANGED row — the newer server value never lands. */
    @Test
    fun changedContentStillAppliesAndSignals() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull("user", pull(listOf(note("n1", "a")), "0:1"))
        engine.pullOnce("user")

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val signals = Recorder<Unit>()
        engine.watchSignal("notes", includeInitial = true).recordInto(scope, signals)
        eventually(3.seconds, "baseline must arm the observation") { signals.count == 1 }

        transport.queuePull("user", pull(listOf(note("n1", "a2")), "0:2"))
        engine.pullOnce("user")

        eventually(3.seconds, "the changed row must commit and signal") { signals.count >= 2 }
        val title = store.read { db -> store.snapshot(db, "notes", "n1")?.data?.get("title")?.string }
        assertEquals("a2", title)
        scope.cancel()
    }

    /**
     * KILL: compare data but not type — a type flip with identical data
     * is skipped and the row keeps the stale type.
     */
    @Test
    fun typeChangeWithIdenticalDataStillApplies() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            pull(listOf(ReplicaFrame.RowSet("notes", "n1", "draft", mapOf("title" to ReplicaValue.Str("a")))), "0:1")
        )
        engine.pullOnce("user")

        transport.queuePull(
            "user",
            pull(listOf(ReplicaFrame.RowSet("notes", "n1", "final", mapOf("title" to ReplicaValue.Str("a")))), "0:2")
        )
        engine.pullOnce("user")

        val type = store.read { db -> store.snapshot(db, "notes", "n1")?.type }
        assertEquals("final", type, "identical data must not shadow a type move")
    }

    /**
     * KILL: apply + rebase the identical echo anyway — the blind upsert
     * AND the rebase replay each bump the sequence for zero information,
     * while a row still owing a write must stay journalled and local.
     */
    @Test
    fun identicalEchoWithOwedWriteSkipsApplyAndKeepsTheJournal() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull("user", pull(listOf(note("n1", "server")), "0:1"))
        engine.pullOnce("user")
        // A local write the server has not seen: snapshot moves locally,
        // journal owes one op — and stays owed (pushes refused).
        transport.failPushes(true)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("local")))
        val owedBefore = engine.pendingOps().map { String(it.payload) }
        assertEquals(1, owedBefore.size)
        val before = store.read { store.changeSequence(it, "notes") }

        // The echo of the LOCAL snapshot (server caught up) — identical
        // bytes, nothing to apply, nothing to rebase.
        transport.queuePull("user", pull(listOf(note("n1", "local")), "0:2"))
        engine.pullOnce("user")

        val after = store.read { store.changeSequence(it, "notes") }
        assertEquals(before, after, "identical echo over an owed row must not move the sequence")
        assertEquals(owedBefore, engine.pendingOps().map { String(it.payload) }, "an identical echo must leave the journal byte-identical")
        val title = store.read { db -> store.snapshot(db, "notes", "n1")?.data?.get("title")?.string }
        assertEquals("local", title)
    }
}
