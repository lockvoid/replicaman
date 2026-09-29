package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Before
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A one-row patch must cost one row's decode. During a processing storm the
 * stream cache invalidates per commit; without per-row reuse every cold
 * reload re-decodes EVERY row's JSON, model, and eagerly-materialized
 * payload — the profiled 26-38ms `editor.rebuild` spikes. The superseded
 * cache entry is a reuse DONOR: rows whose raw snapshot text is unchanged
 * carry their decoded record and model across materializations; only
 * actually-changed rows decode.
 */
class RowReuseTests : ReplicaTestCase() {

    @Before
    fun resetCounter() {
        ReusableNote.decodeCounter.reset()
    }

    /** KILL: drop the donor comparison in `materializedRows` — every commit re-decodes the stream. */
    @Test
    fun oneRowPatchDecodesExactlyOneModel() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, ReusableNote)
        for (id in listOf("n1", "n2", "n3")) {
            engine.saveRow("notes", id, null, mapOf("title" to ReplicaValue.Str("seed-$id")))
        }

        assertEquals(3, notes.where().size)
        val seeded = ReusableNote.decodeCounter.count
        assertEquals(3, seeded)

        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("patched")))

        assertEquals(listOf("seed-n1", "patched", "seed-n3"), notes.where().map { it.title })
        assertEquals(
            seeded + 1, ReusableNote.decodeCounter.count,
            "a one-row patch re-decoded the whole stream — unchanged rows must reuse their donor decode"
        )
    }

    /** KILL: the same removal — a progress storm costs a whole-stream reload per tick. */
    @Test
    fun aProgressShapedStormCostsOneDecodePerCommit() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, ReusableNote)
        for (id in listOf("n1", "n2", "n3", "n4")) {
            engine.saveRow("notes", id, null, mapOf("progress" to ReplicaValue.Num(0.0)))
        }
        assertEquals(4, notes.where().size)
        val seeded = ReusableNote.decodeCounter.count

        // The storm: one row marches, every tick commits, a reader follows
        // each commit — the exact shape of cook progress during processing.
        for (tick in 1..5) {
            engine.saveRow("notes", "n1", null, mapOf("progress" to ReplicaValue.Num(tick / 10.0)))
            assertEquals(4, notes.where().size)
        }

        assertEquals(
            seeded + 5, ReusableNote.decodeCounter.count,
            "five one-row ticks must cost five decodes, not five whole-stream reloads"
        )
    }

    /** KILL: clear the donor on delete — the survivors re-decode for nothing. */
    @Test
    fun deleteReusesTheSurvivors() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, ReusableNote)
        for (id in listOf("n1", "n2", "n3")) {
            engine.saveRow("notes", id, null, mapOf("title" to ReplicaValue.Str(id)))
        }
        assertEquals(3, notes.where().size)
        val seeded = ReusableNote.decodeCounter.count

        engine.deleteRow("notes", "n2")

        assertEquals(listOf("n1", "n3"), notes.where().map { it.id })
        assertEquals(seeded, ReusableNote.decodeCounter.count, "a delete must not re-decode the surviving rows")
    }

    /** KILL: compare only `raw` in the donor check — a type flip reuses the stale model. */
    @Test
    fun aTypeFlipDecodesFresh() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val notes = RowStream(engine, ReusableNote)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one")))
        assertEquals("one", notes.where().first().title)
        val seeded = ReusableNote.decodeCounter.count

        // Same data text, different STI type, arriving the way type flips
        // really do — a pulled row.set. The donor must NOT be reused: the
        // model's decode switches on `type`.
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.RowSet("notes", "n1", "Special", mapOf("title" to ReplicaValue.Str("one")))
                ),
                cursor = "2:", more = false
            )
        )
        engine.pullOnce("user")

        assertTrue(notes.where().first().special)
        assertEquals(seeded + 1, ReusableNote.decodeCounter.count)
    }

    /**
     * Reused records keep their decoded fields — the in-memory predicate
     * path filters over them.
     *
     * KILL: carry the DONOR's record for a changed row — `n2` answers its
     * superseded value and the predicate lies.
     */
    @Test
    fun predicatesFilterOverReusedRecords() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, ReusableNote)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("keep")))
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("other")))
        notes.where()

        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("changed")))

        // The reused donor must carry the UNCHANGED row's decoded fields…
        assertEquals(listOf("n1"), notes.where(mapOf("title" to ReplicaValue.Str("keep"))).map { it.id })
        // …and the CHANGED row must be filtered by its NEW value.
        assertEquals(listOf("n2"), notes.where(mapOf("title" to ReplicaValue.Str("changed"))).map { it.id })
        assertTrue(
            notes.where(mapOf("title" to ReplicaValue.Str("other"))).isEmpty(),
            "a stale reused record answered a predicate with a superseded value"
        )
    }
}

internal data class ReusableNote(
    override val id: String,
    val title: String? = null,
    val special: Boolean = false,
) : ReplicaWritableRowModel {
    override val typeName: String? get() = if (special) "Special" else null

    override fun encode(): Map<String, ReplicaValue> =
        title?.let { mapOf("title" to ReplicaValue.Str(it)) } ?: emptyMap()

    companion object : ReplicaWritableRowModelType<ReusableNote, ReplicaNoField> {
        val decodeCounter = LockedTally()

        override val streamName: String = "notes"

        override val modelKey: KClass<*> = ReusableNote::class

        override fun from(id: String, type: String?, data: Map<String, ReplicaValue>): ReusableNote {
            decodeCounter.increment()
            return ReusableNote(id = id, title = data["title"]?.string, special = type == "Special")
        }
    }
}

internal class LockedTally {
    private val lock = ReentrantLock()
    private var value = 0

    val count: Int get() = lock.withLock { value }

    fun increment() = lock.withLock { value += 1; Unit }

    fun reset() = lock.withLock { value = 0; Unit }
}
