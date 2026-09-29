package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Before
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.Recorder
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.eventually
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.reflect.KClass
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * The warm materialization: one decode per committed sequence, a POINT read
 * that never rebuilds the picture, and a cache that can never pair a new
 * sequence with an old entry.
 */
class RowReadCacheTests : ReplicaTestCase() {

    @Before
    fun resetCounters() {
        CountingNote.decodeCounter.reset()
        CountingNote.decodeEntered.set(null)
        CountingNote.decodeHeld.set(null)
    }

    /** KILL: drop the materialization cache — every current `list`/`find` re-decodes. */
    @Test
    fun warmWhereAndFindMaterializeOncePerCommittedSequence() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, CountingNote)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one")))

        assertEquals(listOf("one"), notes.list().map { it.title })
        assertEquals(1, CountingNote.decodeCounter.count)
        assertEquals(listOf("one"), notes.list().map { it.title })
        assertEquals("one", notes.find("n1")?.title)
        assertEquals(1, CountingNote.decodeCounter.count)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("two")))

        val rows = notes.list()
        assertEquals(listOf("two"), rows.map { it.title })
        assertEquals(rows.first(), notes.find("n1"))
        assertEquals(2, CountingNote.decodeCounter.count)
    }

    /**
     * A find is a POINT read. Under a write cadence the stream's picture is
     * stale almost always, and a find that rebuilt it walked every row of
     * the stream on the caller's thread.
     *
     * KILL: route `ReplicaReads.find` back through `materializedRows` — the
     * unchanged row's find decodes the CHANGED row on its way.
     */
    @Test
    fun findAfterAWriteReadsOneRowInsteadOfRematerializingTheStream() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, CountingNote)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one")))
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("two")))
        assertEquals(listOf("one", "two"), notes.list().map { it.title })
        assertEquals(2, CountingNote.decodeCounter.count)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one!")))

        // The unchanged row comes back from its own decoded record; nothing
        // else in the stream is touched.
        assertEquals("two", notes.find("n2")?.title)
        assertEquals(2, CountingNote.decodeCounter.count)
        // The changed row pays exactly its own decode.
        assertEquals("one!", notes.find("n1")?.title)
        assertEquals(3, CountingNote.decodeCounter.count)
    }

    /** KILL: compare `Bool` to `Num` by identity — `active = 1` stops matching `true`. */
    @Test
    fun warmPredicateFilteringMatchesSQLWithoutRematerializing() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val notes = RowStream(engine, CountingNote)
        val rows = listOf(
            "n1" to mapOf(
                "kind" to ReplicaValue.Str("clip"),
                "score" to ReplicaValue.Num(1.0),
                "active" to ReplicaValue.Bool(true)
            ),
            "n2" to mapOf(
                "kind" to ReplicaValue.Str("clip"),
                "score" to ReplicaValue.Num(1.5),
                "active" to ReplicaValue.Bool(false)
            ),
            "n3" to mapOf(
                "kind" to ReplicaValue.Str("still"),
                "score" to ReplicaValue.Num(0.0),
                "empty" to ReplicaValue.Null
            ),
            "n4" to mapOf(
                "items" to ReplicaValue.Arr(listOf(ReplicaValue.Num(1.0))),
                "meta" to ReplicaValue.Obj(mapOf("x" to ReplicaValue.Num(1.0)))
            ),
            "n5" to mapOf("kind" to ReplicaValue.Str("café")),
        )

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = rows.map { (id, data) -> ReplicaFrame.RowSet("notes", id, null, data) },
                cursor = "5:", more = false
            )
        )
        engine.pullOnce("user")

        assertEquals(rows.map { it.first }, notes.where().map { it.id })
        val initialDecodeCount = CountingNote.decodeCounter.count

        val predicates: List<Map<String, ReplicaValue>> = listOf(
            emptyMap(),
            mapOf("kind" to ReplicaValue.Str("clip")),
            mapOf("score" to ReplicaValue.Num(1.0)),
            mapOf("score" to ReplicaValue.Num(1.5)),
            mapOf("active" to ReplicaValue.Bool(true)),
            mapOf("active" to ReplicaValue.Num(1.0)),
            mapOf("score" to ReplicaValue.Bool(false)),
            mapOf("empty" to ReplicaValue.Null),
            mapOf("items" to ReplicaValue.Arr(listOf(ReplicaValue.Num(1.0)))),
            mapOf("meta" to ReplicaValue.Obj(mapOf("x" to ReplicaValue.Num(1.0)))),
            mapOf("missing" to ReplicaValue.Str("no")),
            mapOf("kind" to ReplicaValue.Str("clip"), "active" to ReplicaValue.Bool(false)),
            mapOf("kind" to ReplicaValue.Str("café")),
        )

        for (predicate in predicates) {
            assertEquals(
                sqlIds(store, "notes", predicate),
                notes.where(predicate).map { it.id },
                "predicate: $predicate"
            )
        }
        assertEquals(initialDecodeCount, CountingNote.decodeCounter.count)
    }

    /**
     * KILL: drop the per-(stream, model) materialization lock — two cold
     * readers both decode. Verified 3/3 after the 50 ms sleep this test used
     * to race on was replaced by the latch hand-off below; with the sleep the
     * same mutation came back green.
     */
    @Test
    fun concurrentColdReadsShareOneMaterialization() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, CountingNote)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one")))
        CountingNote.decodeCounter.reset()

        // No sleeps: the second reader does not start until the first is
        // provably INSIDE `decode`, and the first does not leave until the
        // second has had its chance to race. With the lock the second blocks
        // and reuses; without it, it decodes too — either way deterministic.
        val entered = CountDownLatch(1)
        val held = CountDownLatch(1)
        CountingNote.decodeEntered.set(entered)
        CountingNote.decodeHeld.set(held)

        val outcomes = LockedOutcomes()
        try {
            val first = thread { outcomes.append(runCatching { notes.list().map { note -> note.title } }) }
            assertTrue(entered.await(10, TimeUnit.SECONDS), "the first reader never reached decode")

            val second = thread {
                outcomes.append(runCatching { notes.list().map { note -> note.title } })
            }
            // Give an unlocked build every chance to enter decode a second time.
            Thread.sleep(50)
            held.countDown()
            first.join()
            second.join()
        } finally {
            held.countDown()
            CountingNote.decodeEntered.set(null)
            CountingNote.decodeHeld.set(null)
        }

        for (outcome in outcomes.values) {
            assertEquals(listOf("one"), outcome.getOrThrow())
        }
        assertEquals(1, CountingNote.decodeCounter.count)
    }

    /** KILL: publish a faulted checkpoint into the warm cache; title/decode count change before commit.
     * This grades rollback publication, not the separate cache-generation adoption fence.
     */
    @Test
    fun rolledBackCheckpointKeepsTheWarmMaterialization() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val notes = RowStream(engine, CountingNote)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")
        assertEquals(listOf("one"), notes.list().map { it.title })
        assertEquals(1, CountingNote.decodeCounter.count)

        engine.setCheckpointFault { throw IllegalStateException("injected") }
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "two")), cursor = "6:", more = false)
        )
        val failure = assertFailsWith<IllegalStateException> { engine.pullOnce("user") }
        assertEquals("injected", failure.message)

        assertEquals(listOf("one"), notes.list().map { it.title })
        assertEquals(1, CountingNote.decodeCounter.count)

        engine.setCheckpointFault(null)
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "two")), cursor = "6:", more = false)
        )
        engine.pullOnce("user")
        assertEquals(listOf("two"), notes.list().map { it.title })
        assertEquals(2, CountingNote.decodeCounter.count)
    }

    /** KILL: have the typed watch build its own picture off-cache — the sync read re-decodes after it. */
    @Test
    fun typedWatchPrimesSynchronousListAndFind() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val notes = RowStream(engine, CountingNote)
        val delivered = Recorder<List<CountingNote>>()

        val watch = notes.watch(includeInitial = true) { delivered.record(it) }
        try {
            eventually(5.seconds, "the typed watch baseline was not delivered") { delivered.count == 1 }

            transport.queuePull(
                "user",
                ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one")), cursor = "5:", more = false)
            )
            engine.pullOnce("user")
            eventually(5.seconds, "the typed watch did not deliver") { delivered.count == 2 }

            assertEquals(1, CountingNote.decodeCounter.count)
            assertEquals(listOf("one"), notes.list().map { it.title })
            assertEquals("one", notes.find("n1")?.title)
            assertEquals(1, CountingNote.decodeCounter.count)
        } finally { watch.cancel() }
    }

    /** KILL: remove the observed minimumSequence from typed watch materialization; it emits cached "one" for the new SQL sequence. */
    @Test
    fun typedWatchCannotPairANewSequenceWithTheOldCacheEntry() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, CountingNote)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one")))
        assertEquals(listOf("one"), notes.list().map { it.title })
        assertEquals(1, CountingNote.decodeCounter.count)
        val pictures = Recorder<List<CountingNote>>()
        val watch = notes.watch(includeInitial = true) { pictures.record(it) }
        try {
            eventually(5.seconds, "typed baseline was not delivered") { pictures.count == 1 }
            // Direct real SQLite is the independent writer: it commits a newer sequence while
            // bypassing decoded-cache mutation callbacks, as the Swift held callback does.
            store.write { db ->
                db.exec("UPDATE snapshots SET data = '{\"title\":\"two\"}' WHERE stream='notes' AND row_id='n1'")
                db.exec("UPDATE stream_meta SET change_seq=change_seq+1 WHERE stream='notes'")
            }
            eventually(5.seconds, "typed watch did not deliver the newer committed picture") { pictures.count == 2 }
            assertEquals(listOf("two"), pictures.values[1].map { it.title })
            assertEquals(listOf("two"), notes.list().map { it.title })
            assertEquals("two", notes.find("n1")?.title)
            assertEquals(2, CountingNote.decodeCounter.count)
        } finally { watch.cancel() }
    }

    private fun sqlIds(
        store: ReplicaStateStore,
        stream: String,
        equals: Map<String, ReplicaValue>,
    ): List<String> {
        var sql = "SELECT row_id FROM snapshots WHERE stream = ?"
        val arguments = mutableListOf<Any?>(stream)
        for ((key, value) in equals.entries.sortedBy { it.key }) {
            sql += " AND json_extract(data, ?) = ?"
            arguments.add("$.$key")
            when (value) {
                is ReplicaValue.Str -> arguments.add(value.value)
                is ReplicaValue.Num ->
                    arguments.add(
                        if (value.value == Math.rint(value.value)) value.value.toLong() else value.value
                    )
                is ReplicaValue.Bool -> arguments.add(if (value.value) 1L else 0L)
                else -> arguments.add(null)
            }
        }
        sql += " ORDER BY row_id"
        return store.read { db -> db.queryStrings(sql, arguments) }
    }
}

internal data class CountingNote(
    override val id: String,
    val title: String? = null,
) : ReplicaWritableRowModel {
    override val typeName: String? get() = null

    override fun encode(): Map<String, ReplicaValue> =
        title?.let { mapOf("title" to ReplicaValue.Str(it)) } ?: emptyMap()

    companion object : ReplicaWritableRowModelType<CountingNote, ReplicaNoField> {
        val decodeCounter = LockedTally()

        /** Signalled as soon as a decode is under way; the second reader waits on it. */
        val decodeEntered = java.util.concurrent.atomic.AtomicReference<CountDownLatch?>(null)

        /** Held until the test releases it, so the first decode is provably still running. */
        val decodeHeld = java.util.concurrent.atomic.AtomicReference<CountDownLatch?>(null)

        override val streamName: String = "notes"

        override val modelKey: KClass<*> = CountingNote::class

        override fun from(id: String, type: String?, data: Map<String, ReplicaValue>): CountingNote? {
            decodeCounter.increment()
            decodeEntered.get()?.countDown()
            decodeHeld.get()?.await(10, TimeUnit.SECONDS)
            if (type != null) return null
            return CountingNote(id = id, title = data["title"]?.string)
        }
    }
}

internal class LockedOutcomes {
    private val lock = ReentrantLock()
    private val storage = mutableListOf<Result<List<String?>>>()

    val values: List<Result<List<String?>>> get() = lock.withLock { storage.toList() }

    fun append(outcome: Result<List<String?>>) = lock.withLock { storage.add(outcome); Unit }
}

/** Two threads that must reach the cold read together. */
internal class CountUpLatch(parties: Int) {
    private val latch = CountDownLatch(parties)

    fun arriveAndWait() {
        latch.countDown()
        latch.await(2, TimeUnit.SECONDS)
    }
}
