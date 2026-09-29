package io.replicaman

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import org.junit.Before
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.Recorder
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.Tally
import io.replicaman.support.eventually
import io.replicaman.support.realDelay
import kotlin.reflect.KClass
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * `list` is a sync indexed read over a generated `Field`
 * enum with stable operators; `watch` delivers CHANGES of the scoped
 * picture only. A commit outside the scope is silent; a scoped read after a
 * whole-stream one decodes nothing.
 */
class ScopedReadTests : ReplicaTestCase() {

    private val indexes: List<ReplicaIndexSpec> = listOf(
        ReplicaIndexSpec("notes", "kind"),
        ReplicaIndexSpec("notes", "score"),
        ReplicaIndexSpec("notes", "title"),
        ReplicaIndexSpec("notes", "title", ReplicaIndexKind.FTS5),
    )

    private fun world(): Triple<ReplicaStateStore, ReplicaEngine, RowStream<ScopedNote, ScopedNote.Field>> {
        val schema = ReplicaSchema(Fixture.schema().specs, indexes)
        val store = Fixture.store(indexes = indexes)
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema)
        return Triple(store, engine, RowStream(engine, ScopedNote))
    }

    private suspend fun seed(engine: ReplicaEngine) {
        val rows = listOf(
            "n1" to mapOf(
                "kind" to ReplicaValue.Str("clip"),
                "title" to ReplicaValue.Str("дача"),
                "score" to ReplicaValue.Num(1.0)
            ),
            "n2" to mapOf(
                "kind" to ReplicaValue.Str("clip"),
                "title" to ReplicaValue.Str("дачный участок"),
                "score" to ReplicaValue.Num(2.5)
            ),
            "n3" to mapOf(
                "kind" to ReplicaValue.Str("still"),
                "title" to ReplicaValue.Str("Дача зимой"),
                "score" to ReplicaValue.Num(1.0)
            ),
            "n4" to mapOf("title" to ReplicaValue.Str("café")),
            "n5" to mapOf("kind" to ReplicaValue.Str("clip"), "title" to ReplicaValue.Str("сад")),
        )
        for ((id, data) in rows) engine.saveRow("notes", id, null, data)
    }

    @Before
    fun resetDecodes() {
        ScopedNote.decodes = Tally()
    }

    /** KILL: bind a numeric predicate as TEXT — `score = 1` stops matching a JSON integer. */
    @Test
    fun eqListsOnlyTheScope() = runTest {
        val (_, engine, notes) = world()
        seed(engine)

        assertEquals(listOf("n1", "n2", "n5"), notes.list(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip")).map { it.id })
        assertEquals(listOf("n3"), notes.list(ReplicaPredicate.eq(ScopedNote.Field.KIND, "still")).map { it.id })
        assertEquals(
            listOf("n1", "n3"), notes.list(ReplicaPredicate.eq(ScopedNote.Field.SCORE, 1)).map { it.id },
            "a JSON integer and a bound Double compare numerically"
        )
        assertEquals(listOf("n2"), notes.list(ReplicaPredicate.eq(ScopedNote.Field.SCORE, 2.5)).map { it.id })
        assertEquals(
            emptyList(), notes.list(ReplicaPredicate.eq(ScopedNote.Field.SCORE, "1")).map { it.id },
            "a string never equals a number — json_extract typing holds at the edge"
        )
        assertEquals(listOf("n1", "n2", "n3", "n4", "n5"), notes.list().map { it.id }, "bare list is the whole stream")
    }

    @Test
    fun combinatorsNarrowInSQL() = runTest {
        val (_, engine, notes) = world()
        seed(engine)
        assertEquals(listOf("n1", "n2", "n3", "n5"), notes.list(ReplicaPredicate.oneOf(ScopedNote.Field.KIND, listOf("clip", "still"))).map { it.id })
        assertTrue(notes.list(ReplicaPredicate.oneOf(ScopedNote.Field.KIND, emptyList())).isEmpty())
        assertEquals(listOf("n4"), notes.list(ReplicaPredicate.isNull(ScopedNote.Field.KIND)).map { it.id })
        assertEquals(listOf("n1"), notes.list(ReplicaPredicate.and(listOf(
            ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"), ReplicaPredicate.eq(ScopedNote.Field.SCORE, 1),
        ))).map { it.id })
        assertEquals(listOf("n1", "n2"), notes.list(ReplicaPredicate.and(listOf(
            ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"), ReplicaPredicate.match(ScopedNote.Field.TITLE, "дач"),
        ))).map { it.id })
        assertEquals(listOf("n1", "n2", "n3", "n4", "n5"), notes.list(ReplicaPredicate.and(emptyList())).map { it.id })
        assertEquals(listOf("n3"), notes.list(ReplicaPredicate.not(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"))).map { it.id },
            "SQL NOT leaves a null kind on neither side")
    }

    private object Photo : ReplicaVariant { override val wireType = "Photo" }

    @Test
    fun kindScopesAnSTIStreamByItsWireType() = runTest {
        val (_, engine, notes) = world()
        engine.saveRow("notes", "k1", "Photo", mapOf("kind" to ReplicaValue.Str("clip")))
        engine.saveRow("notes", "k2", "Text", mapOf("kind" to ReplicaValue.Str("clip")))
        assertEquals(listOf("k1"), notes.list(ReplicaPredicate.kind(Photo)).map { it.id })
        assertEquals(listOf("k2"), notes.list(ReplicaPredicate.and(listOf(
            ReplicaPredicate.Kind("Text"), ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"),
        ))).map { it.id })
    }

    @Test
    fun orderAndLimitRideTheIndexedColumns() = runTest {
        val (_, engine, notes) = world()
        seed(engine)
        assertEquals(listOf("n4", "n5", "n1", "n3", "n2"), notes.list(order = listOf(ReplicaOrder.ascending(ScopedNote.Field.SCORE))).map { it.id },
            "nulls first ascending, row identity breaks ties")
        assertEquals(listOf("n2", "n3", "n1", "n5", "n4"), notes.list(order = listOf(ReplicaOrder.descending(ScopedNote.Field.SCORE))).map { it.id },
            "descending reverses both the tie break and null placement")
        assertEquals(listOf("n1", "n2"), notes.list(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"),
            order = listOf(ReplicaOrder.ascending(ScopedNote.Field.TITLE)), limit = 2).map { it.id })
        assertEquals(listOf("n1"), notes.list(limit = 1).map { it.id })
    }

    @Test
    fun anOrderedWatchDeliversTheOrderedPicture() = runTest {
        val (_, engine, notes) = world()
        seed(engine)
        val pictures = Recorder<List<String>>()
        val watch = notes.watch(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"),
            order = listOf(ReplicaOrder.descending(ScopedNote.Field.SCORE)), limit = 1, includeInitial = true) {
            pictures.record(it.map { row -> row.id })
        }
        try {
            eventually(3.seconds, "the baseline is the first row in the order") { pictures.count == 1 }
            assertEquals(listOf(listOf("n2")), pictures.values)
            engine.saveRow("notes", "n6", null, mapOf("kind" to ReplicaValue.Str("clip"), "score" to ReplicaValue.Num(9.0)))
            eventually(3.seconds, "a new top must replace the limited picture") { pictures.count == 2 }
            assertEquals(listOf("n6"), pictures.values.last())
        } finally { watch.cancel() }
    }

    @Test
    fun holdKeepsTheWatchForTheCallingTask() = runTest {
        val (_, engine, notes) = world()
        seed(engine)
        val pictures = Recorder<List<String>>()
        val holder = launch(Dispatchers.Default) {
            notes.watch(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"), includeInitial = true) {
                pictures.record(it.map { row -> row.id })
            }.hold()
        }
        try {
            eventually(3.seconds, "the baseline proves the held observation is armed") { pictures.count == 1 }
            engine.saveRow("notes", "n6", null, mapOf("kind" to ReplicaValue.Str("clip")))
            eventually(3.seconds, "a held watch delivers movement") { pictures.count == 2 }
            holder.cancelAndJoin()
            engine.saveRow("notes", "n7", null, mapOf("kind" to ReplicaValue.Str("clip")))
            realDelay(300)
            assertEquals(2, pictures.count, "cancelling the calling task cancels its held watch")
        } finally { holder.cancelAndJoin() }
    }

    /** KILL: fold case in `hasPrefix` — `Дача` leaks into a BINARY prefix scope. */
    @Test
    fun prefixIsBinaryStringStart() = runTest {
        val (_, engine, notes) = world()
        seed(engine)

        assertEquals(
            listOf("n1", "n2"), notes.list(ReplicaPredicate.hasPrefix(ScopedNote.Field.TITLE, "дач")).map { it.id },
            "BINARY: Дача (capital) is not a дач prefix — that is match's job"
        )
        assertEquals(listOf("n1"), notes.list(ReplicaPredicate.hasPrefix(ScopedNote.Field.TITLE, "дача")).map { it.id })
        assertEquals(
            listOf("n1", "n2", "n3", "n4", "n5"),
            notes.list(ReplicaPredicate.hasPrefix(ScopedNote.Field.TITLE, "")).map { it.id }
        )
    }

    /** KILL: pass the raw query to FTS5 — `"дач* OR (x:y)` executes as syntax. */
    @Test
    fun matchFoldsCaseAndDiacriticsOverTheRawQuery() = runTest {
        val (_, engine, notes) = world()
        seed(engine)

        assertEquals(
            listOf("n1", "n2", "n3"), notes.list(ReplicaPredicate.match(ScopedNote.Field.TITLE, "дач")).map { it.id },
            "unicode61 folds case"
        )
        assertEquals(
            listOf("n4"), notes.list(ReplicaPredicate.match(ScopedNote.Field.TITLE, "cafe")).map { it.id },
            "remove_diacritics 2: café ~ cafe"
        )
        assertEquals(
            listOf("n2"), notes.list(ReplicaPredicate.match(ScopedNote.Field.TITLE, "дач уч")).map { it.id },
            "every word must start-match (AND)"
        )
        assertEquals(
            listOf("n1", "n2", "n3", "n4", "n5"),
            notes.list(ReplicaPredicate.match(ScopedNote.Field.TITLE, "   ")).map { it.id },
            "an empty query is a cleared search box"
        )
        assertEquals(
            emptyList(), notes.list(ReplicaPredicate.match(ScopedNote.Field.TITLE, "\"дач* OR (x:y)")).map { it.id },
            "FTS syntax in the raw query is neutralized, never executed"
        )
    }

    /** KILL: make an empty prefix union match everything — a project with no records sees every cook. */
    @Test
    fun identityPredicatesScopeByAddress() = runTest {
        val (_, engine, notes) = world()
        val keys = listOf(
            "pmck/Project/p1/plan", "pmck/Element/e1/original", "pmck/Element/e1/poster",
            "pmck/Element/e2/original", "pmck/Element/e10/original", "loose"
        )
        for (key in keys) engine.saveRow("notes", key, null, mapOf("kind" to ReplicaValue.Str("cook")))

        assertEquals(
            listOf("pmck/Element/e1/poster"),
            notes.list(ReplicaPredicate.id<ScopedNote.Field>("pmck/Element/e1/poster")).map { it.id }
        )
        assertEquals(
            listOf("pmck/Element/e1/original", "pmck/Element/e1/poster", "pmck/Project/p1/plan"),
            notes.list(
                ReplicaPredicate.idPrefixes<ScopedNote.Field>(listOf("pmck/Project/p1/", "pmck/Element/e1/"))
            ).map { it.id },
            "a project's cooks are the union of its records' address prefixes; e10 is not e1"
        )
        assertEquals(
            emptyList(),
            notes.list(ReplicaPredicate.idPrefixes<ScopedNote.Field>(emptyList())).map { it.id },
            "an empty union is nobody's scope"
        )
    }

    /** KILL: compile `idPrefixes` as an OR of ranges — the planner walks the stream instead of seeking. */
    @Test
    fun scopedReadsAreIndexServed() = runTest {
        val (store, engine, _) = world()
        seed(engine)

        fun plan(predicate: ReplicaPredicate<ScopedNote.Field>): String {
            val compiled = predicate.compile("notes", store.indexes)
            val arguments = listOf<Any?>("notes") + compiled.arguments
            return store.read { db ->
                db.query(
                    "EXPLAIN QUERY PLAN SELECT row_id FROM snapshots WHERE stream = ? AND (${compiled.sql})",
                    arguments
                ) { statement ->
                    (0 until statement.getColumnCount())
                        .firstOrNull { statement.getColumnName(it) == "detail" }
                        ?.let { statement.getText(it) } ?: ""
                }.joinToString(" | ")
            }
        }

        val eq = plan(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"))
        assertTrue(eq.contains("idx_kind"), eq)
        val prefix = plan(ReplicaPredicate.hasPrefix(ScopedNote.Field.TITLE, "да"))
        assertTrue(prefix.contains("idx_title"), prefix)
        val match = plan(ReplicaPredicate.match(ScopedNote.Field.TITLE, "да"))
        assertTrue(match.contains("fts_notes_title"), match)
        val identity = plan(
            ReplicaPredicate.idPrefixes(
                listOf("pmck/Project/p1/", "pmck/Element/e1/", "pmck/Element/e2/")
            )
        )
        assertEquals(
            3, identity.split("row_id>?").size - 1,
            "an address union is one primary-key RANGE SEEK per prefix, never a filtered walk: $identity"
        )
    }

    /** KILL: drop the per-row reuse from `scopedRows` — a scoped read re-decodes the scope. */
    @Test
    fun scopedListReusesTheStreamMaterialization() = runTest {
        val (_, engine, notes) = world()
        seed(engine)
        ScopedNote.decodes = Tally()

        assertEquals(5, notes.list().size)
        assertEquals(5, ScopedNote.decodes.count, "the whole-stream read decodes once per row")
        assertEquals(3, notes.list(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip")).size)
        assertEquals(5, ScopedNote.decodes.count, "a scoped read over unchanged rows decodes nothing")

        engine.saveRow(
            "notes", "n1", null,
            mapOf("kind" to ReplicaValue.Str("clip"), "title" to ReplicaValue.Str("дача 2"))
        )
        assertEquals(
            listOf("дача 2", "дачный участок", "сад"),
            notes.list(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip")).map { it.title }
        )
        assertEquals(6, ScopedNote.decodes.count, "only the changed row pays a decode")
    }

    /** KILL: deliver on every commit without comparing the picture — an out-of-scope write wakes it. */
    @Test
    fun scopedWatchDeliversOnlyScopeChanges() = runTest {
        val (_, engine, notes) = world()
        seed(engine)
        val pictures = Recorder<List<String>>()

        val watch = notes.watch(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip")) { rows ->
            pictures.record(rows.map { it.id })
        }
        realDelay(200)
        assertEquals(emptyList(), pictures.values, "no includeInitial: the sync list gave the first picture")

        engine.saveRow(
            "notes", "n3", null,
            mapOf("kind" to ReplicaValue.Str("still"), "title" to ReplicaValue.Str("Дача весной"))
        )
        realDelay(300)
        assertEquals(emptyList(), pictures.values, "a commit outside the scope is silent")

        engine.saveRow(
            "notes", "n6", null,
            mapOf("kind" to ReplicaValue.Str("clip"), "title" to ReplicaValue.Str("лес"))
        )
        eventually(3.seconds, "an in-scope commit delivers the new picture") { pictures.count == 1 }
        assertEquals(listOf(listOf("n1", "n2", "n5", "n6")), pictures.values)

        engine.saveRow(
            "notes", "n6", null,
            mapOf("kind" to ReplicaValue.Str("clip"), "title" to ReplicaValue.Str("лес"))
        )
        realDelay(300)
        assertEquals(1, pictures.count, "an identical re-save changes nothing and delivers nothing")

        engine.saveRow(
            "notes", "n6", null,
            mapOf("kind" to ReplicaValue.Str("still"), "title" to ReplicaValue.Str("лес"))
        )
        eventually(3.seconds, "leaving the scope delivers the shrunken picture") { pictures.count == 2 }
        assertEquals(listOf("n1", "n2", "n5"), pictures.last)
        watch.cancel()
    }

    /** KILL: ignore the predicate in the watch's re-read — another record's cook wakes the scope. */
    @Test
    fun addressScopedWatchDeliversOnlyItsRecords() = runTest {
        val (_, engine, notes) = world()
        engine.saveRow(
            "notes", "pmck/Element/e1/original", null,
            mapOf("kind" to ReplicaValue.Str("cook"), "title" to ReplicaValue.Str("v1"))
        )
        val pictures = Recorder<List<String>>()

        val watch = notes.watch(
            ReplicaPredicate.idPrefixes(listOf("pmck/Project/p1/", "pmck/Element/e1/"))
        ) { rows -> pictures.record(rows.map { it.id }) }
        realDelay(200)

        engine.saveRow("notes", "pmck/Element/e2/original", null, mapOf("kind" to ReplicaValue.Str("cook")))
        realDelay(300)
        assertEquals(emptyList(), pictures.values, "another record's cook is silent")

        engine.saveRow("notes", "pmck/Project/p1/plan", null, mapOf("kind" to ReplicaValue.Str("cook")))
        eventually(3.seconds, "a cook of one of the scope's records delivers") { pictures.count == 1 }
        assertEquals(listOf(listOf("pmck/Element/e1/original", "pmck/Project/p1/plan")), pictures.values)
        watch.cancel()
    }

    /** KILL: swallow the baseline even when asked — a state-owning consumer never arms. */
    @Test
    fun scopedWatchCanIncludeTheBaseline() = runTest {
        val (_, engine, notes) = world()
        seed(engine)
        val pictures = Recorder<List<String>>()

        val watch = notes.watch(
            ReplicaPredicate.match(ScopedNote.Field.TITLE, "дач"), includeInitial = true
        ) { rows -> pictures.record(rows.map { it.id }) }
        eventually(3.seconds, "includeInitial delivers the committed baseline") { pictures.count == 1 }
        assertEquals(listOf(listOf("n1", "n2", "n3")), pictures.values)
        watch.cancel()
    }
}

internal data class ScopedNote(
    override val id: String,
    val kind: String? = null,
    val title: String? = null,
    val score: Double? = null,
) : ReplicaWritableRowModel {
    enum class Field(override val rawValue: String) : ReplicaIndexedField {
        KIND("kind"),
        TITLE("title"),
        SCORE("score"),
    }

    override val typeName: String? get() = null

    override fun encode(): Map<String, ReplicaValue> {
        val out = LinkedHashMap<String, ReplicaValue>()
        kind?.let { out["kind"] = ReplicaValue.Str(it) }
        title?.let { out["title"] = ReplicaValue.Str(it) }
        score?.let { out["score"] = ReplicaValue.Num(it) }
        return out
    }

    companion object : ReplicaWritableRowModelType<ScopedNote, Field> {
        var decodes = Tally()

        override val streamName: String = "notes"

        override val modelKey: KClass<*> = ScopedNote::class

        override fun from(id: String, type: String?, data: Map<String, ReplicaValue>): ScopedNote {
            decodes.bump()
            return ScopedNote(
                id = id,
                kind = data["kind"]?.string,
                title = data["title"]?.string,
                score = data["score"]?.number
            )
        }
    }
}
