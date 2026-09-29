package io.replicaman

import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Read structures are DERIVATIVES of `snapshots.data`, converged
 * at every open — declared ↔ actual, idempotent, rebuilt from the rows
 * already on disk, stale ones dropped. No migration files, no versions: the
 * manifest is the schema.
 */
class IndexReconcileTests : ReplicaTestCase() {
    private val kind = ReplicaIndexSpec("notes", "kind")
    private val title = ReplicaIndexSpec("notes", "title", ReplicaIndexKind.FTS5)

    private fun generatedColumns(store: ReplicaStateStore): Set<String> = store.read { db ->
        db.queryStrings("SELECT name FROM pragma_table_xinfo('snapshots') WHERE hidden IN (2, 3)").toSet()
    }

    private fun owned(store: ReplicaStateStore, type: String): Set<String> = store.read { db ->
        db.queryStrings(
            """
            SELECT name FROM sqlite_master WHERE type = ?
            AND (name LIKE 'idx\_%' ESCAPE '\' OR name LIKE 'fts\_%' ESCAPE '\')
            AND sql NOT LIKE 'CREATE TABLE ''fts\_%' ESCAPE '\'
            """.trimIndent(),
            listOf(type)
        ).toSet()
    }

    private fun ftsRows(store: ReplicaStateStore): Int = store.read { db ->
        (db.queryLong("SELECT COUNT(*) FROM fts_notes_title") ?: -1L).toInt()
    }

    private fun seed(store: ReplicaStateStore, rows: List<Triple<String, String, String>>) {
        store.write { db ->
            for ((id, kindValue, titleValue) in rows) {
                store.upsertSnapshot(
                    db, "notes", id, "user", null,
                    mapOf("kind" to ReplicaValue.Str(kindValue), "title" to ReplicaValue.Str(titleValue))
                )
            }
        }
    }

    /** KILL: create the btree over `(ix_field)` alone — the plan below loses `stream`. */
    @Test
    fun declaredStructuresExistAfterOpen() {
        val store = Fixture.store(indexes = listOf(kind, title))

        assertEquals(setOf("ix_kind", "ix_title"), generatedColumns(store), "one generated column per indexed FIELD")
        assertEquals(setOf("idx_kind"), owned(store, "index"))
        assertEquals(setOf("fts_notes_title"), owned(store, "table"))
        assertEquals(
            setOf("fts_notes_title_ai", "fts_notes_title_ad", "fts_notes_title_au"),
            owned(store, "trigger")
        )
        // The intents' own indexes are outside the reconcile's scope.
        val intents = store.read { db ->
            db.queryStrings(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'intents' " +
                    "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'"
            )
        }
        assertEquals(setOf("intents_address", "intents_owed", "intents_draft", "intents_sequence"), intents.toSet())
    }

    /** KILL: only ADD structures, never drop — an undeclared derivative outlives the manifest. */
    @Test
    fun reopenWithFewerDeclarationsDropsTheStale() {
        val path = Fixture.path("reconcile")
        val first = Fixture.storeAt(path, listOf(kind, title))
        seed(first, listOf(Triple("n1", "clip", "дача")))
        first.close()

        val second = Fixture.storeAt(path, listOf(kind))

        assertEquals(setOf("ix_kind"), generatedColumns(second), "an undeclared derivative is dropped, column included")
        assertEquals(setOf("idx_kind"), owned(second, "index"))
        assertEquals(emptySet(), owned(second, "table"))
        assertEquals(emptySet(), owned(second, "trigger"))
        assertEquals(
            1L, second.read { it.queryLong("SELECT COUNT(*) FROM snapshots") },
            "dropping a derivative never touches data"
        )
    }

    /** KILL: rebuild the fts table on every open — a converged open double-inserts. */
    @Test
    fun reconcileIsIdempotent() {
        val path = Fixture.path("reconcile")
        val first = Fixture.storeAt(path, listOf(kind, title))
        seed(first, listOf(Triple("n1", "clip", "дача"), Triple("n2", "still", "сад")))
        val schema = first.read { db ->
            db.query(
                "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY type, name"
            ) { listOf(it.getText(0), it.getText(1), it.textOrNull(2)) }
        }
        first.close()

        val second = Fixture.storeAt(path, listOf(kind, title))

        val again = second.read { db ->
            db.query(
                "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY type, name"
            ) { listOf(it.getText(0), it.getText(1), it.textOrNull(2)) }
        }
        assertEquals(schema, again, "a converged open executes no DDL")
        assertEquals(2, ftsRows(second), "a converged open does not rebuild (and never double-inserts) the fts rows")
    }

    /** KILL: seed the fts table from EVERY stream — another stream's rows pollute the index. */
    @Test
    fun structuresBuildFromRowsAlreadyOnDisk() {
        val path = Fixture.path("reconcile")
        val bare = Fixture.storeAt(path)
        seed(bare, listOf(Triple("n1", "clip", "дача"), Triple("n2", "clip", "дачный участок"), Triple("n3", "still", "сад")))
        bare.write { db ->
            bare.upsertSnapshot(db, "jobs", "j1", "user", null, mapOf("title" to ReplicaValue.Str("дача")))
        }
        bare.close()

        val indexed = Fixture.storeAt(path, listOf(kind, title))

        assertEquals(
            3, ftsRows(indexed),
            "the fts table is rebuilt from the stream's rows on disk — another stream's rows stay out"
        )
        val plan = indexed.read { db ->
            db.query(
                "EXPLAIN QUERY PLAN SELECT row_id FROM snapshots WHERE stream = 'notes' AND \"ix_kind\" = 'clip'"
            ) { statement ->
                (0 until statement.getColumnCount())
                    .firstOrNull { statement.getColumnName(it) == "detail" }
                    ?.let { statement.getText(it) } ?: ""
            }.joinToString(" | ")
        }
        assertTrue(plan.contains("idx_kind"), "eq over the generated column is index-served: $plan")
    }

    /** KILL: drop the AFTER UPDATE trigger — a replaced row keeps its old terms forever. */
    @Test
    fun ftsFollowsTheRows() {
        val store = Fixture.store(indexes = listOf(title))
        seed(store, listOf(Triple("n1", "clip", "Дача"), Triple("n2", "clip", "дачный участок"), Triple("n3", "still", "сад")))

        fun matching(query: String): List<String> = store.read { db ->
            db.queryStrings(
                "SELECT row_id FROM fts_notes_title WHERE fts_notes_title MATCH ? ORDER BY row_id",
                listOf(query)
            )
        }

        assertEquals(listOf("n1", "n2"), matching("\"дач\"*"), "unicode61 folds case — Дача matches дач")
        seed(store, listOf(Triple("n2", "clip", "сарай")))
        assertEquals(listOf("n1"), matching("\"дач\"*"), "a replaced row's old terms are gone")
        assertEquals(listOf("n2"), matching("\"сар\"*"))
        store.write { db -> store.deleteSnapshot(db, "notes", "n1") }
        assertEquals(emptyList(), matching("\"дач\"*"), "a deleted row leaves the index")
        assertEquals(2, ftsRows(store))
    }
}
