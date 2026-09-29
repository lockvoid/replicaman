package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.generated.dummy.Item
import io.replicaman.generated.dummy.PhotoItem
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.TestNote
import io.replicaman.support.peekDoc
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The local write door under the generated `save()`/`delete()` verbs:
 * diff against last-known ⇒ `row.create` (absent) / `row.patch` (changed
 * fields ONLY) / nothing (no change), with the client snapshot write in
 * the same transaction. Deletes discharge what the row still owed.
 */
class SaveDeleteTests : ReplicaTestCase() {

    /**
     * `update` speaks to a row that IS there; a missing id is the caller's
     * mistake, answered loudly and written nowhere — the silent upsert is
     * how "it was never created" ships as a green test.
     *
     * KILL: route `TransactionRows.update` through the upsert (`ANY`) — the
     * missing row is minted and the assertion below sees a row.
     */
    @Test
    fun updateOfAMissingRowThrowsAndWritesNothing() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, TestNote)

        try {
            engine.write { it.rows(TestNote).update("n9") { row -> row.copy(title = "ghost", rank = "a") } }
            fail("an update of a missing row went through")
        } catch (error: ReplicaError.UnknownRow) {
            assertEquals(ReplicaError.UnknownRow("notes", "n9"), error)
        }
        assertNull(notes.find("n9"), "the missing row was minted by an update")
        assertEquals(0, store.peekPending().size, "an update of nothing owes the server nothing")
    }

    /**
     * `create` mints; a present id is the caller's mistake — the row's
     * truth stands untouched.
     *
     * KILL: route `TransactionRows.create` through the upsert — the second create
     * patches the title and the assertion sees "second".
     */
    @Test
    fun createOfAPresentRowThrowsAndKeepsTheRow() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val notes = RowStream(engine, TestNote)
        engine.write { it.rows(TestNote).create(TestNote("n1", "first", "a")) }

        try {
            engine.write { it.rows(TestNote).create(TestNote("n1", "second", "a")) }
            fail("a create over a present row went through")
        } catch (error: ReplicaError.RowExists) {
            assertEquals(ReplicaError.RowExists("notes", "n1"), error)
        }
        assertEquals("first", notes.find("n1")?.title, "the present row was overwritten by a create")
        assertEquals(1, store.peekPending().size, "only the mint owes the server anything")
    }

    /** KILL: journal the whole row on update — a patch stops being a diff. */
    @Test
    fun createThenUpdateDiffsIntoCreateThenPatchThenNothing() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        val notes = RowStream(engine, TestNote)

        engine.write { it.rows(TestNote).create(TestNote("n1", "draft", "a")) }
        var pending = store.peekPending()
        assertEquals(1, pending.size)
        var op = pending.first().op()
        assertEquals(ReplicaOp.Verb.ROW_CREATE, op.verb)
        assertEquals(
            mapOf("title" to ReplicaValue.Str("draft"), "rank" to ReplicaValue.Str("a")),
            op.data,
            "creation is the only full-row write"
        )
        assertEquals("draft", notes.find("n1")?.title, "the client write is visible immediately")

        engine.write { it.rows(TestNote).update("n1") { row -> row.copy(title = "final") } }
        pending = store.peekPending()
        assertEquals(2, pending.size)
        op = pending.last().op()
        assertEquals(ReplicaOp.Verb.ROW_PATCH, op.verb)
        assertEquals(
            mapOf("title" to ReplicaValue.Str("final")), op.data,
            "updates are always patches — changed fields ONLY"
        )

        engine.write { it.rows(TestNote).update("n1") { row -> row.copy(title = "final") } }
        assertEquals(2, store.peekPending().size, "an unchanged update owes the server nothing")
    }

    /**
     * The server judges a write against its preconditions, so every patch
     * carries them — changed or not: a completion without its version is
     * read at the server row's version and replaces a born fact.
     *
     * KILL: drop the precondition loop in `applyRowWrite` — the patch carries
     * the title alone.
     */
    @Test
    fun aPatchCarriesTheStreamsPreconditionsChangedOrNot() = runTest {
        val store = Fixture.store()
        val schema = ReplicaSchema(
            listOf(ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, shard = "user", preconditions = listOf("rank")))
        )
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema)

        engine.write { it.rows(TestNote).create(TestNote("n1", "draft", "a")) }
        engine.write { it.rows(TestNote).update("n1") { row -> row.copy(title = "final") } }

        assertEquals(
            mapOf("title" to ReplicaValue.Str("final"), "rank" to ReplicaValue.Str("a")),
            store.peekPending().last().op().data,
        )
    }

    /** KILL: add the preconditions before the empty-diff check — an unchanged row journals a patch. */
    @Test
    fun preconditionsAloneOweNoPatch() = runTest {
        val store = Fixture.store()
        val schema = ReplicaSchema(
            listOf(ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, shard = "user", preconditions = listOf("rank")))
        )
        val engine = Fixture.engine(store = store, transport = StubTransport(), schema = schema)

        engine.write { it.rows(TestNote).create(TestNote("n1", "draft", "a")) }
        engine.write { it.rows(TestNote).update("n1") { row -> row } }

        assertEquals(1, store.peekPending().size, "an unchanged row owes nothing, its preconditions included")
    }

    /**
     * KILL: drop the `?: ReplicaValue.Null` normalization in `applyRowWrite` —
     * an authored clear stops being a patch field and the old value stands.
     */
    @Test
    fun generatedOptionalFieldCanBeClearedWithNullPatch() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val schema = ReplicaSchema(
            listOf(ReplicaStreamSpec("items", ReplicaStreamSpec.Lane.ROW, shard = "user"))
        )
        val engine = Fixture.engine(store = store, transport = transport, schema = schema)
        val items = RowStream(engine, Item)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.RowSet(
                        "items", "i1", "PhotoItem",
                        mapOf(
                            "boardId" to ReplicaValue.Str("b1"),
                            "label" to ReplicaValue.Str("idea"),
                            "rank" to ReplicaValue.Str("a"),
                        )
                    )
                ),
                cursor = "1:", more = false
            )
        )
        engine.pullOnce("user")

        engine.write { it.rows(Item).update("i1") { row -> (row as PhotoItem).copy(label = null) } }

        val pending = store.peekPending()
        assertEquals(1, pending.size)
        val op = pending.first().op()
        assertEquals(ReplicaOp.Verb.ROW_PATCH, op.verb)
        assertEquals(
            mapOf("label" to ReplicaValue.Null), op.data,
            "null is an authored clear, not an omitted diff"
        )

        val written = items.find("i1") as? PhotoItem
            ?: fail("the generated STI projection must survive its client write")
        assertNull(written.label, "the client snapshot must clear the old value immediately")
        assertEquals(ReplicaValue.Null, store.peekSnapshot("items", "i1")?.data?.get("label"))
    }

    /** KILL: drop the readonly guard in `writableSpec` — the client can author a projection. */
    @Test
    fun saveToReadonlyStreamIsRefused() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        try {
            engine.saveRow("jobs", "j1", null, mapOf("state" to ReplicaValue.Str("hacked")))
            fail("readonly streams take no client writes")
        } catch (error: ReplicaError.ReadonlyStream) {
            assertEquals("jobs", error.stream)
        }
    }

    /** KILL: journal a `row.delete` for an unborn row — the server refuses an id it never had. */
    @Test
    fun deleteOfNeverPushedCreateOwesTheServerNothing() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("oops")))
        engine.deleteRow("notes", "n1")

        assertNull(store.peekSnapshot("notes", "n1"))
        assertEquals(
            0, store.peekPending().size,
            "the server never heard n1 — nothing to push, nothing to resurrect"
        )
    }

    /** KILL: treat every delete as unborn — a synced row is never deleted server-side. */
    @Test
    fun deleteOfSyncedRowJournalsRowDelete() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "server copy")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")

        engine.deleteRow("notes", "n1")
        assertNull(store.peekSnapshot("notes", "n1"))
        val pending = store.peekPending()
        assertEquals(1, pending.size)
        val op = pending.first().op()
        assertEquals(ReplicaOp.Verb.ROW_DELETE, op.verb)
        assertEquals("n1", op.rowId)

        // The discriminating half: a row that never existed is ordinary CRUD
        // silence. Asserted HERE, next to the positive, because on its own
        // "pending is empty" is also satisfied by a delete that does nothing.
        assertFalse(engine.deleteRow("notes", "never-existed"))
        assertEquals(1, store.peekPending().size, "an absent row adds no work")
    }

    /** KILL: skip `discardEntries` on the document lane — a dead doc keeps owing its delta. */
    @Test
    fun localDocDeleteCascadesItsOwnJournal() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
        engine.recordDocDelta("boards", "b1", "+edit".toByteArray())
        engine.deleteRow("boards", "b1")

        assertNull(store.peekSnapshot("boards", "b1"))
        assertNull(store.peekDoc("boards", "b1"))
        assertEquals(
            0, store.peekPending().size,
            "an unborn doc dies silently — create and deltas discarded, no delete op"
        )
    }
}
