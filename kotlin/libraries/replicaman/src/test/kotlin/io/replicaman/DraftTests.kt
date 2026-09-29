package io.replicaman

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.*

/** DraftTests.swift: real SQLite journal and captured network operations, without a CRDT substitute oracle. */
class DraftTests : ReplicaTestCase() {
    private fun held(store: ReplicaStateStore) = store.read { it.queryLong("SELECT COUNT(*) FROM intents WHERE state = 'draft'") }
    private fun rows(store: ReplicaStateStore) = store.read { it.queryStrings("SELECT row_id FROM snapshots ORDER BY row_id") }
    private suspend fun birth(engine: ReplicaEngine, id: String) = engine.saveRow("notes", id, null, mapOf("title" to ReplicaValue.Str(id)))

    /** KILL: select held rows in pending; either lane would send an uncommitted draft. */
    @Test fun draftWritesAreVisibleLocallyAndNeverDrained() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        val result = engine.beginDraft {
            engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
            birth(engine, "n1")
        }
        assertEquals(listOf("b1", "n1"), rows(store))
        assertEquals(1L, store.read { it.queryLong("SELECT COUNT(*) FROM docs WHERE row_id='b1'") })
        assertEquals(2L, held(store)); assertEquals(0, store.peekPending().size)
        engine.drain(); engine.drain(ReplicaLane.INTERACTIVE)
        assertEquals(emptyList(), transport.pushedBatches().flatten()); assertTrue(result.draft.key.isNotEmpty())
    }

    /** KILL: include draft in lanesOwed; the pusher spins while a create cover is still open. */
    @Test fun aHeldOnlyJournalOwesNoWork() = runTest {
        val store = Fixture.store(); val engine = Fixture.engine(store, transport = StubTransport())
        engine.beginDraft { birth(engine, "n1") }
        assertEquals(emptySet(), store.read { store.lanesOwed(it) })
    }

    /** KILL: re-enqueue on release; committed births lose journal order behind later writes. */
    @Test fun commitReleasesTheWholeDraftInJournalOrder() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        val result = engine.beginDraft {
            engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL); birth(engine, "n1")
        }
        birth(engine, "n2")
        val positions = store.read { it.queryStrings("SELECT id FROM intents ORDER BY rowid") }
        engine.commitDraft(result.draft)
        assertEquals(positions, store.read { it.queryStrings("SELECT id FROM intents ORDER BY rowid") })
        assertEquals(0L, held(store)); assertEquals(3, store.peekPending().size)
        engine.drain()
        assertEquals(listOf("b1", "n1", "n2"), transport.pushedBatches().flatten().map { it.rowId })
        assertEquals(ReplicaOp.Verb.ROW_CREATE, transport.pushedBatches().flatten().first().verb)
    }

    /** KILL: release creates fresh entries; repeated commit sends the same birth twice. */
    @Test fun commitIsIdempotentAndUnknownKeysAreSilent() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        val result = engine.beginDraft { birth(engine, "n1") }
        engine.commitDraft(result.draft); engine.commitDraft(result.draft); engine.commitDraft(ReplicaDraft("absent"))
        engine.drain(); assertEquals(listOf("n1"), transport.pushedBatches().flatten().map { it.rowId })
    }

    /** KILL: remove journal keys alone; discarded creation rows/documents remain visible after dismissal. */
    @Test fun discardDropsRowsDocumentsAndEntriesAndSendsNothing() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        val result = engine.beginDraft {
            engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
            engine.recordDocDelta("boards", "b1", "+edit".toByteArray()); birth(engine, "n1")
        }
        birth(engine, "n2"); engine.discardDraft(result.draft)
        assertEquals(listOf("n2"), rows(store)); assertEquals(0L, store.read { it.queryLong("SELECT COUNT(*) FROM docs") })
        assertEquals(0L, held(store)); engine.drain()
        assertEquals(listOf("n2"), transport.pushedBatches().flatten().map { it.rowId })
    }

    /** KILL: supersede only owed deltas — a draft's second edit leaves as a second delta after the commit. */
    @Test fun aDraftsDocumentEditsSupersedeIntoOneDelta() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        val result = engine.beginDraft {
            engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
            engine.recordDocDelta("boards", "b1", "+a".toByteArray())
            engine.recordDocDelta("boards", "b1", "+b".toByteArray())
        }
        assertEquals(2L, held(store))
        engine.commitDraft(result.draft); engine.drain()
        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.DOC_DELTA), sent.map { it.verb })
        assertEquals("SEED+a+b", sent.last().payload?.decodeToString())
    }

    /** KILL: enqueue a delete after an undrained birth; the server learns a dismissed draft existed. */
    @Test fun deletingADraftRowCollapsesToNothing() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        val result = engine.beginDraft { birth(engine, "n1") }; engine.drain()
        assertFalse(engine.deleteRow("notes", "n1")); assertEquals(emptyList(), rows(store)); assertEquals(0L, held(store))
        engine.commitDraft(result.draft); engine.drain(); assertEquals(emptyList(), transport.pushedBatches().flatten())
    }

    /** KILL: inherit only lexical scope; a chat message names a draft project but ships ahead of its birth. */
    @Test fun aWriteNamingADraftRowJoinsTheDraft() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        val result = engine.beginDraft { engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL) }
        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "m1", null, mapOf("board_id" to ReplicaValue.Str("b1"), "title" to ReplicaValue.Str("hello")))
        }
        assertEquals(2L, held(store)); engine.drain(ReplicaLane.INTERACTIVE); engine.drain()
        assertEquals(emptyList(), transport.pushedBatches().flatten())
        engine.commitDraft(result.draft); engine.drain(ReplicaLane.INTERACTIVE); engine.drain()
        assertEquals(listOf("b1", "m1"), transport.pushedBatches().flatten().map { it.rowId })
    }

    /** KILL: delete only the lexical draft; dependent messages survive without their project. */
    @Test fun discardTakesTheDependentsWithIt() = runTest {
        val store = Fixture.store(); val engine = Fixture.engine(store, transport = StubTransport())
        val result = engine.beginDraft { birth(engine, "n1") }
        engine.saveRow("notes", "m1", null, mapOf("note_id" to ReplicaValue.Str("n1")))
        engine.discardDraft(result.draft)
        assertEquals(emptyList(), rows(store)); assertEquals(0, store.peekPending().size)
    }

    /** KILL: read draft after the async writer hop; a transaction unexpectedly releases draft rows. */
    @Test fun draftInsideATransactionKeepsItsKey() = runTest {
        val store = Fixture.store(); val engine = Fixture.engine(store, transport = StubTransport())
        val result = engine.beginDraft { engine.writeAsync { tx -> tx.rows(TestNote).create(TestNote("n1", "n1")); tx.rows(TestNote).create(TestNote("n2", "n2")) } }
        assertEquals(2L, held(store)); assertEquals(0, store.peekPending().size)
        engine.commitDraft(result.draft); assertEquals(2, store.peekPending().size)
    }

    /** KILL: return an unrelated draft handle; caller releases the wrong creation scope. */
    @Test fun theBodyValueComesBackWithTheDraft() = runTest {
        val store = Fixture.store(); val engine = Fixture.engine(store, transport = StubTransport())
        val result = engine.beginDraft { birth(engine, "n9"); "n9" }
        assertEquals("n9", result.value)
        assertEquals(result.draft.key, store.read { it.queryString("SELECT draft FROM intents WHERE row_id='n9'") })
    }

    /** KILL: omit open sweep; an abandoned create cover resurfaces after process death. */
    @Test fun aDraftNeverSurvivesAReopen() = runTest {
        val directory = Fixture.directory(); val transport = StubTransport(); val first = Fixture.unopenedEngine(directory, transport)
        first.open(1)
        first.beginDraft { first.createDoc("boards", "b1", "SEED".toByteArray(), 7uL); birth(first, "n1") }
        birth(first, "n2"); first.close()
        val second = Fixture.unopenedEngine(directory, transport); second.open(1)
        val store = assertNotNull(second.store)
        assertEquals(listOf("n2"), rows(store)); assertEquals(0L, store.read { it.queryLong("SELECT COUNT(*) FROM docs") })
        assertEquals(0L, held(store)); assertEquals(listOf("n2"), store.peekPending().map { it.op().rowId })
        assertEquals(setOf("b1", "n1"), store.recoveryRecords().map { it.rowId }.toSet())
    }

    /** KILL: leak coroutine-local draft after cancellation; subsequent ordinary work becomes silently held. */
    @Test fun cancellationAndNestedScopesRestoreTheCallingDraft() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        var inner: ReplicaDraft? = null
        val outer = engine.beginDraft {
            birth(engine, "outer-before")
            inner = engine.beginDraft { birth(engine, "inner") }.draft
            birth(engine, "outer-after")
        }.draft
        assertFailsWith<CancellationException> { engine.beginDraft { birth(engine, "cancelled"); throw CancellationException("test") } }
        birth(engine, "ordinary")
        val keys = store.read { db -> db.query("SELECT row_id, draft FROM intents ORDER BY row_id") { it.getText(0) to it.textOrNull(1) }.toMap() }
        assertEquals(outer.key, keys["outer-before"]); assertEquals(outer.key, keys["outer-after"])
        assertEquals(inner?.key, keys["inner"]); assertNull(keys["ordinary"]); assertNotNull(keys["cancelled"])
        engine.drain(); assertEquals(listOf("ordinary"), transport.pushedBatches().flatten().map { it.rowId })
    }
}
