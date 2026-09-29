package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.testing.FixtureTransport
import io.replicaman.testing.ProtocolFixture

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.generated.app.Theme
import io.replicaman.generated.app.SampleReplica
import io.replicaman.generated.app.themes
import io.replicaman.generated.dummy.Job
import io.replicaman.support.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Current WriteTransactionTests.swift: real SQLite rows, journals, pulls and writer admission. */
class WriteTransactionTests : ReplicaTestCase() {
    private class World(automatic: Boolean = false) {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport, automaticallyPushWrites = automatic)
        val notes = RowStream(engine, TestNote)

        suspend fun pull(vararg frames: ReplicaFrame, cursor: String = "1:") {
            transport.queuePull("user", ReplicaPullResponse(frames = frames.toList(), cursor = cursor, more = false))
            engine.pullOnce("user")
        }
    }

    /** Swift testACreateDecidedOnAPulledRowBecomesTheUpdate. KILL: read before entering the writer. */
    @Test fun aCreateDecidedOnAPulledRowBecomesTheUpdate() = runTest {
        val w = World(); w.pull(Fixture.note("n1", "placeholder"))
        w.engine.write { tx ->
            val notes = tx.rows(TestNote)
            if (notes.find("n1") == null) notes.create(TestNote("n1", "verdict"))
            else notes.update("n1") { it.copy(title = "verdict") }
        }
        assertEquals("verdict", w.store.peekSnapshot("notes", "n1")?.data?.get("title")?.string)
        assertEquals(listOf(ReplicaOp.Verb.ROW_PATCH), w.store.peekPending().map { it.op().verb })
    }

    /** Swift testAPullArrivingMidTransactionAppliesAfterItsCommit. KILL: release SQLite between the read and edit. */
    @Test fun aPullArrivingMidTransactionAppliesAfterItsCommit() = runTest {
        val w = World()
        val onWire = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val answering = CountDownLatch(1)
        w.transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "server")), cursor = "1:", more = false))
        w.transport.onPull { onWire.complete(Unit); release.await(); answering.countDown() }
        val pulling = async(Dispatchers.Default) { w.engine.pullOnce("user") }
        onWire.await()
        withContext(Dispatchers.IO) {
            w.engine.write { tx ->
                assertNull(tx.rows(TestNote).find("n1"))
                release.complete(Unit)
                assertTrue(answering.await(3, TimeUnit.SECONDS), "the real pull must return its response")
                assertNull(tx.rows(TestNote).find("n1"), "the open writer must retain its original read")
                tx.rows(TestNote).create(TestNote("n1", "mine"))
            }
        }
        pulling.await()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), w.store.peekPending().map { it.op().verb })
        assertNotNull(w.store.peekSnapshot("notes", "n1"))
    }

    /** Swift testAnUpdateOwesOnlyWhatItsEditChanged. KILL: encode an earlier cached model as the whole update. */
    @Test fun anUpdateOwesOnlyWhatItsEditChanged() = runTest {
        val w = World(); w.pull(Fixture.note("n1", "draft", "a"))
        assertEquals("a", w.notes.find("n1")?.rank)
        w.pull(Fixture.note("n1", "draft", "b"), cursor = "2:")
        w.engine.write { it.rows(TestNote).update("n1") { row -> row.copy(title = "final") } }
        val row = assertNotNull(w.store.peekSnapshot("notes", "n1"))
        assertEquals("final", row.data["title"]?.string); assertEquals("b", row.data["rank"]?.string)
        assertEquals(setOf("title"), w.store.peekPending().single().op().data?.keys)
    }

    /** Swift testAnUpdateLeavesAFieldTheModelCannotReadAlone. KILL: diff the model against raw storage instead of before/after. */
    @Test fun anUpdateLeavesAFieldTheModelCannotReadAlone() = runTest {
        val store = Fixture.store(indexes = SampleReplica.schema.indexes)
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport, schema = SampleReplica.schema)
        // A generated nullable string re-encodes an unreadable number as null.
        // Raw-vs-model diffing would erase that number despite a name-only edit.
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.RowSet("themes", "theme", null, mapOf(
            "colors" to ReplicaValue.Arr(emptyList()), "createdAt" to ReplicaValue.Str("2026-09-26T12:00:00Z"),
            "updatedAt" to ReplicaValue.Str("2026-09-26T12:00:00Z"), "userId" to ReplicaValue.Num(42.0),
            "name" to ReplicaValue.Str("draft"), "description" to ReplicaValue.Num(7.0),
            "futureField" to ReplicaValue.Str("opaque"),
        ))), cursor = "1:", more = false))
        engine.pullOnce("user")
        engine.write { it.themes.update("theme") { row -> row.copy(name = "final") } }
        val raw = assertNotNull(store.peekSnapshot("themes", "theme")).data
        assertEquals(ReplicaValue.Num(7.0), raw["description"])
        assertEquals(ReplicaValue.Str("opaque"), raw["futureField"])
        assertEquals(setOf("name"), store.peekPending().single().op().data?.keys)
    }

    /** Swift testARowThatDoesNotDecodeIsNotReadAsAbsent. KILL: return null for a present unknown STI type. */
    @Test fun aRowThatDoesNotDecodeIsNotReadAsAbsent() = runTest {
        val w = World(); w.pull(ReplicaFrame.RowSet("notes", "n1", "Future", mapOf("title" to ReplicaValue.Str("unknown"))))
        assertEquals(ReplicaError.UndecodableRow("notes", "n1"), assertFailsWith<ReplicaError.UndecodableRow> {
            w.engine.write { it.rows(TestNote).find("n1") }
        })
        assertEquals(ReplicaError.UndecodableRow("notes", "n1"), assertFailsWith<ReplicaError.UndecodableRow> {
            w.engine.write { it.rows(TestNote).update("n1") { row -> row.copy(title = "x") } }
        })
        assertTrue(w.store.peekPending().isEmpty())
    }

    /** Swift testATransactionThatThrowsLeavesNothing. KILL: commit each row before the callback succeeds. */
    @Test fun aTransactionThatThrowsLeavesNothing() {
        val w = World()
        assertFailsWith<IllegalStateException> { w.engine.write {
            it.rows(TestNote).create(TestNote("n1", "half")); error("refused")
        } }
        assertNull(w.store.peekSnapshot("notes", "n1")); assertTrue(w.store.peekPending().isEmpty())
    }

    /** Swift testAWriteInsideAWriteJoinsIt. KILL: open a second transaction or commit the outer row early. */
    @Test fun aWriteInsideAWriteJoinsIt() {
        val w = World()
        w.engine.write { outer ->
            outer.rows(TestNote).create(TestNote("n1", "outer"))
            w.engine.write { inner ->
                assertSame(outer, inner); assertNotNull(inner.rows(TestNote).find("n1"))
                inner.rows(TestNote).create(TestNote("n2", "inner"))
            }
        }
        assertEquals(2, w.store.peekPending().size)
        assertFailsWith<IllegalStateException> { w.engine.write {
            it.rows(TestNote).create(TestNote("n3", "outer")); w.engine.write { error("refused") }
        } }
        assertNull(w.store.peekSnapshot("notes", "n3")); assertEquals(2, w.store.peekPending().size)
    }

    /** Swift testTheVerbsRefuseWhatTheyRefuseOutside. KILL: implement upsert or omit readonly validation. */
    @Test fun theVerbsKeepTheirRefusalsInsideTheTransaction() = runTest {
        val w = World(); w.pull(Fixture.note("n1", "there"))
        assertEquals(ReplicaError.RowExists("notes", "n1"), assertFailsWith<ReplicaError.RowExists> {
            w.engine.write { it.rows(TestNote).create(TestNote("n1")) }
        })
        assertEquals(ReplicaError.UnknownRow("notes", "n9"), assertFailsWith<ReplicaError.UnknownRow> {
            w.engine.write { it.rows(TestNote).update("n9") { row -> row.copy(title = "x") } }
        })
        assertNull(w.engine.write { it.readonlyRows(Job).find("j1") })
        assertFailsWith<ReplicaError.ReadonlyStream> { w.engine.write { it.rows(WritableJob).create(WritableJob("j1")) } }
        assertTrue(w.store.peekPending().isEmpty())
    }

    /** Swift testTheLaneAndTheDraftAreTheCallers. KILL: read lane/draft after the dispatcher hop. */
    @Test fun theLaneAndTheDraftAreCapturedBeforeAsyncDispatch() = runTest {
        val w = World()
        w.engine.lane(ReplicaLane.INTERACTIVE) {
            w.engine.writeAsync { it.rows(TestNote).create(TestNote("n1", "now")) }
        }
        assertEquals(ReplicaLane.INTERACTIVE, w.store.read { db -> w.store.pendingEntries(db, "notes", "n1").single().lane })
        val result = w.engine.beginDraft { w.engine.writeAsync { it.rows(TestNote).create(TestNote("n2", "held")) } }
        assertEquals(listOf("n2"), w.store.read { db -> db.queryStrings("SELECT row_id FROM intents WHERE state = 'draft'") })
        assertEquals(result.draft.key, w.store.read { it.queryString("SELECT draft FROM intents WHERE row_id='n2'") })
        w.engine.commitDraft(result.draft)
        assertEquals(0L, w.store.read { it.queryLong("SELECT COUNT(*) FROM intents WHERE state = 'draft'") })
    }

    /** Swift testACancelledTasksWriteStillLands. KILL: use cancellable withContext(IO) at entry or discard its result on return. */
    @Test fun aCancelledTasksWriteStillLandsAndReturnsItsValue() = runTest {
        val w = World(); val returned = CompletableDeferred<String>()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineExceptionHandler { _, error -> returned.completeExceptionally(error) })
        try {
            val writing = scope.launch {
                currentCoroutineContext().cancel()
                val value = w.engine.writeAsync { tx -> tx.rows(TestNote).create(TestNote("n1", "kept")); "committed" }
                returned.complete(value)
            }
            // The worker and SQLite writer use real dispatchers. A timeout on
            // runTest's virtual clock can expire before that worker is scheduled.
            withContext(Dispatchers.Default) {
                withTimeout(3_000) {
                    assertEquals("committed", returned.await())
                    writing.join()
                }
            }
            assertTrue(writing.isCancelled, "returning the value must not uncancel the caller")
            assertEquals("kept", w.store.peekSnapshot("notes", "n1")?.data?.get("title")?.string)
        } finally { scope.cancel() }
    }

    /** Swift testATransactionLandsAsOneCommitInOrder. KILL: emit a stream change before each row instead of after commit. */
    @Test fun aTransactionLandsAsOneCommitInOrder() = runTest {
        val w = World(); val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default); val signals = Recorder<Unit>()
        try {
            w.engine.watchSignal("notes", includeInitial = true).recordInto(scope, signals)
            eventually(3.seconds, "baseline arms the observer") { signals.count == 1 }
            w.engine.write {
                val rows = it.rows(TestNote)
                rows.create(TestNote("n1", "a")); rows.create(TestNote("n2", "solo")); rows.update("n1") { row -> row.copy(title = "b") }
            }
            eventually(3.seconds, "one committed transaction reaches the observer") { signals.count >= 2 }
            assertEquals(2, signals.count); assertEquals("b", w.notes.find("n1")?.title); assertEquals("solo", w.notes.find("n2")?.title)
        } finally { scope.cancel() }
    }

    /** Swift testAnUnchangedTransactionNeitherSignalsNorJournals. KILL: journal an unchanged edit. */
    @Test fun anUnchangedTransactionNeitherSignalsNorJournals() = runTest {
        val w = World(); w.engine.write { it.rows(TestNote).create(TestNote("n1", "a")) }
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default); val signals = Recorder<Unit>()
        try {
            w.engine.watchSignal("notes", includeInitial = true).recordInto(scope, signals)
            eventually(3.seconds, "baseline arms the observer") { signals.count == 1 }
            w.engine.write { it.rows(TestNote).update("n1") { row -> row.copy(title = "a") } }
            w.engine.write { it.rows(TestNote).create(TestNote("n3", "sentinel")) }
            eventually(3.seconds, "sentinel commit reaches the observer") { signals.count >= 2 }
            assertEquals(2, signals.count); assertEquals(2, w.store.peekPending().size)
        } finally { scope.cancel() }
    }

    /** Swift testAFailedDeleteRollsTheWholeTransactionBack. KILL: commit deletes independently. */
    @Test fun aFailedDeleteRollsTheWholeTransactionBack() {
        val w = World()
        w.engine.write { tx -> for (id in listOf("n1", "n2", "n3")) tx.rows(TestNote).create(TestNote(id, id)) }
        w.store.write { it.exec("""
            CREATE TRIGGER refuse_second_delete BEFORE DELETE ON snapshots
            WHEN OLD.stream = 'notes' AND OLD.row_id = 'n2'
            BEGIN SELECT RAISE(ABORT, 'forced delete failure'); END
        """.trimIndent()) }
        assertTrue(assertFails { w.engine.write { it.rows(TestNote).delete(listOf("n1", "n2", "n3")) } }.toString().contains("forced delete failure"))
        assertEquals(listOf("n1", "n2", "n3"), w.notes.list().map { it.id }); assertEquals(3, w.store.peekPending().size)
        w.store.write { it.exec("DROP TRIGGER refuse_second_delete") }
        w.engine.write { it.rows(TestNote).delete(listOf("n1", "n1", "n2", "n3", "absent")) }
        assertTrue(w.notes.list().isEmpty()); assertTrue(w.store.peekPending().isEmpty())
    }

    /** Swift testASealWaitsForAnOpenTransaction. KILL: close a store without waiting for an admitted writer. */
    @Test fun aSealWaitsForAnOpenTransactionAndRefusesNewWrites() = runTest {
        val w = World(); val entered = CompletableDeferred<Unit>(); val proceed = CountDownLatch(1)
        val writing = async(Dispatchers.IO) { w.engine.write {
            entered.complete(Unit)
            check(proceed.await(5, TimeUnit.SECONDS)) { "test did not release the writer" }
            it.rows(TestNote).create(TestNote("n1", "in flight"))
        } }
        entered.await()
        val sealing = async(Dispatchers.Default) { w.engine.seal() }
        try {
            eventually(3.seconds, "seal closes admission before waiting on SQLite") { w.engine.isSealed }
            assertFalse(sealing.isCompleted)
            assertNull(w.store.peekSnapshot("notes", "n1"))
        } finally { proceed.countDown() }
        writing.await(); sealing.await()
        assertNotNull(w.store.peekSnapshot("notes", "n1"))
        assertFailsWith<ReplicaError.IdentityTransitionInProgress> { w.engine.write { it.rows(TestNote).create(TestNote("n2")) } }
        w.engine.unseal(); w.engine.write { it.rows(TestNote).create(TestNote("n2")) }
        assertNotNull(w.store.peekSnapshot("notes", "n2"))
    }

    /** Swift testADeleteReadsTheFlightClaimedUnderTheWriter. KILL: collapse a birth already selected for the wire. */
    @Test fun aDeleteReadsTheFlightClaimedUnderTheWriter() = runTest {
        val w = World(); w.engine.write { it.rows(TestNote).create(TestNote("n1", "flying")) }
        val onWire = CompletableDeferred<Unit>(); val land = CompletableDeferred<Unit>()
        w.transport.onPush { onWire.complete(Unit); land.await() }
        val drain = async(Dispatchers.Default) { w.engine.drain() }; onWire.await()
        try { assertTrue(w.engine.write { it.rows(TestNote).delete("n1") }) } finally { land.complete(Unit) }
        drain.await()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_DELETE), w.transport.pushedBatches().flatten().map { it.verb })
        w.engine.write { it.rows(TestNote).create(TestNote("n2", "grounded")) }
        assertFalse(w.engine.write { it.rows(TestNote).delete("n2") })
        assertFalse(w.store.peekPending().any { it.op().rowId == "n2" })
    }

    /** Swift testACommittedTransactionIsPushed. KILL: omit post-commit schedulePush. */
    @Test fun aCommittedTransactionIsPushed() = runTest {
        val w = World(automatic = true)
        w.engine.write { it.rows(TestNote).create(TestNote("n1", "go")) }
        eventually(3.seconds, "the transaction reaches the real transport door") { w.transport.pushedBatches().flatten().map { it.rowId } == listOf("n1") }
    }

    /** Current Swift: losing a create's answer does not cancel the later server delete. */
    @Test fun aDeleteAfterACreateWhoseAnswerWasLostStillReachesTheServer() = runTest {
        val store = Fixture.store()
        val transport = LostAnswerTransport()
        val engine = deliveryEngine(store, transport)
        try {
            engine.write { it.rows(TestNote).create(TestNote("lost-answer", "mine")) }
            assertFailsWith<ReplicaError.Transport> { engine.drain() }
            assertTrue(engine.write { it.rows(TestNote).delete("lost-answer") },
                "A possibly committed birth cannot collapse as never sent")
            engine.drain()
            engine.pullOnce("user")
            assertNull(store.peekSnapshot("notes", "lost-answer"), "The next pull resurrected a deleted row")
        } finally { engine.close() }
    }

    /** Current Swift: a failed connection leaves the birth locally collapsible. */
    @Test fun aFrozenCreateRetainsItsSequenceEvenWhenConnectionFailed() = runTest {
        val store = Fixture.store()
        val engine = deliveryEngine(store, FailedPushTransport(java.net.UnknownHostException("offline")))
        try {
            engine.write { it.rows(TestNote).create(TestNote("offline", "mine")) }
            assertFailsWith<java.net.UnknownHostException> { engine.drain() }
            assertTrue(engine.write { it.rows(TestNote).delete("offline") })
            assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_DELETE), store.peekPending().map { it.op().verb })
        } finally { engine.close() }
    }

    /** Current Swift: the may-have-committed fact belongs to SQLite, across process owners. */
    @Test fun aLostAnswerIsRememberedAcrossARestart() = runTest {
        val first = Fixture.store()
        val path = first.path.path
        val transport = LostAnswerTransport()
        val before = deliveryEngine(first, transport)
        try {
            before.write { it.rows(TestNote).create(TestNote("lost-answer", "mine")) }
            assertFailsWith<ReplicaError.Transport> { before.drain() }
        } finally { before.close() }

        val reopened = Fixture.storeAt(path)
        val after = deliveryEngine(reopened, transport)
        try {
            assertTrue(after.write { it.rows(TestNote).delete("lost-answer") }, "Restart forgot the unanswered push")
            after.drain()
            after.pullOnce("user")
            assertNull(reopened.peekSnapshot("notes", "lost-answer"))
        } finally { after.close() }
    }

    /** A later offline retry must not erase an earlier lost answer's durable debt. */
    @Test fun anOfflineRetryCannotEraseAnEarlierPossiblyCommittedBirth() = runTest {
        val store = Fixture.store()
        val transport = LostAnswerTransport(offlineOnSecondPush = true)
        val engine = deliveryEngine(store, transport)
        try {
            engine.write { it.rows(TestNote).create(TestNote("lost-answer", "mine")) }
            assertFailsWith<ReplicaError.Transport> { engine.drain() }
            assertFailsWith<java.net.UnknownHostException> { engine.drain() }
            assertTrue(engine.write { it.rows(TestNote).delete("lost-answer") })
            engine.drain()
            engine.pullOnce("user")
            assertNull(store.peekSnapshot("notes", "lost-answer"))
        } finally { engine.close() }
    }

    /**
     * A frozen birth waits for its own verdict: no failed push — a request
     * refusal, an unanswered request or an offline retry — lets a delete
     * collapse it. KILL: `applyRowDelete` — treat a frozen birth as unheard.
     */
    @Test fun everyFailedPushKeepsTheDeleteOwed() = runTest {
        val failures = listOf(
            ReplicaError.Protocol("Forbidden", "HTTP 403: refused"),
            ReplicaError.Transport("push answered HTTP 403: refused"),
            ReplicaError.Transport("push answered HTTP 500: answer failed"),
            java.net.SocketTimeoutException("answer timed out"),
            java.net.SocketException("connection reset"),
            java.net.UnknownHostException("retry failed").also {
                it.addSuppressed(java.io.IOException("an earlier attempt lost its answer"))
            },
        )
        for (failure in failures) assertDeleteOwedAfter(failure)
    }

    private suspend fun assertDeleteOwedAfter(failure: Exception) {
        val store = Fixture.store()
        val engine = deliveryEngine(store, FailedPushTransport(failure))
        try {
            engine.write { it.rows(TestNote).create(TestNote("n1", "mine")) }
            val caught = assertFails { engine.drain() }
            // Coroutine stack recovery may copy an exception. Its type and
            // message are the transport contract, not object identity.
            assertEquals(failure::class to failure.message, caught::class to caught.message)
            assertTrue(engine.write { it.rows(TestNote).delete("n1") }, failure.toString())
            assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_DELETE), store.peekPending().map { it.op().verb }, failure.toString())
        } finally { engine.close() }
    }

    private fun deliveryEngine(store: ReplicaStateStore, transport: ReplicaTransport) = ReplicaEngine(
        store = store, owner = Fixture.OWNER, transport = transport, schema = Fixture.schema(),
        codecs = listOf(StubCodec()), coldWindow = 0.seconds, automaticallyPushWrites = false,
    )

    /** Swift testTheAsyncDoorIsTheSameTransaction. KILL: lose earlier writes when the callback switches dispatcher. */
    @Test fun theAsyncDoorIsTheSameTransaction() = runTest {
        val w = World()
        w.engine.writeAsync { it.rows(TestNote).create(TestNote("n1", "awaited")); it.rows(TestNote).update("n1") { row -> row.copy(rank = "a") } }
        assertEquals("a", w.store.peekSnapshot("notes", "n1")?.data?.get("rank")?.string)
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_PATCH), w.store.peekPending().map { it.op().verb })
    }

    /** Kotlin mutable models. KILL: let transaction update mutate the shared decoded read-cache object. */
    @Test fun aFailedMutableEditCannotChangeTheCachedReadModel() {
        val store = Fixture.store(indexes = SampleReplica.schema.indexes)
        val engine = Fixture.engine(store, transport = StubTransport(), schema = SampleReplica.schema)
        val born = Theme(id = "theme", colors = emptyList(), createdAt = "2026-09-26T12:00:00Z", name = "before", updatedAt = "2026-09-26T12:00:00Z", userId = 42L)
        engine.write { it.themes.create(born) }
        val held = assertNotNull(SampleReplica(engine).themes.find("theme"))
        assertFailsWith<IllegalStateException> { engine.write { tx -> tx.themes.update("theme") { row ->
            assertNotSame(held, row); row.name = "uncommitted"; error("rollback")
        } } }
        assertEquals("before", held.name)
        assertEquals("before", store.peekSnapshot("themes", "theme")?.data?.get("name")?.string)
        assertEquals(1, store.peekPending().size)
    }

    /** Kotlin list seam. KILL: query the reader pool instead of the writer, or ignore predicate/order/limit. */
    @Test fun transactionListsSeeTheirWritesAndKeepTheIndexedQueryContract() {
        val indexes = listOf(ReplicaIndexSpec("notes", "score"), ReplicaIndexSpec("notes", "kind"))
        val store = Fixture.store(indexes = indexes)
        val engine = Fixture.engine(store, transport = StubTransport(), schema = ReplicaSchema(Fixture.schema().specs, indexes))
        engine.write { tx ->
            tx.writeRaw("notes", "a", "Photo", mapOf("kind" to ReplicaValue.Str("clip"), "score" to ReplicaValue.Num(1.0)), ReplicaEngine.RowWriteExpectation.ABSENT, null)
            tx.writeRaw("notes", "b", "Photo", mapOf("kind" to ReplicaValue.Str("clip"), "score" to ReplicaValue.Num(2.0)), ReplicaEngine.RowWriteExpectation.ABSENT, null)
            tx.writeRaw("notes", "c", "Photo", mapOf("kind" to ReplicaValue.Str("still"), "score" to ReplicaValue.Num(3.0)), ReplicaEngine.RowWriteExpectation.ABSENT, null)
            assertEquals(listOf("b"), tx.rows(ScopedNote).list(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"), listOf(ReplicaOrder.descending(ScopedNote.Field.SCORE)), 1).map { it.id })
            assertEquals(3, tx.rows(ScopedNote).list().size)
            assertTrue(RowStream(engine, ScopedNote).list().isEmpty(), "ordinary read handles stay committed pictures")
        }
    }
}

/** Saves every pushed value; the first response disappears after the commit. */
private class LostAnswerTransport(private val offlineOnSecondPush: Boolean = false) : FixtureTransport {
    override val protocolFixture = ProtocolFixture()
    private val rows = linkedSetOf<String>()
    private var pushes = 0

    override suspend fun push(ops: List<ReplicaOp>): List<ReplicaVerdict> {
        pushes += 1
        if (offlineOnSecondPush && pushes == 2) throw java.net.UnknownHostException("offline retry")
        for (op in ops) {
            if (op.verb == ReplicaOp.Verb.ROW_CREATE) rows.add(op.rowId)
            if (op.verb == ReplicaOp.Verb.ROW_DELETE) rows.remove(op.rowId)
        }
        if (pushes == 1) throw ReplicaError.Transport("the answer was lost")
        return ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.ACCEPTED) }
    }

    override suspend fun pull(shard: String, cursor: String?, limit: Int) = ReplicaPullResponse(
        frames = rows.map { Fixture.note(it, "committed") }, cursor = "lost:1", more = false,
    )
}

private class FailedPushTransport(private val failure: Exception) : FixtureTransport {
    override val protocolFixture = ProtocolFixture()
    override suspend fun push(ops: List<ReplicaOp>): List<ReplicaVerdict> = throw failure
    override suspend fun pull(shard: String, cursor: String?, limit: Int): ReplicaPullResponse = throw failure
}

private data class WritableJob(override val id: String) : ReplicaWritableRowModel {
    override val typeName: String? = null
    override fun encode(): Map<String, ReplicaValue> = emptyMap()
    companion object : ReplicaWritableRowModelType<WritableJob, ReplicaNoField> {
        override val streamName: String = "jobs"
        override val modelKey: KClass<*> = WritableJob::class
        override fun from(id: String, type: String?, data: Map<String, ReplicaValue>): WritableJob = WritableJob(id)
    }
}
