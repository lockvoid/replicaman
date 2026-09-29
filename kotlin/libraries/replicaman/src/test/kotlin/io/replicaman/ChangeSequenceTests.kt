package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import kotlinx.coroutines.test.runTest
import java.io.File
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekSnapshot
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * `stream_meta.change_seq` is the durable signal source: it moves on every
 * COMMITTED write to its stream and never on a rolled-back one.
 */
class ChangeSequenceTests : ReplicaTestCase() {

    private class Fault : Exception("injected")

    /**
     * KILL: take a file with tables but no `user_version` for a new store —
     * the layout is written over the earlier journal.
     */
    @Test
    fun unsupportedLegacyStoreIsRefusedWithoutChangingTheJournal() {
        val path = Fixture.path("legacy")
        val legacy = BundledSQLiteDriver().open(path)
        legacy.exec(
            """
            CREATE TABLE journal (
                id TEXT PRIMARY KEY,
                op TEXT NOT NULL,
                payload TEXT NOT NULL,
                preimage TEXT,
                parked TEXT,
                created_at REAL NOT NULL
            )
            """.trimIndent()
        )
        legacy.exec(
            "INSERT INTO journal (id, op, payload, created_at) VALUES (?, ?, ?, ?)",
            listOf("legacy-op", "row.create", "{}", 1.0)
        )
        legacy.close()
        val bytes = File(path).readBytes()

        val error = kotlin.test.assertFailsWith<ReplicaError.Storage> { Fixture.storeAt(path) }

        assertTrue(error.reason.contains("earlier store format"), error.reason)
        assertContentEquals(bytes, File(path).readBytes())
        BundledSQLiteDriver().open(path).use { db ->
            assertEquals(listOf("legacy-op"), db.queryStrings("SELECT id FROM journal"))
            assertEquals("{}", db.queryString("SELECT payload FROM journal"))
            assertEquals(listOf("journal"), db.queryStrings("SELECT name FROM sqlite_master WHERE type = 'table'"))
            assertEquals(0L, db.queryLong("PRAGMA user_version"))
        }
    }

    /** KILL: open any store below the format — an earlier layout would synchronize against tables it lacks. */
    @Test
    fun aStoreOfAnEarlierFormatIsRefusedUntouched() {
        for (format in listOf(1L, 2L)) {
            val path = Fixture.path("earlier")
            ReplicaStateStore(path).close()
            BundledSQLiteDriver().open(path).use { it.exec("PRAGMA user_version = $format") }
            val bytes = File(path).readBytes()

            val error = kotlin.test.assertFailsWith<ReplicaError.Storage> { Fixture.storeAt(path) }

            assertTrue(error.reason.contains("earlier store format"), error.reason)
            assertContentEquals(bytes, File(path).readBytes())
            BundledSQLiteDriver().open(path).use { db ->
                assertEquals(format, db.queryLong("PRAGMA user_version"))
            }
        }
    }

    /** KILL: refuse only formats below this build's — a newer layout is rewritten by a build that cannot read it. */
    @Test
    fun aStoreOfANewerFormatIsRefusedUntouched() {
        val path = Fixture.path("newer")
        ReplicaStateStore(path).close()
        BundledSQLiteDriver().open(path).use { db ->
            assertEquals(3L, db.queryLong("PRAGMA user_version"))
            db.exec("PRAGMA user_version = 4")
        }
        val bytes = File(path).readBytes()

        val error = kotlin.test.assertFailsWith<ReplicaError.Storage> { Fixture.storeAt(path) }

        assertTrue(error.reason.contains("upgrade required"), error.reason)
        assertContentEquals(bytes, File(path).readBytes())
        BundledSQLiteDriver().open(path).use { db ->
            assertEquals(4L, db.queryLong("PRAGMA user_version"))
        }
    }

    /** KILL: bump the sequence outside the checkpoint transaction — a rollback leaves it moved. */
    @Test
    fun checkpointResetAndRollbackMoveOnlyCommittedStreamSequences() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one")), cursor = "5:", more = false)
        )
        engine.pullOnce("user")
        val afterCheckpoint = sequence(store, "notes")
        val untouchedAssetSequence = sequence(store, "assets")
        engine.resetCursors()

        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = emptyList(), cursor = "6:", more = false)
        )
        engine.pullOnce("user")
        val afterReset = sequence(store, "notes")
        assertTrue(afterReset > afterCheckpoint)
        assertEquals(untouchedAssetSequence, sequence(store, "assets"))

        engine.setCheckpointFault { throw Fault() }
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n2", "rolled back")), cursor = "7:", more = false)
        )
        try {
            engine.pullOnce("user")
            fail("the faulted checkpoint unexpectedly committed")
        } catch (_: Fault) {
            // Expected.
        }
        assertEquals(afterReset, sequence(store, "notes"))

        engine.setCheckpointFault(null)
        transport.queuePull(
            "user",
            ReplicaPullResponse(frames = listOf(Fixture.note("n2", "restored")), cursor = "7:", more = false)
        )
        engine.pullOnce("user")
        assertTrue(sequence(store, "notes") > afterReset)
    }

    /** KILL: skip the bump in `deleteSnapshot` — a local delete never wakes a watcher. */
    @Test
    fun localDeleteAndRejectedCreateRevertAdvanceTheSequence() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("one")))
        val afterCreate = sequence(store, "notes")
        engine.deleteRow("notes", "n1")
        val afterDelete = sequence(store, "notes")
        assertTrue(afterDelete > afterCreate)

        transport.scriptPush { ops ->
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "refused") }
        }
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("two")))
        val beforeRevert = sequence(store, "notes")
        engine.drain()

        assertNull(store.peekSnapshot("notes", "n2"))
        assertTrue(sequence(store, "notes") > beforeRevert)
    }

    private suspend fun pullBoard(transport: StubTransport, engine: ReplicaEngine) {
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", "stub@1", "SNAP".toByteArray(), emptyMap())
        ), cursor = "5:", more = false))
        engine.pullOnce("user")
    }

    /**
     * A rebuild writes the document alone, so its upsert is the only bump.
     * KILL: skip the bump in `upsertDoc` — a rebuilt document never wakes `watchDoc`.
     * KILL: skip the bump in `updateDoc` — a pulled delta never wakes `watchDoc`.
     */
    @Test
    fun documentUpsertUpdateAndDeleteAdvanceTheSequence() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        pullBoard(transport, engine)
        val beforeUpsert = sequence(store, "boards")
        engine.rebuildDocument("boards", "b1", "REBUILT".toByteArray(), 999uL)
        val afterUpsert = sequence(store, "boards")
        assertTrue(afterUpsert > beforeUpsert)
        engine.recordDocDelta("boards", "b1", "+delta".toByteArray())
        val afterUpdate = sequence(store, "boards")
        assertTrue(afterUpdate > afterUpsert)
        engine.resyncDocument("boards", "b1")
        assertTrue(sequence(store, "boards") > afterUpdate)
    }

    private fun sequence(store: ReplicaStateStore, stream: String): Long = store.read { db ->
        db.queryLong("SELECT change_seq FROM stream_meta WHERE stream = ?", listOf(stream)) ?: 0L
    }
}
