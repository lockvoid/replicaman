package io.replicaman

import androidx.sqlite.SQLiteConnection
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * GRDB's pool contract, which the store owes whatever sits under it:
 * `write` is one transaction that ends CLOSED whichever way it ends, `read`
 * is a read-only snapshot and is not reentrant, `close` waits for the
 * readers it is closing, and a `move` that cannot carry the whole store
 * says so.
 */
class StoreContractTests : ReplicaTestCase() {
    private fun put(store: ReplicaStateStore, db: SQLiteConnection, id: String) {
        store.upsertSnapshot(db, "notes", id, "user", null, mapOf("t" to ReplicaValue.Str(id)))
    }

    /**
     * KILL: move `writeConnection.exec("COMMIT")` out of the guarded `try`
     * in `write` (back above the `catch`). A deferred constraint fails AT
     * commit with the transaction still open, so the connection is left
     * mid-transaction at depth 1 and every later write joins it and never
     * lands.
     */
    @Test
    fun aFailedCommitRollsBackAndTheStoreKeepsWriting() {
        val store = Fixture.store("commit-fail")
        store.write { db ->
            db.exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            db.exec(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER " +
                    "REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
        }
        val before = store.commits.value

        val thrown = runCatching {
            store.write { db -> db.exec("INSERT INTO child (id, pid) VALUES (1, 999)") }
        }.exceptionOrNull()
        assertTrue(thrown != null, "a deferred violation must surface at commit")

        store.write { db -> put(store, db, "n1") }

        assertEquals(before + 1, store.commits.value, "the next write commits and ticks")
        assertEquals(
            1L, store.read { it.queryLong("SELECT COUNT(*) FROM snapshots") },
            "the next write is durable, not trapped in the failed transaction"
        )
        assertEquals(
            0L, store.read { it.queryLong("SELECT COUNT(*) FROM child") },
            "the failed transaction is rolled back whole"
        )
    }

    /**
     * KILL: drop the `reading` re-entrancy check from `read`. GRDB answers a
     * nested read with a precondition; a pool of N answers it with a hang at
     * depth N + 1.
     */
    @Test
    fun aNestedReadIsRefusedInsteadOfBlocking() {
        val store = Fixture.store("reentrant")
        val outcome = AtomicReference("no error")
        val worker = Thread {
            try {
                store.read { _ -> store.read { _ -> store.read { _ -> } } }
            } catch (error: Throwable) {
                outcome.set(error.message ?: error.javaClass.simpleName)
            }
        }
        worker.isDaemon = true
        worker.start()
        worker.join(5_000)
        assertFalse(worker.isAlive, "a nested read must not block")
        assertEquals("storage: reads are not reentrant", outcome.get())
    }

    /** KILL: drop the `BEGIN DEFERRED` / `COMMIT` pair from `read`. */
    @Test
    fun aReadBlockSeesOneSnapshot() {
        val store = Fixture.store("snapshot")
        val entered = CountDownLatch(1)
        val committed = CountDownLatch(1)
        val writer = Thread {
            entered.await(5, TimeUnit.SECONDS)
            store.write { db -> put(store, db, "n1") }
            committed.countDown()
        }
        writer.isDaemon = true
        writer.start()

        val seen = store.read { db ->
            val first = db.queryLong("SELECT COUNT(*) FROM snapshots")
            entered.countDown()
            committed.await(5, TimeUnit.SECONDS)
            first to db.queryLong("SELECT COUNT(*) FROM snapshots")
        }
        assertEquals(seen.first, seen.second, "a concurrent commit is invisible inside the block")
        assertEquals(
            1L, store.read { it.queryLong("SELECT COUNT(*) FROM snapshots") },
            "the next block sees it"
        )
    }

    /**
     * KILL: open the readers with `driver.open(path)` instead of
     * `driver.open(path, SQLITE_OPEN_READONLY)`. The read plane is the app's
     * half of "app code reads, only the engine writes" — an unguarded reader
     * writes outside the write lock, outside any transaction, and raises no
     * commit tick, so no watcher and no row cache ever learns.
     */
    @Test
    fun aReadConnectionCannotWrite() {
        val store = Fixture.store("readonly")
        store.write { db -> put(store, db, "n1") }
        val refused = runCatching { store.read { db -> db.exec("DELETE FROM snapshots") } }
        assertTrue(refused.isFailure, "a read connection is read-only")
        assertEquals(1L, store.read { it.queryLong("SELECT COUNT(*) FROM snapshots") })
    }

    /**
     * KILL: replace the `List(READER_COUNT) { readers.take() }` barrier in
     * `close` with `readers.clear()`. The bundled driver builds sqlite in
     * MULTI-THREAD mode: closing a connection another thread is stepping is
     * not an error, it is undefined.
     */
    @Test
    fun closeWaitsForAnInFlightRead() {
        val store = Fixture.store("close-barrier")
        store.write { db -> put(store, db, "n1") }
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val closed = CountDownLatch(1)
        val outcome = AtomicReference("never ran")
        val reader = Thread {
            outcome.set(
                runCatching {
                    store.read { db ->
                        entered.countDown()
                        release.await(5, TimeUnit.SECONDS)
                        db.queryLong("SELECT COUNT(*) FROM snapshots")
                    }
                    "read completed"
                }.getOrElse { "${it.javaClass.simpleName}: ${it.message}" }
            )
        }
        reader.isDaemon = true
        reader.start()
        entered.await(5, TimeUnit.SECONDS)

        val closer = Thread {
            store.close()
            closed.countDown()
        }
        closer.isDaemon = true
        closer.start()
        assertFalse(
            closed.await(500, TimeUnit.MILLISECONDS),
            "close() returned while a read still held a connection"
        )

        release.countDown()
        assertTrue(closed.await(5, TimeUnit.SECONDS), "close() never completed")
        reader.join(5_000)
        assertEquals("read completed", outcome.get())
    }

    /**
     * KILL: restore the `?: ByteArray(0)` fallback in `DocRow.equals` /
     * `JournalRow.equals`. An empty blob then equals a missing one in one
     * direction only, which is an `equals` no collection can trust.
     */
    @Test
    fun anEmptyBlobIsNotAMissingOne() {
        val emptyAcked = ReplicaStateStore.DocRow("s", "r", "c", ByteArray(0), ByteArray(0), 1uL)
        val noAcked = ReplicaStateStore.DocRow("s", "r", "c", ByteArray(0), null, 1uL)
        assertNotEquals(emptyAcked, noAcked)
        assertEquals(emptyAcked == noAcked, noAcked == emptyAcked)

        val emptyPreimage = ReplicaStateStore.JournalRow("i", "v", ByteArray(0), ByteArray(0))
        val noPreimage = ReplicaStateStore.JournalRow("i", "v", ByteArray(0), null)
        assertNotEquals(emptyPreimage, noPreimage)
        assertEquals(emptyPreimage == noPreimage, noPreimage == emptyPreimage)
    }
}
