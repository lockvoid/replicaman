package io.replicaman

import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * GRDB owned the pool on iOS; here it is `ReplicaStateStore`'s own, so its
 * failure modes are ours to pin. Nothing in the ported suite exercises the
 * read pool under saturation or the write lock under contention.
 */
class StoreConcurrencyTests : ReplicaTestCase() {

    /**
     * A read taken while another is held on the SAME thread must not wait for
     * itself, and 16 threads over a pool of 4 must all finish.
     *
     * KILL: size the reader pool at 1 (`READER_COUNT = 1` in
     * `ReplicaStateStore`) — the nested read waits for the connection its own
     * caller is holding and this test times out.
     */
    @Test
    fun readPoolSurvivesReentrantAndSaturatedReaders() {
        val store = Fixture.store()
        store.write { db ->
            store.upsertSnapshot(db, "notes", "n1", "user", null, mapOf("t" to ReplicaValue.Str("x")))
        }

        val nested = thread { store.read { store.read { store.read { store.read { } } } } }
        nested.join(10_000)
        assertTrue(!nested.isAlive, "a re-entrant read deadlocked on the connection its caller holds")

        val start = CountDownLatch(1)
        val done = CountDownLatch(16)
        repeat(16) {
            thread {
                start.await()
                repeat(20) { store.read { db -> db.queryLong("SELECT COUNT(*) FROM snapshots") } }
                done.countDown()
            }
        }
        start.countDown()
        assertTrue(done.await(30, TimeUnit.SECONDS), "the read pool deadlocked under saturation")
    }

    /**
     * The store is the raw escape hatch, so its write lock — not the engine's
     * dispatcher — is what keeps one direct writer's transaction its own.
     * `write()` re-enters (`transactionDepth > 0` runs the body bare), so
     * without the lock a second thread does not fail — it silently JOINS the
     * first thread's open transaction and dies with it on rollback.
     *
     * Here writer 0 opens a transaction, writes, and throws. Fifteen others
     * pile in behind it. Every one of the fifteen must land; only writer 0's
     * row rolls back.
     *
     * KILL: make `writeLock.withLock` a no-op in `ReplicaStateStore.write`
     * — the fifteen ride writer 0's transaction into the ROLLBACK and the
     * surviving row count collapses.
     */
    @Test
    fun theWriteLockKeepsOneWritersRollbackToItself() {
        val store = Fixture.store()
        val inside = CountDownLatch(1)
        val piled = CountDownLatch(15)
        val failures = ConcurrentLinkedQueue<String>()

        val thrower = thread {
            try {
                store.write { db ->
                    store.upsertSnapshot(db, "notes", "r0", "user", null, mapOf("t" to ReplicaValue.Str("0")))
                    inside.countDown()
                    // With the lock held nobody can pile in — this waits out
                    // its budget and the rollback stays private either way.
                    piled.await(300, TimeUnit.MILLISECONDS)
                    throw ReplicaError.Storage("writer 0 refuses")
                }
            } catch (_: Throwable) {
                // Expected: writer 0's transaction rolls back.
            }
        }

        val others = (1..15).map { index ->
            thread {
                inside.await()
                piled.countDown()
                try {
                    store.write { db ->
                        store.upsertSnapshot(
                            db, "notes", "r$index", "user", null,
                            mapOf("t" to ReplicaValue.Str("$index"))
                        )
                    }
                } catch (error: Throwable) {
                    failures.add("$error")
                }
            }
        }
        thrower.join(30_000)
        others.forEach { it.join(30_000) }

        assertTrue(failures.isEmpty(), "a bystander write was refused: ${failures.toList().take(3)}")
        val landed = store.read { db -> db.queryLong("SELECT COUNT(*) FROM snapshots") }
        assertEquals(15L, landed, "writer 0's rollback took bystanders with it")
    }
}
