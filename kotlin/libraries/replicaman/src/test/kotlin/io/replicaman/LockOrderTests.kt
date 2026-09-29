package io.replicaman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekSnapshot
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LockOrderTests : ReplicaTestCase() {
    /**
     * Hold document publication, make the real delete wait for it, then require
     * the real SQLite writer to be available. Evicting from inside store.write
     * instead creates the publication -> writer -> publication cycle.
     */
    @Test
    fun aDocumentDeleteNeverWaitsOnHeldDocumentsInsideItsWrite(): Unit = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store, transport = StubTransport())
        engine.createDoc("boards", "b1", "seed".toByteArray(), 7uL)
        // Read the existing lock only to establish that deletion has reached
        // publication. No timing guess, replacement lock, or fake DB response.
        val publicationLock = LiveDocuments::class.java.getDeclaredField("lock").let {
            it.isAccessible = true
            it.get(engine.liveDocuments) as ReentrantLock
        }
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val writer = Executors.newSingleThreadExecutor()
        var deletion: Deferred<Boolean>? = null
        try {
            engine.liveDocuments.publishing {
                deletion = scope.async { engine.deleteRow("boards", "b1") }
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
                while (!publicationLock.hasQueuedThreads() && System.nanoTime() < deadline) Thread.sleep(5)
                assertTrue(publicationLock.hasQueuedThreads(), "the real delete never reached document publication")
                writer.submit {
                    store.write { db -> db.exec("INSERT INTO stream_meta(stream, change_seq) VALUES ('lock-order-probe', 1)") }
                }.get(3, TimeUnit.SECONDS)
            }
            withTimeout(5_000) { deletion!!.await() }
            assertNull(store.peekSnapshot("boards", "b1"))
            assertNull(engine.docRow("boards", "b1"))
        } finally {
            // The publication lock is already released on assertion failure,
            // so even a lock-order mutant can finish before fixture teardown.
            deletion?.let { withTimeout(5_000) { it.await() } }
            scope.cancel()
            writer.shutdown()
            assertTrue(writer.awaitTermination(5, TimeUnit.SECONDS), "the SQLite probe must join")
        }
    }
}
