package io.replicaman

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.*

/** Android coroutine admission: raw owner stores grade writes after inference/dispatcher suspension. */
class LocalSessionTests : ReplicaTestCase() {
    private suspend fun write(engine: ReplicaEngine, id: String) = engine.saveRow("notes", id, null, mapOf("title" to ReplicaValue.Str(id)))
    private fun rowCount(engine: ReplicaEngine) = engine.store!!.read { it.queryLong("SELECT COUNT(*) FROM snapshots") }

    /** KILL: validate only on entry — a suspended old-world producer writes into B on resume. */
    @Test fun aSuspendedProducerCannotWriteIntoTheReplacementWorld() = runTest {
        val engine = Fixture.unopenedEngine(Fixture.directory(), StubTransport()); engine.open(1)
        val admission = engine.captureLocalSession()
        val entered = CompletableDeferred<Unit>(); val resume = CompletableDeferred<Unit>()
        val oldWork = async {
            assertFailsWith<ReplicaError.StaleLocalSession> {
                engine.withLocalSession(admission) { entered.complete(Unit); resume.await(); write(engine, "old-result") }
            }
        }
        entered.await(); engine.close(); engine.open(2); resume.complete(Unit); oldWork.await()
        assertEquals(0L, rowCount(engine))
        assertTrue(engine.pendingOps().isEmpty())
    }

    /** KILL: check only owner id — A → B → A would admit work from A's retired binding. */
    @Test fun returningToTheSameOwnerDoesNotReviveAnOldAdmission() = runTest {
        val engine = Fixture.unopenedEngine(Fixture.directory(), StubTransport()); engine.open(1)
        val admission = engine.captureLocalSession(); engine.close(); engine.open(2); engine.close(); engine.open(1)
        assertFailsWith<ReplicaError.StaleLocalSession> { engine.withLocalSession(admission) { write(engine, "stale") } }
        assertEquals(0L, rowCount(engine))
    }

    /** KILL: omit engine identity — a token from a different replica with matching generation is accepted. */
    @Test fun aDifferentEngineCannotUseTheAdmission() = runTest {
        val first = Fixture.engine(Fixture.store(), transport = StubTransport())
        val second = Fixture.engine(Fixture.store(), transport = StubTransport())
        assertFailsWith<ReplicaError.StaleLocalSession> { second.withLocalSession(first.captureLocalSession()) { write(second, "wrong-engine") } }
        assertEquals(0L, rowCount(second))
    }

    /** KILL: guard only row verbs — a late tool could create its document inside B. */
    @Test fun documentWritesUseTheSameAdmissionAfterASuspension() = runTest {
        val engine = Fixture.unopenedEngine(Fixture.directory(), StubTransport()); engine.open(1)
        val admission = engine.captureLocalSession()
        engine.withLocalSession(admission) {
            engine.close(); engine.open(2)
            assertFailsWith<ReplicaError.StaleLocalSession> { engine.createDoc("boards", "wrong-doc", "SEED".toByteArray(), 7uL) }
        }
        assertEquals(0L, engine.store!!.read { it.queryLong("SELECT COUNT(*) FROM docs") })
    }

    /** KILL: admit after the IO hop — seal overtakes a queued write and its outgoing row is lost. */
    @Test fun anAdmittedAsyncWriteKeepsItsOriginalOwnerWhileWaitingForTheWriter() = runTest {
        val engine = Fixture.unopenedEngine(Fixture.directory(), StubTransport()); engine.open(1)
        val admission = engine.captureLocalSession()
        val held = CompletableDeferred<Unit>(); val release = CountDownLatch(1)
        val source = engine.store!!
        val blocker = async(Dispatchers.IO) { source.write {
            held.complete(Unit); check(release.await(5, TimeUnit.SECONDS)) { "test did not release SQLite writer" }
        } }
        held.await()
        try {
            engine.withLocalSession(admission) {
                coroutineScope {
                    val writing = async(start = CoroutineStart.UNDISPATCHED) {
                        engine.writeAsync { it.rows(TestNote).create(TestNote("admitted", "owner A")) }
                    }
                    val closing = async(Dispatchers.Default) { engine.close() }
                    eventually(3.seconds, "close must seal before waiting for the admitted write") { engine.isSealed }
                    assertFalse(closing.isCompleted)
                    release.countDown(); writing.await(); closing.await()
                }
            }
        } finally { release.countDown() }
        blocker.await()
        engine.open(2); assertEquals(0L, rowCount(engine))
        engine.close(); engine.open(1)
        assertEquals("owner A", RowStream(engine, TestNote).find("admitted")?.title)
    }

    /** KILL: leak the coroutine-local scope — subsequent valid owner work inherits the expired token. */
    @Test fun leavingAFailedScopeRestoresNormalAuthoring() = runTest {
        val engine = Fixture.unopenedEngine(Fixture.directory(), StubTransport()); engine.open(1)
        val admission = engine.captureLocalSession()
        assertFailsWith<IllegalStateException> { engine.withLocalSession(admission) { error("tool failed") } }
        engine.close(); engine.open(2); write(engine, "current")
        assertEquals(1L, rowCount(engine))
    }
}
