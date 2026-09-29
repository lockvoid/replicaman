package io.replicaman

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.eventually
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The two barriers the identity transition stands on: the wire-operation
 * ledger `seal()` waits out, and the per-lane cold check the pull barrier
 * consults. Both are places the actor → dispatcher substitution changed the
 * cancellation and re-evaluation semantics Swift gave for free.
 */
class EngineBarrierTests : ReplicaTestCase() {

    /**
     * Swift's `pullPage` releases its wire operation from a `defer`, which
     * runs on task cancellation too. The Kotlin `finally` hops back with
     * `withContext(engineContext)` — and a `withContext` inside a CANCELLED
     * coroutine throws before it runs the body, so `endWireOperation()` never
     * fires and `activeWireOperations` is stuck above zero forever.
     *
     * KILL: `pullPage`'s `finally` — a cancelled pull must still balance the
     * ledger, or `seal()` never returns and sign-out/merge wedge.
     */
    @Test
    fun aCancelledPullStillReleasesItsWireOperation() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val held = CompletableDeferred<Unit>()
        transport.onPull { held.await() }
        val engine = Fixture.engine(store = store, transport = transport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        val pull = scope.launch { engine.pullOnce("user") }
        eventually(5.seconds, "the pull never reached the wire") { transport.pullCount() == 1 }
        pull.cancelAndJoin()

        val sealReturned = withContext(Dispatchers.Default) {
            withTimeoutOrNull(3.seconds) { engine.seal() } != null
        }
        scope.cancel()
        assertTrue(
            sealReturned,
            "seal() never returned: the cancelled pull left activeWireOperations above zero"
        )
    }

    @Test
    fun aFailedPrefixIsNotRetriedThroughTheOtherLane() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.lane(ReplicaLane.INTERACTIVE) {
            engine.saveRow("notes", "urgent", null, mapOf("title" to ReplicaValue.Str("a tap")))
        }
        engine.saveRow("notes", "background", null, mapOf("title" to ReplicaValue.Str("an import")))
        transport.failPushes(true)

        engine.drainIfWarm()

        assertEquals(1, transport.pushCount())
        assertEquals(2, engine.pendingOps().size)
        assertTrue(engine.isColdForTesting(ReplicaLane.INTERACTIVE))
        assertTrue(engine.isColdForTesting(ReplicaLane.BULK))
        assertNotNull(engine.health.failure.value)
    }
}
