package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.testing.FixtureTransport
import io.replicaman.testing.ProtocolFixture

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.eventually
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * The identity boundary lives inside ReplicaEngine because generated CRUD
 * schedules transport without passing back through the app host. These tests
 * hold real engine flights at the transport seam: timing sleeps cannot prove
 * that the seal waited, or that an automatic push was covered.
 */
class IdentityTransitionFenceTests : ReplicaTestCase() {

    /**
     * KILL: return from `seal()` without awaiting `activeWireOperations` —
     * the merge moves the store while an outgoing push is still applying.
     */
    @Test
    fun transitionWaitsForAutomaticPushAndRejectsPostWipeWritesUntilResume() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val pushGate = AsyncGate()
        transport.onPush { pushGate.arriveAndWait() }
        val engine = Fixture.engine(
            store = store, transport = transport, automaticallyPushWrites = true
        )
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        engine.saveRow(
            "notes", "outgoing", null,
            mapOf("title" to ReplicaValue.Str("must finish under outgoing bearer"))
        )
        pushGate.waitUntilArrived()

        val transitionFinished = AsyncFlag()
        val transition = scope.async {
            engine.seal()
            transitionFinished.set()
        }
        eventually(5.seconds, "engine never closed identity admissions") { engine.sealed }

        assertFalse(
            transitionFinished.value,
            "seal() returned while an automatic push was still on the wire"
        )
        assertIdentityTransitionRejects {
            engine.saveRow(
                "notes", "late-before-wipe", null,
                mapOf("title" to ReplicaValue.Str("must not enter outgoing journal"))
            )
        }
        assertNull(store.peekSnapshot("notes", "late-before-wipe"))

        pushGate.open()
        transition.await()
        assertTrue(store.peekPending().isEmpty(), "the outgoing flight must settle before the boundary opens")

        engine.unseal()
        engine.saveRow(
            "notes", "replacement", null,
            mapOf("title" to ReplicaValue.Str("more work under the same owner"))
        )
        eventually(5.seconds, "automatic delivery did not resume after the seal lifted") {
            transport.pushCount() == 2 && store.peekPending().isEmpty()
        }
        scope.cancel()
    }

    /**
     * KILL: count only pushes as wire operations — the seal returns before an
     * outgoing pull's response has committed locally.
     */
    @Test
    fun transitionWaitsUntilActivePullResponseIsAppliedAndBlocksAnotherPull() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val pullGate = AsyncGate()
        transport.onPull { pullGate.arriveAndWait() }
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(Fixture.note("outgoing", "old identity response")),
                cursor = "1:", more = false
            )
        )
        val engine = Fixture.engine(store = store, transport = transport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        val pull = scope.async { engine.pullOnce("user") }
        pullGate.waitUntilArrived()

        val transitionFinished = AsyncFlag()
        val transition = scope.async {
            engine.seal()
            transitionFinished.set()
        }
        eventually(5.seconds, "engine never closed identity admissions") { engine.sealed }
        assertFalse(
            transitionFinished.value,
            "seal() returned before the outgoing pull response settled"
        )

        pullGate.open()
        assertEquals(1, pull.await())
        transition.await()
        assertEquals(
            ReplicaValue.Str("old identity response"),
            store.peekSnapshot("notes", "outgoing")?.data?.get("title"),
            "the boundary returned before the already-started response committed locally"
        )

        // A NEW pull while sealed answers zero and never touches the wire.
        val pullsBefore = transport.pullCount()
        assertEquals(0, engine.pullOnce("user"))
        assertEquals(pullsBefore, transport.pullCount(), "a sealed engine must stay off the wire for pulls")

        engine.unseal()
        transport.queuePull("user", ReplicaPullResponse(frames = emptyList(), cursor = "2:", more = false))
        assertEquals(0, engine.pullOnce("user"))
        scope.cancel()
    }

    /**
     * KILL: let `sealAndDrain` use the engine's own transport — the frozen
     * journal rides live credentials the sign-out has already revoked.
     */
    @Test
    fun pinnedSourceDrainSealsFirstRejectsConcurrentCrudAndIsReplaySafe() = runTest {
        val store = Fixture.store()
        val ordinaryTransport = StubTransport()
        val pinnedSourceTransport = StubTransport()
        val pushGate = AsyncGate()
        pinnedSourceTransport.onPush { pushGate.arriveAndWait() }
        val engine = Fixture.engine(store = store, transport = ordinaryTransport)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        engine.saveRow(
            "notes", "outgoing", null,
            mapOf("title" to ReplicaValue.Str("must use pinned source wire"))
        )

        val drain = scope.async { engine.sealAndDrain(pinnedSourceTransport) }
        pushGate.waitUntilArrived()

        assertEquals(
            0, ordinaryTransport.pushCount(),
            "the engine's ordinary/live transport must never carry the frozen source journal"
        )
        assertEquals(1, pinnedSourceTransport.pushCount())
        assertIdentityTransitionRejects {
            engine.saveRow(
                "notes", "late", null,
                mapOf("title" to ReplicaValue.Str("must not arrive after drain snapshot"))
            )
        }
        assertNull(store.peekSnapshot("notes", "late"))

        pushGate.open()
        assertEquals(1, drain.await().size)
        assertTrue(store.peekPending().isEmpty())
        assertTrue(engine.sealed)

        assertTrue(engine.sealAndDrain(pinnedSourceTransport).isEmpty())
        assertEquals(
            1, pinnedSourceTransport.pushCount(),
            "recovery must not resend an entry whose accepted verdict already committed"
        )
        scope.cancel()
    }

    /**
     * KILL: drop the `activeDrains.remove(...)` from the `performDrain`
     * failure path — a cancelled flush wedges the lane and the retry hangs.
     */
    @Test
    fun pinnedSourceDrainReleasesSingleFlightAfterCancellationError() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.saveRow(
            "notes", "retry-after-cancel", null, mapOf("title" to ReplicaValue.Str("still pending"))
        )
        engine.seal()

        assertFailsWith<CancellationException> {
            engine.sealAndDrain(CancellationTransport())
        }
        assertEquals(1, store.peekPending().size)

        val retryTransport = StubTransport()
        assertEquals(1, engine.sealAndDrain(retryTransport).size)
        assertTrue(store.peekPending().isEmpty())
        assertEquals(1, retryTransport.pushCount())
    }

    /**
     * Pulls WRITE — checkpoint rows and cursors land in the store. A merge
     * rebinds and moves that store while sealed, so a pull that slips in
     * mid-seal races the swap exactly the way local writes would.
     *
     * KILL: drop the `sealed` check from `pullOnce`/`pullUntilCaughtUp`.
     */
    @Test
    fun aSealedEngineStaysOffTheWireForPulls() = runTest {
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(Fixture.directory(), transport)
        engine.open(606)
        engine.seal()

        assertEquals(0, engine.pullUntilCaughtUp())
        assertEquals(0, transport.pullCount(), "a sealed engine must not touch the wire for pulls")

        engine.unseal()
        engine.pullOnce("user")
        assertTrue(transport.pullCount() > 0, "unseal re-admits the wire")
    }

    private suspend fun assertIdentityTransitionRejects(operation: suspend () -> Unit) {
        try {
            operation()
            fail("operation crossed a paused identity boundary")
        } catch (_: ReplicaError.IdentityTransitionInProgress) {
            // Expected: deterministic retryable admission refusal.
        } catch (error: Throwable) {
            fail("unexpected boundary error: $error")
        }
    }

    private class AsyncFlag {
        @Volatile
        var value: Boolean = false
            private set

        fun set() {
            value = true
        }
    }

    private class CancellationTransport : FixtureTransport {
    override val protocolFixture = ProtocolFixture()
        override suspend fun pull(shard: String, cursor: String?, limit: Int): ReplicaPullResponse =
            throw CancellationException("external source request cancelled")

        override suspend fun push(ops: List<ReplicaOp>): List<ReplicaVerdict> =
            throw CancellationException("external source request cancelled")
    }

    private class AsyncGate {
        private val arrived = CompletableDeferred<Unit>()
        private val opened = CompletableDeferred<Unit>()

        suspend fun arriveAndWait() {
            arrived.complete(Unit)
            opened.await()
        }

        suspend fun waitUntilArrived() {
            arrived.await()
        }

        fun open() {
            opened.complete(Unit)
        }
    }
}
