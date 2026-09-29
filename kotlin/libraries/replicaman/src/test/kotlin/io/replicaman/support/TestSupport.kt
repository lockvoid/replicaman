package io.replicaman.support

import io.replicaman.testing.FixtureTransport
import io.replicaman.testing.ProtocolFixture
import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.testing.fixtureSynchronized

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.withTimeout
import org.junit.After
import io.replicaman.ReplicaCodec
import io.replicaman.ReplicaEngine
import io.replicaman.ReplicaFrame
import io.replicaman.ReplicaIndexSpec
import io.replicaman.ReplicaOp
import io.replicaman.ReplicaSchema
import io.replicaman.query
import io.replicaman.textOrNull
import io.replicaman.ReplicaStateStore
import io.replicaman.ReplicaStreamSpec
import io.replicaman.ReplicaTransport
import io.replicaman.ReplicaValue
import io.replicaman.ReplicaVerdict
import io.replicaman.ReplicaWritableRowModel
import io.replicaman.ReplicaWritableRowModelType
import io.replicaman.ReplicaNoField
import io.replicaman.SyncGate
import io.replicaman.SyncGateSignal
import io.replicaman.SyncChange
import io.replicaman.ReplicaStamp
import io.replicaman.ReplicaMerge
import io.replicaman.ReplicaReflection
import io.replicaman.SyncVerdict
import java.io.File
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass
import kotlin.test.fail
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// MARK: - Stub transport

/**
 * Canned wire: scripted pull responses per shard, scripted push verdicts,
 * injectable failures, and a full event log — matrix assertions about
 * ORDER (drain-before-pull) and COUNT (cold window, nudge coalescing) read
 * the log.
 *
 * The iOS actor becomes one `limitedParallelism(1)` dispatcher, so awaiting
 * a hook INSIDE `push`/`pull` releases the transport exactly as actor
 * reentrancy did — which is what makes the mid-flight seams observable.
 */
class StubTransport(dataset: String = ProtocolFixture.DATASET) : FixtureTransport {
    override val protocolFixture = ProtocolFixture(dataset)
    sealed interface Event {
        data class Pull(val shard: String, val cursor: String?) : Event

        data class Push(val ids: List<String>) : Event
    }

    @Suppress("OPT_IN_USAGE")
    private val context = Dispatchers.Default.limitedParallelism(1)

    private val eventLog = mutableListOf<Event>()
    private val batches = mutableListOf<List<ReplicaOp>>()
    private val pullQueues = mutableMapOf<String, MutableList<ReplicaPullResponse>>()
    private val pullRefusals = mutableMapOf<String, MutableList<String>>()
    private var pushScript: ((List<ReplicaOp>) -> List<ReplicaVerdict>)? = null
    private var pullFails = false
    private var pushFails = false
    private var pushSuccessBudget: Int? = null
    private var pullDelay: Duration = Duration.ZERO
    private var pushDelay: Duration = Duration.ZERO
    private var pushHook: (suspend (List<ReplicaOp>) -> Unit)? = null
    private var pullHook: (suspend (String) -> Unit)? = null

    suspend fun queuePull(shard: String, response: ReplicaPullResponse) = withContext(context) {
        pullQueues.getOrPut(shard) { mutableListOf() }.add(response)
    }

    suspend fun scriptPush(script: (List<ReplicaOp>) -> List<ReplicaVerdict>) = withContext(context) {
        pushScript = script
    }

    /** The next pull of `shard` is refused with `code`, as the server refuses a cursor it cannot read. */
    suspend fun refuseNextPull(shard: String, code: String) = withContext(context) {
        pullRefusals.getOrPut(shard) { mutableListOf() }.add(code)
    }

    suspend fun failPulls(fail: Boolean) = withContext(context) { pullFails = fail }

    suspend fun failPushes(fail: Boolean) = withContext(context) { pushFails = fail }

    /**
     * Succeed the first `calls` pushes, then fail — the chunked-drain
     * mid-flight transport death.
     */
    suspend fun failPushesAfter(calls: Int) = withContext(context) { pushSuccessBudget = calls }

    /**
     * Awaited inside `push` BEFORE verdicts return — the observation seam
     * for "what had already happened when this chunk went on the wire"
     * (the incremental-ack contract: earlier chunks' entries must be gone
     * from the journal by now).
     */
    suspend fun onPush(hook: suspend (List<ReplicaOp>) -> Unit) = withContext(context) {
        pushHook = hook
    }

    /**
     * Awaited inside `pull` after the request is observable but before its
     * response is returned. Identity-boundary tests use it to hold one
     * outgoing-bearer request on the wire deterministically.
     */
    suspend fun onPull(hook: suspend (String) -> Unit) = withContext(context) { pullHook = hook }

    suspend fun delayPulls(duration: Duration) = withContext(context) { pullDelay = duration }

    suspend fun delayPushes(duration: Duration) = withContext(context) { pushDelay = duration }

    suspend fun events(): List<Event> = withContext(context) { eventLog.toList() }

    suspend fun pushedBatches(): List<List<ReplicaOp>> = withContext(context) { batches.toList() }

    suspend fun pullCount(): Int =
        withContext(context) { eventLog.count { it is Event.Pull } }

    suspend fun pushCount(): Int =
        withContext(context) { eventLog.count { it is Event.Push } }

    override suspend fun pull(shard: String, cursor: String?, limit: Int): ReplicaPullResponse =
        withContext(context) {
            eventLog.add(Event.Pull(shard, cursor))
            if (pullFails) throw io.replicaman.ReplicaError.Transport("pull refused (stub)")
            pullRefusals[shard]?.removeFirstOrNull()?.let { throw io.replicaman.ReplicaError.Protocol(it, "HTTP 409: $it") }
            pullHook?.invoke(shard)
            if (pullDelay > Duration.ZERO) delay(pullDelay)
            val queue = pullQueues[shard]
            if (queue != null && queue.isNotEmpty()) {
                queue.removeAt(0)
            } else {
                ReplicaPullResponse(frames = emptyList(), cursor = cursor ?: "0:", more = false)
            }
        }

    override suspend fun push(ops: List<ReplicaOp>): List<ReplicaVerdict> = withContext(context) {
        eventLog.add(Event.Push(ops.map { it.id }))
        if (pushFails) throw io.replicaman.ReplicaError.Transport("push refused (stub)")
        val budget = pushSuccessBudget
        if (budget != null) {
            if (budget <= 0) {
                throw io.replicaman.ReplicaError.Transport("push budget exhausted (stub)")
            }
            pushSuccessBudget = budget - 1
        }
        pushHook?.invoke(ops)
        batches.add(ops.toList())
        if (pushDelay > Duration.ZERO) delay(pushDelay)
        pushScript?.invoke(ops)
            ?: ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.ACCEPTED) }
    }
}

// MARK: - Stub codec (loro-free doc plumbing)

/**
 * Enough codec to exercise the ENGINE's document plumbing without loro:
 * fold = concatenated payloads, version = byte count. The real merge
 * semantics live in the `:replicaman-loro` suite; this one keeps the core
 * suite rows-only (matrix 12).
 */
class StubCodec : ReplicaCodec {
    override val name: String = "stub@1"

    override fun merge(fold: ByteArray?, payload: ByteArray, reflections: List<ReplicaReflection>): ReplicaMerge =
        ReplicaMerge((fold ?: ByteArray(0)) + payload, emptyMap())

    override fun diff(fold: ByteArray, since: ByteArray?): ByteArray {
        val skip = since?.let { String(it).toIntOrNull() ?: 0 } ?: 0
        return fold.drop(skip).toByteArray()
    }

    override fun version(fold: ByteArray): ByteArray = "${fold.size}".toByteArray()

    override fun payloadVersion(payload: ByteArray): ByteArray = "${payload.size}".toByteArray()

    override fun mergeVersions(a: ByteArray?, b: ByteArray): ByteArray {
        val x = a?.let { String(it).toIntOrNull() ?: 0 } ?: 0
        val y = String(b).toIntOrNull() ?: 0
        return "${maxOf(x, y)}".toByteArray()
    }

    override fun isEmptyDiff(payload: ByteArray): Boolean = payload.isEmpty()
}

// MARK: - Typed test model (hand-written; codegen's shape in miniature)

data class TestNote(
    override val id: String,
    val title: String? = null,
    val rank: String? = null,
) : ReplicaWritableRowModel {
    override val typeName: String? get() = null

    override fun encode(): Map<String, ReplicaValue> {
        val data = LinkedHashMap<String, ReplicaValue>()
        title?.let { data["title"] = ReplicaValue.Str(it) }
        rank?.let { data["rank"] = ReplicaValue.Str(it) }
        return data
    }

    companion object : ReplicaWritableRowModelType<TestNote, ReplicaNoField> {
        override val streamName: String = "notes"

        override val modelKey: KClass<*> = TestNote::class

        override fun from(id: String, type: String?, data: Map<String, ReplicaValue>): TestNote? {
            if (type != null && type != "Note") return null
            return TestNote(id = id, title = data["title"]?.string, rank = data["rank"]?.string)
        }
    }
}

// MARK: - Fixtures

object Fixture {
    private val root: File = File(
        System.getProperty("java.io.tmpdir"),
        "replica-man-tests-${UUID.randomUUID()}"
    )

    private val lock = ReentrantLock()
    private val openStores = mutableListOf<ReplicaStateStore>()

    /**
     * An engine opens stores of its own for every binding it takes, and those
     * are invisible to `openStores`. Left alone they hold ~8 descriptors each
     * for the life of the JVM — a full run ended with 197 open.
     */
    private val openEngines = mutableListOf<ReplicaEngine>()

    private fun trackEngine(engine: ReplicaEngine): ReplicaEngine {
        lock.withLock { openEngines.add(engine) }
        return engine
    }

    init {
        Runtime.getRuntime().addShutdownHook(Thread { root.deleteRecursively() })
    }

    /**
     * notes: writable row · boards: document (stub codec) · jobs: readonly
     * row · assets: row on the global shard.
     */
    fun schema(boardStamp: ReplicaStamp? = null): ReplicaSchema = ReplicaSchema(
        streams = listOf(
            ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, shard = "user"),
            ReplicaStreamSpec(
                "boards", ReplicaStreamSpec.Lane.DOCUMENT, shard = "user", codec = "stub@1", stamp = boardStamp
            ),
            ReplicaStreamSpec("jobs", ReplicaStreamSpec.Lane.ROW, readonly = true, shard = "user"),
            ReplicaStreamSpec("assets", ReplicaStreamSpec.Lane.ROW, shard = "global"),
        )
    )

    /** A store that has already synchronized with the fixture server's dataset. */
    fun store(name: String = "store", indexes: List<ReplicaIndexSpec> = emptyList()): ReplicaStateStore {
        val path = File(root, "$name-${UUID.randomUUID()}.sqlite").path
        val store = ReplicaStateStore(path, indexes)
        lock.withLock { openStores.add(store) }
        store.fixtureSynchronized()
        return store
    }

    /** A store the fixture does not own — the caller closes or reopens it. */
    fun storeAt(path: String, indexes: List<ReplicaIndexSpec> = emptyList()): ReplicaStateStore {
        val store = ReplicaStateStore(path, indexes)
        lock.withLock { openStores.add(store) }
        store.fixtureSynchronized()
        return store
    }

    fun path(name: String = "store"): String = File(root.also { it.mkdirs() }, "$name-${UUID.randomUUID()}.sqlite").path

    /** A per-test home for per-owner files — the shape the app host uses. */
    fun directory(name: String = "engine"): File = File(root, "$name-${UUID.randomUUID()}")

    /** An engine that owns its own files: nothing is bound until `open(owner)`. */
    fun unopenedEngine(
        directory: File,
        transport: StubTransport,
        schema: ReplicaSchema = schema(),
        codecs: List<ReplicaCodec> = listOf(StubCodec()),
        coldWindow: Duration = 10.seconds,
        automaticallyPushWrites: Boolean = false,
        syncGates: List<SyncGate> = emptyList(),
        documentMode: io.replicaman.ReplicaDocumentMode = io.replicaman.ReplicaDocumentMode.REPLICATED,
    ): ReplicaEngine = trackEngine(
        ReplicaEngine(
            home = directory,
            transport = transport,
            schema = schema,
            codecs = codecs,
            coldWindow = coldWindow,
            peerMinter = sequentialMinter(),
            automaticallyPushWrites = automaticallyPushWrites,
            syncGates = syncGates,
            documentMode = documentMode
        )
    )

    /** Deterministic peer minter: 100, 101, 102… */
    fun sequentialMinter(start: ULong = 100uL): () -> ULong {
        val counter = Counter(start)
        return { counter.next() }
    }

    class Counter(private var value: ULong) {
        private val lock = ReentrantLock()

        fun next(): ULong = lock.withLock {
            val current = value
            value += 1uL
            current
        }
    }

    /**
     * The suite's owner: every fixture engine is bound to user 42, which is
     * what a create stamps.
     */
    const val OWNER: Long = 42L

    /**
     * `automaticallyPushWrites` is PRODUCTION-`true`. This fixture defaulted
     * it to `false` unconditionally on iOS, which hid the production default
     * from every test in the package. It is a
     * parameter: suites that grade an explicit `drain()` keep `false` so
     * their pending counts stay deterministic; the suites that grade the
     * engine's OWN delivery pass `true` and never call `drain()` at all.
     */
    fun engine(
        store: ReplicaStateStore,
        owner: Long = OWNER,
        transport: StubTransport,
        schema: ReplicaSchema = schema(),
        codecs: List<ReplicaCodec> = listOf(StubCodec()),
        coldWindow: Duration = 10.seconds,
        minter: (() -> ULong)? = null,
        automaticallyPushWrites: Boolean = false,
        clock: (() -> java.time.Instant)? = null,
        syncGates: List<SyncGate> = emptyList(),
        documentMode: io.replicaman.ReplicaDocumentMode = io.replicaman.ReplicaDocumentMode.REPLICATED,
    ): ReplicaEngine = trackEngine(
        ReplicaEngine(
            store = store,
            owner = owner,
            transport = transport,
            schema = schema,
            codecs = codecs,
            coldWindow = coldWindow,
            peerMinter = minter ?: sequentialMinter(),
            clock = clock ?: { java.time.Instant.now() },
            automaticallyPushWrites = automaticallyPushWrites,
            syncGates = syncGates,
            documentMode = documentMode
        )
    )

    fun note(id: String, title: String, rank: String? = null): ReplicaFrame {
        val data = LinkedHashMap<String, ReplicaValue>()
        data["title"] = ReplicaValue.Str(title)
        if (rank != null) data["rank"] = ReplicaValue.Str(rank)
        return ReplicaFrame.RowSet(stream = "notes", id = id, type = null, data = data)
    }

    /**
     * Every store this test opened, closed. GRDB's pools died with ARC on
     * iOS; a JVM store holds native handles until it is told to let go.
     */
    fun cleanup() {
        val failures = mutableListOf<Throwable>()
        val engines = lock.withLock {
            val copy = openEngines.toList()
            openEngines.clear()
            copy
        }
        for (engine in engines) {
            try {
                // Bounded: a wedged seal must fail the test that caused it, never hang the suite.
                runBlocking { withTimeout(5_000) { engine.close() } }
            } catch (failure: Throwable) {
                // Finish closing the other resources, then fail the test below.
                failures += failure
            }
        }
        val stores = lock.withLock {
            val copy = openStores.toList()
            openStores.clear()
            copy
        }
        for (store in stores) {
            try {
                store.close()
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        if (failures.isNotEmpty()) {
            val error = AssertionError("Replica fixture cleanup failed", failures.first())
            failures.drop(1).forEach(error::addSuppressed)
            throw error
        }
    }
}

/** Closes the stores a test opened; iOS got the same from ARC. */
abstract class ReplicaTestCase {
    @After
    fun closeFixtureStores() {
        Fixture.cleanup()
    }
}

/** A thread-safe event tally for observation tests. */
class Tally {
    private val lock = ReentrantLock()
    private var value = 0

    fun bump() = lock.withLock { value += 1; Unit }

    val count: Int get() = lock.withLock { value }
}

// MARK: - Store peeks (raw truth assertions)

fun ReplicaStateStore.allSnapshots(): List<ReplicaStateStore.SnapshotRow> = read { db ->
    db.query(
        "SELECT stream, row_id, type, data FROM snapshots ORDER BY stream, row_id"
    ) { statement ->
        ReplicaStateStore.SnapshotRow(
            stream = statement.getText(0),
            rowId = statement.getText(1),
            type = statement.textOrNull(2),
            data = ReplicaStateStore.decodeData(statement.textOrNull(3))
        )
    }
}

fun ReplicaStateStore.peekSnapshot(stream: String, rowId: String): ReplicaStateStore.SnapshotRow? =
    read { snapshot(it, stream, rowId) }

fun ReplicaStateStore.peekDoc(stream: String, rowId: String): ReplicaStateStore.DocRow? =
    read { doc(it, stream, rowId) }

fun ReplicaStateStore.peekPending(): List<ReplicaStateStore.JournalRow> = read { pending(it) }

fun ReplicaStateStore.peekParked(): List<ReplicaStateStore.JournalRow> = read { parked(it) }

/** Entries held back as a draft — never on the wire until released. */
fun ReplicaStateStore.peekDrafted(): List<ReplicaStateStore.JournalRow> = read { drafted(it) }

// MARK: - Async polling

/**
 * Bounded wait for an async condition — the suite's hang defense: every
 * wait has a deadline, a missed one fails the test instead of wedging the
 * runner.
 */
suspend fun eventually(
    timeout: Duration = 3.seconds,
    message: String = "condition not met in time",
    condition: suspend () -> Boolean,
) {
    val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
    while (System.currentTimeMillis() < deadline) {
        if (condition()) return
        realDelay(20)
    }
    fail(message)
}

/**
 * A bounded wait that FAILS BY THROWING. A bounded poll needs a named
 * reason; `reason` is that name and it is required.
 *
 * This is never a substitute for a deterministic seam. Where the engine
 * offers one — `StubTransport.onPush` / `onPull`, a watch baseline — the
 * seam is used and this helper is not.
 */
class PollTimeout(val reason: String) : Exception("condition never held: $reason")

suspend fun until(
    reason: String,
    timeout: Duration = 5.seconds,
    condition: suspend () -> Boolean,
) {
    val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
    while (System.currentTimeMillis() < deadline) {
        if (condition()) return
        realDelay(5)
    }
    throw PollTimeout(reason)
}

/**
 * A REAL pause. `runTest` skips `delay` on its virtual clock; a throttle is
 * a statement about wall time, so these waits leave the test scheduler.
 */
suspend fun realDelay(millis: Long) {
    withContext(Dispatchers.Default) { delay(millis) }
}

// MARK: - Deterministic release ledger (the byte plane's predicate, in miniature)

/**
 * The mutable set a sync gate consults — an upload ledger in miniature.
 * Landing a key fires the gate's signal: the engine asks the gate's
 * holds again.
 */
class ReleaseLedger(released: List<String> = emptyList()) {
    private val lock = ReentrantLock()
    private val keys = mutableSetOf<String>().also { it.addAll(released) }
    val signal = SyncGateSignal()

    fun land(key: String) {
        lock.withLock { keys.add(key) }
        signal.fire()
    }

    /** Lands without the signal — what a process that died mid-landing leaves behind for the next open. */
    fun landQuietly(key: String) = lock.withLock { keys.add(key); Unit }

    fun contains(key: String): Boolean = lock.withLock { keys.contains(key) }
}

/** A gate written as a closure — what a test needs, nothing more. */
class TestGate(
    override val stream: String? = "notes",
    override val id: String = "test",
    private val signal: SyncGateSignal? = null,
    private val body: (SyncChange) -> SyncVerdict,
) : SyncGate {
    override fun judge(change: SyncChange): SyncVerdict = body(change)

    override val changes: kotlinx.coroutines.flow.Flow<Unit>
        get() = signal?.changes ?: kotlinx.coroutines.flow.emptyFlow()
}

/** Hold the row while its `blob` names bytes that have not landed. */
fun blobGate(released: ReleaseLedger): TestGate = TestGate(id = "blob", signal = released.signal) { change ->
    val key = change.local["blob"]?.string
    if (key != null && key.isNotEmpty() && !released.contains(key)) SyncVerdict.Gate("blob $key in flight") else SyncVerdict.Push
}

/** Hold the WHOLE row until its key has landed. */
fun gateWholeRowGate(released: ReleaseLedger): TestGate = TestGate(id = "cook", signal = released.signal) { change ->
    if (released.contains(change.rowId)) SyncVerdict.Push else SyncVerdict.Gate("row ${change.rowId} waits for its bytes")
}

/** A backup switch: every stream, one flag, flipped by the host. */
class BackupFlag(on: Boolean) {
    private val lock = ReentrantLock()
    private var value = on
    val signal = SyncGateSignal()

    val isOn: Boolean get() = lock.withLock { value }

    fun set(on: Boolean) {
        lock.withLock { value = on }
        signal.fire()
    }

    val gate: TestGate get() = TestGate(null, id = "backup", signal = signal) { if (isOn) SyncVerdict.Push else SyncVerdict.Gate("cloud backup off") }
}

/** Counts every judge — the proof a drain asks no gate. */
class JudgeCount {
    private val lock = ReentrantLock()
    private var count = 0

    val value: Int get() = lock.withLock { count }

    fun tick() = lock.withLock { count += 1; Unit }
}

/**
 * Captures an error raised inside a NON-throwing transport hook so the test
 * can assert it never happened. Swallowing it is a hidden green, and a hook is the one place a test is tempted to.
 */
class HookOutcome {
    private val lock = ReentrantLock()
    private var stored: String? = null

    fun record(error: Throwable) = lock.withLock { stored = "$error"; Unit }

    val failure: String? get() = lock.withLock { stored }
}

// MARK: - Flow collectors

/**
 * Every value a flow delivered, in order — what proves a re-arm happened and
 * that no post-retirement picture carries the old world. The iOS suites read
 * an `AsyncStream` iterator; a list plus `eventually` says the same thing
 * without holding the consumer inside the assertion.
 */
class Recorder<T> {
    private val lock = ReentrantLock()
    private val stored = mutableListOf<T>()

    fun record(value: T) = lock.withLock { stored.add(value); Unit }

    val values: List<T> get() = lock.withLock { stored.toList() }

    val count: Int get() = lock.withLock { stored.size }

    val last: T? get() = lock.withLock { stored.lastOrNull() }
}

fun <T> kotlinx.coroutines.flow.Flow<T>.recordInto(
    scope: kotlinx.coroutines.CoroutineScope,
    recorder: Recorder<T>,
): kotlinx.coroutines.Job = scope.launch(Dispatchers.Default) {
    collect { recorder.record(it) }
}
