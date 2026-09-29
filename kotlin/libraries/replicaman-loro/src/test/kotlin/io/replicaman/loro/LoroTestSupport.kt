package io.replicaman.loro

import io.replicaman.testing.FixtureTransport
import io.replicaman.testing.ProtocolFixture
import io.replicaman.testing.fixtureSynchronized

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.withTimeout
import org.junit.After
import io.replicaman.loro.binding.ExportMode
import io.replicaman.loro.binding.LoroDoc
import io.replicaman.loro.binding.LoroValue
import io.replicaman.loro.binding.getMap
import io.replicaman.loro.binding.insert
import io.replicaman.ReplicaDocModel
import io.replicaman.ReplicaDocModelType
import io.replicaman.ReplicaDocState
import io.replicaman.ReplicaDocStateType
import io.replicaman.recoveryParts
import io.replicaman.recoveryChunk
import io.replicaman.ReplicaRecoveryRecord
import io.replicaman.ReplicaStateStore
import io.replicaman.ReplicaEngine
import io.replicaman.ReplicaError
import io.replicaman.ReplicaNoField
import io.replicaman.ReplicaOp
import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.ReplicaSchema
import io.replicaman.ReplicaStreamSpec
import io.replicaman.ReplicaTransport
import io.replicaman.ReplicaValue
import io.replicaman.ReplicaVerdict
import io.replicaman.SyncGate
import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Loro-side fixtures: the same stub transport discipline as the core
 * suite, plus loro authoring helpers standing in for the app's document
 * layer — a client doc is born FROM the store's fold under the store's
 * peer, edits export as update payloads. Its own copy, as on iOS: the core
 * suite's support is that module's test source set, and `:replicaman-loro`
 * reaches the engine through public doors only.
 */
class LoroStubTransport : FixtureTransport {
    override val protocolFixture = ProtocolFixture()
    private val lock = ReentrantLock()
    private val batches = mutableListOf<List<ReplicaOp>>()
    private var pushes = 0
    private val pullQueues = mutableMapOf<String, MutableList<ReplicaPullResponse>>()
    private var pushScript: ((List<ReplicaOp>) -> List<ReplicaVerdict>)? = null
    private var pushFails = false

    fun queuePull(shard: String, response: ReplicaPullResponse) = lock.withLock {
        pullQueues.getOrPut(shard) { mutableListOf() }.add(response)
        Unit
    }

    fun scriptPush(script: (List<ReplicaOp>) -> List<ReplicaVerdict>) = lock.withLock { pushScript = script }

    fun failPushes(fail: Boolean) = lock.withLock { pushFails = fail }

    val pushedBatches: List<List<ReplicaOp>> get() = lock.withLock { batches.toList() }

    val pushCount: Int get() = lock.withLock { pushes }

    override suspend fun pull(shard: String, cursor: String?, limit: Int): ReplicaPullResponse = lock.withLock {
        val queue = pullQueues[shard]
        if (queue != null && queue.isNotEmpty()) {
            queue.removeAt(0)
        } else {
            ReplicaPullResponse(frames = emptyList(), cursor = cursor ?: "0:", more = false)
        }
    }

    override suspend fun push(ops: List<ReplicaOp>): List<ReplicaVerdict> = lock.withLock {
        pushes += 1
        if (pushFails) throw ReplicaError.Transport("push refused (stub)")
        batches.add(ops.toList())
        pushScript?.invoke(ops) ?: ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.ACCEPTED) }
    }
}

object LoroFixture {
    const val SERVER_PEER: ULong = 1uL
    const val OWNER: Long = 42L

    private val root = File(System.getProperty("java.io.tmpdir"), "replica-man-loro-tests-${UUID.randomUUID()}")
    private val lock = ReentrantLock()
    private val engines = mutableListOf<ReplicaEngine>()

    init {
        Runtime.getRuntime().addShutdownHook(Thread { root.deleteRecursively() })
    }

    fun schema(): ReplicaSchema = ReplicaSchema(
        streams = listOf(
            ReplicaStreamSpec("boards", ReplicaStreamSpec.Lane.DOCUMENT, shard = "user", codec = LoroReplicaCodec.CODEC_NAME),
            ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, shard = "user"),
        ),
    )

    /** Deterministic peer minter: 100, 101, 102… */
    fun minter(start: ULong = 100uL): () -> ULong {
        val counter = AtomicLong(start.toLong())
        return { counter.getAndIncrement().toULong() }
    }

    /** An engine over its own directory, opened for the suite's owner on a store that has synchronized before. */
    suspend fun engine(transport: LoroStubTransport, minterStart: ULong = 100uL, schema: ReplicaSchema = schema(), clock: () -> java.time.Instant = { java.time.Instant.now() }, syncGates: List<SyncGate> = emptyList()): ReplicaEngine {
        val engine = ReplicaEngine(
            home = File(root, "engine-${UUID.randomUUID()}"),
            transport = transport,
            schema = schema,
            clock = clock,
            codecs = listOf(LoroReplicaCodec()),
            peerMinter = minter(minterStart),
            automaticallyPushWrites = false,
            syncGates = syncGates,
        )
        engine.open(OWNER)
        requireNotNull(engine.store).fixtureSynchronized()
        lock.withLock { engines.add(engine) }
        return engine
    }

    fun cleanup() {
        val failures = mutableListOf<Throwable>()
        val opened = lock.withLock {
            val copy = engines.toList()
            engines.clear()
            copy
        }
        for (engine in opened) {
            try {
                runBlocking { withTimeout(5_000) { engine.close() } }
            } catch (failure: Throwable) {
                // Finish closing the other resources, then fail the test below.
                failures += failure
            }
        }
        if (failures.isNotEmpty()) {
            val error = AssertionError("Loro fixture cleanup failed", failures.first())
            failures.drop(1).forEach(error::addSuppressed)
            throw error
        }
    }

    // MARK: - Loro authoring (the app layer's half, in miniature)

    fun doc(peer: ULong, fold: ByteArray? = null): LoroDoc {
        val doc = LoroDoc()
        doc.setRecordTimestamp(false)
        doc.setPeerId(peer)
        if (fold != null && fold.isNotEmpty()) doc.import(fold)
        return doc
    }

    fun setMeta(doc: LoroDoc, key: String, value: String) {
        doc.getMap("meta").insert(key, LoroValue.String(value))
        doc.commit()
    }

    /**
     * Edit-and-export: the payload a real client would journal for this
     * mutation (updates since the doc's version before the edit).
     */
    fun editPayload(doc: LoroDoc, key: String, value: String): ByteArray {
        val before = doc.oplogVv()
        setMeta(doc, key, value)
        return doc.export(ExportMode.Updates(before))
    }

    fun meta(doc: LoroDoc, key: String): String? {
        val root = (doc.getDeepValue() as? LoroValue.Map)?.value ?: return null
        val meta = (root["meta"] as? LoroValue.Map)?.value ?: return null
        return (meta[key] as? LoroValue.String)?.value
    }

    fun meta(fold: ByteArray, key: String): String? = meta(doc(999_999uL, fold), key)
}

/** Retires the engines a test opened; iOS got the same from ARC. */
abstract class LoroTestCase {
    @After
    fun retireFixtureEngines() {
        LoroFixture.cleanup()
    }
}

// MARK: - The test document

data class Board(override val id: String, val name: String?) : ReplicaDocModel {
    companion object : ReplicaDocModelType<Board, ReplicaNoField> {
        override val streamName: String = "boards"

        override val modelKey: KClass<*> = Board::class

        override fun from(id: String, data: Map<String, ReplicaValue>): Board? =
            Board(id = id, name = data["name"]?.string)
    }
}

class BoardState(
    val name: String?,
    val version: ByteArray,
    val canUndo: Boolean,
    val canRedo: Boolean,
) : ReplicaDocState {
    override fun equals(other: Any?): Boolean =
        other is BoardState && name == other.name && version.contentEquals(other.version) &&
            canUndo == other.canUndo && canRedo == other.canRedo

    override fun hashCode(): Int = (name?.hashCode() ?: 0) * 31 + version.contentHashCode()

    companion object : ReplicaDocStateType<LoroDocument, BoardState> {
        /** How many times a state was minted from a document — the warm law's counter. */
        val decodes = AtomicInteger(0)

        override val codecName: String = LoroReplicaCodec.CODEC_NAME

        override fun state(document: LoroDocument, version: ByteArray, canUndo: Boolean, canRedo: Boolean): BoardState {
            decodes.incrementAndGet()
            return BoardState(LoroFixture.meta(document.doc, "name"), version, canUndo, canRedo)
        }
    }
}

/** A thread-safe list of what a watch delivered, in order. */
class Delivered<T> {
    private val lock = ReentrantLock()
    private val stored = mutableListOf<T>()

    fun add(value: T) = lock.withLock { stored.add(value); Unit }

    val values: List<T> get() = lock.withLock { stored.toList() }
}

/**
 * A bounded wait that FAILS BY THROWING, with a named reason. The watch delivers on the engine's own scope,
 * so the arrival is the thing being waited for, not a timer.
 */
class PollTimeout(reason: String) : Exception("condition never held: $reason")

fun until(reason: String, timeout: Duration = 3.seconds, condition: () -> Boolean) {
    val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
    while (System.currentTimeMillis() < deadline) {
        if (condition()) return
        Thread.sleep(10)
    }
    throw PollTimeout(reason)
}

internal fun recoveryBytes(store: ReplicaStateStore, record: ReplicaRecoveryRecord, kind: String): ByteArray {
    val part = store.recoveryParts(record.id).single { it.kind == kind }
    val output = java.io.ByteArrayOutputStream()
    while (output.size().toLong() < part.byteCount) {
        val chunk = store.recoveryChunk(record.id, part, output.size().toLong())
        check(chunk.isNotEmpty()) { "Truncated recovery export" }
        output.write(chunk)
    }
    return output.toByteArray()
}
