package io.replicaman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass

/**
 * Generated read handles and the row/document model protocols. Row mutation
 * verbs live only on ReplicaTransaction's typed views; ordinary row handles
 * serve committed reads and watches. DocumentStream retains the current
 * document API until the iOS document adapter lands.
 *
 * Kotlin companions provide Swift's static stream/decode requirements.
 */

// MARK: - Model protocols

public interface ReplicaRowModel {
    public val id: String

    public val typeName: String?

    public fun encode(): Map<String, ReplicaValue>
}

/**
 * The marker split that makes readonly compile-time: writable models hang
 * write verbs off `TransactionRows`; a readonly model never conforms.
 */
public interface ReplicaWritableRowModel : ReplicaRowModel {
    /** Complete local value at birth; [encode] remains the writable journal payload. */
    public fun encodeSnapshot(): Map<String, ReplicaValue> = encode()
}

public interface ReplicaRowModelType<M : ReplicaRowModel, F : ReplicaIndexedField> {
    public val streamName: String

    /** The materialization cache key — one picture per (stream, model). */
    public val modelKey: KClass<*>

    /**
     * Best-effort typed projection of a raw snapshot row: an unknown STI
     * `type` or a missing required field returns null — the raw row stays in
     * the store either way (decode tolerance, ARCHITECTURE §4.2).
     */
    public fun from(id: String, type: String?, data: Map<String, ReplicaValue>): M?
}

public interface ReplicaWritableRowModelType<M : ReplicaWritableRowModel, F : ReplicaIndexedField> :
    ReplicaRowModelType<M, F>

public interface ReplicaDocModel {
    public val id: String
}

public interface ReplicaDocModelType<M : ReplicaDocModel, F : ReplicaIndexedField> {
    public val streamName: String

    public val modelKey: KClass<*>

    public fun from(id: String, data: Map<String, ReplicaValue>): M?
}

/**
 * Columns whose values come from the authenticated engine session rather
 * than from a CRUD caller. Generated create verbs opt into this convention;
 * it is runtime metadata only and never changes the replica wire grammar.
 */
public data class ReplicaStamp(
    val userId: String? = null,
    val createdAt: String? = null,
    val updatedAt: String? = null,
) {
    public companion object {
        public val standard: ReplicaStamp = ReplicaStamp(
            userId = "userId",
            createdAt = "createdAt",
            updatedAt = "updatedAt"
        )
    }
}

// MARK: - Observation

/**
 * GRDB's `ValueObservation … .removeDuplicates().values(in: pool)`, raced
 * against the binding's generation: the tracked value is re-fetched after
 * every committed write, delivered only when it moved, and the whole
 * observation ends the moment the process changes owners. RACED, not
 * awaited: a closed store would otherwise serve the outgoing owner forever.
 */
internal suspend fun <V> observeValue(
    store: ReplicaStateStore,
    binding: ReplicaBinding,
    generation: ULong,
    fetch: (ReplicaStateStore) -> V,
    onValue: (value: V, baseline: Boolean) -> Unit,
) {
    coroutineScope {
        val watcher = launch {
            var baseline = true
            var last: V? = null
            store.commits.collect {
                val value = try {
                    fetch(store)
                } catch (error: Exception) {
                    // A closed outgoing store can race its subscription's end.
                    // Failures for the active store propagate to the collector.
                    if (binding.snapshot().second != generation) return@collect
                    throw error
                }
                if (!baseline && value == last) return@collect
                last = value
                val isBaseline = baseline
                baseline = false
                onValue(value, isBaseline)
            }
        }
        binding.waitForChange(generation)
        watcher.cancel()
    }
}

internal object ReplicaReads {
    /**
     * A find is a POINT read: the warm model when this type is materialized
     * at the live sequence, else one index-served row that reuses the
     * stream's decoded record — never a materialization of the stream. A
     * find that rebuilt the picture walked every row on the caller's
     * thread after each write, and under the device pipeline's write
     * cadence that was every find.
     */
    fun <M : Any> find(
        store: ReplicaStateStore?,
        stream: String,
        modelKey: KClass<*>,
        id: String,
        decode: (String, String?, Map<String, ReplicaValue>) -> M?,
    ): M? {
        if (store == null) return null
        store.warmModel<M>(stream, modelKey, id)?.let { return it }
        return store.scopedRows(stream, modelKey, "row_id = ?", listOf(id), decode).firstOrNull()
    }

    /**
     * Simple equality predicates over generated columns, evaluated in SQL
     * semantics over the materialized raw data — anything richer is what
     * the raw-SQL escape hatch below is for.
     */
    fun <M : Any> rows(
        store: ReplicaStateStore?,
        stream: String,
        modelKey: KClass<*>,
        equals: Map<String, ReplicaValue>,
        minimumSequence: Long? = null,
        decode: (String, String?, Map<String, ReplicaValue>) -> M?,
    ): List<M> {
        if (store == null) return emptyList()
        val materialization =
            store.materializedRows(stream, modelKey, minimumSequence, decode)
        if (equals.isEmpty()) return materialization.rows.map { it.model }
        return materialization.rows.filter { it.record.matches(equals) }.map { it.model }
    }

    /**
     * The scoped read: a predicate compiles to an indexed WHERE over
     * `snapshots`; only the matching rows are fetched, and each one reuses
     * the stream's decoded record / model when its raw text is unchanged.
     * No predicate = the whole stream through the materialization cache.
     */
    fun <M : Any, F : ReplicaIndexedField> list(
        store: ReplicaStateStore?,
        stream: String,
        modelKey: KClass<*>,
        predicate: ReplicaPredicate<F>?,
        order: List<ReplicaOrder<F>> = emptyList(),
        limit: Int? = null,
        minimumSequence: Long? = null,
        decode: (String, String?, Map<String, ReplicaValue>) -> M?,
    ): List<M> {
        if (store == null) return emptyList()
        if (predicate == null && order.isEmpty() && limit == null) return rows(store, stream, modelKey, emptyMap(), minimumSequence, decode)
        val compiled = predicate?.compile(stream, store.indexes)
        return store.scopedRows(stream, modelKey, compiled?.sql ?: "1", compiled?.arguments.orEmpty(), decode, ReplicaOrder.clause(order, stream, store.indexes), limit)
    }

    /**
     * The scoped live query (the Cachebay shape): the sync `list` gave the
     * first picture, so the watch delivers CHANGES only — each stream
     * commit re-runs the scoped read and delivers when the picture differs
     * from the last one delivered. A commit outside the scope is silent.
     * `includeInitial` opts a background consumer into the baseline. Same
     * owner-rebinding loop as `watch`.
     */
    fun <M : Any, F : ReplicaIndexedField> watch(
        scope: CoroutineScope,
        binding: ReplicaBinding,
        stream: String,
        modelKey: KClass<*>,
        predicate: ReplicaPredicate<F>?,
        order: List<ReplicaOrder<F>>,
        limit: Int?,
        includeInitial: Boolean,
        decode: (String, String?, Map<String, ReplicaValue>) -> M?,
        deliver: (List<M>) -> Unit,
    ): ReplicaWatch {
        val last = LastPicture<M>()
        val job = scope.launch {
            var armed = false
            while (isActive) {
                val (bound, generation) = binding.snapshot()
                val store = bound?.store
                if (store == null) {
                    if (binding.snapshot().second == generation && ((includeInitial && !armed) || (armed && !last.value.isNullOrEmpty()))) {
                        last.value = emptyList()
                        deliver(emptyList())
                    }
                    armed = true
                    binding.waitForChange(generation)
                    continue
                }
                val wasArmed = armed
                armed = true
                observeValue(
                    store = store,
                    binding = binding,
                    generation = generation,
                    fetch = { it.read { db -> it.changeSequence(db, stream) } }
                ) { sequence, baseline ->
                    val picture = list(store, stream, modelKey, predicate, order, limit, minimumSequence = sequence, decode = decode)
                    if (!isActive || binding.snapshot().second != generation) return@observeValue
                    if (baseline && !wasArmed && !includeInitial) {
                        last.value = picture
                        return@observeValue
                    }
                    if (last.value == picture) return@observeValue
                    last.value = picture
                    deliver(picture)
                }
                binding.waitForChange(generation)
            }
        }
        return ReplicaWatch(job)
    }

    private class LastPicture<M : Any> {
        private val lock = ReentrantLock()
        private var stored: List<M>? = null

        var value: List<M>?
            get() = lock.withLock { stored }
            set(newValue) = lock.withLock { stored = newValue }
    }

    /**
     * A live query over the same rows — delivered after every commit that
     * touches the stream (client writes and checkpoint imports alike).
     *
     * It outlives its store, like `watchSignal`: with no owner it delivers
     * the empty picture and waits, and a foreign switch re-arms it on the
     * arriving owner rather than leaving it on a store nobody writes to.
     */
    fun <M : Any> watch(
        binding: ReplicaBinding,
        stream: String,
        modelKey: KClass<*>,
        decode: (String, String?, Map<String, ReplicaValue>) -> M?,
    ): Flow<List<M>> = callbackFlow {
        val worker = launch {
            while (isActive) {
                val (bound, generation) = binding.snapshot()
                val store = bound?.store
                if (store == null) {
                    trySend(emptyList())
                    binding.waitForChange(generation)
                    continue
                }
                observeValue(
                    store = store,
                    binding = binding,
                    generation = generation,
                    fetch = { it.read { db -> it.changeSequence(db, stream) } }
                ) { sequence, _ ->
                    trySend(rows(store, stream, modelKey, emptyMap(), sequence, decode))
                }
                binding.waitForChange(generation)
            }
        }
        awaitClose { worker.cancel() }
    }.buffer(Channel.UNLIMITED)
}

private fun ReplicaStateStore.RowRecord.matches(equals: Map<String, ReplicaValue>): Boolean =
    equals.all { (key, expected) ->
        val actual = fields[key] ?: return@all false
        when {
            actual is ReplicaValue.Str && expected is ReplicaValue.Str ->
                actual.value == expected.value

            actual is ReplicaValue.Integer && expected.long != null -> actual.value == expected.long
            expected is ReplicaValue.Integer && actual.long != null -> expected.value == actual.long

            actual is ReplicaValue.Num && expected is ReplicaValue.Num ->
                actual.value == expected.value

            actual is ReplicaValue.Bool && expected is ReplicaValue.Bool ->
                actual.value == expected.value

            actual is ReplicaValue.Bool && expected is ReplicaValue.Num ->
                (if (actual.value) 1.0 else 0.0) == expected.value

            actual is ReplicaValue.Num && expected is ReplicaValue.Bool ->
                actual.value == (if (expected.value) 1.0 else 0.0)

            else -> false
        }
    }

// MARK: - Stream handles

public class RowStream<M : ReplicaWritableRowModel, F : ReplicaIndexedField>(
    public val engine: ReplicaEngine,
    public val type: ReplicaWritableRowModelType<M, F>,
) {
    public fun find(id: String): M? =
        ReplicaReads.find(engine.store, type.streamName, type.modelKey, id, type::from)

    public fun where(equals: Map<String, ReplicaValue> = emptyMap()): List<M> =
        ReplicaReads.rows(engine.store, type.streamName, type.modelKey, equals, null, type::from)

    public fun watch(): Flow<List<M>> =
        ReplicaReads.watch(engine.binding, type.streamName, type.modelKey, type::from)

    /**
     * Sync read on the warm store — the first picture. Only indexed
     * predicates exist; bare `list()` is the whole stream.
     */
    public fun list(predicate: ReplicaPredicate<F>? = null, order: List<ReplicaOrder<F>> = emptyList(), limit: Int? = null): List<M> =
        ReplicaReads.list(engine.store, type.streamName, type.modelKey, predicate, order, limit, decode = type::from)

    /** Changes after `list`; hold the handle. */
    public fun watch(
        predicate: ReplicaPredicate<F>? = null,
        order: List<ReplicaOrder<F>> = emptyList(),
        limit: Int? = null,
        includeInitial: Boolean = false,
        deliver: (List<M>) -> Unit,
    ): ReplicaWatch = ReplicaReads.watch(
        engine.watchScope, engine.binding, type.streamName, type.modelKey,
        predicate, order, limit, includeInitial, type::from, deliver
    )
}

public class ReadonlyRowStream<M : ReplicaRowModel, F : ReplicaIndexedField>(
    public val engine: ReplicaEngine,
    public val type: ReplicaRowModelType<M, F>,
) {
    public fun find(id: String): M? =
        ReplicaReads.find(engine.store, type.streamName, type.modelKey, id, type::from)

    public fun where(equals: Map<String, ReplicaValue> = emptyMap()): List<M> =
        ReplicaReads.rows(engine.store, type.streamName, type.modelKey, equals, null, type::from)

    public fun watch(): Flow<List<M>> =
        ReplicaReads.watch(engine.binding, type.streamName, type.modelKey, type::from)

    public fun list(predicate: ReplicaPredicate<F>? = null, order: List<ReplicaOrder<F>> = emptyList(), limit: Int? = null): List<M> =
        ReplicaReads.list(engine.store, type.streamName, type.modelKey, predicate, order, limit, decode = type::from)

    public fun watch(
        predicate: ReplicaPredicate<F>? = null,
        order: List<ReplicaOrder<F>> = emptyList(),
        limit: Int? = null,
        includeInitial: Boolean = false,
        deliver: (List<M>) -> Unit,
    ): ReplicaWatch = ReplicaReads.watch(
        engine.watchScope, engine.binding, type.streamName, type.modelKey,
        predicate, order, limit, includeInitial, type::from, deliver
    )
}

/**
 * A server-authored document stream: readable fold and projection, no
 * write verbs at all — the readonly counterpart of `DocumentStream`.
 */
public class ReadonlyDocumentStream<M : ReplicaDocModel, F : ReplicaIndexedField>(
    public val engine: ReplicaEngine,
    public val type: ReplicaDocModelType<M, F>,
) {
    private val decode: (String, String?, Map<String, ReplicaValue>) -> M? =
        { id, _, data -> type.from(id, data) }

    public fun fold(id: String): ByteArray? = engine.docFold(type.streamName, id)

    public fun find(id: String): M? =
        ReplicaReads.find(engine.store, type.streamName, type.modelKey, id, decode)

    public fun where(equals: Map<String, ReplicaValue> = emptyMap()): List<M> =
        ReplicaReads.rows(engine.store, type.streamName, type.modelKey, equals, null, decode)

    public fun watch(): Flow<List<M>> =
        ReplicaReads.watch(engine.binding, type.streamName, type.modelKey, decode)

    public fun list(predicate: ReplicaPredicate<F>? = null, order: List<ReplicaOrder<F>> = emptyList(), limit: Int? = null): List<M> =
        ReplicaReads.list(engine.store, type.streamName, type.modelKey, predicate, order, limit, decode = decode)

    public fun watch(
        predicate: ReplicaPredicate<F>? = null,
        order: List<ReplicaOrder<F>> = emptyList(),
        limit: Int? = null,
        includeInitial: Boolean = false,
        deliver: (List<M>) -> Unit,
    ): ReplicaWatch = ReplicaReads.watch(
        engine.watchScope, engine.binding, type.streamName, type.modelKey,
        predicate, order, limit, includeInitial, decode, deliver
    )
}

public class DocumentStream<M : ReplicaDocModel, F : ReplicaIndexedField>(
    public val engine: ReplicaEngine,
    public val type: ReplicaDocModelType<M, F>,
) {
    private val decode: (String, String?, Map<String, ReplicaValue>) -> M? =
        { id, _, data -> type.from(id, data) }

    /**
     * Birth the document: `row.create` carrying codec + seed (creation is
     * gated server-side; the row exists only after the verdict). `peer` is
     * the loro peer the seed was authored under — the engine records it and
     * rotates it if the fold is ever lost.
     */
    public suspend fun create(
        id: String,
        seed: ByteArray,
        peer: ULong,
        data: Map<String, ReplicaValue> = emptyMap(),
    ): Boolean = engine.createDoc(type.streamName, id, seed, peer, data)

    public suspend fun delete(id: String) {
        engine.deleteRow(type.streamName, id)
    }

    /**
     * Fold a local edit in: merged into the fold, superseded into the ONE
     * pending `doc.delta` for this document.
     */
    public suspend fun delta(id: String, payload: ByteArray) {
        engine.recordDocDelta(type.streamName, id, payload)
    }

    // MARK: - The document's own doors (the rows' law: sync first read, then watch)

    /**
     * Birth — the seed is the whole document; a present id is refused
     * (false), like `create` on a row.
     */
    public suspend fun createDoc(
        id: String,
        seed: ByteArray,
        peer: ULong,
        data: Map<String, ReplicaValue> = emptyMap(),
    ): Boolean = create(id, seed, peer, data)

    /**
     * The document's state, read where the caller stands — SYNC. Null
     * when the store holds no document under `id`.
     */
    public fun <D : Any, S : ReplicaDocState> findDoc(
        id: String,
        state: ReplicaDocStateType<D, S>,
    ): S? = engine.documentState(type.streamName, id, state)

    /** A synchronous history read; the body may not move the held document. */
    public fun <D : Any, T> readDoc(
        id: String,
        codec: DocumentCodec<D>,
        body: (D) -> T,
    ): T? = engine.readDocument(type.streamName, id, codec, body)

    /**
     * The state of a HELD document, null otherwise — never opens one. A
     * reader over many documents asks this first and `findDoc` only what
     * it must open.
     */
    public fun docFingerprints(ids: List<String>): Map<String, ByteArray> =
        engine.documentFingerprints(type.streamName, ids)

    public fun <D : Any, S : ReplicaDocState> heldDoc(
        id: String,
        state: ReplicaDocStateType<D, S>,
    ): S? = engine.documentHeldState(type.streamName, id, state)

    /**
     * Changes of the document's state — local edits and pulled deltas
     * through the one door. `includeInitial` opts into the baseline.
     */
    public fun <D : Any, S : ReplicaDocState> watchDoc(
        id: String,
        state: ReplicaDocStateType<D, S>,
        includeInitial: Boolean = false,
        deliver: (S?) -> Unit,
    ): ReplicaWatch = watchDocument(
        engine.watchScope, engine.binding, type.streamName, includeInitial,
        read = { engine.documentState(type.streamName, id, state) },
        deliver = deliver
    )

    /**
     * A local edit: `body` moves the held document, the delta is
     * journaled, `watchDoc` delivers. False when nothing moved; throws
     * `UnknownDocument` for an id the store does not hold.
     */
    public suspend fun <D : Any> updateDoc(
        id: String,
        codec: DocumentCodec<D>,
        body: (D) -> Unit,
    ): Boolean = engine.updateDocument(type.streamName, id, codec, body)

    public suspend fun <D : Any> undoDoc(id: String, codec: DocumentCodec<D>): Boolean =
        engine.undoDocument(type.streamName, id, codec)

    public suspend fun <D : Any> redoDoc(id: String, codec: DocumentCodec<D>): Boolean =
        engine.redoDocument(type.streamName, id, codec)

    /** A session's hold: the document stays held while pinned. */
    public fun pinDoc(id: String) {
        engine.pinDocument(type.streamName, id)
    }

    public fun unpinDoc(id: String) {
        engine.unpinDocument(type.streamName, id)
    }

    public fun fold(id: String): ByteArray? = engine.docFold(type.streamName, id)

    public fun peer(id: String): ULong? = engine.docPeer(type.streamName, id)

    public fun find(id: String): M? =
        ReplicaReads.find(engine.store, type.streamName, type.modelKey, id, decode)

    public fun where(equals: Map<String, ReplicaValue> = emptyMap()): List<M> =
        ReplicaReads.rows(engine.store, type.streamName, type.modelKey, equals, null, decode)

    public fun watch(): Flow<List<M>> =
        ReplicaReads.watch(engine.binding, type.streamName, type.modelKey, decode)

    public fun list(predicate: ReplicaPredicate<F>? = null, order: List<ReplicaOrder<F>> = emptyList(), limit: Int? = null): List<M> =
        ReplicaReads.list(engine.store, type.streamName, type.modelKey, predicate, order, limit, decode = decode)

    public fun watch(
        predicate: ReplicaPredicate<F>? = null,
        order: List<ReplicaOrder<F>> = emptyList(),
        limit: Int? = null,
        includeInitial: Boolean = false,
        deliver: (List<M>) -> Unit,
    ): ReplicaWatch = ReplicaReads.watch(
        engine.watchScope, engine.binding, type.streamName, type.modelKey,
        predicate, order, limit, includeInitial, decode, deliver
    )
}
