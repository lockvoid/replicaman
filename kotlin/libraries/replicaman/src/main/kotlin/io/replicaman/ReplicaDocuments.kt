package io.replicaman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The live half of a document codec. `ReplicaCodec` merges FOLDS — bytes
 * the store keeps; this opens a fold as a document the engine holds,
 * reads a state off it, edits it, and hands back the delta the fold lane
 * journals. Rows-only consumers never see it.
 */
public interface DocumentCodec<D : Any> : ReplicaCodec {
    /** The wire name the schema registers the codec under (`loro@1`). */
    public val codecName: String

    /** A document from a fold (null = a blank one), authored as `peer`. */
    public fun open(fold: ByteArray?, peer: ULong): D

    public fun snapshot(document: D): ByteArray

    /**
     * The document's own version, encoded — the cursor every state and
     * delta is stamped with.
     */
    public fun documentVersion(document: D): ByteArray

    public fun exportDelta(document: D, since: ByteArray): ByteArray

    public fun importDeltas(document: D, payloads: List<ByteArray>)

    public fun peer(document: D): ULong

    public fun canUndo(document: D): Boolean

    public fun canRedo(document: D): Boolean

    public fun undo(document: D): Boolean

    public fun redo(document: D): Boolean

    public fun write(value: ReplicaValue, path: List<String>, document: D)
}

internal fun <D : Any> DocumentCodec<D>.version(document: D): ByteArray =
    documentVersion(document)

/**
 * What a consumer READS of a document: a value minted from the held
 * document at a version. The generated document model names its state
 * (`Project.State`); the engine mints one per version and hands the same
 * value out until the document moves.
 */
public interface ReplicaDocState

/**
 * Swift expresses the state minter as a STATIC requirement on
 * `ReplicaDocState`; Kotlin has no static protocol members, so the minter is
 * a value the caller supplies — generated document models emit one as their
 * companion.
 */
public interface ReplicaDocStateType<D : Any, S : ReplicaDocState> {
    public val codecName: String

    public fun state(document: D, version: ByteArray, canUndo: Boolean, canRedo: Boolean): S
}

/** The default for a document model that reads no state. */
public class NoDocState : ReplicaDocState {
    override fun equals(other: Any?): Boolean = other is NoDocState

    override fun hashCode(): Int = 0

    public companion object : ReplicaDocStateType<NoDocumentCodec.Document, NoDocState> {
        override val codecName: String = NoDocumentCodec.CODEC_NAME

        override fun state(
            document: NoDocumentCodec.Document,
            version: ByteArray,
            canUndo: Boolean,
            canRedo: Boolean,
        ): NoDocState = NoDocState()
    }
}

/** The codec behind `NoDocState` — never registered, never opened. */
public class NoDocumentCodec : DocumentCodec<NoDocumentCodec.Document> {
    public class Document

    override val codecName: String get() = CODEC_NAME

    override val name: String get() = CODEC_NAME

    override fun merge(fold: ByteArray?, payload: ByteArray, reflections: List<ReplicaReflection>): ReplicaMerge = refuse()

    override fun diff(fold: ByteArray, since: ByteArray?): ByteArray = refuse()

    override fun version(fold: ByteArray): ByteArray = refuse()

    override fun payloadVersion(payload: ByteArray): ByteArray = refuse()

    override fun mergeVersions(a: ByteArray?, b: ByteArray): ByteArray = refuse()

    override fun isEmptyDiff(payload: ByteArray): Boolean = true

    override fun open(fold: ByteArray?, peer: ULong): Document = refuse()

    override fun snapshot(document: Document): ByteArray = refuse()

    override fun documentVersion(document: Document): ByteArray = ByteArray(0)

    override fun exportDelta(document: Document, since: ByteArray): ByteArray = refuse()

    override fun importDeltas(document: Document, payloads: List<ByteArray>) {}

    override fun peer(document: Document): ULong = 0uL

    override fun canUndo(document: Document): Boolean = false

    override fun canRedo(document: Document): Boolean = false

    override fun undo(document: Document): Boolean = false

    override fun redo(document: Document): Boolean = false

    override fun write(value: ReplicaValue, path: List<String>, document: Document): Unit = refuse()

    private fun refuse(): Nothing = throw ReplicaError.Codec("no document codec")

    public companion object {
        public const val CODEC_NAME: String = "none"
    }
}

// MARK: - The engine's document doors

/**
 * The document's stored row — fold, peer, codec, acked — read where
 * the caller stands, like `docFold`.
 */
public fun ReplicaEngine.docRow(stream: String, id: String): ReplicaStateStore.DocRow? {
    val store = store ?: return null
    return store.read { db -> store.doc(db, stream, id) }
}

public fun ReplicaEngine.documentFingerprints(stream: String, ids: List<String>): Map<String, ByteArray> {
    val store = store ?: return emptyMap()
    return store.read { store.docFingerprints(it, stream, ids) }
}

@Suppress("UNCHECKED_CAST")
internal fun <D : Any> ReplicaEngine.documentCodec(codecName: String): DocumentCodec<D> =
    codecs[codecName] as? DocumentCodec<D>
        ?: throw ReplicaError.Codec("no document codec registered as $codecName")

/**
 * The document's state, read where the caller stands — SYNC, the
 * rows' law: the held document's state, minted once per version; a
 * document not held yet opens from its fold here. Null when the store
 * holds no document under `id`.
 */
public fun <D : Any, S : ReplicaDocState> ReplicaEngine.documentState(
    stream: String,
    id: String,
    type: ReplicaDocStateType<D, S>,
): S? {
    val codec = documentCodec<D>(type.codecName)
    return liveDocuments.publishing {
        ReplicaEngine.currentLocalSessionLocal.get()?.let(::requireLocalSession)
        val source = store ?: return@publishing null
        val row = source.read { source.doc(it, stream, id) } ?: return@publishing null
        liveDocuments.state(LiveDocuments.Key(stream, id), codec, type) { openDocument(codec, source, row, stream, id) }
    }
}

/**
 * The state of a document the engine already HOLDS — null when it is
 * not held, and the document is left closed. The reader over many
 * documents (a grid) asks this way: a warm document answers, a cold
 * one costs nothing.
 */
public fun <D : Any, S : ReplicaDocState> ReplicaEngine.documentHeldState(
    stream: String,
    id: String,
    type: ReplicaDocStateType<D, S>,
): S? {
    val codec = documentCodec<D>(type.codecName)
    return liveDocuments.heldState(LiveDocuments.Key(stream, id), codec, type)
}

/**
 * A detached durable snapshot, including past versions. An escaped read handle
 * cannot mutate the engine's authoring object. Use documentState for its cached projection.
 */
public fun <D : Any, T> ReplicaEngine.readDocument(
    stream: String,
    id: String,
    codec: DocumentCodec<D>,
    body: (D) -> T,
): T? {
    val registered = documentCodec<D>(codec.codecName)
    ReplicaEngine.currentLocalSessionLocal.get()?.let(::requireLocalSession)
    val source = store ?: return null
    val row = source.read { source.doc(it, stream, id) } ?: return null
    val document = registered.open(row.fold, peerMinter())
    val before = registered.version(document).copyOf()
    val result = body(document)
    if (!registered.version(document).contentEquals(before)) {
        throw ReplicaError.Codec("a read of $stream/$id moved the document")
    }
    return result
}

/** Reading corrupt history must not invent a blank document or write to storage. */
internal fun <D : Any> ReplicaEngine.openDocument(
    codec: DocumentCodec<D>, source: ReplicaStateStore,
    row: ReplicaStateStore.DocRow, stream: String, id: String,
): D = codec.open(row.fold, row.peer)

/**
 * A local edit on the held document: `body` moves it, the movement
 * since before is the delta the fold lane journals (`recordDocDelta`),
 * the stream's change sequence bumps and every `watchDoc` re-reads.
 * False when the body moved nothing.
 */
public suspend fun <D : Any> ReplicaEngine.updateDocument(
    stream: String,
    id: String,
    codec: DocumentCodec<D>,
    body: (D) -> Unit,
): Boolean {
    val lane = ReplicaEngine.currentLane()
    return withContext(engineContext) {
        val source = writableStore()
        val key = LiveDocuments.Key(stream, id)
        liveDocuments.with(key, codec, open = {
            val row = source.read { source.doc(it, stream, id) }
                ?: throw ReplicaError.UnknownDocument(stream, id)
            openDocument(codec, source, row, stream, id)
        }) { document, held ->
            try {
                val before = codec.version(document)
                body(document)
                val after = codec.version(document)
                if (after.contentEquals(before)) false else {
                    val delta = codec.exportDelta(document, before)
                    recordDocDeltaInScope(stream, id, delta, lane)
                    held.version = after
                    held.state = null
                    true
                }
            } catch (error: Throwable) {
                liveDocuments.evict(key)
                throw error
            }
        }
    }
}

/** A session's hold on a document: pinned, it never leaves the LRU. */
public fun ReplicaEngine.pinDocument(stream: String, id: String) {
    liveDocuments.pin(LiveDocuments.Key(stream, id))
}

public fun ReplicaEngine.unpinDocument(stream: String, id: String) {
    liveDocuments.unpin(LiveDocuments.Key(stream, id))
}

public suspend fun <D : Any> ReplicaEngine.undoDocument(
    stream: String,
    id: String,
    codec: DocumentCodec<D>,
): Boolean = updateDocument(stream, id, codec) { document -> codec.undo(document) }

public suspend fun <D : Any> ReplicaEngine.redoDocument(
    stream: String,
    id: String,
    codec: DocumentCodec<D>,
): Boolean = updateDocument(stream, id, codec) { document -> codec.redo(document) }

/**
 * The document's live state: the sync `findDoc` gave the first
 * picture, the watch delivers CHANGES — every commit of the stream
 * re-reads the state off the caller's thread and delivers when it
 * differs from the last delivered. Local edits and pulled deltas both
 * bump the sequence, so both arrive through this one door.
 */
internal fun <S : ReplicaDocState> watchDocument(
    scope: CoroutineScope,
    binding: ReplicaBinding,
    stream: String,
    includeInitial: Boolean,
    read: (ReplicaStateStore) -> S?,
    deliver: (S?) -> Unit,
): ReplicaWatch {
    val last = LastDocState<S>()
    val job = scope.launch {
        var armed = false
        while (isActive) {
            val (bound, generation) = binding.snapshot()
            val store = bound?.store
            if (store == null) {
                if (binding.snapshot().second == generation && ((includeInitial && !armed) || (armed && last.value != null))) {
                    last.value = null
                    deliver(null)
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
            ) { _, baseline ->
                val state = read(store)
                if (!isActive || binding.snapshot().second != generation) return@observeValue
                if (baseline && !wasArmed && !includeInitial) {
                    last.value = state
                    last.hasValue = true
                    return@observeValue
                }
                if (last.hasValue && last.value == state) return@observeValue
                last.value = state
                last.hasValue = true
                deliver(state)
            }
            binding.waitForChange(generation)
        }
    }
    return ReplicaWatch(job)
}

private class LastDocState<S : ReplicaDocState> {
    private val lock = ReentrantLock()
    private var stored: S? = null
    private var present = false

    var value: S?
        get() = lock.withLock { stored }
        set(newValue) = lock.withLock { stored = newValue }

    var hasValue: Boolean
        get() = lock.withLock { present }
        set(newValue) = lock.withLock { present = newValue }
}
