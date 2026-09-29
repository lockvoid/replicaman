package io.replicaman

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The documents the engine HOLDS: the codec's live object per (stream, id)
 * with its state minted once per version — the rows' warm law for
 * documents (an unmoved document decodes nothing). A held document is
 * single-threaded by nature, so every touch — open, read, edit, absorb —
 * runs under the one lock, and a read never sees a half-edit.
 *
 * The fold in the store stays the durable truth: a pulled payload merges
 * into the fold inside the pull's transaction and reaches the held copy
 * right after commit (`absorb`); a held copy that cannot take a payload is
 * dropped, and the next open reads the fold. Unpinned documents live in an
 * LRU of `capacity`; a session pins the one it edits — a declaration on
 * the key, so a pin placed before the open holds the document once it is.
 */
internal class LiveDocuments(val capacity: Int = 32) {
    data class Key(val stream: String, val id: String)

    class Held(
        val codecName: String,
        val document: Any,
        var version: ByteArray,
        val currentVersion: () -> ByteArray,
        val importing: (List<ByteArray>) -> Unit,
    ) {
        /** The state type token and the value minted at [version]. */
        var state: Pair<Any, Any>? = null
        var lastUse: ULong = 0uL
    }

    private val lock = ReentrantLock()
    private val held = mutableMapOf<Key, Held>()
    private val pins = mutableMapOf<Key, Int>()
    private var tick: ULong = 0uL

    fun <T> publishing(body: () -> T): T = lock.withLock(body)

    /**
     * `body` on the held document, opened through `open` when it is not
     * held yet (or held under another codec).
     */
    @Suppress("UNCHECKED_CAST")
    fun <D : Any, T> with(
        key: Key,
        codec: DocumentCodec<D>,
        open: () -> D,
        body: (D, Held) -> T,
    ): T = lock.withLock {
        val entry: Held
        tick++
        val existing = held[key]
        if (existing != null && existing.codecName == codec.codecName) {
            validate(existing, key)
            entry = existing
            entry.lastUse = tick
        } else {
            val document = open()
            entry = Held(
                codecName = codec.codecName,
                document = document,
                version = codec.version(document),
                currentVersion = { codec.version(document) },
                importing = { codec.importDeltas(document, it) }
            )
            entry.lastUse = tick
            held[key] = entry
            evictOverflow()
        }
        val document = entry.document as? D
            ?: throw ReplicaError.Codec(
                "held document under ${key.stream}/${key.id} is not ${codec.codecName}"
            )
        body(document, entry)
    }

    /**
     * The state at the document's current version — the memo when the
     * document has not moved, minted otherwise.
     */
    fun <D : Any, S : ReplicaDocState> state(
        key: Key,
        codec: DocumentCodec<D>,
        type: ReplicaDocStateType<D, S>,
        open: () -> D,
    ): S = with(key, codec, open) { document, entry ->
        state(document, entry, codec, type)
    }

    /**
     * The state of a HELD document — null when the document is not held.
     * Never opens one: a peek, for a reader that must not pay for a
     * document it does not need (the grid over every listed project).
     */
    @Suppress("UNCHECKED_CAST")
    fun <D : Any, S : ReplicaDocState> heldState(
        key: Key,
        codec: DocumentCodec<D>,
        type: ReplicaDocStateType<D, S>,
    ): S? = lock.withLock {
        val entry = held[key] ?: return@withLock null
        if (entry.codecName != codec.codecName) return@withLock null
        val document = entry.document as? D ?: return@withLock null
        validate(entry, key)
        tick++
        entry.lastUse = tick
        state(document, entry, codec, type)
    }

    /** An escaped edit handle cannot become unjournaled authoring. */
    private fun validate(entry: Held, key: Key) {
        if (!entry.currentVersion().contentEquals(entry.version)) {
            held.remove(key)
            throw ReplicaError.Codec("Document was mutated outside its edit transaction")
        }
    }

    /** lock held. */
    @Suppress("UNCHECKED_CAST")
    private fun <D : Any, S : ReplicaDocState> state(
        document: D,
        entry: Held,
        codec: DocumentCodec<D>,
        type: ReplicaDocStateType<D, S>,
    ): S {
        val version = codec.version(document)
        val memo = entry.state
        if (memo != null && memo.first === type && entry.version.contentEquals(version)) {
            return memo.second as S
        }
        val state = type.state(
            document,
            version,
            codec.canUndo(document),
            codec.canRedo(document)
        )
        entry.version = version
        entry.state = type to (state as Any)
        return state
    }

    /**
     * A pulled payload into the held copy, if there is one. A copy that
     * cannot take it is dropped — the fold already merged it, the next open
     * reads that.
     */
    fun absorb(key: Key, payloads: List<ByteArray>) = lock.withLock {
        val entry = held[key] ?: return@withLock
        try {
            validate(entry, key)
            entry.importing(payloads)
            entry.version = entry.currentVersion()
            entry.state = null
        } catch (error: Throwable) {
            Log.logger.error(
                "[docs] ${key.stream}/${key.id} held copy refused a pulled payload — " +
                    "dropped, the fold stands: ${error.message}"
            )
            held.remove(key)
        }
    }

    fun evict(key: Key) = lock.withLock {
        held.remove(key)
        Unit
    }

    fun evictAll() = lock.withLock {
        held.clear()
    }

    fun evictAll(except: Set<Key>) = lock.withLock {
        held.keys.retainAll(except)
        Unit
    }

    /** A session's hold: pinned keys never leave the LRU. */
    fun pin(key: Key) = lock.withLock {
        pins[key] = (pins[key] ?: 0) + 1
        Unit
    }

    fun unpin(key: Key) = lock.withLock {
        val count = pins[key] ?: return@withLock
        if (count > 1) pins[key] = count - 1 else pins.remove(key)
        evictOverflow()
    }

    val heldCount: Int
        get() = lock.withLock { held.size }

    /** lock held. */
    private fun evictOverflow() {
        val unpinned = held.entries
            .filter { pins[it.key] == null }
            .sortedBy { it.value.lastUse }
            .toMutableList()
        while (unpinned.size > capacity) {
            held.remove(unpinned.first().key)
            unpinned.removeAt(0)
        }
    }
}
