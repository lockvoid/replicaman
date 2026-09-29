package io.replicaman

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.suspendCancellableCoroutine
import java.io.File
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The engine's ONE identity fact: which owner's file this process holds open,
 * and the store over it. `null` is the whole closed world — no owner, no file,
 * no pool, nothing a write could land in.
 *
 * Read without the engine's dispatcher (`store`, `owner`, `database` are all
 * plain reads, and the doc plane loads folds without a hop), so the state
 * lives under a lock rather than in dispatcher-confined storage. Every WRITE
 * to it goes through the engine, so a write and a rebind can never interleave.
 */
internal class ReplicaBinding {
    data class Bound(
        val owner: Long,
        val store: ReplicaStateStore,
        val path: File,
    )

    private val lock = ReentrantLock()
    private var bound: Bound? = null

    /**
     * Bumped whenever the bound store changes. Watchers ride it: an
     * observation is only ever valid for one pool, so every change has to
     * re-arm the ones that were riding the old one.
     */
    private var generation: ULong = 0uL
    private val waiters = mutableListOf<Pair<UUID, CancellableContinuation<Unit>>>()

    val current: Bound?
        get() = lock.withLock { bound }

    val owner: Long?
        get() = lock.withLock { bound?.owner }

    val store: ReplicaStateStore?
        get() = lock.withLock { bound?.store }

    /**
     * The bound store plus the generation it was read at — a watcher arms on
     * the pair so it can park for the NEXT store without missing one that
     * arrived while it was arming.
     */
    fun snapshot(): Pair<Bound?, ULong> = lock.withLock { bound to generation }

    fun bind(replacement: Bound) {
        resume { bound = replacement }
    }

    fun unbind(): Bound? {
        var released: Bound? = null
        resume {
            released = bound
            bound = null
        }
        return released
    }

    /**
     * The cold-boot bind: build and publish under the lock, so two first
     * touches from different threads cannot each open the file.
     */
    fun bindIfUnbound(make: () -> Bound) {
        lock.lock()
        if (bound != null) {
            lock.unlock()
            return
        }
        val woken: List<Pair<UUID, CancellableContinuation<Unit>>>
        try {
            val made = make()
            bound = made
            generation++
            woken = waiters.toList()
            waiters.clear()
        } catch (error: Throwable) {
            lock.unlock()
            throw error
        }
        lock.unlock()
        for (waiter in woken) waiter.second.resumeWith(Result.success(Unit))
    }

    /**
     * The merge's swap: one store out, one store in, ONE generation. A
     * watcher must never see the unbound moment in between — an empty
     * picture mid-sign-in is the wipe the preserved world exists to avoid.
     */
    fun replace(replacement: Bound) {
        resume { bound = replacement }
    }

    /**
     * Parks until the bound store differs from the one read at `generation`.
     * Cancellable: a watcher whose consumer went away must not hold the task
     * group that is racing it.
     */
    suspend fun waitForChange(after: ULong) {
        val id = UUID.randomUUID()
        suspendCancellableCoroutine { continuation ->
            lock.lock()
            if (generation != after) {
                lock.unlock()
                continuation.resumeWith(Result.success(Unit))
                return@suspendCancellableCoroutine
            }
            waiters.add(id to continuation)
            lock.unlock()
            continuation.invokeOnCancellation { release(id) }
        }
    }

    private fun release(id: UUID) {
        lock.lock()
        val index = waiters.indexOfFirst { it.first == id }
        if (index < 0) {
            lock.unlock()
            return
        }
        waiters.removeAt(index)
        lock.unlock()
    }

    private fun resume(mutate: () -> Unit) {
        lock.lock()
        mutate()
        generation++
        val woken = waiters.toList()
        waiters.clear()
        lock.unlock()
        for (waiter in woken) waiter.second.resumeWith(Result.success(Unit))
    }
}
