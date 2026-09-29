package io.replicaman

import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** ReplicaWriteGate.swift: identity changes wait for every admitted local transaction. */
internal class ReplicaWriteGate {
    private val lock = ReentrantLock()
    private var closed = false
    private var active = 0
    private val waiters = mutableListOf<CompletableDeferred<Unit>>()

    fun enter() = lock.withLock {
        if (closed) throw ReplicaError.IdentityTransitionInProgress
        active += 1
    }

    fun leave() {
        val woken = lock.withLock {
            active -= 1
            if (closed && active == 0) waiters.toList().also { waiters.clear() }
            else emptyList()
        }
        for (waiter in woken) waiter.complete(Unit)
    }

    suspend fun close() {
        val waiter = lock.withLock {
            closed = true
            if (active == 0) null else CompletableDeferred<Unit>().also { waiters.add(it) }
        }
        waiter?.await()
    }

    fun open() = lock.withLock { closed = false }
}
