package io.replicaman

import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/** Server backpressure applies to every endpoint sharing this transport. */
internal class ReplicaRetryDelay {
    private var deadline: TimeSource.Monotonic.ValueTimeMark? = null

    suspend fun await() {
        while (true) {
            val remaining = synchronized(this) { deadline?.elapsedNow()?.unaryMinus() }
            if (remaining == null || !remaining.isPositive()) break
            delay(remaining)
        }
        currentCoroutineContext().ensureActive()
    }

    fun record(value: String?) {
        val seconds = seconds(value) ?: return
        if (seconds <= 0) return
        val next = TimeSource.Monotonic.markNow() + seconds.coerceAtMost(86_400.0).seconds + Random.nextLong(256).milliseconds
        synchronized(this) {
            if (deadline == null || next > deadline!!) deadline = next
        }
    }

    companion object {
        fun seconds(value: String?, now: Instant = Instant.now()): Double? {
            val text = value?.trim() ?: return null
            if (text.isNotEmpty() && text.all { it in '0'..'9' }) {
                return text.toDoubleOrNull()?.takeIf { it.isFinite() }
            }
            return try {
                val date = ZonedDateTime.parse(text, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                java.time.Duration.between(now, date).toMillis().coerceAtLeast(0) / 1000.0
            } catch (_: DateTimeParseException) {
                // Optional advice cannot turn an HTTP failure into success.
                // Invalid advice leaves the normal failure policy in force.
                null
            }
        }
    }
}
