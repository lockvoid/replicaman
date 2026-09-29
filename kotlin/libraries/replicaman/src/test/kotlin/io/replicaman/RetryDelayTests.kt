package io.replicaman

import java.time.Instant
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RetryDelayTests {
    @Test
    fun secondsDatesAndInvalidAdvice() {
        val now = Instant.EPOCH
        assertEquals(12.0, ReplicaRetryDelay.seconds("12", now))
        assertEquals(12.0, ReplicaRetryDelay.seconds("Thu, 01 Jan 1970 00:00:12 GMT", now))
        assertEquals(0.0, ReplicaRetryDelay.seconds("Thu, 01 Jan 1970 00:00:00 GMT", now.plusSeconds(1)))
        for (invalid in listOf("-1", "1.5", "NaN", "Infinity", "tomorrow", "")) {
            assertNull(ReplicaRetryDelay.seconds(invalid, now), invalid)
        }
    }

    @Test(timeout = 5000)
    fun cancellationDoesNotWaitForServerDeadline() = runBlocking {
        val delay = ReplicaRetryDelay()
        delay.record("86400")
        val waiting = async(start = CoroutineStart.UNDISPATCHED) { delay.await() }
        waiting.cancelAndJoin()
        assertTrue(waiting.isCancelled)
    }
}
