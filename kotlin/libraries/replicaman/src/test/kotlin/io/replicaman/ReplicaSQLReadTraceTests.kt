package io.replicaman

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import org.junit.After
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails

class ReplicaSQLReadTraceTests {
    private val previous = ReplicaSQLReadTrace.observer

    @After fun restoreObserver() { ReplicaSQLReadTrace.observer = previous }

    @Test fun theTraceCountsActualPreparedReadsRatherThanRowsOrWrites() {
        BundledSQLiteDriver().open(":memory:").use { db ->
            val reads = mutableListOf<ReplicaSQLReadTrace.Read>()
            ReplicaSQLReadTrace.observer = { reads.add(it) }
            db.exec("CREATE TABLE notes (title TEXT NOT NULL)")
            db.exec("INSERT INTO notes VALUES (?), (?), (?)", listOf("alpha", "beta", "gamma"))
            val all = db.queryStrings("SELECT title FROM notes ORDER BY title")
            val one = db.queryString("SELECT title FROM notes WHERE title = ?", listOf("beta"))
            assertEquals(listOf("alpha", "beta", "gamma"), all)
            assertEquals("beta", one)
            assertEquals(listOf(
                ReplicaSQLReadTrace.Read("SELECT title FROM notes ORDER BY title", emptyList()),
                ReplicaSQLReadTrace.Read("SELECT title FROM notes WHERE title = ?", listOf("beta")),
            ), reads)
            assertFails { db.queryLong("SELECT absent_column FROM notes") }
            assertEquals(2, reads.size, "failed prepares are not successful prepared reads")
        }
    }

    @Test fun theReadOnlyTraceCannotChangeBindingsOrTurnAReadIntoAFailure() {
        BundledSQLiteDriver().open(":memory:").use { db ->
            val bytes = byteArrayOf(1, 2, 3)
            ReplicaSQLReadTrace.observer = { event ->
                (event.arguments.single() as ByteArray)[0] = 9
                error("a diagnostic observer failed")
            }
            assertEquals("010203", db.queryString("SELECT hex(?)", listOf(bytes)))
            assertEquals(1.toByte(), bytes[0])
        }
    }
}
