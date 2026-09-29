package io.replicaman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** The public commit stream describes durable transactions, including nested writers. */
class CommitNotificationTests : ReplicaTestCase() {
    /** KILL: let a nested write publish before the outer commit — observers see partial or uncommitted data. */
    @Test
    fun nestedWritesPublishOneCompleteDurablePicture(): Unit = runBlocking {
        val store = Fixture.store("nested-notification")
        store.write { it.exec("CREATE TABLE notification_probe (value TEXT PRIMARY KEY)") }
        val pictures = mutableListOf<List<String>>()
        val observer = CoroutineScope(Dispatchers.Unconfined).launch(start = CoroutineStart.UNDISPATCHED) {
            store.commits.collect { pictures.add(store.read { it.queryStrings("SELECT value FROM notification_probe ORDER BY value") }) }
        }
        try {
            val before = store.commits.value
            store.write { db ->
                db.exec("INSERT INTO notification_probe VALUES ('a')")
                store.write { it.exec("INSERT INTO notification_probe VALUES ('b')") }
                assertEquals(before, store.commits.value, "nested authoring is not a committed picture")
                assertEquals(listOf(emptyList()), pictures)
            }
            assertEquals(listOf(emptyList(), listOf("a", "b")), pictures)
            assertEquals(before + 1, store.commits.value)
        } finally { observer.cancelAndJoin() }
    }

    /** KILL: publish a nested write even when the outer transaction rolls back — a notification claims absent bytes. */
    @Test
    fun anOuterRollbackPublishesNeitherNestedRowsNorACommit(): Unit = runBlocking {
        val store = Fixture.store("rolled-back-notification")
        store.write { it.exec("CREATE TABLE notification_probe (value TEXT PRIMARY KEY)") }
        val before = store.commits.value
        assertFailsWith<IllegalStateException> {
            store.write { db ->
                db.exec("INSERT INTO notification_probe VALUES ('a')")
                store.write { it.exec("INSERT INTO notification_probe VALUES ('b')") }
                error("abort the outer transaction")
            }
        }
        assertEquals(before, store.commits.value)
        assertEquals(emptyList(), store.read { it.queryStrings("SELECT value FROM notification_probe") })
        store.write { it.exec("INSERT INTO notification_probe VALUES ('next')") }
        assertEquals(before + 1, store.commits.value)
        assertEquals(listOf("next"), store.read { it.queryStrings("SELECT value FROM notification_probe") })
    }
}
