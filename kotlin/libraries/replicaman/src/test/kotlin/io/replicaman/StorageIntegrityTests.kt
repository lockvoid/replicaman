package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.*

class StorageIntegrityTests : ReplicaTestCase() {
    @Test fun corruptRowCannotBecomeANewWrite() = runTest {
        val store = Fixture.store()
        val engine = Fixture.engine(store, transport = StubTransport())
        store.write { it.exec("INSERT INTO snapshots (stream, row_id, shard, data) VALUES ('notes', 'n1', 'user', '{broken')") }
        assertFails { engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("replacement"))) }
        assertTrue(store.peekPending().isEmpty())
        assertFails { store.peekSnapshot("notes", "n1") }
    }

    @Test fun consumerDatabaseRejectsRawWrites() {
        val store = Fixture.store()
        assertFails { store.read { it.exec("DELETE FROM snapshots") } }
    }
}
