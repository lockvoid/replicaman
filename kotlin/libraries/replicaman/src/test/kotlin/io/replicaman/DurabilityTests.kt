package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.*

class DurabilityTests : ReplicaTestCase() {
    @Test fun savedWritesRequestDurableWALCommits() {
        val store = Fixture.store()
        store.write { db ->
            assertEquals("wal", db.queryString("PRAGMA journal_mode"))
            assertEquals(2L, db.queryLong("PRAGMA synchronous"))
            assertEquals(1L, db.queryLong("PRAGMA fullfsync"))
        }
    }

    @Test fun projectionStoreCannotBecomeEditableWithAnIncompleteHistory() = runTest {
        val directory = Fixture.directory()
        val projections = Fixture.unopenedEngine(directory, StubTransport(), codecs = emptyList(), documentMode = ReplicaDocumentMode.PROJECTIONS_ONLY)
        projections.openForColdBoot(42)
        projections.close()
        val editable = Fixture.unopenedEngine(directory, StubTransport())
        assertFailsWith<ReplicaError.Storage> { editable.openForColdBoot(42) }
    }
}
