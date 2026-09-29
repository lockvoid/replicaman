package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.assertEquals

class StaleReadFloorTests : ReplicaTestCase() {
    /** KILL: ignore minimumSequence; a delivered finalizing→succeeded change reads stale cache and is lost. */
    @Test fun aReadAtTheObservedSequenceBypassesAStaleCacheEntry() = runTest {
        val store = Fixture.store(); val transport = StubTransport(); val engine = Fixture.engine(store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(Fixture.note("n1", "one")), cursor = "1:", more = false)); engine.pullOnce("user")
        val decode: (String, String?, Map<String, ReplicaValue>) -> TestNote? = { id, _, fields -> TestNote.from(id, null, fields) }
        assertEquals(listOf("one"), ReplicaReads.rows(store, "notes", TestNote::class, emptyMap(), decode = decode).map { it.title })
        val before = store.read { it.queryLong("SELECT change_seq FROM stream_meta WHERE stream='notes'") }!!
        store.write { db ->
            db.exec("UPDATE snapshots SET data = '{\"title\":\"two\"}' WHERE stream='notes' AND row_id='n1'")
            db.exec("UPDATE stream_meta SET change_seq=change_seq+1 WHERE stream='notes'")
        }
        val after = store.read { it.queryLong("SELECT change_seq FROM stream_meta WHERE stream='notes'") }!!
        assertEquals(before + 1, after)
        assertEquals(listOf("one"), ReplicaReads.rows(store, "notes", TestNote::class, emptyMap(), decode = decode).map { it.title })
        assertEquals(listOf("two"), ReplicaReads.rows(store, "notes", TestNote::class, emptyMap(), minimumSequence = after, decode = decode).map { it.title })
    }
}
