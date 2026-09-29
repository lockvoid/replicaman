package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.TestNote
import io.replicaman.support.peekSnapshot
import kotlin.test.assertFails
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The importer is TOTAL, typing is best-effort: unknown stream / unknown STI `type` / unknown fields are
 * stored-and-skipped, never a thrown import.
 */
class DecodeToleranceTests : ReplicaTestCase() {

    /** KILL: refuse a frame whose stream the schema does not declare — the batch throws. */
    @Test
    fun knownStreamRetainsUnknownTypeAndFields() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    // A known stream with an unknown STI type and an unknown field.
                    ReplicaFrame.RowSet(
                        "notes", "n1", "HoloNote",
                        mapOf(
                            "title" to ReplicaValue.Str("future"),
                            "hologram" to ReplicaValue.Bool(true)
                        )
                    ),
                    Fixture.note("n2", "plain"),
                ),
                cursor = "4:", more = false
            )
        )

        // The import itself must not throw.
        engine.pullOnce("user")

        val holo = store.peekSnapshot("notes", "n1")
        assertNotNull(holo)
        assertEquals(ReplicaValue.Bool(true), holo.data["hologram"], "unknown fields are stored verbatim")

        // The typed layer skips what it cannot decode — and only that.
        val stream = RowStream(engine, TestNote)
        assertNull(stream.find("n1"), "unknown STI type decodes to nothing, not a crash")
        assertEquals(listOf("n2"), stream.where().map { it.id }, "typed reads skip the undecodable row")
    }

    @Test fun unknownStreamRefusesTheWholeCheckpoint() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            Fixture.note("n1", "valid"),
            ReplicaFrame.RowSet("widgets", "w1", null, emptyMap())
        ), cursor = "4:", more = false))
        val error = kotlin.test.assertFailsWith<ReplicaError.Protocol> { engine.pullOnce() }
        assertEquals("UpgradeRequired", error.code)
        assertNull(store.peekSnapshot("notes", "n1"))
        assertNull(engine.currentCursor())
    }

    /** A page cannot pass a frame that the client cannot apply. */
    @Test
    fun pullRefusesTheWholePageWhenAFrameIsInvalid() {
        val json = """
        {"shard": "user", "reset": false, "frames": [
            {"frame": "row.set", "stream": "notes", "id": "n1", "incarnation": "life-1", "revision": "1", "data": {"title": "ok"}},
            {"frame": "row.vanish", "stream": "notes", "id": "n2", "incarnation": "life-2"},
            {"frame": "doc.delta", "stream": "boards", "id": "b1", "incarnation": "life-3", "seq": 3, "codec": "loro@1", "payload": "AAEC"},
            {"frame": "doc.delta", "stream": "boards", "id": "b2", "incarnation": "life-4", "codec": "loro@1", "payload": "%%%not-base64%%%"}
        ], "cursor": "7:k", "more": true}
        """.trimIndent()

        assertFails { ReplicaPullPage.decode(ReplicaJSON.decodeValue(json)) }
    }
}
