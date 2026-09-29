package io.replicaman

import io.replicaman.support.*
import io.replicaman.testing.ReplicaPullResponse
import java.io.File
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import org.junit.Test
import kotlin.test.*

class IntegrityTests : ReplicaTestCase() {
    @Test fun frozenIndependentHashVectors() {
        val vector = Json.parseToJsonElement(File(System.getProperty("replicaman.root"), "protocol/fixtures/integrity.json").readText()).jsonObject
        val hash = ReplicaIntegrityHash("replicaman-view")
        for (item in vector.getValue("rows").jsonArray) {
            val row = item.jsonObject
            for (field in listOf("stream", "id", "incarnation", "revision")) hash.append(row.getValue(field).jsonPrimitive.content)
        }
        assertEquals(vector.getValue("view_digest").jsonPrimitive.content, hash.finish())
        assertEquals(vector.getValue("empty_view_digest").jsonPrimitive.content, ReplicaIntegrityHash("replicaman-view").finish())
        assertEquals(vector.getValue("base_digest").jsonPrimitive.content,
            ReplicaIntegrityHash.base("notes", "é/🙂", "user", "life-e", 2, null,
                """{"n":9007199254740993}""", "loro@1", byteArrayOf(0, 1, -1)))
    }

    @Test fun storedCorruptionFailsVerificationWithoutChangingLocalIntent() = runBlocking {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.createRow("notes", "offline", null, mapOf("title" to ReplicaValue.Str("keep me")))
        val row = ReplicaBaseRow("lifetime", 2, null, mapOf("title" to ReplicaValue.Str("server")), "loro@1", byteArrayOf(0, 1, 2))
        store.write { db ->
            store.saveBase(db, "notes", "server", "user", row)
            store.setCursor(db, "checkpoint", "user")
        }
        val healthy = store.read { store.integritySnapshot(it, "user") }
        assertEquals(1, healthy.count)
        val before = store.peekPending().map { it.payload.toList() }
        for (assignment in listOf("data = '{}'", "fold = X'FF'", "revision = 3", "incarnation = 'other'", "type = ''", "integrity = NULL")) {
            store.write { db ->
                store.saveBase(db, "notes", "server", "user", row)
                db.exec("UPDATE base SET $assignment")
            }
            assertFailsWith<ReplicaError.Storage>(assignment) { store.read { store.integritySnapshot(it, "user") } }
            assertEquals(before, store.peekPending().map { it.payload.toList() })
        }
        store.write { store.saveBase(it, "notes", "server", "user", row) }
        assertEquals(healthy.digest, store.read { store.integritySnapshot(it, "user") }.digest)
    }

    /** KILL: verify against the staged cursor, or compare counts without the membership digest. */
    @Test fun verificationComparesThePublishedBaseWithTheServersMembership() = runBlocking {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "a"), Fixture.note("n2", "b")), "c1", more = false))
        engine.pullOnce()

        engine.verifyIntegrity("user")
        assertEquals(ReplicaValue.Str("c1"), transport.protocolFixture.requests(ReplicaEndpoint.VERIFY).single()["cursor"])

        store.write { it.exec("DELETE FROM base WHERE stream = 'notes' AND row_id = 'n2'") }
        assertEquals("ReplicaDiverged", assertFailsWith<ReplicaError.Protocol> { engine.verifyIntegrity("user") }.code)
        assertEquals(ReplicaValue.Str("b"), store.peekSnapshot("notes", "n2")?.data?.get("title"), "verification rewrites nothing")
    }

    /** KILL: pull inside `verifyIntegrity` when the server answers `CursorBehind` — the caller is not told its base is behind. */
    @Test fun aCursorBehindTheServerVerifiesOnceThePullCatchesUp() = runBlocking {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "a")), "c1", more = false))
        engine.pullOnce()
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n2", "b")), "c2", more = true))
        engine.pullOnce()
        val pulls = transport.pullCount()

        assertEquals("CursorBehind", assertFailsWith<ReplicaError.Protocol> { engine.verifyIntegrity("user") }.code)
        assertEquals(pulls, transport.pullCount(), "verification never pulls")

        transport.queuePull("user", ReplicaPullResponse(emptyList(), "c3", more = false))
        engine.pullOnce()
        engine.verifyIntegrity("user")
    }
}
