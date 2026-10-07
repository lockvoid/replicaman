package io.replicaman

import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.io.ByteArrayOutputStream
import kotlin.test.*

class RecoveryStoreTests : ReplicaTestCase() {
    @Test
    fun damagedAuthoringAndLargeFoldsExportInBoundedParts() = runBlocking<Unit> {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        val fold = ByteArray(600_001) { 97 }
        engine.createDoc("boards", "b1", fold, 7uL)
        store.write { db ->
            db.exec("UPDATE snapshots SET data = '{damaged' WHERE row_id = 'b1'")
            db.exec("UPDATE intents SET payload = '{damaged' WHERE row_id = 'b1'")
            store.archiveEntity(db, "boards", "b1", "test export")
        }

        val record = store.recoveryRecords(limit = 1).single()
        val parts = mutableListOf<ReplicaRecoveryPart>()
        while (true) {
            val page = store.recoveryParts(record.id, parts.lastOrNull(), limit = 2)
            if (page.isEmpty()) break
            assertTrue(page.size <= 2)
            parts += page
        }
        val document = parts.single { it.kind == "document.fold" }
        val exported = ByteArrayOutputStream()
        while (exported.size().toLong() < document.byteCount) {
            val chunk = store.recoveryChunk(record.id, document, exported.size().toLong(), limit = 65_536)
            assertTrue(chunk.isNotEmpty() && chunk.size <= 65_536)
            exported.write(chunk)
        }
        assertContentEquals(fold, exported.toByteArray())
        for (kind in listOf("row.data", "intent")) {
            val part = parts.single { it.kind == kind }
            assertContentEquals("{damaged".toByteArray(), store.recoveryChunk(record.id, part))
        }
        assertContentEquals(fold, store.fold("boards", "b1"))

        var attempts = 0
        val failure = java.io.IOException("export disk full")
        assertSame(failure, assertFailsWith<java.io.IOException> {
            store.exportRecovery(record.id) {
                attempts++
                if (attempts == 3) throw failure
            }
        })
        assertEquals(1, store.recoveryRecords().size, "failed export must retain the archive")

        val lines = mutableListOf<ReplicaValue>()
        store.exportRecovery(record.id) {
            assertTrue(it.size < 360_000, "export must stream large folds")
            lines += ReplicaJSON.decodeValue(it.toString(Charsets.UTF_8))
        }
        assertEquals("replicaman-recovery", lines.first()["format"]?.string)
        assertEquals("complete", lines.last()["type"]?.string)
        assertEquals(parts.size, lines.last()["parts"]?.int)
        assertEquals(parts.sumOf { it.byteCount }.toString(), lines.last()["bytes"]?.string)
        var kind: String? = null
        val reconstructed = ByteArrayOutputStream()
        for (line in lines) {
            if (line["type"]?.string == "part") kind = line["kind"]?.string
            if (kind == "document.fold" && line["type"]?.string == "chunk") {
                assertEquals(reconstructed.size().toString(), line["offset"]?.string)
                val chunk = java.util.Base64.getDecoder().decode(line["content"]?.string)
                // SHA-256 constants computed independently from the original fixture bytes.
                val digest = if (chunk.size == 262_144)
                    "dd3dde87623d9a6b354c68c943d189c89c63652d945e7bbdf0986cae91a49521"
                else "845671f868efb188716917bfc3b8a3c61c74a8a77195edaae9df445de3ec0a45"
                assertEquals(digest, line["sha256"]?.string)
                reconstructed.write(chunk)
            }
        }
        assertContentEquals(fold, reconstructed.toByteArray())

        store.removeRecoveryRecord(record.id)
        assertTrue(store.recoveryRecords().isEmpty())
        assertTrue(store.recoveryParts(record.id).isEmpty())
    }

    @Test
    fun archiveFailureRollsBackEveryPartAndKeepsTheOriginal() = runBlocking<Unit> {
        val store = Fixture.store()
        val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("keep")))
        val pending = store.peekPending()
        store.write { db ->
            db.exec("""
                CREATE TRIGGER fail_archive BEFORE INSERT ON recovery_parts
                WHEN NEW.kind = 'intent' BEGIN SELECT RAISE(ABORT, 'archive failed'); END
            """.trimIndent())
        }
        assertFails { store.write { store.archiveEntity(it, "notes", "n1", "must roll back") } }
        assertTrue(store.recoveryRecords().isEmpty())
        assertEquals(0, store.read { it.queryLong("SELECT count(*) FROM recovery_parts") }?.toInt())
        assertEquals(pending, store.peekPending())
        assertEquals(ReplicaValue.Str("keep"), store.peekSnapshot("notes", "n1")?.data?.get("title"))
    }

    /**
     * Push and pull are independent: a journal the engine cannot read fails every explicit
     * drain loudly and reaches health from the pull's own barrier, while the shard keeps
     * receiving. The damaged bytes stay as they are.
     */
    @Test
    fun aCorruptJournalFailsTheDrainLoudlyAndNeverKeepsTheShardFromReceiving() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("keep")))
        store.write { it.exec("UPDATE intents SET payload = '{damaged'") }
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n2", "from the server")), "c1", more = false))

        repeat(2) { assertFailsWith<ReplicaError.Storage> { engine.drain() } }
        val published = engine.pullOnce("user")

        assertEquals(1, published)
        assertEquals(ReplicaValue.Str("from the server"), store.peekSnapshot("notes", "n2")?.data?.get("title"))
        assertEquals(1, transport.pullCount())
        assertEquals("c1", engine.currentCursor("user"))
        assertEquals("push before pull", engine.health.failure.value?.operation)
        assertEquals("{damaged", store.read { it.queryString("SELECT payload FROM intents") })
    }
}
