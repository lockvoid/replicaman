package io.replicaman.loro

import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.testing.fixtureWrite

import kotlinx.coroutines.runBlocking
import org.junit.Test
import io.replicaman.loro.binding.ExportMode
import io.replicaman.*
import java.time.Instant
import kotlin.test.*

/** Real Loro and SQLite: the reflected row follows the merged document in the same commit. */
class ReflectionTests : LoroTestCase() {
    private val codec = LoroReplicaCodec()
    private suspend fun world(stamp: ReplicaStamp? = null, clock: () -> Instant = { Instant.now() }): Pair<ReplicaEngine, LoroStubTransport> {
        val transport = LoroStubTransport()
        val schema = ReplicaSchema(listOf(ReplicaStreamSpec("boards", ReplicaStreamSpec.Lane.DOCUMENT,
            codec = LoroReplicaCodec.CODEC_NAME, reflections = listOf(ReplicaReflection("name", listOf("meta", "name"))), stamp = stamp)))
        return LoroFixture.engine(transport, schema = schema, clock = clock) to transport
    }
    private fun seed(name: String): ByteArray = LoroFixture.doc(7uL).let {
        LoroFixture.setMeta(it, "name", name); it.export(ExportMode.Snapshot)
    }
    private fun row(engine: ReplicaEngine): Map<String, ReplicaValue> = assertNotNull(engine.database).read { db ->
        db.prepare("SELECT data FROM snapshots WHERE stream='boards' AND row_id='b1'").use {
            if (it.step()) (ReplicaValueSerializer.fromJson(ReplicaJSON.json.parseToJsonElement(it.getText(0))) as ReplicaValue.Obj).fields else emptyMap()
        }
    }
    private suspend fun edit(engine: ReplicaEngine, key: String, value: String) {
        engine.updateDocument("boards", "b1", codec) { LoroFixture.setMeta(it.doc, key, value) }
    }
    @Test fun birthReadsTheSeedAndOverridesACallersReflectedField(): Unit = runBlocking {
        val (engine, _) = world()
        engine.createDoc("boards", "b1", seed("Plans"), 7uL, mapOf("name" to ReplicaValue.Str("Caller"), "color" to ReplicaValue.Str("red")))
        assertEquals(ReplicaValue.Str("Plans"), row(engine)["name"])
        assertEquals(ReplicaValue.Str("red"), row(engine)["color"])
    }
    @Test fun absentReflectedPathReadsNull(): Unit = runBlocking {
        val (engine, _) = world()
        val doc = LoroFixture.doc(7uL); LoroFixture.setMeta(doc, "color", "red")
        engine.createDoc("boards", "b1", doc.export(ExportMode.Snapshot), 7uL)
        assertEquals(ReplicaValue.Null, row(engine)["name"])
    }
    @Test fun rowAndFoldCommitTogetherOnlyWhenTheReflectedFieldMoves(): Unit = runBlocking {
        val (engine, _) = world(); engine.createDoc("boards", "b1", seed("Plans"), 7uL)
        val db = assertNotNull(engine.database)
        db.fixtureWrite { sql ->
            sql.prepare("CREATE TABLE reflected_writes(kind TEXT)").use { it.step() }
            sql.prepare("CREATE TRIGGER record_snapshot_update AFTER UPDATE ON snapshots BEGIN INSERT INTO reflected_writes VALUES('row'); END").use { it.step() }
        }
        edit(engine, "name", "Renamed")
        assertEquals("Renamed", LoroFixture.meta(assertNotNull(engine.docFold("boards", "b1")), "name"))
        assertEquals(ReplicaValue.Str("Renamed"), row(engine)["name"])
        val writes = db.read { sql -> sql.prepare("SELECT COUNT(*) FROM reflected_writes").use { it.step(); it.getLong(0) } }
        edit(engine, "color", "red")
        val later = db.read { sql -> sql.prepare("SELECT COUNT(*) FROM reflected_writes").use { it.step(); it.getLong(0) } }
        assertEquals(1L, writes); assertEquals(writes, later)
    }
    @Test fun undoingARenameRestoresTheRow(): Unit = runBlocking {
        val (engine, _) = world(); engine.createDoc("boards", "b1", seed("Plans"), 7uL)
        edit(engine, "name", "Renamed")
        assertTrue(engine.undoDocument("boards", "b1", codec))
        assertEquals(ReplicaValue.Str("Plans"), row(engine)["name"])
    }
    @Test fun servedDeltaMovesTheReflectedRow(): Unit = runBlocking {
        val (engine, transport) = world(); val server = LoroFixture.doc(1uL)
        LoroFixture.setMeta(server, "name", "Plans")
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocSnapshot("boards", "b1", codec.name, server.export(ExportMode.Snapshot), emptyMap())), cursor = "1:", more = false))
        engine.pullOnce("user")
        val delta = LoroFixture.editPayload(server, "name", "Elsewhere")
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocDelta("boards", "b1", 1, codec.name, delta)), cursor = "2:", more = false))
        engine.pullOnce("user")
        assertEquals(ReplicaValue.Str("Elsewhere"), row(engine)["name"])
    }
    @Test fun servedSnapshotReadsTheDocumentItBrings(): Unit = runBlocking {
        val (engine, transport) = world()
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocSnapshot("boards", "b1", codec.name, seed("Plans"), mapOf("color" to ReplicaValue.Str("red")))), cursor = "1:", more = false))
        engine.pullOnce("user")
        assertEquals(ReplicaValue.Str("Plans"), row(engine)["name"])
        assertEquals(ReplicaValue.Str("red"), row(engine)["color"])
    }
    @Test fun serverRowAndSnapshotNeverOverwriteAnOwedLocalRename(): Unit = runBlocking {
        val (engine, transport) = world(); val bytes = seed("Plans")
        engine.createDoc("boards", "b1", bytes, 7uL)
        engine.drain()
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.DocSnapshot("boards", "b1", codec.name, bytes, emptyMap())
        ), cursor = "baseline", more = false))
        engine.pullOnce()
        edit(engine, "name", "Mine")
        transport.failPushes(true)
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.RowSet("boards", "b1", null, mapOf("name" to ReplicaValue.Str("Plans"), "color" to ReplicaValue.Str("red")))), cursor = "1:", more = false))
        engine.pullOnce("user")
        assertEquals(ReplicaValue.Str("Mine"), row(engine)["name"])
        assertEquals(ReplicaValue.Str("red"), row(engine)["color"])
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.DocSnapshot("boards", "b1", codec.name, bytes, mapOf("name" to ReplicaValue.Str("Plans")))), cursor = "2:", more = false))
        engine.pullOnce("user")
        assertEquals(ReplicaValue.Str("Mine"), row(engine)["name"])
    }
    @Test fun explicitRebuildHasNoSyntheticUndoStep(): Unit = runBlocking {
        val (engine, _) = world(); engine.createDoc("boards", "b1", seed("Plans"), 7uL)
        engine.rebuildDocument("boards", "b1", seed("Plans"), 8uL)
        assertEquals("Plans", engine.documentState("boards", "b1", BoardState)?.name)
        assertEquals(8uL, engine.docRow("boards", "b1")?.peer)
        edit(engine, "color", "red")
        assertTrue(engine.undoDocument("boards", "b1", codec))
        assertFalse(engine.undoDocument("boards", "b1", codec))
        assertEquals(ReplicaValue.Str("Plans"), row(engine)["name"])
    }
    @Test fun localDocumentMovementUpdatesTheRowsClock(): Unit = runBlocking {
        var clock = Instant.ofEpochSecond(1_800_000_000)
        val (engine, _) = world(ReplicaStamp.standard) { clock }
        engine.createDoc("boards", "b1", seed("Plans"), 7uL)
        val born = row(engine)["createdAt"]
        clock = clock.plusSeconds(60); edit(engine, "color", "red")
        assertEquals(ReplicaValue.Str("2027-01-15T08:00:00Z"), born)
        assertEquals(born, row(engine)["createdAt"])
        assertEquals(ReplicaValue.Str("2027-01-15T08:01:00Z"), row(engine)["updatedAt"])
    }
    @Test fun rowWithoutALocalDocumentTakesTheServerImage(): Unit = runBlocking {
        val (engine, transport) = world()
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(ReplicaFrame.RowSet("boards", "b1", null, mapOf("name" to ReplicaValue.Str("Plans")))), cursor = "1:", more = false))
        engine.pullOnce("user"); assertEquals(ReplicaValue.Str("Plans"), row(engine)["name"])
    }
}
