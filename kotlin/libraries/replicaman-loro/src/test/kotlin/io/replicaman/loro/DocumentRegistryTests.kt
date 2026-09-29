package io.replicaman.loro

import io.replicaman.*
import io.replicaman.loro.binding.*
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.*

class DocumentRegistryTests : LoroTestCase() {
    private fun entry(key: String, value: Long) = DocumentEntry(key, mapOf("value" to DocumentValue.Int(value)))
    private fun values(doc: LoroDoc) = (doc.getDeepValue() as LoroValue.Map).value

    @Test fun refusedRegistryEntryRollsBackTheWholeEditAndCannotLeakIntoTheNextSave() = runBlocking {
        val engine = LoroFixture.engine(LoroStubTransport())
        val author = LoroFixture.doc(7uL)
        LoroFixture.setMeta(author, "name", "Before")
        author.getMap("items").insert("broken", LoroValue.String("peer value"))
        val boards = DocumentStream(engine, Board)
        boards.createDoc("b1", author.export(ExportMode.Snapshot), 100uL)
        engine.drain()
        val before = assertNotNull(boards.findDoc("b1", BoardState))
        val fold = assertNotNull(engine.docRow("boards", "b1")).fold

        assertFailsWith<LoroException> {
            boards.updateDoc("b1", LoroReplicaCodec()) {
                it.doc.writeFields("meta", mapOf("name" to DocumentValue.String("Unsaved")))
                it.doc.writeRegistry("items", listOf(entry("broken", 9)))
            }
        }
        assertEquals("Before", boards.findDoc("b1", BoardState)?.name)
        assertContentEquals(before.version, boards.findDoc("b1", BoardState)?.version)
        assertContentEquals(fold, engine.docRow("boards", "b1")?.fold)
        assertTrue(engine.pendingOps().isEmpty())
        boards.updateDoc("b1", LoroReplicaCodec()) { it.doc.writeMapField("meta", "color", DocumentValue.String("blue")) }
        assertEquals("Before", boards.findDoc("b1", BoardState)?.name)
        val saved = LoroFixture.doc(200uL, assertNotNull(engine.docRow("boards", "b1")).fold)
        assertEquals(LoroValue.String("peer value"), saved.getMap("items").get("broken")?.asValue())
    }

    @Test fun staleSavePreservesConcurrentAdditionsDeletionsAndUntouchedFields() {
        val doc = LoroFixture.doc(7uL)
        val base = listOf(entry("a", 1), entry("b", 2), entry("d", 4))
        doc.writeRegistry("items", base)
        doc.getMap("items").delete("b")
        doc.writeEntryField("items", "a", "value", DocumentValue.Int(9))
        doc.writeEntryField("items", "c", "value", DocumentValue.Int(3))
        doc.writeRegistry("items", listOf(entry("a", 1), entry("b", 2)), base)
        val items = (values(doc)["items"] as LoroValue.Map).value
        assertEquals(setOf("a", "c"), items.keys)
        assertEquals(LoroValue.I64(9), (items["a"] as LoroValue.Map).value["value"])
    }

    @Test fun independentlyCreatedSameKeyMergesFieldsAcrossPeers() {
        val left = LoroFixture.doc(7uL)
        val right = LoroFixture.doc(8uL)
        left.writeEntryField("items", "shared", "title", DocumentValue.String("left"))
        right.writeEntryField("items", "shared", "count", DocumentValue.Int(2))
        left.import(right.export(ExportMode.Snapshot))
        right.import(left.export(ExportMode.Snapshot))
        val expected = LoroValue.Map(mapOf("shared" to LoroValue.Map(mapOf("title" to LoroValue.String("left"), "count" to LoroValue.I64(2)))))
        assertEquals(expected, values(left)["items"])
        assertEquals(values(left), values(right))
    }

    @Test fun birthRetainsNullAndAnIdleEditProducesNoOperations() {
        val doc = LoroFixture.doc(7uL)
        val fields = mapOf("nullable" to DocumentValue.Null, "count" to DocumentValue.Int(2))
        doc.initializeRegistry("items", listOf(DocumentEntry("a", fields)))
        doc.commit()
        val before = doc.oplogVv().encode()
        doc.writeRegistry("items", listOf(DocumentEntry("a", fields)))
        doc.writeEntryField("items", "a", "absent", DocumentValue.Null)
        doc.commit()
        assertContentEquals(before, doc.oplogVv().encode())
        val item = ((values(doc)["items"] as LoroValue.Map).value["a"] as LoroValue.Map).value
        assertEquals(LoroValue.Null, item["nullable"])
        assertFalse(item.containsKey("absent"))
    }

    @Test fun authoritativeEmptyRegistryClearsIt() {
        val doc = LoroFixture.doc(7uL)
        doc.writeRegistry("items", listOf(entry("a", 1)))
        doc.writeRegistry("items", emptyList())
        assertTrue(doc.getMap("items").keys().isEmpty())
    }
}
