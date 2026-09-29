package io.replicaman

import java.io.File
import kotlinx.serialization.json.*
import org.junit.Test
import kotlin.test.*

class WireDecodeContractTests {
    @Test fun sharedJournalContract() {
        val file = File(System.getProperty("replicaman.root"), "protocol/fixtures/journal-decode.json")
        val cases = Json.parseToJsonElement(file.readText()).jsonArray
        assertTrue(cases.size > 10)
        for (item in cases) {
            val scenario = item.jsonObject
            val name = scenario.getValue("name").jsonPrimitive.content
            val op = scenario.getValue("op").jsonPrimitive.content
            if (scenario.getValue("valid").jsonPrimitive.boolean) {
                assertEquals("op1", ReplicaOp.fromValue(ReplicaJSON.decodeValue(op)).id, name)
            } else {
                assertFails(name) { ReplicaOp.fromValue(ReplicaJSON.decodeValue(op)) }
            }
        }
    }

    /** The engine's own envelope path: header, then the page with every frame. */
    private fun decodePull(text: String): ReplicaPullPage {
        val value = ReplicaProtocol.decode(text.toByteArray())
        ReplicaProtocol.validateHeader(value, ReplicaSchema(emptyList(), namespace = "notes"), null)
        return ReplicaPullPage.decode(value)
    }

    @Test fun sharedPullContract() {
        val file = File(System.getProperty("replicaman.root"), "protocol/fixtures/pull-decode.json")
        val cases = Json.parseToJsonElement(file.readText()).jsonArray
        assertTrue(cases.size > 20)
        for (item in cases) {
            val scenario = item.jsonObject
            val name = scenario.getValue("name").jsonPrimitive.content
            val pull = scenario.getValue("pull").jsonPrimitive.content
            if (scenario.getValue("valid").jsonPrimitive.boolean) {
                assertEquals(1, decodePull(pull).frames.size, name)
            } else {
                assertFails(name) { decodePull(pull) }
            }
        }
    }
}
