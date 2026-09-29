package io.replicaman.loro

import io.replicaman.DocumentValue
import io.replicaman.DocumentValueSerializer
import io.replicaman.loro.binding.LoroDoc
import kotlinx.serialization.json.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.File
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@RunWith(Parameterized::class)
class DocumentCorpusTests(private val name: String) {
    companion object {
        private val root = File(System.getProperty("replicaman.root"), "protocol/fixtures/crdt_convergence")
        private val index = Json.parseToJsonElement(File(root, "INDEX.json").readText()).jsonObject["cases"]!!.jsonObject

        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun cases(): List<Array<String>> {
            check(index.size == 9) { "the frozen nine-case corpus must execute" }
            return index.keys.sorted().map { arrayOf(it) }
        }
    }

    @Test
    fun frozenServerProjectionSurvivesReorderingReplayAndReopen() {
        val directory = File(root, name)
        for ((file, digest) in index.getValue(name).jsonObject) {
            val bytes = File(directory, file).readBytes()
            val actual = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
            assertEquals(digest.jsonPrimitive.content, actual, "$name/$file changed")
        }
        val manifest = Json.parseToJsonElement(File(directory, "manifest.json").readText()).jsonObject
        val blobs = manifest.getValue("blobs").jsonArray.map { File(directory, it.jsonPrimitive.content).readBytes() }
        assertEquals(3, blobs.size)
        val projection = Json.parseToJsonElement(File(directory, "expected.json").readText()).jsonObject
        val expected = JsonObject(projection.mapValues { (_, value) ->
            if (value is JsonArray) JsonObject(value.associate { entry ->
                val fields = entry.jsonObject
                fields.getValue("key").jsonPrimitive.content to JsonObject(fields - "key")
            }) else value.also { assertTrue(it is JsonObject) }
        })
        val expectedValue = Json.decodeFromJsonElement(DocumentValueSerializer, expected) as DocumentValue.Map
        val codec = LoroReplicaCodec()
        for (order in listOf(listOf(0, 1, 2, 0, 1, 2), listOf(0, 2, 1, 2, 1, 0))) {
            var fold: ByteArray? = null
            for (index in order) fold = codec.merge(fold, blobs[index], emptyList()).fold
            LoroDoc().use { reopened ->
                reopened.import(checkNotNull(fold))
                val actual = documentValueOf(reopened.getDeepValue()) as DocumentValue.Map
                val roots = expectedValue.value.keys.associateWith { DocumentValue.Map(emptyMap()) } + actual.value
                assertEquals(expectedValue, DocumentValue.Map(roots), "$name: order $order")
            }
        }
    }
}
