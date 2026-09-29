package io.replicaman

import io.replicaman.ReplicaValue.Arr
import io.replicaman.ReplicaValue.Bool
import io.replicaman.ReplicaValue.Integer
import io.replicaman.ReplicaValue.Null
import io.replicaman.ReplicaValue.Num
import io.replicaman.ReplicaValue.Obj
import io.replicaman.ReplicaValue.Str
import java.io.File
import kotlinx.serialization.json.*
import org.junit.Test
import kotlin.test.*

class DataDecodeCorpusTests {
    /** Every valid scenario's fields, written by hand; a new valid scenario must bring its own. */
    private val expected: Map<String, Map<String, ReplicaValue>> = mapOf(
        "empty object" to emptyMap(),
        "Unicode, null and future fields" to mapOf(
            "title" to Str("дача 🎬"),
            "future" to obj("tags" to arr(Null, Bool(true), Num(0.25), Str("a\u0000b"))),
        ),
        "signed integer boundaries" to mapOf(
            "safe" to Num(9_007_199_254_740_991.0),
            "big" to Integer(9_007_199_254_740_993L),
            "max" to Integer(Long.MAX_VALUE),
            "min" to Integer(Long.MIN_VALUE),
        ),
        "nested future data" to mapOf(
            "o" to obj("a" to obj("b" to obj("c" to arr(Num(1.0), arr(Num(2.0), arr(Num(3.0))))))),
            "arr" to arr(obj("k" to Str("v")), obj("k" to Null)),
            "empty" to arr(),
        ),
        "finite binary64" to mapOf(
            "tiny" to Num(1e-7),
            "negativeZero" to Num(-0.0),
            "large" to Num(1e300),
            "fraction" to Num(0.1),
        ),
    )

    private fun obj(vararg fields: Pair<String, ReplicaValue>): ReplicaValue = Obj(mapOf(*fields))

    private fun arr(vararg values: ReplicaValue): ReplicaValue = Arr(listOf(*values))

    private fun scenarios(): List<JsonObject> =
        Json.parseToJsonElement(File(System.getProperty("replicaman.root"), "protocol/fixtures/stored-values.json").readText())
            .jsonArray.map { it.jsonObject }

    @Test fun storedValueContract() {
        for (scenario in scenarios()) {
            val name = scenario.getValue("name").jsonPrimitive.content
            val json = scenario.getValue("json").jsonPrimitive.content
            if (!scenario.getValue("valid").jsonPrimitive.boolean) {
                assertFails(name) { ReplicaStateStore.decodeData(json) }
                continue
            }
            val fields = ReplicaStateStore.decodeData(json)
            assertEquals(expected.getValue(name), fields, name)
            assertStoredIntegers(scenario, fields)
        }
    }

    private fun assertStoredIntegers(scenario: JsonObject, fields: Map<String, ReplicaValue>) {
        val encoded = ReplicaStateStore.encodeData(fields)
        assertEquals(fields, ReplicaStateStore.decodeData(encoded), encoded)
        for ((key, literal) in (scenario["integers"] as? JsonObject).orEmpty()) {
            val value = literal.jsonPrimitive.content
            assertEquals(value.toLong(), fields[key]?.long, key)
            assertTrue(encoded.contains("\"$key\":$value"), "integer changed: $encoded")
        }
    }

    @Test fun integerAccessNeverRoundsOrSaturates() {
        for (value in listOf(0.5, -0.5, Double.POSITIVE_INFINITY, Double.NaN, 9_223_372_036_854_775_808.0)) {
            assertNull(ReplicaValue.Num(value).long)
            assertNull(ReplicaValue.Num(value).int)
        }
    }
}
