package io.replicaman

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNamingStrategy
import io.replicaman.generated.app.documents.DeckDocument
import io.replicaman.generated.app.documents.from
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * The direct decoder must be a drop-in for the JSON round-trip it replaces
 * (`Map<String, DocumentValue>` → Json encode → Json decode + snake_case naming):
 * same successes, same refusals, byte-free.
 */
class DocumentValueDecodingTests {

    @Serializable
    private data class Style(
        val fontName: String,
        val fontSize: Double,
        val lineCount: Int,
        val letterSpacing: Float? = null,
        val shadow: Shadow? = null,
        val tags: List<String>,
    ) {
        @Serializable
        data class Shadow(val offsetX: Double, val opacity: Double)
    }

    private val fields: Map<String, DocumentValue> = mapOf(
        "font_name" to DocumentValue.String("Diatype"),
        "font_size" to DocumentValue.Double(17.5),
        "line_count" to DocumentValue.Int(3),
        "letter_spacing" to DocumentValue.Double(0.4),
        "shadow" to DocumentValue.Map(
            mapOf("offset_x" to DocumentValue.Double(1.5), "opacity" to DocumentValue.Int(1)),
        ),
        "tags" to DocumentValue.List(
            listOf(DocumentValue.String("caption"), DocumentValue.String("bold")),
        ),
    )

    /**
     * The old bridge, verbatim — the parity oracle it has to be a drop-in
     * for. It THROWS rather than returning null: a refusal is a fact to assert,
     * and a `runCatching` here would turn an unexpected encode failure into a
     * passing `null == null` (ban 4).
     */
    @OptIn(ExperimentalSerializationApi::class)
    private val bridgeJson = Json { namingStrategy = JsonNamingStrategy.SnakeCase }

    private inline fun <reified T> jsonBridge(fields: Map<String, DocumentValue>): T =
        bridgeJson.decodeFromString(Json.encodeToString(fields))

    /** Kill: drop the snake_case fallback in `KeyedDecoder.lookup` and every key misses. */
    @Test
    fun `decodes snake cased fields into camel case properties`() {
        val style = DocumentValueDecoding.decode<Style>(fields)

        assertEquals(jsonBridge<Style>(fields), style)
        assertEquals("Diatype", style.fontName)
        assertEquals(3, style.lineCount)
        assertEquals(1.0, style.shadow?.opacity, "an exact Int decodes into a Double property")
        assertEquals(listOf("caption", "bold"), style.tags)
    }

    /**
     * Kill: make `decodeNotNullMark` answer true for `DocumentValue.Null`.
     * (The non-nullable half of that rule is graded by `a null on a
     * non-nullable field with a default takes the default` — `Style` has no
     * such property, so dropping the rule leaves THIS test green. Measured.)
     */
    @Test
    fun `missing optional and null both land as null`() {
        val sparse = (fields - "letter_spacing") + ("shadow" to DocumentValue.Null)

        val style = DocumentValueDecoding.decode<Style>(sparse)

        assertEquals(jsonBridge<Style>(sparse), style)
        assertNull(style.letterSpacing)
        assertNull(style.shadow)
    }

    /**
     * Kill: decode `Int` with `toLong()` instead of the exact check — 5.5 then
     * silently becomes 5 and a fractional value from a peer is swallowed.
     */
    @Test
    fun `int properties accept exact doubles and refuse fractions`() {
        // No JSON-bridge parity line here: kotlinx's Json refuses `5.0` for an
        // Int outright, where Foundation's accepted it. The tree decoder keeps
        // the iOS contract, which is what a document written by iOS needs.
        val whole = fields + ("line_count" to DocumentValue.Double(5.0))
        assertEquals(5, DocumentValueDecoding.decode<Style>(whole).lineCount)

        val fractional = fields + ("line_count" to DocumentValue.Double(5.5))
        assertFailsWith<SerializationException> { DocumentValueDecoding.decode<Style>(fractional) }
    }

    /**
     * Kill: return `raw.doubleValue.toFloat()` bare — 1e40 becomes `Infinity`
     * and travels on as a number, which is how a NaN reaches a layout pass.
     */
    @Test
    fun `float overflow refuses like the JSON bridge`() {
        val overflow = fields + ("letter_spacing" to DocumentValue.Double(1e40))

        assertFailsWith<SerializationException> { DocumentValueDecoding.decode<Style>(overflow) }
        assertFailsWith<SerializationException> { jsonBridge<Style>(overflow) }
    }

    /** Kill: make `unwrap` return a default instead of throwing on a type mismatch. */
    @Test
    fun `type mismatches refuse`() {
        val wrong = fields + ("font_name" to DocumentValue.Int(7))
        assertFailsWith<SerializationException> { DocumentValueDecoding.decode<Style>(wrong) }

        val wrongList = fields + ("tags" to DocumentValue.String("caption"))
        assertFailsWith<SerializationException> { DocumentValueDecoding.decode<Style>(wrongList) }
    }

    /**
     * Kill: give every element a synthesized default so a missing required key
     * decodes as an empty string instead of failing.
     */
    @Test
    fun `missing required key refuses`() {
        val missing = fields - "font_name"

        assertFailsWith<SerializationException> { DocumentValueDecoding.decode<Style>(missing) }
        assertFailsWith<SerializationException> { jsonBridge<Style>(missing) }
    }

    /**
     * The payload's own spelling wins; the snake_case spelling only answers when
     * the key is absent.
     *
     * Kill: swap the two arms of `lookup` — a document carrying both spellings
     * then reads the wrong one, and which one it is depends on the writer.
     */
    @Test
    fun `the payload's own key wins over its snake_case spelling`() {
        val both = fields +
            mapOf("fontName" to DocumentValue.String("own"), "font_name" to DocumentValue.String("snake"))

        assertEquals("own", DocumentValueDecoding.decode<Style>(both).fontName)
    }

    @Serializable
    private data class Box(val n: Long)

    /**
     * Swift's `Int64(exactly:)` takes `-2^63` and refuses `+2^63`, NaN and both
     * infinities.
     *
     * Kill: bound the upper end with `<=` — `toLong()` then CLAMPS 2^63 to
     * Long.MAX_VALUE, so an out-of-range number decodes as a plausible one.
     */
    @Test
    fun `the Long boundary refuses rather than clamps`() {
        val twoTo63 = 9.223372036854776E18

        assertEquals(Long.MAX_VALUE, DocumentValueDecoding.decode<Box>(mapOf("n" to DocumentValue.Int(Long.MAX_VALUE))).n)
        assertEquals(Long.MIN_VALUE, DocumentValueDecoding.decode<Box>(mapOf("n" to DocumentValue.Double(-twoTo63))).n)

        for (refused in listOf(twoTo63, 1e19, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            assertFailsWith<SerializationException>("$refused is not a Long") {
                DocumentValueDecoding.decode<Box>(mapOf("n" to DocumentValue.Double(refused)))
            }
        }
    }

    /**
     * The other half of the same rule: a null on a NON-nullable element is the
     * absence Swift's `decodeIfPresent(_:) ?? default` reads it as — so a field
     * a peer explicitly cleared reads as its default, not as a decode failure.
     *
     * Kill: drop the `found == Null && !isNullable` skip and every slide whose
     * `opacity` was cleared stops materializing.
     */
    @Test
    fun `a null on a non-nullable field with a default takes the default`() {
        val slide = DeckDocument.Slide.from(
            DocumentEntry(
                "a",
                mapOf(
                    "layout_key" to DocumentValue.String("title"),
                    "start" to DocumentValue.Double(0.0),
                    "duration" to DocumentValue.Double(4.0),
                    "opacity" to DocumentValue.Null,
                    "fit_mode" to DocumentValue.Null,
                ),
            ),
        )

        assertEquals(1.0, assertNotNull(slide).opacity)
        assertEquals(DeckDocument.Slide.FitMode.AUTO, slide.fitMode)
    }
}
