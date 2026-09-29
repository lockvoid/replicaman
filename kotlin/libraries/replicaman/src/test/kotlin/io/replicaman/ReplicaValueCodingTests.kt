package io.replicaman

import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.encodeToString
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.fail

/**
 * The direct coder is the ONLY typed bridge on this platform (iOS kept a
 * JSON round-trip as its oracle; Kotlin never had one), so its contract is
 * pinned by explicit expectation plus, where a second implementation
 * genuinely exists, the JSON-TEXT path through kotlinx: same input, same
 * result. The contract: snake_case fallback on lookup, exactly-representable
 * integers, finite floats, and nil OMITTED on the way out.
 */
class ReplicaValueCodingTests {

    @Serializable
    data class Shadow(val x: Double, val y: Double)

    @Serializable
    data class Style(
        val fontFamily: String? = null,
        val fontSize: Double? = null,
        val shadowOffset: Shadow? = null,
        val strokeWidth: Int? = null,
        val visible: Boolean? = null,
        val tags: List<String>? = null,
        val weights: List<Double>? = null,
    )

    @Serializable
    data class Labeled(val label: String? = "untitled")

    @Serializable
    enum class Kind { video, audio }

    @Serializable
    data class EnumRow(val kind: Kind)

    @Serializable
    data class FloatRow(val scale: Float)

    /** The independent path: `ReplicaValue` → JSON text → kotlinx streaming decode. */
    private inline fun <reified T> textBridgeDecode(value: ReplicaValue): T? = try {
        ReplicaJSON.json.decodeFromString<T>(ReplicaJSON.encodeToString(value))
    } catch (_: Throwable) {
        null
    }

    private inline fun <reified T> textBridgeEncode(value: T): ReplicaValue = try {
        ReplicaJSON.decodeValue(ReplicaJSON.json.encodeToString(value))
    } catch (_: Throwable) {
        ReplicaValue.Null
    }

    // MARK: - Decode parity

    /** KILL: read a nested object through `toString()` instead of a child decoder. */
    @Test
    fun decodesCamelCasePayloadIdenticallyToBridge() {
        val value = ReplicaValue.Obj(
            mapOf(
                "fontFamily" to ReplicaValue.Str("Diatype"),
                "fontSize" to ReplicaValue.Num(24.0),
                "shadowOffset" to ReplicaValue.Obj(
                    mapOf("x" to ReplicaValue.Num(1.5), "y" to ReplicaValue.Num(-2.0))
                ),
                "strokeWidth" to ReplicaValue.Num(3.0),
                "visible" to ReplicaValue.Bool(true),
                "tags" to ReplicaValue.Arr(listOf(ReplicaValue.Str("a"), ReplicaValue.Str("b"))),
                "weights" to ReplicaValue.Arr(listOf(ReplicaValue.Num(0.25), ReplicaValue.Num(1.0))),
            )
        )

        val direct = ReplicaValueCoding.decode<Style>(value)
        assertEquals(textBridgeDecode<Style>(value), direct)
    }

    /**
     * KILL: drop the snake_case fallback from `ObjectDecoder.lookup` — a
     * doc/catalog payload's `font_family` stops answering `fontFamily` and
     * every such field silently reads null.
     */
    @Test
    fun decodesSnakeCasePayload() {
        val value = ReplicaValue.Obj(
            mapOf(
                "font_family" to ReplicaValue.Str("Inter"),
                "font_size" to ReplicaValue.Num(12.0),
                "shadow_offset" to ReplicaValue.Obj(
                    mapOf("x" to ReplicaValue.Num(0.0), "y" to ReplicaValue.Num(4.0))
                ),
                "stroke_width" to ReplicaValue.Num(0.0),
                "visible" to ReplicaValue.Bool(false),
            )
        )

        val direct = ReplicaValueCoding.decode<Style>(value)
        assertEquals("Inter", direct.fontFamily, "snake_case keys answer camelCase properties")
        assertEquals(12.0, direct.fontSize)
        assertEquals(Shadow(0.0, 4.0), direct.shadowOffset)
        assertEquals(0, direct.strokeWidth)
        assertEquals(false, direct.visible)
    }

    /** KILL: skip an explicit `Null` for a NULLABLE field — `label` reads its default, not null. */
    @Test
    fun missingAndNullFieldsMatchBridge() {
        val value = ReplicaValue.Obj(
            mapOf("fontFamily" to ReplicaValue.Null, "visible" to ReplicaValue.Bool(true))
        )

        val direct = ReplicaValueCoding.decode<Style>(value)
        assertEquals(textBridgeDecode<Style>(value), direct)
        assertNull(direct.fontSize)
        assertNull(ReplicaValueCoding.decode<Labeled>(ReplicaValue.Obj(mapOf("label" to ReplicaValue.Null))).label)
    }

    /** KILL: truncate instead of rejecting in `exactLong` — 3.5 lands as 3. */
    @Test
    fun nonIntegralIntRejects() {
        val value = ReplicaValue.Obj(mapOf("strokeWidth" to ReplicaValue.Num(3.5)))
        assertNull(textBridgeDecode<Style>(value), "oracle: the JSON path refuses 3.5 for Int")
        assertFailsWith<Exception> { ReplicaValueCoding.decode<Style>(value) }
    }

    /**
     * KILL: drop the `isFinite` check in `finiteFloat` — 1e40 silently
     * becomes infinity. This is STRICTER than the JSON path (kotlinx answers
     * `Infinity` there) and is exactly what the iOS bridge refused.
     */
    @Test
    fun floatOverflowRejects() {
        val value = ReplicaValue.Obj(mapOf("scale" to ReplicaValue.Num(1e40)))
        assertFailsWith<Exception>("1e40 must be a decode failure, never a silent infinity") {
            ReplicaValueCoding.decode<FloatRow>(value)
        }
    }

    /** KILL: decode an enum by ordinal — an unknown raw value silently reads case 0. */
    @Test
    fun stringBackedEnumDecodes() {
        val value = ReplicaValue.Obj(mapOf("kind" to ReplicaValue.Str("audio")))
        assertEquals(EnumRow(Kind.audio), ReplicaValueCoding.decode<EnumRow>(value))
        assertFailsWith<Exception> {
            ReplicaValueCoding.decode<EnumRow>(
                ReplicaValue.Obj(mapOf("kind" to ReplicaValue.Str("hologram")))
            )
        }
    }

    /** KILL: require an object at the top level — a `List<Shadow>` payload stops decoding. */
    @Test
    fun topLevelArrayDecodes() {
        val value = ReplicaValue.Arr(
            listOf(
                ReplicaValue.Obj(mapOf("x" to ReplicaValue.Num(1.0), "y" to ReplicaValue.Num(2.0))),
                ReplicaValue.Obj(mapOf("x" to ReplicaValue.Num(3.0), "y" to ReplicaValue.Num(4.0))),
            )
        )
        assertEquals(
            listOf(Shadow(1.0, 2.0), Shadow(3.0, 4.0)),
            ReplicaValueCoding.decode<List<Shadow>>(value)
        )
    }

    // MARK: - Encode parity

    /** KILL: encode Int as `Str` — every numeric column changes type on the wire. */
    @Test
    fun encodesIdenticallyToBridge() {
        val style = Style(
            fontFamily = "Diatype", fontSize = 24.0,
            shadowOffset = Shadow(1.5, -2.0), strokeWidth = 3,
            visible = true, tags = listOf("a", "b"), weights = listOf(0.25, 1.0)
        )
        assertEquals(textBridgeEncode(style), ReplicaValueCoding.encode(style))
    }

    /**
     * KILL: write `Null` for an absent optional — a patch diff then claims
     * the caller authored a clear it never made.
     */
    @Test
    fun encodeOmitsNil() {
        val style = Style(fontFamily = null, fontSize = 12.0)
        val direct = ReplicaValueCoding.encode(style)
        val obj = direct as? ReplicaValue.Obj ?: fail("expected object")
        assertNull(obj.fields["fontFamily"], "a nil property is omitted, never written as null")
        assertEquals(ReplicaValue.Num(12.0), obj.fields["fontSize"])
    }

    // MARK: - Key conversion

    /**
     * Repeated because the conversion is memoized: the cached answer must be
     * the computed answer, or snake-key fallback silently breaks everywhere.
     *
     * KILL: store the INPUT in the memo instead of the result.
     */
    @Test
    fun snakeCasing() {
        repeat(3) {
            assertEquals("animation_in_id", ReplicaValueCoding.snakeCased("animationInId"))
            assertEquals("x", ReplicaValueCoding.snakeCased("x"))
            assertEquals("font_family", ReplicaValueCoding.snakeCased("fontFamily"))
            assertEquals("visible", ReplicaValueCoding.snakeCased("visible"))
        }
    }

    /**
     * The contract line says "exactly representable" — and Swift enforces it
     * with `T(exactly:)` for EVERY width.
     *
     * KILL: `decodeShort() = decodeInt().toShort()` / `decodeByte() =
     * decodeInt().toByte()` — 300 lands as 44 and 70000 as 4464, silently.
     */
    @Test
    fun narrowIntegersRejectOutOfRangeInsteadOfTruncating() {
        assertEquals(44, ReplicaValueCoding.decode(Byte.serializer(), ReplicaValue.Num(44.0)))
        assertFailsWith<Exception>("300 is not a Byte") {
            ReplicaValueCoding.decode(Byte.serializer(), ReplicaValue.Num(300.0))
        }
        assertEquals(4464, ReplicaValueCoding.decode(Short.serializer(), ReplicaValue.Num(4464.0)))
        assertFailsWith<Exception>("70000 is not a Short") {
            ReplicaValueCoding.decode(Short.serializer(), ReplicaValue.Num(70000.0))
        }
        assertFailsWith<Exception>("2^31 is not an Int") {
            ReplicaValueCoding.decode(Int.serializer(), ReplicaValue.Num(2147483648.0))
        }
    }

    /**
     * `Double.toLong()` SATURATES: 2^63 answers `Long.MAX_VALUE`, whose own
     * `toDouble()` rounds back to 2^63, so the naive round-trip check passes
     * and a value the payload never carried is handed to the model. Swift's
     * `Int64(exactly:)` is nil there.
     *
     * KILL: `val exact = number.toLong(); if (exact.toDouble() != number)`.
     */
    @Test
    fun longDecodeRejectsTheSaturationBoundary() {
        assertEquals(
            4611686018427387904L,
            ReplicaValueCoding.decode(Long.serializer(), ReplicaValue.Num(4611686018427387904.0))
        )
        assertFailsWith<Exception>("the double 2^63 is not an Int64") {
            ReplicaValueCoding.decode(Long.serializer(), ReplicaValue.Num(9223372036854775808.0))
        }
        assertEquals(
            Long.MIN_VALUE,
            ReplicaValueCoding.decode(Long.serializer(), ReplicaValue.Num(-9223372036854775808.0)),
            "-2^63 IS exactly representable"
        )
    }
}
