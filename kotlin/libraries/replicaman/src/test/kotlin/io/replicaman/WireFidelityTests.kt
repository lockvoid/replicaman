package io.replicaman

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The byte layer, graded against Foundation's own answers (captured from a
 * `swift` run of `JSONEncoder([.sortedKeys, .withoutEscapingSlashes])` and
 * `Data(base64Encoded:)` on this machine) — the writer, the base64 door and
 * the exact-integer rule the wire and the journal both stand on.
 */
class WireFidelityTests {

    private fun obj(vararg pairs: Pair<String, ReplicaValue>) = ReplicaValue.Obj(mapOf(*pairs))

    private fun hex(bytes: ByteArray) = bytes.joinToString(" ") { "%02x".format(it) }

    /**
     * KILL: `val whole = value.toLong(); if (whole.toDouble() == value)` —
     * `toLong()` SATURATES, so the double 2^63 encodes as
     * `9223372036854775807`, a number nobody held. Only the integer
     * spelling is graded against Foundation; a non-integer travels as the
     * JVM's shortest `Double.toString` (`E18`, where Foundation writes
     * `e+18`), which every JSON reader takes as the same double.
     */
    @Test
    fun theDoubleTwoToThe63IsNotAnInteger() {
        val encoded = ReplicaJSON.encodeToString(obj("n" to ReplicaValue.Num(9223372036854775808.0)))
        assertEquals("""{"n":9.223372036854776E18}""", encoded, "2^63 must not be spelled as a Long")
        assertEquals(
            """{"n":-9223372036854775808}""",
            ReplicaJSON.encodeToString(obj("n" to ReplicaValue.Num(-9223372036854775808.0))),
            "-2^63 IS an exact Int64 and keeps its integer spelling"
        )
        assertEquals(
            """{"n":10000000}""",
            ReplicaJSON.encodeToString(obj("n" to ReplicaValue.Num(1e7)))
        )
    }

    /**
     * KILL: append a lone surrogate raw — `toByteArray(UTF_8)` turns it into
     * `?` (0x3f) and the value is gone. A `take()` that split an emoji is all
     * it takes to produce one; Swift's String cannot hold one, so Foundation
     * never had to answer this.
     */
    @Test
    fun anUnpairedSurrogateSurvivesTheWire() {
        for (text in listOf("a\uD800b", "a\uDC00b", "\uD83D", "\uDE00x")) {
            val value = obj("k" to ReplicaValue.Str(text))
            val bytes = ReplicaJSON.encodeToBytes(value)
            assertTrue(
                bytes.none { it == '?'.code.toByte() },
                "an unpaired surrogate was replaced: ${hex(bytes)}"
            )
            assertEquals(
                text,
                ReplicaValueJSON.decodeObject(String(bytes, Charsets.UTF_8))["k"]?.string,
                "round trip lost the unpaired surrogate"
            )
        }
    }

    /**
     * A valid pair still travels as UTF-8, byte-identical to Foundation.
     * KILL: hex-escape every surrogate in `writeString`, paired or not.
     */
    @Test
    fun aValidSurrogatePairStaysUtf8() {
        assertEquals(
            "7b 22 6b 22 3a 22 61 f0 9f 98 80 62 22 7d",
            hex(ReplicaJSON.encodeToBytes(obj("k" to ReplicaValue.Str("a😀b"))))
        )
    }

    /**
     * Foundation's escape set exactly: quote, backslash, `< 0x20`; no slash, no DEL.
     * KILL: escape `/` (or DEL, or U+2028) in `writeString`.
     */
    @Test
    fun theEscapeSetMatchesFoundation() {
        assertEquals("""{"k":"a/b"}""", ReplicaJSON.encodeToString(obj("k" to ReplicaValue.Str("a/b"))))
        // DEL travels raw, exactly as Foundation leaves it.
        assertEquals(
            "7b 22 6b 22 3a 22 61 7f 62 22 7d",
            hex(ReplicaJSON.encodeToBytes(obj("k" to ReplicaValue.Str("a\u007Fb"))))
        )
        assertEquals(
            """{"k":"a\u000bb"}""",
            ReplicaJSON.encodeToString(obj("k" to ReplicaValue.Str("a\u000Bb")))
        )
        assertEquals(
            """{"k":"a\u0000b"}""",
            ReplicaJSON.encodeToString(obj("k" to ReplicaValue.Str("a\u0000b")))
        )
        // U+2028 is a LINE SEPARATOR, not a JSON control character.
        assertEquals(
            "7b 22 6b 22 3a 22 61 e2 80 a8 62 22 7d",
            hex(ReplicaJSON.encodeToBytes(obj("k" to ReplicaValue.Str("a\u2028b"))))
        )
    }

    /** KILL: drop the `isFinite` throw in `writeNumber` — `NaN` is written as text. */
    @Test
    fun nonFiniteNumbersAreRefusedLikeFoundation() {
        assertFailsWith<ReplicaError.Codec> {
            ReplicaJSON.encodeToString(obj("n" to ReplicaValue.Num(Double.NaN)))
        }
        assertFailsWith<ReplicaError.Codec> {
            ReplicaJSON.encodeToString(obj("n" to ReplicaValue.Num(Double.POSITIVE_INFINITY)))
        }
    }

    /**
     * KILL: hand the text straight to `Base64.getDecoder()` — Java accepts an
     * unpadded tail, `Data(base64Encoded:)` answers nil for it, and the two
     * platforms then disagree about which frames landed.
     */
    @Test
    fun base64RefusesEverythingFoundationRefuses() {
        for (rejected in listOf("QQ", "QUJDRA", "QQ=", "Q", "QUJD\n", "QU JD", "QU\nJD", "a-_b", "QQ==QQ==", "QUJDé")) {
            assertNull(ReplicaBase64.decode(rejected), "must answer null for ${rejected.trim()}")
        }
        assertEquals("41 42 43", hex(ReplicaBase64.decode("QUJD")!!))
        assertEquals("41", hex(ReplicaBase64.decode("QQ==")!!))
        assertEquals(0, ReplicaBase64.decode("")!!.size)
        assertEquals("6b ef db", hex(ReplicaBase64.decode("a+/b")!!))
    }

    /**
     * Whatever `encode` writes, `decode` must read back.
     * KILL: encode with `Base64.getEncoder().withoutPadding()` — every tail that needs padding stops decoding.
     */
    @Test
    fun base64RoundTripsEveryTailLength() {
        for (size in 0..8) {
            val bytes = ByteArray(size) { (it * 37 + 11).toByte() }
            assertTrue(
                ReplicaBase64.decode(ReplicaBase64.encode(bytes))!!.contentEquals(bytes),
                "size $size did not round trip"
            )
        }
    }

    /**
     * The journal compares SENT bytes with re-encoded bytes: order cannot drift.
     * KILL: walk `value.fields` in insertion order instead of `keys.sorted()`.
     */
    @Test
    fun keyOrderIsIndependentOfInsertionOrder() {
        val keys = listOf("b", "A", "a", "B", "_x", "0", "zz", "Z")
        assertEquals(
            ReplicaJSON.encodeToString(ReplicaValue.Obj(keys.associateWith { ReplicaValue.Num(1.0) })),
            ReplicaJSON.encodeToString(ReplicaValue.Obj(keys.reversed().associateWith { ReplicaValue.Num(1.0) }))
        )
    }
}
