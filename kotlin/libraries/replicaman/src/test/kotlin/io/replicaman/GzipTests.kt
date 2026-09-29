package io.replicaman

import org.junit.Test
import java.util.zip.GZIPInputStream
import java.util.Base64
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import java.io.IOException
import kotlin.test.assertTrue

/**
 * The push body's framing: real gzip (RFC 1952 — magic bytes, inflatable
 * by any gzip reader), round-trips, and actually shrinks the JSON rows it
 * exists for.
 */
class GzipTests {

    /** KILL: return the input unchanged from `compress` — the magic bytes go. */
    @Test
    fun roundTripsAndCarriesGzipMagic() {
        val text = "{\"op\":\"row.set\",\"stream\":\"projects\",\"data\":{\"name\":\"x\"}}"
            .repeat(200).toByteArray()
        val zipped = Gzip.compress(text)
        assertEquals(listOf<Byte>(0x1f, 0x8b.toByte()), zipped.take(2))
        assertTrue(zipped.size < text.size / 5, "gzip must shrink repeated JSON")
        assertContentEquals(text, GZIPInputStream(zipped.inputStream()).use { it.readBytes() })

        // Empty input still produces a well-formed member, not an empty array:
        // a pair of identity functions would satisfy the round trip alone,
        // which is why this rides the magic-byte assertion instead of standing
        // as its own test.
        val empty = Gzip.compress(ByteArray(0))
        assertEquals(listOf<Byte>(0x1f, 0x8b.toByte()), empty.take(2))
        assertTrue(GZIPInputStream(empty.inputStream()).use { it.readBytes() }.isEmpty())
        // Frozen Python zlib gzip bytes, not produced by the Android encoder under test.
        val fixture = Base64.getDecoder().decode("H4sIAAAAAAAC/8vMS0ktSAUSeSUK6VWZBQr5RYnJOakK+goXtlzYcLH9wgYAxjE5tCIAAAA=")
        assertContentEquals("independent gzip oracle / дача".toByteArray(), Gzip.decompress(fixture))
    }

    /** KILL: make `decompress` answer the input on failure — garbage inflates. */
    @Test
    fun garbageDoesNotInflate() {
        assertFailsWith<IOException> { Gzip.decompress(byteArrayOf(1, 2, 3, 4, 5)) }
    }
    @Test
    fun rejectsTruncationCorruptionAndExpansionPastTheLimit() {
        val input = ByteArray(262_145) { 'x'.code.toByte() }
        val zipped = Gzip.compress(input)
        assertContentEquals(input, Gzip.decompress(zipped, input.size))
        assertFailsWith<IOException> { Gzip.decompress(zipped, input.size - 1) }
        assertFailsWith<IOException> { Gzip.decompress(zipped.copyOf(zipped.size - 1)) }
        val corrupt = zipped.copyOf()
        corrupt[corrupt.size - 8] = (corrupt[corrupt.size - 8].toInt() xor 1).toByte()
        assertFailsWith<IOException> { Gzip.decompress(corrupt) }
    }
}
