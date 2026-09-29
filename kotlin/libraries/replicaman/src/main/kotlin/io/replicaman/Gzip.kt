package io.replicaman

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/** Gzip framing with explicit errors and a bounded decoded body. */
public object Gzip {
    public fun compress(data: ByteArray): ByteArray {
        val output = ByteArrayOutputStream()
        GZIPOutputStream(output).use { it.write(data) }
        return output.toByteArray()
    }

    public fun decompress(data: ByteArray, limit: Int = ReplicaProtocol.ENTITY_BYTES): ByteArray {
        require(limit >= 0) { "gzip output limit must be nonnegative" }
        val output = ByteArrayOutputStream()
        val buffer = ByteArray(8192)

        GZIPInputStream(ByteArrayInputStream(data)).use { input ->
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                if (count > limit - output.size()) throw IOException("gzip output exceeds size limit")
                output.write(buffer, 0, count)
            }
        }
        return output.toByteArray()
    }
}
