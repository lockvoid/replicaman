package io.replicaman

import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.security.MessageDigest

public enum class ReplicaEndpoint(public val path: String) {
    PULL("pull"), PUSH("push"), VERIFY("verify")
}

internal object ReplicaProtocol {
    const val VERSION = 2
    const val MAX_OPERATIONS = 100
    const val PAGE_BYTES = 256 * 1024
    const val ENTITY_BYTES = 32 * 1024 * 1024
    const val RESPONSE_BYTES = ENTITY_BYTES + 2 * PAGE_BYTES

    fun invalid(message: String): Nothing = throw ReplicaError.Protocol("InvalidResponse", message)

    fun digest(content: ByteArray): String = MessageDigest.getInstance("SHA-256")
        .digest(content).joinToString("") { "%02x".format(it.toInt() and 0xff) }

    fun isDigest(value: String): Boolean = value.length == 64 && value.all { it in '0'..'9' || it in 'a'..'f' }

    fun counter(text: String): Long {
        if (text.isEmpty() || text.length > 19 || (text.length > 1 && text[0] == '0') || !text.all { it in '0'..'9' }) {
            invalid("Expected a decimal int64 counter")
        }
        return text.toLongOrNull()?.takeIf { it >= 0 } ?: invalid("Counter exceeds int64")
    }

    fun binary(text: String, limit: Int): ByteArray {
        if (text.length > (limit + 2) / 3 * 4) invalid("Oversized binary field")
        val bytes = ReplicaBase64.decode(text) ?: invalid("Invalid base64 field")
        if (bytes.size > limit || ReplicaBase64.encode(bytes) != text) invalid("Invalid or noncanonical base64 field")
        return bytes
    }

    fun decode(bytes: ByteArray): ReplicaValue {
        val text = Charsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes)).toString()
        if (ReplicaJSON.nestsTooDeep(text)) invalid("JSON nesting exceeds the supported limit")
        return ReplicaJSON.decodeValue(text)
    }

    fun validateHeader(response: ReplicaValue, schema: ReplicaSchema, dataset: String?): String {
        if (response.requiredLong("protocol") != VERSION.toLong() || response.requiredText("namespace") != schema.namespace ||
            response.requiredLong("schema") != schema.version.toLong()) {
            throw ReplicaError.Protocol("UpgradeRequired", "Incompatible protocol, namespace or schema")
        }
        val actual = response.requiredText("dataset")
        if (actual.isEmpty() || (dataset != null && dataset != actual)) {
            throw ReplicaError.Protocol("DatasetChanged", "The authoritative dataset changed")
        }
        return actual
    }
}

internal fun ReplicaValue.requiredText(key: String): String = this[key]?.string ?: ReplicaProtocol.invalid("Missing string: $key")
internal fun ReplicaValue.optionalText(key: String): String? = when (val field = this[key]) {
    null, ReplicaValue.Null -> null
    else -> field.string ?: ReplicaProtocol.invalid("Expected optional string: $key")
}
internal fun ReplicaValue.requiredLong(key: String): Long = this[key]?.long ?: ReplicaProtocol.invalid("Missing integer: $key")
internal fun ReplicaValue.requiredBool(key: String): Boolean = this[key]?.bool ?: ReplicaProtocol.invalid("Missing boolean: $key")
internal fun ReplicaValue.requiredArray(key: String): List<ReplicaValue> = this[key]?.items ?: ReplicaProtocol.invalid("Missing array: $key")
internal fun ReplicaValue.requiredObject(key: String): ReplicaValue.Obj = this[key] as? ReplicaValue.Obj ?: ReplicaProtocol.invalid("Missing object: $key")

/** A pulled frame with the lifetime it describes: the base is keyed by both. */
internal data class ReplicaPulledFrame(val incarnation: String, val frame: ReplicaFrame) {
    companion object {
        fun decode(value: ReplicaValue): ReplicaPulledFrame {
            val kind = value.requiredText("frame")
            val stream = value.requiredText("stream")
            val id = value.requiredText("id")
            val incarnation = value.requiredText("incarnation")
            if (stream.isEmpty() || id.isEmpty() || incarnation.isEmpty()) ReplicaProtocol.invalid("Empty entity identity")
            val frame = when (kind) {
                "row.set" -> ReplicaFrame.RowSet(stream, id, value.optionalText("type"), value.requiredObject("data").fields, revision(value))
                "row.delete" -> ReplicaFrame.RowDelete(stream, id, revision(value))
                "doc.delta" -> ReplicaFrame.DocDelta(stream, id,
                    value.requiredLong("seq").takeIf { it > 0 } ?: ReplicaProtocol.invalid("doc.delta requires a positive seq"),
                    codec(value), ReplicaProtocol.binary(value.requiredText("payload"), ReplicaProtocol.ENTITY_BYTES))
                "doc.snapshot" -> ReplicaFrame.DocSnapshot(stream, id, codec(value),
                    ReplicaProtocol.binary(value.requiredText("snapshot"), ReplicaProtocol.ENTITY_BYTES),
                    value.requiredObject("data").fields, revision(value))
                else -> ReplicaProtocol.invalid("Unknown frame: $kind")
            }
            return ReplicaPulledFrame(incarnation, frame)
        }

        private fun revision(value: ReplicaValue): Long =
            ReplicaProtocol.counter(value.requiredText("revision")).takeIf { it > 0 }
                ?: ReplicaProtocol.invalid("Revision must be positive")

        private fun codec(value: ReplicaValue): String =
            value.requiredText("codec").takeIf { it.isNotEmpty() } ?: ReplicaProtocol.invalid("Empty codec")
    }
}

/**
 * One `/pull` answer. A page with an uninterpretable frame is refused whole,
 * so its cursor can never pass an update the client could not apply.
 */
internal data class ReplicaPullPage(
    val shard: String,
    val reset: Boolean,
    val frames: List<ReplicaPulledFrame>,
    val cursor: String,
    val more: Boolean,
) {
    companion object {
        fun decode(value: ReplicaValue): ReplicaPullPage {
            val cursor = value.requiredText("cursor")
            if (cursor.isEmpty()) ReplicaProtocol.invalid("Empty cursor")
            return ReplicaPullPage(value.requiredText("shard"), value.requiredBool("reset"),
                decodeFrames(value.requiredArray("frames")), cursor, value.requiredBool("more"))
        }

        fun decodeFrames(values: List<ReplicaValue>): List<ReplicaPulledFrame> = values.map(ReplicaPulledFrame::decode)
    }
}
