package io.replicaman

import kotlinx.serialization.json.Json
import java.util.Base64

/**
 * The wire grammar, frozen by the server engine (`ruby/lib/replica_man`):
 * `noun.verb`, two nouns (`row`, `doc`), everything addressed by
 * `(stream, id)`. Frames flow down on pull; ops flow up on push and come
 * back as verdicts keyed by the operation's UUID.
 */

// MARK: - Frames (down)

public sealed interface ReplicaFrame {
    /**
     * Unconditional replacement of the local copy — deliberately NOT
     * "upsert": no insert-or-update decision exists.
     */
    public data class RowSet(
        override val stream: String,
        override val id: String,
        val type: String?,
        val data: Map<String, ReplicaValue>,
        val revision: Long? = null,
    ) : ReplicaFrame

    /**
     * Both lanes; on a document stream the ENGINE cascades (fold, owed
     * journal ops) — driven by the manifest's lane.
     */
    public data class RowDelete(
        override val stream: String,
        override val id: String,
        val revision: Long? = null,
    ) : ReplicaFrame

    /** History the client lacks; the document's `row.set` follows in the same response. */
    public data class DocDelta(
        override val stream: String,
        override val id: String,
        val seq: Long,
        val codec: String,
        val payload: ByteArray,
    ) : ReplicaFrame {
        override fun equals(other: Any?): Boolean =
            other is DocDelta && stream == other.stream && id == other.id &&
                seq == other.seq && codec == other.codec && payload.contentEquals(other.payload)

        override fun hashCode(): Int {
            var result = stream.hashCode()
            result = 31 * result + id.hashCode()
            result = 31 * result + seq.hashCode()
            result = 31 * result + codec.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }

        override fun toString(): String =
            "DocDelta(stream=$stream, id=$id, seq=$seq, codec=$codec, payload=${payload.size}B)"
    }

    /**
     * Snapshot import is a MERGE into any local doc (loro semantics, never
     * a blind replace); `data` carries the server projection so the
     * importer stays dumb.
     */
    public data class DocSnapshot(
        override val stream: String,
        override val id: String,
        val codec: String,
        val snapshot: ByteArray,
        val data: Map<String, ReplicaValue>,
        val revision: Long? = null,
    ) : ReplicaFrame {
        override fun equals(other: Any?): Boolean =
            other is DocSnapshot && stream == other.stream && id == other.id &&
                codec == other.codec && snapshot.contentEquals(other.snapshot) && data == other.data && revision == other.revision

        override fun hashCode(): Int {
            var result = stream.hashCode()
            result = 31 * result + id.hashCode()
            result = 31 * result + codec.hashCode()
            result = 31 * result + snapshot.contentHashCode()
            result = 31 * result + data.hashCode()
            result = 31 * result + (revision?.hashCode() ?: 0)
            return result
        }

        override fun toString(): String =
            "DocSnapshot(stream=$stream, id=$id, codec=$codec, snapshot=${snapshot.size}B, data=$data)"
    }

    public val stream: String

    public val id: String
}

// MARK: - Ops (up)

/**
 * One client op. In the journal `id` is the local entry id; frozen into a
 * submission it is the operation's UUIDv7, and the members of one atomic
 * action share a `group` UUID. The encoded JSON is byte-stable (`row_id`
 * snake_case, binary fields base64): the frozen bytes are what every retry
 * sends.
 */
public data class ReplicaOp(
    val id: String,
    val verb: String,
    val stream: String,
    val rowId: String,
    val type: String? = null,
    val data: Map<String, ReplicaValue>? = null,
    val codec: String? = null,
    val seed: ByteArray? = null,
    val payload: ByteArray? = null,
    val incarnation: String? = null,
    val references: List<ReplicaReference> = emptyList(),
    val replaces: String? = null,
    val group: String? = null,
) {
    public object Verb {
        public const val ROW_CREATE: String = "row.create"
        public const val ROW_PATCH: String = "row.patch"
        public const val ROW_DELETE: String = "row.delete"
        public const val DOC_DELTA: String = "doc.delta"
    }

    override fun equals(other: Any?): Boolean =
        other is ReplicaOp && id == other.id && verb == other.verb && stream == other.stream &&
            rowId == other.rowId && incarnation == other.incarnation && replaces == other.replaces && references == other.references && type == other.type && data == other.data &&
            codec == other.codec && seed.contentEquals(other.seed) && payload.contentEquals(other.payload) && group == other.group

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + verb.hashCode()
        result = 31 * result + stream.hashCode()
        result = 31 * result + rowId.hashCode()
        result = 31 * result + (incarnation?.hashCode() ?: 0)
        result = 31 * result + (replaces?.hashCode() ?: 0)
        result = 31 * result + references.hashCode()
        result = 31 * result + (type?.hashCode() ?: 0)
        result = 31 * result + (data?.hashCode() ?: 0)
        result = 31 * result + (codec?.hashCode() ?: 0)
        result = 31 * result + (seed?.contentHashCode() ?: 0)
        result = 31 * result + (payload?.contentHashCode() ?: 0)
        result = 31 * result + (group?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String =
        "ReplicaOp(id=$id, op=$verb, stream=$stream, rowId=$rowId, type=$type, data=$data, " +
            "codec=$codec, seed=${seed?.size}, payload=${payload?.size}, group=$group)"

    /** The wire/journal shape — snake_case address, binary fields base64. */
    internal fun toValue(): ReplicaValue {
        val fields = LinkedHashMap<String, ReplicaValue>()
        fields["id"] = ReplicaValue.Str(id)
        fields["op"] = ReplicaValue.Str(verb)
        fields["stream"] = ReplicaValue.Str(stream)
        fields["row_id"] = ReplicaValue.Str(rowId)
        incarnation?.let { fields["incarnation"] = ReplicaValue.Str(it) }
        replaces?.let { fields["replaces"] = ReplicaValue.Str(it) }
        if (references.isNotEmpty()) fields["references"] = ReplicaValue.Arr(references.map { it.toValue() })
        type?.let { fields["type"] = ReplicaValue.Str(it) }
        data?.let { fields["data"] = ReplicaValue.Obj(it) }
        codec?.let { fields["codec"] = ReplicaValue.Str(it) }
        seed?.let { fields["seed"] = ReplicaValue.Str(ReplicaBase64.encode(it)) }
        payload?.let { fields["payload"] = ReplicaValue.Str(ReplicaBase64.encode(it)) }
        group?.let { fields["group"] = ReplicaValue.Str(it) }
        return ReplicaValue.Obj(fields)
    }

    public companion object {
        internal fun fromValue(value: ReplicaValue): ReplicaOp {
            val id = value["id"]?.string ?: throw ReplicaError.Codec("op has no id")
            val verb = value["op"]?.string ?: throw ReplicaError.Codec("op has no op")
            val stream = value["stream"]?.string ?: throw ReplicaError.Codec("op has no stream")
            val rowId = value["row_id"]?.string ?: throw ReplicaError.Codec("op has no row_id")
            if (id.isEmpty() || stream.isEmpty() || rowId.isEmpty() ||
                verb !in setOf("row.create", "row.patch", "row.delete", "doc.delta")) {
                throw ReplicaError.Codec("invalid operation identity or verb")
            }
            fun text(key: String): String? = value[key]?.let {
                it.string ?: throw ReplicaError.Codec("op $key must be a string")
            }
            fun binary(key: String): ByteArray? = text(key)?.let {
                val bytes = ReplicaBase64.decode(it) ?: throw ReplicaError.Codec("invalid op $key base64")
                if (ReplicaBase64.encode(bytes) != it) throw ReplicaError.Codec("noncanonical op $key base64")
                bytes
            }
            val data = value["data"]?.let {
                (it as? ReplicaValue.Obj)?.fields ?: throw ReplicaError.Codec("op data must be an object")
            }
            val codec = text("codec")
            val payload = binary("payload")
            if (verb == "row.patch" && data == null) throw ReplicaError.Codec("patch requires fields")
            if (verb == "doc.delta" && (payload == null || codec.isNullOrEmpty())) {
                throw ReplicaError.Codec("document delta requires payload and codec")
            }
            val references = value["references"]?.let { refs ->
                val items = (refs as? ReplicaValue.Arr)?.items ?: throw ReplicaError.Codec("references must be an array")
                if (items.size > 64) throw ReplicaError.Codec("too many entity references")
                items.map { ReplicaReference.fromValue(it) }
            } ?: emptyList()
            if (references.map { it.name }.toSet().size != references.size) throw ReplicaError.Codec("duplicate entity references")
            val incarnation = text("incarnation")
            val replaces = text("replaces")
            if (incarnation == "" || replaces == "" || (replaces != null && verb != "row.create")) {
                throw ReplicaError.Codec("invalid entity lifetime")
            }
            return ReplicaOp(
                id = id,
                verb = verb,
                stream = stream,
                rowId = rowId,
                incarnation = incarnation,
                replaces = replaces,
                references = references,
                type = text("type"),
                data = data,
                codec = codec,
                seed = binary("seed"),
                payload = payload,
                group = text("group"),
            )
        }
    }
}

// MARK: - Verdicts

/**
 * The server's word on one op. `rejected` is a VERDICT — the entry parks,
 * never auto-retries; transport failure never produces one (it throws, and
 * the journal retries). Never conflate — v1's core discipline. On the wire
 * `id` is the operation's UUID; `drain()` reports it by journal entry id.
 */
public data class ReplicaVerdict(
    val id: String,
    val outcome: Outcome,
    val reason: String? = null,
) {
    public enum class Outcome(public val rawValue: String) {
        ACCEPTED("accepted"),
        REJECTED("rejected");

        public companion object {
            public fun fromRaw(rawValue: String?): Outcome? =
                entries.firstOrNull { it.rawValue == rawValue }
        }
    }
}

// MARK: - Base64

internal object ReplicaBase64 {
    private val encoder: Base64.Encoder = Base64.getEncoder()
    private val decoder: Base64.Decoder = Base64.getDecoder()

    fun encode(bytes: ByteArray): String = encoder.encodeToString(bytes)

    /**
     * Foundation's `Data(base64Encoded:)`: any stray character answers nil,
     * and so does a length that is not a multiple of 4 — Java's decoder
     * accepts an unpadded tail, Foundation refuses it.
     */
    fun decode(text: String): ByteArray? {
        if (text.length % 4 != 0) return null
        return try {
            decoder.decode(text)
        } catch (_: IllegalArgumentException) {
            null
        }
    }
}

// MARK: - Shared serialization

public object ReplicaJSON {
    /**
     * The decode side. Unknown keys are skipped — the wire evolves
     * additively and an old build must never fail an import.
     */
    public val json: Json = Json {
        ignoreUnknownKeys = true
        isLenient = false
        explicitNulls = false
        coerceInputValues = false
    }

    /**
     * Byte-stable encoding: a frozen submission leaves byte for byte on every
     * retry and the server tells a changed operation by its digest, so
     * encoding must be deterministic. Keys are sorted; slashes are not escaped; whole numbers
     * lose their fraction — Foundation's `[.sortedKeys, .withoutEscapingSlashes]`.
     */
    public fun encodeToString(value: ReplicaValue): String =
        StringBuilder().also { write(value, it) }.toString()

    public fun encodeToBytes(value: ReplicaValue): ByteArray =
        encodeToString(value).toByteArray(Charsets.UTF_8)

    internal fun decodeValue(text: String): ReplicaValue =
        ReplicaValueSerializer.fromJson(json.parseToJsonElement(text))

    /**
     * The nesting a decoded payload may carry. kotlinx's tree reader, its
     * streaming decoder and our own tree walk all recurse per level, so a
     * deep payload is a `StackOverflowError` — an `Error` that no
     * `catch (Exception)` holds, which would leave a stored row throwing out
     * of every future `snapshot()` instead of reading empty (ARCHITECTURE
     * §4.2: the importer is TOTAL). yyjson and Foundation both cap; this is
     * the same cap, applied to the TEXT before a recursive parser sees it.
     */
    internal const val MAX_DEPTH: Int = 512

    /** One pass, string-aware, no allocation. */
    internal fun nestsTooDeep(text: String): Boolean {
        var depth = 0
        var inString = false
        var escaped = false
        for (character in text) {
            when {
                escaped -> escaped = false
                inString && character == '\\' -> escaped = true
                character == '"' -> inString = !inString
                inString -> Unit
                character == '[' || character == '{' -> {
                    depth += 1
                    if (depth > MAX_DEPTH) return true
                }
                character == ']' || character == '}' -> depth -= 1
            }
        }
        return false
    }

    private fun write(value: ReplicaValue, out: StringBuilder) {
        when (value) {
            is ReplicaValue.Str -> writeString(value.value, out)
            is ReplicaValue.Num -> writeNumber(value.value, out)
            is ReplicaValue.Integer -> out.append(value.value)
            is ReplicaValue.Bool -> out.append(if (value.value) "true" else "false")
            ReplicaValue.Null -> out.append("null")
            is ReplicaValue.Arr -> {
                out.append('[')
                value.values.forEachIndexed { index, item ->
                    if (index > 0) out.append(',')
                    write(item, out)
                }
                out.append(']')
            }

            is ReplicaValue.Obj -> {
                out.append('{')
                var first = true
                for (key in value.fields.keys.sorted()) {
                    if (!first) out.append(',')
                    first = false
                    writeString(key, out)
                    out.append(':')
                    write(value.fields.getValue(key), out)
                }
                out.append('}')
            }
        }
    }

    private fun writeNumber(value: Double, out: StringBuilder) {
        if (!value.isFinite()) {
            throw ReplicaError.Codec("non-finite number cannot be encoded: $value")
        }
        val whole = exactLong(value)
        if (whole != null) out.append(whole) else out.append(value)
    }

    /**
     * Foundation's escaping with `.withoutEscapingSlashes`: quote, backslash
     * and the control range only. Non-ASCII travels as UTF-8.
     *
     * A Kotlin String may hold an UNPAIRED surrogate — a `take()` that split
     * an emoji leaves one — and `toByteArray(UTF_8)` would replace it with
     * `?`. Swift's String cannot hold one, so Foundation never faced this;
     * JSON's `\uXXXX` carries it through the parser unchanged.
     */
    private fun writeString(value: String, out: StringBuilder) {
        out.append('"')
        var index = 0
        while (index < value.length) {
            val character = value[index]
            when (character) {
                '"' -> out.append("\\\"")
                '\\' -> out.append("\\\\")
                '\n' -> out.append("\\n")
                '\r' -> out.append("\\r")
                '\t' -> out.append("\\t")
                '\b' -> out.append("\\b")
                '\u000C' -> out.append("\\f")
                else -> when {
                    character < ' ' -> writeHexEscape(character, out)
                    character.isHighSurrogate() -> {
                        val low = value.getOrNull(index + 1)
                        if (low != null && low.isLowSurrogate()) {
                            out.append(character).append(low)
                            index++
                        } else {
                            writeHexEscape(character, out)
                        }
                    }

                    character.isLowSurrogate() -> writeHexEscape(character, out)
                    else -> out.append(character)
                }
            }
            index++
        }
        out.append('"')
    }

    private fun writeHexEscape(character: Char, out: StringBuilder) {
        out.append("\\u")
        out.append(HEX[(character.code shr 12) and 0xF])
        out.append(HEX[(character.code shr 8) and 0xF])
        out.append(HEX[(character.code shr 4) and 0xF])
        out.append(HEX[character.code and 0xF])
    }

    private const val HEX = "0123456789abcdef"
}
