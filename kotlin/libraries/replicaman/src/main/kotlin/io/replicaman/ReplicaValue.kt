package io.replicaman

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * A JSON value on ReplicaMan's wire — frame `data`, op `data`, patch
 * fields. Its own small vocabulary on purpose (ported from Syncer v1's
 * `SyncerValue`): payloads are plain domain fields in the server's
 * camelized wire shape, and the engine stores them verbatim.
 */
@Serializable(with = ReplicaValueSerializer::class)
public sealed interface ReplicaValue {
    public data class Str(val value: String) : ReplicaValue

    /**
     * Equality is Swift's `==` on `Double`, not Kotlin's boxed `equals`:
     * `-0.0` equals `0.0` and `NaN` never equals itself, exactly as the
     * iOS `ReplicaValue` compares. The engine's idempotent-apply check
     * trades in this currency.
     */
    public class Num(public val value: Double) : ReplicaValue {
        override fun equals(other: Any?): Boolean = other is Num && value == other.value

        override fun hashCode(): Int = if (value == 0.0) 0 else value.hashCode()

        override fun toString(): String = "Num($value)"
    }

    public data class Integer(val value: Long) : ReplicaValue

    public data class Bool(val value: Boolean) : ReplicaValue

    public data object Null : ReplicaValue

    public data class Arr(val values: List<ReplicaValue>) : ReplicaValue

    /** `object` is a Kotlin keyword — the case is `Obj`, the factory is `obj`. */
    public data class Obj(val fields: Map<String, ReplicaValue>) : ReplicaValue

    public val string: String?
        get() = (this as? Str)?.value

    public val number: Double?
        get() = when (this) { is Num -> value; is Integer -> value.toDouble(); else -> null }

    public val int: Int?
        get() = long?.takeIf { it in Int.MIN_VALUE..Int.MAX_VALUE }?.toInt()

    public val long: Long?
        get() = when (this) {
            is Integer -> value
            is Num -> exactLong(value)
            else -> null
        }

    public val bool: Boolean?
        get() = (this as? Bool)?.value

    public val items: List<ReplicaValue>?
        get() = (this as? Arr)?.values

    /** Read a field on an object value. */
    public operator fun get(key: String): ReplicaValue? = (this as? Obj)?.fields?.get(key)

    public companion object {
        public fun string(value: String): ReplicaValue = Str(value)

        public fun number(value: Double): ReplicaValue = Num(value)

        public fun number(value: Int): ReplicaValue = Num(value.toDouble())

        public fun signedInteger(value: Long): ReplicaValue =
            if (value in -9_007_199_254_740_991L..9_007_199_254_740_991L) Num(value.toDouble()) else Integer(value)

        public fun bool(value: Boolean): ReplicaValue = Bool(value)

        public fun array(values: List<ReplicaValue>): ReplicaValue = Arr(values)

        public fun obj(fields: Map<String, ReplicaValue>): ReplicaValue = Obj(fields)
    }
}

/**
 * Swift's `Int64(exactly:)`: an integral Double inside `[-2^63, 2^63)`, else
 * null. `Double.toLong()` alone SATURATES — it answers `Long.MAX_VALUE` for
 * `2^63`, whose own `toDouble()` rounds back to `2^63`, so a naive round-trip
 * check reports a match and the wire carries a number nobody had.
 */
internal fun exactLong(value: Double): Long? {
    if (!value.isFinite()) return null
    if (value < Long.MIN_VALUE.toDouble() || value >= -Long.MIN_VALUE.toDouble()) return null
    val whole = value.toLong()
    return if (whole.toDouble() == value) whole else null
}

/**
 * The Codable twin. Decoding accepts anything kotlinx's JSON lexer produces;
 * encoding puts whole numbers out without a trailing `.0` so the bytes match
 * what the server's JSON emits for integer columns.
 */
public object ReplicaValueSerializer : KSerializer<ReplicaValue> {
    override val descriptor: SerialDescriptor =
        buildClassSerialDescriptor("io.replicaman.ReplicaValue")

    override fun deserialize(decoder: Decoder): ReplicaValue {
        (decoder as? ReplicaValueDecoder)?.let { return it.replicaValue }
        val input = decoder as? JsonDecoder
            ?: throw ReplicaError.Codec("ReplicaValue decodes from JSON only")
        return fromJson(input.decodeJsonElement())
    }

    override fun serialize(encoder: Encoder, value: ReplicaValue) {
        (encoder as? ReplicaValueEncoder)?.let { it.encodeReplicaValue(value); return }
        val output = encoder as? JsonEncoder
            ?: throw ReplicaError.Codec("ReplicaValue encodes to JSON only")
        output.encodeJsonElement(toJson(value))
    }

    public fun fromJson(element: JsonElement): ReplicaValue = when (element) {
        is JsonNull -> ReplicaValue.Null
        is JsonPrimitive ->
            if (element.isString) {
                ReplicaValue.Str(element.content)
            } else {
                when (element.content) {
                    "true" -> ReplicaValue.Bool(true)
                    "false" -> ReplicaValue.Bool(false)
                    else -> element.content.toLongOrNull()?.let { ReplicaValue.signedInteger(it) }
                        ?: element.content.toDoubleOrNull()?.takeIf { it.isFinite() }?.let { ReplicaValue.Num(it) }
                        ?: throw ReplicaError.Codec("Invalid JSON number")
                }
            }

        is JsonArray -> ReplicaValue.Arr(element.map { fromJson(it) })
        is JsonObject -> ReplicaValue.Obj(element.mapValues { fromJson(it.value) })
    }

    public fun toJson(value: ReplicaValue): JsonElement = when (value) {
        is ReplicaValue.Str -> JsonPrimitive(value.value)
        is ReplicaValue.Num -> {
            val whole = exactLong(value.value)
            if (whole != null) JsonPrimitive(whole) else JsonPrimitive(value.value)
        }

        is ReplicaValue.Integer -> JsonPrimitive(value.value)
        is ReplicaValue.Bool -> JsonPrimitive(value.value)
        ReplicaValue.Null -> JsonNull
        is ReplicaValue.Arr -> JsonArray(value.values.map { toJson(it) })
        is ReplicaValue.Obj -> JsonObject(value.fields.mapValues { toJson(it.value) })
    }
}
