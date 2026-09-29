package io.replicaman

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull

/**
 * A value in the project document, in the vocabulary both platforms share.
 *
 * Deliberately NOT `Any`: the projection crosses isolation boundaries (the
 * document service hands it to the editor on the main dispatcher), and it makes
 * equality — which is what stops an unchanged field being written as an op — a
 * compiler-checked `==` rather than a hand-rolled comparison that quietly gets a
 * case wrong.
 *
 * The cases mirror `LoroValue` exactly, minus `container` (a projection is
 * already materialized) and `binary` (nothing in the timeline is bytes).
 */
@Serializable(with = DocumentValueSerializer::class)
public sealed interface DocumentValue {
    public data object Null : DocumentValue

    public data class Bool(val value: Boolean) : DocumentValue

    public data class Int(val value: Long) : DocumentValue

    public data class Double(val value: kotlin.Double) : DocumentValue

    public data class String(val value: kotlin.String) : DocumentValue

    public data class List(val value: kotlin.collections.List<DocumentValue>) : DocumentValue

    public data class Map(
        val value: kotlin.collections.Map<kotlin.String, DocumentValue>,
    ) : DocumentValue
}

/**
 * The twin of the Swift `init(from:)`/`encode(to:)` pair: an untagged single
 * value, tried in the same order (nil, bool, int, double, string, list, map).
 *
 * JSON only — decoding casts to `JsonDecoder`, because recovering int-from-double
 * needs the literal, which a format-agnostic `Decoder` does not expose.
 */
@OptIn(ExperimentalSerializationApi::class)
public object DocumentValueSerializer : KSerializer<DocumentValue> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("DocumentValue")

    override fun deserialize(decoder: Decoder): DocumentValue =
        fromJson((decoder as JsonDecoder).decodeJsonElement())

    override fun serialize(encoder: Encoder, value: DocumentValue) {
        when (value) {
            is DocumentValue.Null -> encoder.encodeNull()
            is DocumentValue.Bool -> encoder.encodeBoolean(value.value)
            is DocumentValue.Int -> encoder.encodeLong(value.value)
            is DocumentValue.Double -> encoder.encodeDouble(value.value)
            is DocumentValue.String -> encoder.encodeString(value.value)
            is DocumentValue.List ->
                encoder.encodeSerializableValue(ListSerializer(DocumentValueSerializer), value.value)
            is DocumentValue.Map ->
                encoder.encodeSerializableValue(
                    MapSerializer(kotlin.String.serializer(), DocumentValueSerializer),
                    value.value,
                )
        }
    }

    private fun fromJson(element: JsonElement): DocumentValue = when (element) {
        is JsonNull -> DocumentValue.Null
        is JsonPrimitive ->
            if (element.isString) {
                DocumentValue.String(element.content)
            } else {
                element.booleanOrNull?.let(DocumentValue::Bool)
                    ?: element.longOrNull?.let(DocumentValue::Int)
                    ?: element.doubleOrNull?.let(DocumentValue::Double)
                    ?: throw SerializationException("unsupported project document value: $element")
            }
        is JsonArray -> DocumentValue.List(element.map(::fromJson))
        is JsonObject -> DocumentValue.Map(element.mapValues { fromJson(it.value) })
    }
}

public val DocumentValue.isNull: Boolean get() = this == DocumentValue.Null

public val DocumentValue.stringValue: String?
    get() = (this as? DocumentValue.String)?.value

public val DocumentValue.doubleValue: Double?
    get() = when (this) {
        is DocumentValue.Double -> value
        is DocumentValue.Int -> value.toDouble()
        else -> null
    }

public val DocumentValue.boolValue: Boolean?
    get() = (this as? DocumentValue.Bool)?.value

public val DocumentValue.mapValue: Map<String, DocumentValue>?
    get() = (this as? DocumentValue.Map)?.value

/**
 * One registry entry — a clip or a track — as a bag of fields plus the key
 * that identifies its slot. The key is NEVER also stored inside the field bag,
 * so the two cannot drift.
 */
public data class DocumentEntry(
    val key: String,
    val fields: Map<String, DocumentValue>,
) {
    public operator fun get(field: String): DocumentValue? = fields[field]
}

