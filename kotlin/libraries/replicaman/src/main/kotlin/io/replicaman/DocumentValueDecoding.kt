package io.replicaman

import java.util.concurrent.ConcurrentHashMap
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.elementNames
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer

// Direct Decodable bridge over the `DocumentValue` tree — the doc-lane twin
// of ReplicaMan's `ReplicaValueCoding`. The generated document models used to
// round-trip through JSON (`Map<String, DocumentValue>` → Json bytes →
// Json → typed data class) on EVERY typed read — hang-sampler stacks
// caught `TextStyle(from:)` grinding through that double serialization on
// main. This decoder walks the tree in place: zero bytes, zero parsing.
//
// DECODE-ONLY by design: encode stays on the shaped JSON path (the manifest
// `Shape` descriptor drives `.int` vs `.double` on the way OUT, which a
// blind Encoder cannot know).
//
// Contract parity with the JSON bridge it replaces:
// - Keyed lookup accepts the payload's own key and its snake_case variant
//   (the old bridge ran `convertFromSnakeCase`; doc keys are snake_case).
// - `Int(Long)` and `Double` stay distinct: integers decode from `Int`
//   exactly, and from `Double` only when exactly representable (JSON had no
//   int/double distinction, so 5.0 decoded as Int 5 — that stands); Double
//   decodes from both; Float additionally rejects non-finite results.
public object DocumentValueDecoding {
    /** Decode a value straight off a document field bag. */
    public fun <T> decode(
        deserializer: DeserializationStrategy<T>,
        fields: Map<String, DocumentValue>,
    ): T = deserializer.deserialize(SingleDecoder(DocumentValue.Map(fields), emptyList()))

    public inline fun <reified T> decode(fields: Map<String, DocumentValue>): T =
        decode(serializer<T>(), fields)

    /**
     * camelCase → snake_case, far enough for identifier-shaped keys
     * ("animationInId" → "animation_in_id").
     *
     * Memoized: keys are serializable field names — a small closed set — and
     * this runs on every keyed-lookup MISS (absent optional fields hit it
     * once per decode), where the per-scalar uppercase walk was a
     * hang-sampler stall (225ms of TextStyle decode inside one open).
     */
    internal fun snakeCased(key: String): String = memo.computeIfAbsent(key) {
        buildString(key.length + 4) {
            for (character in it) {
                if (character.isUpperCase()) {
                    append('_')
                    append(character.lowercaseChar())
                } else {
                    append(character)
                }
            }
        }
    }

    private val memo = ConcurrentHashMap<String, String>()
}

// MARK: - Decoder

private fun mismatch(type: String, raw: DocumentValue?, path: List<String>): Nothing =
    throw SerializationException(
        "expected $type, got ${raw ?: "nothing"}${if (path.isEmpty()) "" else " at ${path.joinToString(".")}"}",
    )

private fun <T : Any> unwrap(value: T?, type: String, raw: DocumentValue, path: List<String>): T =
    value ?: mismatch(type, raw, path)

private fun exactLong(raw: DocumentValue, path: List<String>): Long = when (raw) {
    is DocumentValue.Int -> raw.value
    is DocumentValue.Double ->
    // `Long.MIN_VALUE.toDouble()` is exactly -2^63, so the low bound is
    // inclusive; `Long.MAX_VALUE.toDouble()` rounds UP to 2^63, so the high one
    // must not be. Without that, `toLong()` CLAMPS 2^63 to Long.MAX_VALUE and a
    // number one past the maximum decodes as the maximum, where Swift's
    // `Int64(exactly:)` refuses.
        raw.value
            .takeIf { it % 1.0 == 0.0 && it >= Long.MIN_VALUE.toDouble() && it < Long.MAX_VALUE.toDouble() }
            ?.toLong()
            ?: mismatch("exact Long", raw, path)
    else -> mismatch("exact Long", raw, path)
}

private fun exactInRange(raw: DocumentValue, path: List<String>, min: Long, max: Long, type: String): Long =
    exactLong(raw, path).takeIf { it in min..max } ?: mismatch("exact $type", raw, path)

/**
 * A Double that overflows Float is a decode FAILURE, not `Infinity` — the JSON
 * bridge this replaces rejected it.
 */
private fun finiteFloat(raw: DocumentValue, path: List<String>): Float {
    val float = raw.doubleValue?.toFloat()
    return if (float != null && float.isFinite()) float else mismatch("finite Float", raw, path)
}

/**
 * The value the tree holds for the element about to be decoded. `SingleDecoder`
 * holds one; the keyed and unkeyed containers move theirs as they walk.
 */
@OptIn(ExperimentalSerializationApi::class)
private abstract class ValueSink : AbstractDecoder() {
    override val serializersModule: SerializersModule = EmptySerializersModule()

    abstract fun current(): DocumentValue

    abstract fun currentPath(): List<String>

    override fun decodeNotNullMark(): Boolean = current() != DocumentValue.Null

    override fun decodeNull(): Nothing? = null

    override fun decodeBoolean(): Boolean =
        unwrap(current().boolValue, "Boolean", current(), currentPath())

    override fun decodeString(): String =
        unwrap(current().stringValue, "String", current(), currentPath())

    override fun decodeDouble(): Double =
        unwrap(current().doubleValue, "Double", current(), currentPath())

    override fun decodeFloat(): Float = finiteFloat(current(), currentPath())

    override fun decodeLong(): Long = exactLong(current(), currentPath())

    override fun decodeInt(): Int =
        exactInRange(current(), currentPath(), Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong(), "Int").toInt()

    override fun decodeShort(): Short =
        exactInRange(current(), currentPath(), Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong(), "Short").toShort()

    override fun decodeByte(): Byte =
        exactInRange(current(), currentPath(), Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong(), "Byte").toByte()

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val raw = unwrap(current().stringValue, "String", current(), currentPath())
        val index = enumDescriptor.getElementIndex(raw)
        return if (index == CompositeDecoder.UNKNOWN_NAME) {
            mismatch("one of ${enumDescriptor.elementNames.toList()}", current(), currentPath())
        } else {
            index
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder =
        structureDecoder(current(), descriptor, currentPath())
}

@OptIn(ExperimentalSerializationApi::class)
private fun structureDecoder(
    value: DocumentValue,
    descriptor: SerialDescriptor,
    path: List<String>,
): CompositeDecoder = when (descriptor.kind) {
    StructureKind.LIST ->
        UnkeyedDecoder(
            (value as? DocumentValue.List ?: mismatch("list", value, path)).value,
            path,
        )
    StructureKind.MAP ->
        EntriesDecoder(
            (value as? DocumentValue.Map ?: mismatch("map", value, path)).value,
            path,
        )
    else ->
        KeyedDecoder(
            (value as? DocumentValue.Map ?: mismatch("map", value, path)).value,
            descriptor,
            path,
        )
}

private class SingleDecoder(
    private val value: DocumentValue,
    private val path: List<String>,
) : ValueSink() {
    override fun current(): DocumentValue = value

    override fun currentPath(): List<String> = path

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        throw SerializationException("a single value has no elements")
}

/**
 * The old bridge's `convertFromSnakeCase` equivalence: the payload's
 * own key wins, its snake_case spelling answers second.
 *
 * An element the bag does not answer for is simply never reported by
 * `decodeElementIndex`, which is how a declared default applies and how a
 * required field still fails — the two halves of `decodeIfPresent(_:) ?? x`
 * and `decode(_:)` in one mechanism.
 */
@OptIn(ExperimentalSerializationApi::class)
private class KeyedDecoder(
    private val map: Map<String, DocumentValue>,
    private val descriptor: SerialDescriptor,
    private val path: List<String>,
) : ValueSink() {
    private var index = 0
    private var value: DocumentValue = DocumentValue.Null

    private fun lookup(name: String): DocumentValue? =
        map[name] ?: map[DocumentValueDecoding.snakeCased(name)]

    override fun current(): DocumentValue = value

    override fun currentPath(): List<String> = path + descriptor.getElementName(index - 1)

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        while (index < descriptor.elementsCount) {
            val at = index++
            val found = lookup(descriptor.getElementName(at)) ?: continue
            val element = descriptor.getElementDescriptor(at)

            // A null for a non-nullable property is the absence Swift's
            // `decodeIfPresent(_:) ?? default` reads it as, not a value.
            if (found == DocumentValue.Null && !element.isNullable) continue
            // An enum VALUE this build does not know is absence too, so the
            // declared default applies (`flatMap(init(rawValue:)) ?? default`).
            // A non-STRING is not absence: `decodeIfPresent(String.self)` throws
            // there, so it is reported and `decodeEnum` refuses. Inside a list
            // there is no default either — which is what the generated `try?` sees.
            if (element.kind == SerialKind.ENUM) {
                val raw = found.stringValue
                if (raw != null && element.getElementIndex(raw) == CompositeDecoder.UNKNOWN_NAME) continue
            }

            value = found
            return at
        }
        return CompositeDecoder.DECODE_DONE
    }
}

@OptIn(ExperimentalSerializationApi::class)
private class UnkeyedDecoder(
    private val list: List<DocumentValue>,
    private val path: List<String>,
) : ValueSink() {
    private var index = 0

    override fun current(): DocumentValue = list[index - 1]

    override fun currentPath(): List<String> = path + "[${index - 1}]"

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        if (index >= list.size) CompositeDecoder.DECODE_DONE else index++
}

/**
 * A `Map<String, _>` property: the bag's own entries, keys first, exactly as
 * Swift's keyed container answers `allKeys` and then each value.
 */
@OptIn(ExperimentalSerializationApi::class)
private class EntriesDecoder(
    map: Map<String, DocumentValue>,
    private val path: List<String>,
) : ValueSink() {
    private val entries = map.entries.toList()
    private var index = 0

    override fun current(): DocumentValue {
        val at = index - 1
        val entry = entries[at / 2]
        return if (at % 2 == 0) DocumentValue.String(entry.key) else entry.value
    }

    override fun currentPath(): List<String> = path + entries[(index - 1) / 2].key

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        if (index >= entries.size * 2) CompositeDecoder.DECODE_DONE else index++
}
