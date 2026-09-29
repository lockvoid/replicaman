@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package io.replicaman

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

// Direct serialization bridge over the `ReplicaValue` tree. The generated
// models used to round-trip through JSON (`ReplicaValue` → encoder bytes →
// decoder → typed value) on EVERY typed read — the iOS hang-sampler stacks
// caught that double serialization grinding on main inside project open.
// These coders walk the tree in place: zero bytes, zero parsing.
//
// Contract parity with the JSON bridge it replaces:
// - Decoding accepts BOTH the payload's own key and its snake_case variant
//   (the old bridge ran `convertFromSnakeCase`; row data is camelized at
//   capture, doc/catalog payloads may carry snake_case).
// - Encoding emits property names AS IS (the old bridge used the default
//   encoder strategy — no snake conversion on the way out).
// - Integer preserves signed 64-bit values; Num decodes to an integer only
//   when exactly representable (the JSON
//   decoder rejects 3.5 for Int; so do we), and Float rejects non-finite
//   results (1e40 was a decode failure through JSON; it must not silently
//   become infinity here).
// - A nil property is OMITTED, never written as an explicit null — Swift's
//   synthesized `encodeIfPresent`. A non-optional property is always
//   written, default or not.
//
// KNOWN divergence (deliberate): encoding a NaN Double lands `Num(NaN)` in
// the tree, which then fails at wire/journal encode time — a loud downstream
// error beats a silent data wipe.
public object ReplicaValueCoding {
    /** Decode a value straight off a `ReplicaValue` tree. */
    public fun <T> decode(deserializer: DeserializationStrategy<T>, value: ReplicaValue): T =
        deserializer.deserialize(SingleValueDecoder(value, emptyList()))

    public inline fun <reified T> decode(value: ReplicaValue): T = decode(serializer(), value)

    /** Encode a value into a `ReplicaValue` tree. */
    public fun <T> encode(serializer: SerializationStrategy<T>, value: T): ReplicaValue {
        val box = ValueBox()
        serializer.serialize(SingleValueEncoder(box), value)
        return box.value ?: ReplicaValue.Null
    }

    public inline fun <reified T> encode(value: T): ReplicaValue = encode(serializer(), value)

    /**
     * camelCase → snake_case, mirroring the JSON bridge's `convertToSnakeCase`
     * far enough for identifier-shaped keys ("animationInId" →
     * "animation_in_id").
     *
     * Memoized: keys are serial field names — a small closed set — and this
     * runs on every keyed-lookup MISS (absent optional fields hit it once),
     * where the per-scalar uppercase walk was a measured stall on iOS.
     */
    internal fun snakeCased(key: String): String {
        memoLock.withLock { memo[key] }?.let { return it }
        val out = StringBuilder(key.length + 4)
        for (character in key) {
            if (character.isUpperCase()) {
                out.append('_')
                out.append(character.lowercaseChar())
            } else {
                out.append(character)
            }
        }
        val result = out.toString()
        memoLock.withLock { memo[key] = result }
        return result
    }

    private val memoLock = ReentrantLock()
    private val memo = HashMap<String, String>()
}

/**
 * The seam a generated discriminated shape decodes through: its serializer
 * must peek at the discriminator before choosing a variant, which the
 * streaming `Decoder` cannot answer. Every decoder this file builds is one.
 */
public interface ReplicaValueDecoder {
    public val replicaValue: ReplicaValue
}

/**
 * The encoder's half of the same seam: a `ReplicaValue` field inside a value
 * being encoded to a tree is put into the tree as-is, never re-serialized
 * through JSON.
 */
public interface ReplicaValueEncoder {
    public fun encodeReplicaValue(value: ReplicaValue)
}

// MARK: - Decoder

private class TreeDecodeException(message: String) : Exception(message)

private fun mismatch(expected: String, raw: ReplicaValue, path: List<String>): Nothing =
    throw TreeDecodeException(
        "expected $expected, got $raw" + if (path.isEmpty()) "" else " at ${path.joinToString(".")}"
    )

private fun exactLong(raw: ReplicaValue, path: List<String>): Long {
    return raw.long ?: mismatch("exact Long", raw, path)
}

private fun exactInRange(raw: ReplicaValue, path: List<String>, min: Long, max: Long): Long {
    val exact = exactLong(raw, path)
    if (exact < min || exact > max) mismatch("exact $min..$max", raw, path)
    return exact
}

/**
 * A Double that overflows Float is a decode FAILURE, not infinity — the JSON
 * bridge this replaces rejected it (the generated init returned null).
 */
private fun finiteFloat(raw: ReplicaValue, path: List<String>): Float {
    val number = raw.number ?: mismatch("finite Float", raw, path)
    val float = number.toFloat()
    if (!float.isFinite()) mismatch("finite Float", raw, path)
    return float
}

private abstract class TreeDecoder : AbstractDecoder(), ReplicaValueDecoder {
    override val serializersModule: SerializersModule = EmptySerializersModule()

    override val replicaValue: ReplicaValue get() = currentValue()

    abstract fun currentValue(): ReplicaValue

    abstract fun currentPath(): List<String>

    override fun decodeNotNullMark(): Boolean = currentValue() != ReplicaValue.Null

    override fun decodeNull(): Nothing? = null

    override fun decodeBoolean(): Boolean =
        currentValue().bool ?: mismatch("Boolean", currentValue(), currentPath())

    override fun decodeString(): String =
        currentValue().string ?: mismatch("String", currentValue(), currentPath())

    override fun decodeDouble(): Double =
        currentValue().number ?: mismatch("Double", currentValue(), currentPath())

    override fun decodeFloat(): Float = finiteFloat(currentValue(), currentPath())

    override fun decodeInt(): Int =
        exactInRange(currentValue(), currentPath(), Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong())
            .toInt()

    override fun decodeLong(): Long = exactLong(currentValue(), currentPath())

    override fun decodeShort(): Short =
        exactInRange(currentValue(), currentPath(), Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong())
            .toShort()

    override fun decodeByte(): Byte =
        exactInRange(currentValue(), currentPath(), Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong())
            .toByte()

    override fun decodeChar(): Char {
        val text = decodeString()
        if (text.length != 1) mismatch("Char", currentValue(), currentPath())
        return text[0]
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val name = decodeString()
        val index = enumDescriptor.getElementIndex(name)
        if (index == CompositeDecoder.UNKNOWN_NAME) {
            mismatch("${enumDescriptor.serialName} case", currentValue(), currentPath())
        }
        return index
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        val value = currentValue()
        val path = currentPath()
        return when (descriptor.kind) {
            StructureKind.LIST -> {
                val items = (value as? ReplicaValue.Arr)?.values
                    ?: mismatch("array", value, path)
                ListDecoder(items, path)
            }

            StructureKind.MAP -> {
                val fields = (value as? ReplicaValue.Obj)?.fields
                    ?: mismatch("object", value, path)
                MapDecoder(fields, path)
            }

            else -> {
                val fields = (value as? ReplicaValue.Obj)?.fields
                    ?: mismatch("object", value, path)
                ObjectDecoder(fields, path)
            }
        }
    }
}

private class SingleValueDecoder(
    private val value: ReplicaValue,
    private val path: List<String>,
) : TreeDecoder() {
    override fun currentValue(): ReplicaValue = value

    override fun currentPath(): List<String> = path

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        CompositeDecoder.DECODE_DONE
}

private class ObjectDecoder(
    private val fields: Map<String, ReplicaValue>,
    private val path: List<String>,
) : TreeDecoder() {
    private var next = 0
    private var value: ReplicaValue = ReplicaValue.Null
    private var key: String = ""

    /**
     * The old bridge's `convertFromSnakeCase` equivalence: the payload's
     * own key wins, its snake_case spelling answers second.
     */
    private fun lookup(name: String): ReplicaValue? =
        fields[name] ?: fields[ReplicaValueCoding.snakeCased(name)]

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        while (next < descriptor.elementsCount) {
            val index = next++
            val name = descriptor.getElementName(index)
            val found = lookup(name) ?: continue
            if (found == ReplicaValue.Null &&
                descriptor.isElementOptional(index) &&
                !descriptor.getElementDescriptor(index).isNullable
            ) {
                continue
            }
            key = name
            value = found
            return index
        }
        return CompositeDecoder.DECODE_DONE
    }

    override fun currentValue(): ReplicaValue = value

    override fun currentPath(): List<String> = path + key
}

private class ListDecoder(
    private val items: List<ReplicaValue>,
    private val path: List<String>,
) : TreeDecoder() {
    private var next = 0

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = items.size

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        if (next < items.size) next++ else CompositeDecoder.DECODE_DONE

    override fun currentValue(): ReplicaValue = items[(next - 1).coerceAtLeast(0)]

    override fun currentPath(): List<String> = path + "${next - 1}"
}

private class MapDecoder(
    fields: Map<String, ReplicaValue>,
    private val path: List<String>,
) : TreeDecoder() {
    private val entries = fields.entries.toList()
    private var position = -1

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = entries.size

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        position++
        return if (position < entries.size * 2) position else CompositeDecoder.DECODE_DONE
    }

    override fun currentValue(): ReplicaValue {
        val entry = entries[position / 2]
        return if (position % 2 == 0) ReplicaValue.Str(entry.key) else entry.value
    }

    override fun currentPath(): List<String> = path + entries[position / 2].key
}

// MARK: - Encoder

/** Shared mutable landing slot: a child writes its finished subtree up. */
private class ValueBox {
    var value: ReplicaValue? = null
}

private abstract class TreeEncoder : AbstractEncoder(), ReplicaValueEncoder {
    override fun encodeReplicaValue(value: ReplicaValue) = put(value)

    override val serializersModule: SerializersModule = EmptySerializersModule()

    abstract fun put(value: ReplicaValue)

    /**
     * Swift always writes a non-optional property, default or not — the
     * synthesized `encode(to:)` has no notion of "same as default".
     */
    override fun shouldEncodeElementDefault(descriptor: SerialDescriptor, index: Int): Boolean = true

    override fun encodeNull() = put(ReplicaValue.Null)

    override fun encodeBoolean(value: Boolean) = put(ReplicaValue.Bool(value))

    override fun encodeString(value: String) = put(ReplicaValue.Str(value))

    override fun encodeDouble(value: Double) = put(ReplicaValue.Num(value))

    override fun encodeFloat(value: Float) = put(ReplicaValue.Num(value.toDouble()))

    override fun encodeInt(value: Int) = put(ReplicaValue.Num(value.toDouble()))

    override fun encodeLong(value: Long) = put(ReplicaValue.signedInteger(value))

    override fun encodeShort(value: Short) = put(ReplicaValue.Num(value.toDouble()))

    override fun encodeByte(value: Byte) = put(ReplicaValue.Num(value.toDouble()))

    override fun encodeChar(value: Char) = put(ReplicaValue.Str(value.toString()))

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) =
        put(ReplicaValue.Str(enumDescriptor.getElementName(index)))

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder =
        when (descriptor.kind) {
            StructureKind.LIST -> ListEncoder { put(it) }
            StructureKind.MAP -> MapEncoder { put(it) }
            else -> ObjectEncoder { put(it) }
        }
}

private class SingleValueEncoder(private val box: ValueBox) : TreeEncoder() {
    override fun put(value: ReplicaValue) {
        box.value = value
    }
}

private class ObjectEncoder(private val finish: (ReplicaValue) -> Unit) : TreeEncoder() {
    private val fields = LinkedHashMap<String, ReplicaValue>()
    private var key: String? = null

    override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
        key = descriptor.getElementName(index)
        return true
    }

    /** Swift's `encodeIfPresent`: a nil property is omitted, never null. */
    override fun <T : Any> encodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T?,
    ) {
        if (value == null) return
        encodeElement(descriptor, index)
        encodeSerializableValue(serializer, value)
    }

    override fun put(value: ReplicaValue) {
        val name = key ?: return
        fields[name] = value
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        finish(ReplicaValue.Obj(fields))
    }
}

private class ListEncoder(private val finish: (ReplicaValue) -> Unit) : TreeEncoder() {
    private val values = mutableListOf<ReplicaValue>()

    override fun put(value: ReplicaValue) {
        values.add(value)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        finish(ReplicaValue.Arr(values))
    }
}

private class MapEncoder(private val finish: (ReplicaValue) -> Unit) : TreeEncoder() {
    private val fields = LinkedHashMap<String, ReplicaValue>()
    private var pendingKey: String? = null

    override fun put(value: ReplicaValue) {
        val key = pendingKey
        if (key == null) {
            pendingKey = value.string
                ?: throw TreeDecodeException("a map key must be a string, got $value")
        } else {
            fields[key] = value
            pendingKey = null
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        finish(ReplicaValue.Obj(fields))
    }
}
