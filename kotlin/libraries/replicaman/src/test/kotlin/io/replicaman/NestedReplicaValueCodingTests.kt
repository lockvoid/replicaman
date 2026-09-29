package io.replicaman

import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * A generated model may carry a raw `ReplicaValue` column inside a `data`
 * blob (`NetworkData.Operators.cookOptions`, `CookData.GraphicMetadata.inputs`).
 * The tree coder hands that subtree to `ReplicaValueSerializer` as-is; the
 * serializer used to refuse anything but a JSON decoder, so every pulled
 * topology row whose operator carried options decoded to null.
 */
class NestedReplicaValueCodingTests {

    @Serializable
    data class Operator(val key: String, val options: ReplicaValue = ReplicaValue.Obj(emptyMap()))

    @Serializable
    data class Topology(val operators: Map<String, Operator> = emptyMap())

    private val options = ReplicaValue.Obj(
        mapOf(
            "threshold" to ReplicaValue.Num(0.5),
            "tags" to ReplicaValue.Arr(listOf(ReplicaValue.Str("a"), ReplicaValue.Null)),
            "nested" to ReplicaValue.Obj(mapOf("on" to ReplicaValue.Bool(true))),
        ),
    )

    /** KILL: drop the `ReplicaValueDecoder` fast path from `ReplicaValueSerializer.deserialize`. */
    @Test
    fun `a nested ReplicaValue decodes from the tree as-is`() {
        val raw = ReplicaValue.Obj(
            mapOf(
                "operators" to ReplicaValue.Obj(
                    mapOf(
                        "device" to ReplicaValue.Obj(mapOf("key" to ReplicaValue.Str("device"), "options" to options)),
                        "server" to ReplicaValue.Obj(mapOf("key" to ReplicaValue.Str("server"))),
                    ),
                ),
            ),
        )
        val decoded = ReplicaValueCoding.decode(Topology.serializer(), raw)
        assertEquals(options, decoded.operators.getValue("device").options)
        assertEquals(ReplicaValue.Obj(emptyMap()), decoded.operators.getValue("server").options)
    }

    /** KILL: drop the `ReplicaValueEncoder` fast path from `ReplicaValueSerializer.serialize`. */
    @Test
    fun `a nested ReplicaValue encodes into the tree as-is and round trips`() {
        val topology = Topology(mapOf("device" to Operator("device", options)))
        val encoded = ReplicaValueCoding.encode(Topology.serializer(), topology)
        val operators = (encoded as ReplicaValue.Obj).fields.getValue("operators") as ReplicaValue.Obj
        val device = operators.fields.getValue("device") as ReplicaValue.Obj
        assertEquals(options, device.fields.getValue("options"))
        assertEquals(topology, ReplicaValueCoding.decode(Topology.serializer(), encoded))
    }
}
