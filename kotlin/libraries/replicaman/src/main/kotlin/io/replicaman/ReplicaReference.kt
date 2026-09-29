package io.replicaman

/** A relationship whose target lifetime travels with each mutation. */
public data class ReplicaReferenceSpec(
    val name: String,
    val stream: String,
    val field: String? = null,
    val keySegment: Int? = null,
    val keyPrefix: String? = null,
    val optional: Boolean = false,
) {
    internal fun target(rowId: String, data: Map<String, ReplicaValue>): String? {
        if (keyPrefix != null && !rowId.startsWith(keyPrefix)) return null
        val id = if (keySegment != null) {
            rowId.split('/').getOrNull(keySegment)
        } else {
            val value = data[field ?: name]
            if (optional && (value == null || value == ReplicaValue.Null)) return null
            value?.string
        }
        if (id.isNullOrEmpty()) throw ReplicaError.Storage("Missing or invalid reference: $name")
        return id
    }
}

public data class ReplicaReference(val name: String, val stream: String, val id: String, val incarnation: String) {
    internal fun toValue(): ReplicaValue = ReplicaValue.Obj(mapOf(
        "name" to ReplicaValue.Str(name), "stream" to ReplicaValue.Str(stream),
        "id" to ReplicaValue.Str(id), "incarnation" to ReplicaValue.Str(incarnation),
    ))

    internal companion object {
        fun fromValue(value: ReplicaValue): ReplicaReference {
            fun text(key: String): String = value[key]?.string?.takeIf { it.isNotEmpty() }
                ?: throw ReplicaError.Codec("Reference $key must be a nonempty string")
            return ReplicaReference(text("name"), text("stream"), text("id"), text("incarnation"))
        }
    }
}

internal object ReplicaLifetime {
    fun derived(namespace: String, stream: String, id: String, parent: ReplicaReference): String {
        val parts = listOf("replicaman:derived:1", namespace, stream, id,
            parent.stream, parent.id, parent.incarnation)
        val content = parts.joinToString("") { "${it.toByteArray(Charsets.UTF_8).size}:$it" }
        return "derived:" + ReplicaProtocol.digest(content.toByteArray(Charsets.UTF_8))
    }
}
