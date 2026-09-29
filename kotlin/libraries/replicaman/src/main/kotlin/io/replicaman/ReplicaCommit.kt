package io.replicaman

import java.util.UUID

/** Capture before sending a command; binds its reply to this engine and owner. */
public class ReplicaCommitSession internal constructor(
    internal val engine: UUID,
    internal val binding: ULong,
)

/** A command's reply: the shards its transaction changed, under the server's header. */
internal class ReplicaCommit(encoded: String, schema: ReplicaSchema, dataset: String?) {
    val shards: List<String>

    init {
        val bytes = ReplicaProtocol.binary(encoded, 64 * 1024)
        val value = ReplicaProtocol.decode(bytes)
        ReplicaProtocol.validateHeader(value, schema, dataset)
        shards = value.requiredArray("shards").map {
            it.string ?: throw ReplicaError.InvalidCommit
        }
        if (shards.distinct().size != shards.size || shards.any { it !in schema.shards }) {
            throw ReplicaError.InvalidCommit
        }
    }
}

/** A journal draft scope, distinct from an editor document draft. */
public data class ReplicaDraft(val key: String)
public data class ReplicaDraftResult<T>(val draft: ReplicaDraft, val value: T)
