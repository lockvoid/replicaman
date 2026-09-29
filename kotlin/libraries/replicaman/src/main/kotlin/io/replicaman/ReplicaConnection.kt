package io.replicaman

internal class ReplicaConnection(private val transport: ReplicaTransport, private val schema: ReplicaSchema) {
    /** `dataset` is null only before the first pull has learned it. */
    suspend fun send(endpoint: ReplicaEndpoint, dataset: String?, fields: Map<String, ReplicaValue>): ReplicaValue {
        val header = mapOf(
            "protocol" to ReplicaValue.Integer(ReplicaProtocol.VERSION.toLong()),
            "namespace" to ReplicaValue.Str(schema.namespace),
            "schema" to ReplicaValue.Integer(schema.version.toLong()),
            "dataset" to (dataset?.let(ReplicaValue::Str) ?: ReplicaValue.Null),
        )
        val request = ReplicaJSON.encodeToBytes(ReplicaValue.Obj(header + fields))
        val response = ReplicaProtocol.decode(transport.exchange(endpoint, request))
        ReplicaProtocol.validateHeader(response, schema, dataset)
        return response
    }
}
