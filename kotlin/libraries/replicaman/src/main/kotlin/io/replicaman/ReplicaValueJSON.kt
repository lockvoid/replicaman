package io.replicaman

import kotlinx.serialization.json.JsonObject

/** A damaged snapshot must fail before a caller can author from an empty baseline. */
internal object ReplicaValueJSON {
    fun decodeObject(json: String?): Map<String, ReplicaValue> {
        if (json.isNullOrEmpty() || ReplicaJSON.nestsTooDeep(json)) throw ReplicaError.Storage("Invalid snapshot JSON")
        val obj = ReplicaJSON.json.parseToJsonElement(json) as? JsonObject
            ?: throw ReplicaError.Storage("Snapshot JSON must be an object")
        return obj.mapValues { ReplicaValueSerializer.fromJson(it.value) }
    }
}
