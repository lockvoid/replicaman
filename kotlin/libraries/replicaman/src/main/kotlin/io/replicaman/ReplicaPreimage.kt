package io.replicaman

/**
 * What a client write displaced, captured with its journal
 * entry (client-local, never on the wire) so a rejected verdict can undo
 * the write atomically in the verdict transaction.
 */
internal sealed interface ReplicaPreimage {
    /** The row did not exist — a rejected create removes it. */
    data object Absent : ReplicaPreimage

    /**
     * A patch touched exactly these fields: `values` restore, `missing`
     * were introduced by the patch and are removed. Interim server fields
     * stay untouched.
     */
    data class Fields(
        val values: Map<String, ReplicaValue>,
        val missing: List<String>,
    ) : ReplicaPreimage

    /** A delete displaced this whole row — restore it byte-identical. */
    data class Row(
        val shard: String,
        val type: String?,
        val data: Map<String, ReplicaValue>,
    ) : ReplicaPreimage

    fun toValue(): ReplicaValue = when (this) {
        Absent -> ReplicaValue.Obj(mapOf("absent" to ReplicaValue.Obj(emptyMap())))
        is Fields -> ReplicaValue.Obj(
            mapOf(
                "fields" to ReplicaValue.Obj(
                    mapOf(
                        "values" to ReplicaValue.Obj(values),
                        "missing" to ReplicaValue.Arr(missing.map { ReplicaValue.Str(it) })
                    )
                )
            )
        )

        is Row -> ReplicaValue.Obj(
            mapOf(
                "row" to ReplicaValue.Obj(
                    mapOf(
                        "shard" to ReplicaValue.Str(shard),
                        "type" to (type?.let { ReplicaValue.Str(it) } ?: ReplicaValue.Null),
                        "data" to ReplicaValue.Obj(data)
                    )
                )
            )
        )
    }

    fun encoded(): ByteArray = ReplicaJSON.encodeToBytes(toValue())

    fun undoing(images: List<ReplicaPreimage>): ReplicaPreimage = images.asReversed().fold(this) { state, image ->
        when (image) {
            Absent, is Row -> image
            is Fields -> if (state is Row) state.copy(data = (state.data - image.missing.toSet()) + image.values) else state
        }
    }

    fun forRelease(op: ReplicaOp): ReplicaPreimage {
        if (op.verb == ReplicaOp.Verb.ROW_CREATE) return Absent
        if (op.verb != ReplicaOp.Verb.ROW_PATCH || this !is Row) return this
        val touched = op.data.orEmpty()
        return Fields(data.filterKeys { it in touched }, touched.keys.filter { it !in data })
    }

    companion object {
        fun fromValue(value: ReplicaValue): ReplicaPreimage? {
            val outer = value as? ReplicaValue.Obj ?: return null
            if (outer.fields.size != 1) return null
            (value["absent"] as? ReplicaValue.Obj)?.let { return Absent }
            (value["fields"] as? ReplicaValue.Obj)?.let { fields ->
                val values = (fields["values"] as? ReplicaValue.Obj)?.fields ?: return null
                val items = (fields["missing"] as? ReplicaValue.Arr)?.items ?: return null
                val missing = items.map { it.string ?: return null }
                return Fields(values, missing)
            }
            (value["row"] as? ReplicaValue.Obj)?.let { row ->
                val shard = row["shard"]?.string ?: return null
                val data = (row["data"] as? ReplicaValue.Obj)?.fields ?: return null
                val type = row["type"]
                if (type != null && type != ReplicaValue.Null && type !is ReplicaValue.Str) return null
                return Row(shard, type?.string, data)
            }
            return null
        }

        fun decode(bytes: ByteArray): ReplicaPreimage = require(bytes)

        /** Strict decode — the identity rebind's preflight must fail closed. */
        fun require(bytes: ByteArray): ReplicaPreimage =
            fromValue(ReplicaJSON.decodeValue(bytes.toString(Charsets.UTF_8)))
                ?: throw ReplicaError.Storage("journal preimage would not decode")
    }
}

