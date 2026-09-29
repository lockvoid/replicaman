package io.replicaman

/**
 * Which drain a write leaves on. Claimed by an ACTION, never by a stream:
 * the same stream carries a foreground write and a background one (a chat
 * send authors elements; so does an import), so stream granularity cannot
 * express "someone is waiting on this one".
 *
 * Lanes drain concurrently, so an interactive write OVERTAKES a bulk backlog.
 * Order holds within a lane only — which is safe because the engine keeps two
 * invariants automatically: a row's later ops join its pending lane, and an
 * interactive op promotes any pending row it names.
 */
public enum class ReplicaLane(public val rawValue: String) {
    /** A human is waiting on this write — a tap, a send, a foreground edit. */
    INTERACTIVE("interactive"),

    /** Background work: imports, cook fan-out, catalog fill. The default. */
    BULK("bulk");

    public companion object {
        public val allCases: List<ReplicaLane> = listOf(INTERACTIVE, BULK)

        public fun fromRaw(rawValue: String?): ReplicaLane? =
            allCases.firstOrNull { it.rawValue == rawValue }
    }
}

/**
 * One declared stream, as the manifest describes it. The manifest knows the
 * lane and the direction — that is what drives the engine's `row.delete`
 * cascade and lets codegen omit write verbs entirely on readonly streams
 * (the client *can't* express the mistake).
 */
public data class ReplicaStreamSpec(
    val name: String,
    val lane: Lane,
    val readonly: Boolean = false,
    val shard: String = "user",
    /** Document-lane codec wire name (`loro@1`); null on row streams. */
    val codec: String? = null,
    val reflections: List<ReplicaReflection> = emptyList(),
    val stamp: ReplicaStamp? = null,
    /** Fields every patch carries, moved or not, for the server's write preconditions. */
    val preconditions: List<String> = emptyList(),
    /**
     * The fields a device may send — the manifest's `push` columns, every
     * variant's included; the server refuses any other. A row a gate held
     * leaves as these fields of its state. Null: every field.
     */
    val pushed: Set<String>? = null,
    val references: List<ReplicaReferenceSpec> = emptyList(),
    val lifetimeFrom: String? = null,
) {
    public enum class Lane(public val rawValue: String) {
        ROW("row"),
        DOCUMENT("document"),
    }
}

/**
 * One physical read structure the store builds over a pulled field — the
 * manifest's `indexes:` entry. `btree` is an index over a generated column
 * (`json_extract(data, '$.field')`), partial per stream; `fts5` is a
 * shadow table kept by triggers. Indexes are DERIVATIVES of `data`: the
 * store reconciles declared ↔ actual at every open, so there are no
 * migrations — only the manifest.
 */
public data class ReplicaIndexSpec(
    val stream: String,
    val field: String,
    val kind: ReplicaIndexKind = ReplicaIndexKind.BTREE,
) {
    /**
     * The generated column — one per FIELD, shared by every stream that
     * indexes it (the expression is stream-agnostic). So is the btree:
     * `(stream, ix_field)`, one per field — two equalities always beat the
     * primary key's one for the planner, stats or no stats (a partial
     * `WHERE stream = …` index lost that race the moment `sqlite_stat1`
     * existed for the PK). The fts5 table is per (stream, field): its
     * triggers are stream-gated.
     */
    // `field` inside an accessor is the backing-field keyword — the
    // constructor property needs `this.`, or Kotlin demands an initializer.
    internal val column: String
        get() = "ix_" + this.field

    internal val btreeIndex: String
        get() = "idx_" + this.field

    internal val ftsTable: String
        get() = "fts_" + this.stream + "_" + this.field
}

public enum class ReplicaIndexKind(public val rawValue: String) {
    BTREE("btree"),
    FTS5("fts5"),
}

/**
 * A generated per-stream field enum: the raw value is the wire field name.
 * Only indexed fields are cases, so a predicate over anything else does
 * not compile.
 */
public interface ReplicaIndexedField {
    public val rawValue: String
}

/** The `Field` of a model with no indexes — nothing to scope on. */
public enum class ReplicaNoField : ReplicaIndexedField {
    ;

    override val rawValue: String
        get() = throw IllegalStateException("ReplicaNoField has no cases")
}

/**
 * The engine's map of the replica: stream specs plus the shard list, in
 * declaration order. Generated code ships one of these; tests build them by
 * hand.
 */
public class ReplicaSchema(
    streams: List<ReplicaStreamSpec>,
    /** Every declared read structure, in manifest order. */
    public val indexes: List<ReplicaIndexSpec> = emptyList(),
    public val namespace: String = "replicaman",
    public val version: Int = 1,
) {
    public val specs: List<ReplicaStreamSpec> = streams

    /**
     * Shards in first-appearance order — pulled independently, one cursor
     * each.
     */
    public val shards: List<String>

    private val byName: Map<String, ReplicaStreamSpec> = streams.associateBy { it.name }

    init {
        val seen = mutableListOf<String>()
        for (spec in streams) {
            if (!seen.contains(spec.shard)) seen.add(spec.shard)
        }
        shards = if (seen.isEmpty()) listOf("user") else seen
    }

    public fun spec(name: String): ReplicaStreamSpec? = byName[name]

    /**
     * The lane an incoming frame's stream belongs to. Unknown streams are
     * row-lane by definition (nothing to cascade) — the importer stays
     * total.
     */
    public fun lane(stream: String): ReplicaStreamSpec.Lane =
        byName[stream]?.lane ?: ReplicaStreamSpec.Lane.ROW

    public fun streams(shard: String): List<String> =
        specs.filter { it.shard == shard }.map { it.name }
}

/** A row field owned by the value at a path in its document. */
public data class ReplicaReflection(val field: String, val path: List<String>)

public interface ReplicaColumn { public val rawValue: String }

public interface ReplicaVariant { public val wireType: String }
