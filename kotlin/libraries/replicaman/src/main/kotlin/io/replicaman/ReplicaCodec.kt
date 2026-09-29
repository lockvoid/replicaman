package io.replicaman

/**
 * The document-lane merge protocol behind an opaque-bytes seam: folds,
 * payloads and version vectors are all `ByteArray`, so the core neither
 * imports nor understands loro — `:replicaman-loro` is the one plugin that
 * does, and a rows-only consumer never links it (module-graph enforcement).
 *
 * Contracts the engine leans on:
 * - `merge` is a real CRDT merge (idempotent, order-free) and REFUSES a
 *   payload whose causal deps the fold has not seen (`MissingCausalDeps`) —
 *   a silently-parked edit would be absent from every future fold.
 * - `diff(fold, since)` returns exactly the ops past `since` — the
 *   per-document supersede (one pending merged delta) is literally
 *   `diff(fold, since = acked)`.
 * - `payloadVersion` reads a payload's end version WITHOUT applying it —
 *   how acked advances on server frames and accepted pushes alike.
 */
public interface ReplicaCodec {
    /** Wire name (`loro@1`) — matched against frame/op `codec` fields. */
    public val name: String

    /**
     * Merge an update or snapshot payload into a fold; null fold = a fresh
     * document born from the payload.
     */
    public fun merge(fold: ByteArray?, payload: ByteArray, reflections: List<ReplicaReflection> = emptyList()): ReplicaMerge

    /** Updates past `since` (null = everything). */
    public fun diff(fold: ByteArray, since: ByteArray?): ByteArray

    /** The fold's own version vector, encoded. */
    public fun version(fold: ByteArray): ByteArray

    /**
     * The end version covered by an update/snapshot payload, encoded —
     * read from the blob's metadata, never by applying it.
     */
    public fun payloadVersion(payload: ByteArray): ByteArray

    /** Union of two encoded version vectors. */
    public fun mergeVersions(a: ByteArray?, b: ByteArray): ByteArray

    /**
     * True when an update payload carries no ops — an empty diff is not
     * worth a journal entry.
     */
    public fun isEmptyDiff(payload: ByteArray): Boolean
}

/** Fold and its reflected row fields from one codec merge. */
public class ReplicaMerge(public val fold: ByteArray, public val reflected: Map<String, ReplicaValue>) {
    override fun equals(other: Any?): Boolean = other is ReplicaMerge && fold.contentEquals(other.fold) && reflected == other.reflected
    override fun hashCode(): Int = 31 * fold.contentHashCode() + reflected.hashCode()
}

/** Projection mode deliberately excludes document history from this store. */
public enum class ReplicaDocumentMode { REPLICATED, PROJECTIONS_ONLY }
