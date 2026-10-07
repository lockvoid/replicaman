package io.replicaman

public sealed class ReplicaError(message: String) : Exception(message) {
    public data class Protocol(val code: String, val detail: String) : ReplicaError("$code: $detail")

    /** The entire atomic action remains uncommitted. */
    public data class AtomicWriteBlocked(val reason: String) : ReplicaError("atomic write blocked: $reason")

    public data class Storage(val reason: String) : ReplicaError("storage: $reason")

    public data class Transport(val reason: String) : ReplicaError("transport: $reason")

    /** A local write addressed a stream the schema does not declare. */
    public data class UnknownStream(val stream: String) : ReplicaError("unknown stream: $stream")

    /**
     * `create` of a row the store already holds — the caller believed
     * it was minting; the row's truth stands, nothing is written.
     */
    public data class RowExists(val stream: String, val id: String) :
        ReplicaError("row exists: $stream/$id")

    /**
     * `update` of a row the store does not hold — the caller believed it
     * was editing; nothing is written.
     */
    public data class UnknownRow(val stream: String, val id: String) :
        ReplicaError("unknown row: $stream/$id")

    /** A present row whose stored shape this model cannot decode. */
    public data class UndecodableRow(val stream: String, val id: String) :
        ReplicaError("undecodable row: $stream/$id")

    /** A local doc edit addressed a document the store does not hold. */
    public data class UnknownDocument(val stream: String, val id: String) :
        ReplicaError("unknown document: $stream/$id")

    /**
     * A local write addressed a readonly stream — generated code cannot
     * express this; reaching it means a caller bypassed the verbs.
     */
    public data class ReadonlyStream(val stream: String) :
        ReplicaError("readonly stream: $stream")

    /**
     * A row id outside the protocol's business key — UTF-8 of 1 to 1024
     * bytes without NUL. Nothing is written: the server would refuse the
     * whole push it rode in, forever.
     */
    public data class InvalidRowId(val stream: String, val id: String) :
        ReplicaError("invalid row id: $stream/${id.take(64)}")

    /**
     * A write whose wire operation exceeds the request limit. Nothing is
     * written: an intent that can never leave would stop the queue behind it.
     */
    public data class OversizedWrite(val stream: String, val id: String, val bytes: Int) :
        ReplicaError("oversized write: $stream/$id ($bytes bytes)")

    /**
     * A draft write the draft could not undo: a document the draft did not
     * create, or a document's deletion. Nothing is written.
     */
    public data class DraftBlocked(val reason: String) : ReplicaError("draft blocked: $reason")

    /**
     * Nobody owns this process, so there is no store to work in. Every write
     * verb answers this while the engine is closed; reads answer empty.
     */
    public data object NoOwner : ReplicaError("no owner")

    /** Work captured before an owner switch may never author into a later world, including A → B → A. */
    public data object StaleLocalSession : ReplicaError("local authoring session is stale")

    /**
     * A row verb hit a document stream (or the reverse) — the lane rides
     * the manifest and the generated verbs can't express the mismatch.
     */
    public data class LaneMismatch(val stream: String) : ReplicaError("lane mismatch: $stream")

    /**
     * A payload whose causal dependencies the local doc has not seen —
     * applying it would silently drop the edit into a pending queue the
     * store cannot persist.
     */
    public data object MissingCausalDeps : ReplicaError("missing causal deps")

    /**
     * A local write or authenticated wire exchange tried to start while the
     * host was moving the durable store between session identities. Retrying
     * after the transition is safe; admitting it now could recreate the
     * outgoing journal after its wipe or send it with the replacement bearer.
     */
    public data object IdentityTransitionInProgress :
        ReplicaError("identity transition in progress")

    /**
     * An identity-store operation was attempted without first closing write
     * admissions and waiting for every outgoing authenticated exchange.
     */
    public data object IdentityTransitionRequired : ReplicaError("identity transition required")

    public data object InvalidCommit : ReplicaError("invalid commit")

    public data object StaleCommit : ReplicaError("stale commit")

    public data class Codec(val reason: String) : ReplicaError("codec: $reason")
}
