package io.replicaman

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.emptyFlow

/**
 * A gate the HOST puts on what leaves the device. The engine asks it when a
 * row is WRITTEN — never on a drain — and knows nothing about bytes,
 * settings or accounts: a change goes, is held, or is never sent.
 *
 * A held row leaves the journal alone: the engine keeps it in `holds` and
 * asks again only when the gate says its answer may have changed
 * ([changes]), when the row is written again, and when the store opens. A
 * released row leaves as its current state, never as the history of its
 * writes.
 */
public interface SyncGate {
    /** Stable across launches — the holds it placed are released by it. */
    public val id: String

    /**
     * The stream this gate judges; null judges every stream. A gate over
     * every stream is a policy (Cloud Backup): what it lets go does not wait
     * for the rows it holds.
     */
    public val stream: String?

    public fun judge(change: SyncChange): SyncVerdict

    /**
     * Emits when something this gate consults moved: a setting flipped,
     * bytes landed. Every held row of the gate is asked again.
     */
    public val changes: Flow<Unit> get() = emptyFlow()
}

public sealed interface SyncVerdict {
    /** Send the change. */
    public data object Push : SyncVerdict

    /** Hold the whole row: none of its writes leave until the gate lets it go. */
    public data class Gate(val reason: String) : SyncVerdict

    /**
     * Never send this change: the device keeps its value, the row's later
     * writes go on. Never a row's birth — every later write stands on it.
     */
    public data object Discard : SyncVerdict
}

/**
 * One change as a gate sees it: one write of one row, the values it set and
 * the values it replaced.
 */
public data class SyncChange(
    val stream: String,
    val rowId: String,
    val kind: Kind,
    /** The value this write set for every field it changed — empty for a delete and a document. */
    val local: Map<String, ReplicaValue> = emptyMap(),
    /**
     * The value each of those fields held before this write — the whole row
     * a delete displaced. A field the row did not have is absent — every
     * field of a create.
     */
    val previous: Map<String, ReplicaValue> = emptyMap(),
) {
    public enum class Kind { CREATE, PATCH, DELETE, DOCUMENT }
}

/** A gate's "ask me again": any number of engines listen, the host fires. */
public class SyncGateSignal {
    private val flow = MutableSharedFlow<Unit>(extraBufferCapacity = 1, onBufferOverflow = BufferOverflow.DROP_OLDEST)

    public val changes: Flow<Unit> get() = flow

    public fun fire() {
        flow.tryEmit(Unit)
    }
}

/**
 * The declared gates applied as one — the gates over every stream, then the
 * change's stream's: a discard by any gate discards (a change never needed is
 * not held for later either); else the first hold holds.
 */
public class SyncGates(gates: List<SyncGate>) {
    public sealed interface Outcome {
        public data object Push : Outcome
        public data class Hold(val gate: String, val reason: String) : Outcome
        public data object Discard : Outcome
    }

    private val everyStream = gates.filter { it.stream == null }
    private val byStream = gates.filter { it.stream != null }.groupBy { it.stream!! }

    public val isEmpty: Boolean get() = everyStream.isEmpty() && byStream.isEmpty()

    public val all: List<SyncGate> get() = everyStream + byStream.values.flatten()

    /**
     * Whether a row held by this gate makes the rows naming it wait — every
     * gate but a policy over every stream.
     */
    public fun orders(gateId: String): Boolean = everyStream.none { it.id == gateId }

    public fun judge(change: SyncChange): Outcome {
        var held: Outcome.Hold? = null
        for (gate in everyStream + byStream[change.stream].orEmpty()) {
            when (val verdict = gate.judge(change)) {
                SyncVerdict.Discard -> return Outcome.Discard
                is SyncVerdict.Gate -> if (held == null) held = Outcome.Hold(gate.id, verdict.reason)
                SyncVerdict.Push -> Unit
            }
        }
        return held ?: Outcome.Push
    }
}
