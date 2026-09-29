package io.replicaman

import androidx.sqlite.SQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import java.io.File
import java.time.Instant
import java.time.format.DateTimeFormatter
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource


/**
 * The loop. One class owns the whole replication surface: pull rounds
 * (staged pages → base → cursor, published in one transaction), the outbound
 * op journal (per-document supersede, verdicts, cold window), and the local
 * write door the generated verbs call.
 *
 * ISOLATION: the iOS `actor` becomes ONE `limitedParallelism(1)` dispatcher.
 * Only one engine body runs at a time, and every suspension inside releases
 * it — actor reentrancy, which drain-joins, seal waiters and the
 * mid-flight transport hooks all depend on.
 *
 * ORDER IS THE CONTRACT (ported from Syncer v1):
 * - A pull round publishes ATOMICALLY with its cursor — a crash mid-round
 *   leaves the previous base and cursor intact and resumes from staging.
 * - Drain-before-pull, so the echo is in the answer; `drainIfWarm` skips
 *   inside the cold window so an offline fetch burst doesn't stack
 *   timeouts on a wire that just proved dead.
 * - Journal durability and the client snapshot write share one
 *   transaction — a crash between them cannot exist.
 * - Rejection is a VERDICT (park, keep the reason), transport failure is a
 *   RETRY (entries stay pending) — never conflated.
 */
public class ReplicaEngine(
    public val home: File = ReplicaMan.Configuration.homePath,
    internal val transport: ReplicaTransport,
    public val schema: ReplicaSchema,
    codecs: List<ReplicaCodec> = emptyList(),
    internal val batchLimit: Int = 500,
    private val coldWindow: Duration = 10.seconds,
    internal val peerMinter: () -> ULong = { ReplicaID.peer() },
    private val clock: () -> Instant = { Instant.now() },
    private val automaticallyPushWrites: Boolean = true,
    private val storeSuffix: String = "",
    /**
     * Host-supplied at INIT — like a cache normalizer. Asked when a row is
     * WRITTEN, never on a drain: a held row waits in `holds` until its gate
     * emits `changes`, the row is written again, or the store opens.
     * Birth-only by design: the engine self-drains from its first write, so
     * a registration door would race the first judge.
     */
    syncGates: List<SyncGate> = emptyList(),
    internal val documentMode: ReplicaDocumentMode = ReplicaDocumentMode.REPLICATED,
) {
    /**
     * Test seam: bind a store the caller already built, without the
     * filesystem naming. Internal — production has exactly one door, and it
     * is `open(owner)`.
     */
    internal constructor(
        store: ReplicaStateStore,
        owner: Long,
        transport: ReplicaTransport,
        schema: ReplicaSchema,
        codecs: List<ReplicaCodec> = emptyList(),
        batchLimit: Int = 500,
        coldWindow: Duration = 10.seconds,
        peerMinter: () -> ULong = { ReplicaID.peer() },
        clock: () -> Instant = { Instant.now() },
        automaticallyPushWrites: Boolean = true,
        syncGates: List<SyncGate> = emptyList(),
        documentMode: ReplicaDocumentMode = ReplicaDocumentMode.REPLICATED,
    ) : this(
        home = File(System.getProperty("java.io.tmpdir")),
        transport = transport,
        schema = schema,
        codecs = codecs,
        batchLimit = batchLimit,
        coldWindow = coldWindow,
        peerMinter = peerMinter,
        clock = clock,
        automaticallyPushWrites = automaticallyPushWrites,
        storeSuffix = "",
        syncGates = syncGates,
        documentMode = documentMode,
    ) {
        binding.bind(ReplicaBinding.Bound(owner, store, store.path))
    }

    /**
     * The host's gates, declared before anything that listens to them: a
     * held row waits in `holds` until its gate emits `changes`, the row is
     * written again, or the store opens.
     */
    internal val syncGates = SyncGates(syncGates)

    /**
     * Which owner's file this process holds open. Everything below reads it;
     * only `open`/`close`/`retire` write it.
     */
    internal val binding: ReplicaBinding = ReplicaBinding()

    /** The documents held live — the state cache behind `findDoc`. */
    internal val liveDocuments: LiveDocuments = LiveDocuments()

    internal val codecs: Map<String, ReplicaCodec> = codecs.associateBy { it.name }
    public val health: ReplicaHealth = ReplicaHealth()

    private val backgroundFailures = CoroutineExceptionHandler { _, failure ->
        if (failure is Exception) health.record(failure, "background replica task")
        else throw failure
    }

    @Suppress("OPT_IN_USAGE")
    internal val engineContext = Dispatchers.Default.limitedParallelism(1)

    /** Engine-owned unstructured work — `Task { }`'s seat. */
    internal val scope: CoroutineScope = CoroutineScope(SupervisorJob() + engineContext + backgroundFailures)

    init {
        // One listener per gate for the life of the engine, subscribed before
        // init returns: a signal fired right after construction is heard.
        for (gate in this.syncGates.all) {
            scope.launch(start = CoroutineStart.UNDISPATCHED) {
                gate.changes.collect { gateChanged(gate.id) }
            }
        }
    }

    /** Observers run off the engine's slot so a delivery never holds it. */
    internal val watchScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default + backgroundFailures)

    /**
     * Per LANE, not per engine: a fat bulk batch is what dies on a slow link
     * (the server needs ~9s to apply 20 ops), and the one-row message that
     * would have succeeded must not wait out ITS backoff. A genuinely dead
     * wire cools each track on that track's own failed attempt.
     */
    private val coldUntil = mutableMapOf<ReplicaLane, TimeSource.Monotonic.ValueTimeMark>()
    private val scheduledPushes = mutableMapOf<ReplicaLane, Job>()

    /**
     * One flight per lane: that is what lets an interactive write leave while
     * a bulk push is still on the wire.
     */
    private val activeDrains = mutableMapOf<ReplicaLane, Deferred<List<ReplicaVerdict>>>()
    private val activePulls = mutableMapOf<String, Deferred<Pair<Int, Boolean>>>()
    private val writeGate = ReplicaWriteGate()

    /**
     * Every write inside rides one lane, in order. Reads as the fact the
     * caller actually knows: someone is waiting on this.
     *
     * ```kotlin
     * replica.lane(ReplicaLane.INTERACTIVE) {
     *     ProjectMutations.createElements(...)   // inherits
     *     replica.writeAsync { tx -> tx.agentMessages.create(message) }
     * }
     * ```
     *
     * Not a transaction: writes land locally as they happen, the server
     * applies each op in its own savepoint, and a refusal is per op. It is a
     * routing and ordering scope, nothing more.
     */
    public suspend fun <T> lane(lane: ReplicaLane, body: suspend () -> T): T =
        withContext(currentLaneLocal.asContextElement(lane)) { body() }

    /**
     * A sealed engine admits no new local authoring and no authenticated
     * transport. `seal()` returns only once every operation that already
     * captured the outgoing bearer has finished applying its response
     * locally — which is what makes the merge barrier and the sign-out
     * flush safe. Closing seals; opening unseals.
     */
    @Volatile
    private var sealedFlag = false
    private var activeWireOperations = 0
    private val sealWaiters = mutableListOf<CompletableDeferred<Unit>>()

    /**
     * Read-only cross-module proof for the app host's pinned-source drain
     * admission. Mutation remains owned by seal/unseal.
     */
    public val isSealed: Boolean get() = sealedFlag

    /** Test/consumer read of the seal — the iOS `sealed` property. */
    public val sealed: Boolean get() = sealedFlag

    /**
     * Test seam: thrown between frame apply and cursor advance to prove
     * checkpoint atomicity. Internal on purpose.
     */
    internal var checkpointFault: (() -> Unit)? = null

    /**
     * The rejection seam: fired once per rejected op AFTER the
     * verdict transaction (revert included) commits. The app hangs its
     * "changes couldn't sync and were undone" surface off it.
     */
    internal var onRejected: ((ReplicaOp, String) -> Unit)? = null

    /**
     * How many client writes have been undone by rejected verdicts
     * this session — the debug surface reads it next to the parked count.
     */
    public var revertedCount: Int = 0
        internal set

    public suspend fun setRejectionHandler(handler: ((ReplicaOp, String) -> Unit)?) {
        withContext(engineContext) { onRejected = handler }
    }

    // MARK: - Identity boundary

    /**
     * Freeze local write admissions and authenticated transport, then wait
     * for every already-started push/pull to settle under the outgoing
     * credentials. The caller may merge or release the store only after this
     * returns. Idempotent for the one serialized host transition.
     */
    public suspend fun seal() {
        withContext(engineContext) { liveDocuments.publishing { sealedFlag = true } }
        writeGate.close()
        val waiter = withContext(engineContext) {
            if (activeWireOperations == 0) {
                null
            } else {
                CompletableDeferred<Unit>().also { sealWaiters.add(it) }
            }
        }
        waiter?.await()
    }

    /**
     * Re-admit local authoring and the wire once credentials and the durable
     * store agree on one identity.
     */
    public suspend fun unseal() {
        withContext(engineContext) { unsealLocked() }
    }

    private fun unsealLocked() {
        sealedFlag = false
        writeGate.open()
        // A gate's signal fired while sealed was dropped: ask its holds now.
        askHoldsAgain()
        if (!automaticallyPushWrites) return
        val store = binding.store ?: return
        for (lane in ReplicaLane.allCases) {
            if (scheduledPushes[lane] != null) continue
            val owed = try {
                store.read { db -> store.owesWork(db, lane) }
            } catch (error: Exception) {
                // This lifecycle notification has no caller to receive a throw.
                // Health reports the failure; the durable queue remains intact.
                health.record(error, "resume automatic delivery")
                Log.logger.error("[push] unseal could not read what $lane owes — no push scheduled: $error")
                false
            }
            if (owed) schedulePush(lane)
        }
    }

    /**
     * Seal, then push the frozen outgoing journal through a caller-pinned
     * transport. Deliberately narrower than ordinary `drain()`:
     *
     * - the engine is sealed for the whole push, so local CRUD stays refused;
     * - the normal engine transport is never consulted, so a durable sign-out
     *   marker can keep live/target credentials unavailable while Auth supplies
     *   only the captured source bearer to this final outgoing exchange.
     *
     * Replaying after a crash is idempotent: accepted entries were removed by
     * their verdict transaction; unacknowledged entries remain pending and are
     * the only entries selected by the next call.
     */
    public suspend fun sealAndDrain(
        pinnedSourceTransport: ReplicaTransport,
    ): List<ReplicaVerdict> {
        seal()
        if (binding.store == null) return emptyList()

        // `drain()` claims its flight before that task begins its wire operation.
        // If the seal won in that narrow window, let the refused flight release
        // its claim before installing the pinned-source flight below.
        val inFlight = withContext(engineContext) { activeDrains.values.toList() }
        for (flight in inFlight) {
            try {
                awaitDrain(flight)
            } catch (error: ReplicaError.IdentityTransitionRequired) {
                // Sealing refused this flight before wire admission. The
                // pinned flight below takes responsibility for its frozen bytes.
            }
        }
        val flight = withContext(engineContext) {
            if (!sealedFlag || activeWireOperations != 0 || activeDrains.isNotEmpty()) {
                throw ReplicaError.IdentityTransitionRequired
            }
            val started = scope.async {
                performDrain(
                    lane = null,
                    selectedTransport = pinnedSourceTransport,
                    sealedFlush = true
                )
            }
            // The sign-out flush drains EVERY lane (`lane = null` above): the
            // engine is sealed and nothing can race it. What it leaves behind —
            // owed entries, held rows — stays in the owner's file for their
            // next sign-in, the message typed a second ago included.
            activeDrains[ReplicaLane.BULK] = started
            started
        }
        return flight.await()
    }

    internal fun beginWireOperation(): ReplicaStateStore {
        val store = writableStore()
        activeWireOperations += 1
        return store
    }

    internal fun endWireOperation() {
        activeWireOperations = maxOf(0, activeWireOperations - 1)
        if (!sealedFlag || activeWireOperations != 0) return
        val waiters = sealWaiters.toList()
        sealWaiters.clear()
        for (waiter in waiters) waiter.complete(Unit)
    }

    /**
     * Deltas intentionally omitted in persisted projection-only mode.
     * Full replication refuses unsupported or corrupt document checkpoints.
     */
    public var skippedDeltaCount: Int = 0
        private set

    /**
     * The per-doc resync lever: drop the fold and blank its shard's
     * cursor; the next pull re-bootstraps and the doc is reborn from server
     * truth (with a fresh peer). The journal is untouched.
     */
    public suspend fun resyncDocument(stream: String, id: String) {
        withContext(engineContext) {
            val store = writableStore()
            liveDocuments.publishing {
                store.write { db ->
                    store.archiveDocument(db, stream, id, "explicit resync")
                    store.deleteDoc(db, stream, id)
                    store.clearCursor(db, schema.spec(stream)?.shard ?: "user")
                }
                liveDocuments.evict(LiveDocuments.Key(stream, id))
            }
        }
    }

    /**
     * Corrupt-fold recovery: REPLACE an unreadable fold with `fold` under a
     * NEW peer, and blank the shard's cursor so the re-bootstrap merges the
     * server's true history back in.
     *
     * A replacement rather than a drop, because the row must stay WRITABLE:
     * an edit made between the recovery and the next pull has to have
     * somewhere to land. The new peer is persisted here — rotating and then
     * forgetting would walk the next launch straight back into the reused
     * counter range (loro dedups by (peer, counter)); `acked` resets to
     * nothing, so the whole recovered history is owed again.
     */
    public suspend fun rebuildDocument(
        stream: String,
        id: String,
        fold: ByteArray,
        peer: ULong,
    ) {
        withContext(engineContext) {
            val store = writableStore()
            val spec = writableSpec(stream)
            if (spec.lane != ReplicaStreamSpec.Lane.DOCUMENT) throw ReplicaError.LaneMismatch(stream)
            val codecName = codecName(spec)
            val codec = codecs[codecName] ?: throw ReplicaError.Codec("no codec registered for $codecName")
            val replacement = codec.merge(null, fold, spec.reflections)
            val owed = codec.diff(replacement.fold, null)
            liveDocuments.publishing {
                store.write { db ->
                    if (store.doc(db, stream, id)?.peer == peer) {
                        throw ReplicaError.Codec("A rebuilt document requires a fresh authoring peer")
                    }
                    store.archiveDocument(db, stream, id, "explicit rebuild")
                    store.upsertDoc(
                        db, stream, id, spec.shard,
                        codec = codecName, fold = replacement.fold, acked = null, peer = peer
                    )
                    reflect(replacement.reflected, db, stream, id, spec.shard, store)
                    if (!codec.isEmptyDiff(owed)) {
                        val op = ReplicaOp(id = ReplicaID.ulid(), verb = ReplicaOp.Verb.DOC_DELTA,
                            stream = stream, rowId = id, codec = codecName, payload = owed)
                        enqueueOp(db, op, store, null, currentLane())
                    }
                    store.clearCursor(db, spec.shard)
                }
                liveDocuments.evict(LiveDocuments.Key(stream, id))
            }
            schedulePush()
        }
    }

    // MARK: - Owner lifecycle

    /** The owner this process is writing for — null while the engine is closed. */
    public val owner: Long? get() = binding.owner

    /**
     * The bound store — null while the engine is closed. Reads go straight
     * through it; only the engine writes.
     */
    public val store: ReplicaStateStore? get() = binding.store

    /**
     * The raw-query escape hatch (read contract: app code reads, only the
     * engine writes).
     */
    public val database: ReplicaStateStore? get() = binding.store

    /**
     * The byte plane's reference harvest: the string values of the named
     * wire fields across every row of a stream. Feeds the staged-blob
     * reference-watch GC — a staged blob no row and no journal op names is
     * sweepable.
     */
    public fun rowFieldStrings(stream: String, fields: List<String>): Set<String> {
        val store = binding.store ?: return emptySet()
        val harvest = store.materializedRows<List<String>>(
            stream,
            StringListKey::class,
            null
        ) { _, _, data -> fields.mapNotNull { data[it]?.string } }
        return harvest.rows.flatMap { it.model }.toSet()
    }

    private class StringListKey

    internal fun storeURL(owner: Long): File =
        File(home, "replica-$owner$storeSuffix.sqlite")

    /**
     * Open this owner's file — creating it on first sight. Reopening the
     * owner already bound is a no-op, so same-owner reauthentication keeps
     * its world and its live observations.
     */
    public suspend fun open(owner: Long) {
        withContext(engineContext) {
            if (binding.owner == owner) {
                unsealLocked()
                return@withContext
            }
            seal()
            releaseBinding(retiring = false)
            bindStore(storeURL(owner), owner)
            sealedFlag = false
            unsealLocked()
        }
    }

    /**
     * Cold boot: bind the identity the keychain already holds BEFORE the
     * first read, without a dispatcher hop — a returning user's grid renders
     * from disk on the first frame, and waiting on a suspension here would
     * paint them an empty world first.
     *
     * It can only bind from NOTHING. Changing owners is a transition and
     * goes through `open(owner)`, where the in-flight work is quiesced.
     */
    public fun openForColdBoot(owner: Long) {
        binding.bindIfUnbound {
            val path = storeURL(owner)
            home.mkdirs()
            val store = ReplicaStateStore(path.path, schema.indexes)
            prepareStore(store)
            ReplicaBinding.Bound(owner, store, path)
        }
        askHoldsAgain()
    }

    private fun bindStore(path: File, owner: Long) {
        home.mkdirs()
        val store = ReplicaStateStore(path.path, schema.indexes)
        prepareStore(store)
        binding.bind(ReplicaBinding.Bound(owner, store, path))
    }

    private fun prepareStore(store: ReplicaStateStore) {
        try {
            healCursors(store)
            sweepDrafts(store)
        } catch (failure: Throwable) {
            try { store.close() } catch (closing: Throwable) { failure.addSuppressed(closing) }
            throw failure
        }
    }

    /** Where the bound owner's world lives on disk — null while closed. */
    public val storePath: File? get() = binding.current?.path

    /**
     * The store/cursor invariant, checked where the store is opened rather
     * than by whoever remembers to ask: an EMPTY replica holding a warm
     * cursor can never heal — the cursor claims coverage the store does not
     * hold, so tail pulls serve nothing and every row is stranded
     * server-side. Blank the cursors and the next pull re-snapshots. The
     * journal is untouched: it still owes its ops.
     */
    private fun healCursors(store: ReplicaStateStore) {
        store.requireSchema(schema)
        store.requireDocumentMode(documentMode)
    }

    /**
     * Let this owner's world go: quiesce, then close the store. The file
     * stays — a sign-out that keeps the device's world for a later sign-in
     * closes, it does not retire.
     */
    public suspend fun close() {
        seal()
        withContext(engineContext) { releaseBinding(retiring = false) }
    }

    /**
     * A foreign identity took the device: close AND delete. Wipe is the file
     * going away, so there is no wiping pass that could miss a table.
     */
    public suspend fun retire() {
        seal()
        withContext(engineContext) { releaseBinding(retiring = true) }
    }

    private fun releaseBinding(retiring: Boolean) {
        liveDocuments.publishing {
            val released = binding.current ?: return@publishing
            released.store.close()
            binding.unbind()
            quiesceInFlight()
            liveDocuments.evictAll()
            if (retiring) ReplicaStateStore.remove(released.path)
        }
    }

    private fun quiesceInFlight() {
        for (task in scheduledPushes.values) task.cancel()
        scheduledPushes.clear()
        activeDrains.clear()
        coldUntil.clear()
    }

    /** One local transaction on the caller's thread. Its callback cannot suspend. */
    public fun <T> write(body: (ReplicaTransaction) -> T): T {
        ReplicaTransaction.open(this)?.let { return body(it) }
        val admission = admitWrite()
        return try { performWrite(admission, body) } finally { writeGate.leave() }
    }

    /**
     * The same transaction without holding the caller's thread behind the writer.
     * A cancellation cleanup may enter from an already cancelled coroutine: an
     * admitted write still commits and returns its result.
     */
    public suspend fun <T> writeAsync(body: (ReplicaTransaction) -> T): T {
        val admission = admitWrite()
        return try {
            // The outer context changes only the Job, so returning to a cancelled
            // caller does not discard the result after the IO dispatcher completes.
            withContext(NonCancellable) {
                withContext(Dispatchers.IO) { performWrite(admission, body) }
            }
        } finally { writeGate.leave() }
    }

    private data class WriteAdmission(
        val store: ReplicaStateStore,
        val lane: ReplicaLane,
        val draft: String?,
    )

    private fun admitWrite(): WriteAdmission {
        val lane = currentLane()
        val draft = currentDraftLocal.get()
        val session = currentLocalSessionLocal.get()
        if (binding.store == null) throw ReplicaError.NoOwner
        writeGate.enter()
        return try {
            session?.let(::requireLocalSession)
            WriteAdmission(binding.store ?: throw ReplicaError.NoOwner, lane, draft)
        } catch (error: Throwable) {
            writeGate.leave()
            throw error
        }
    }

    private fun <T> performWrite(admission: WriteAdmission, body: (ReplicaTransaction) -> T): T {
        var journaled = false
        val value = admission.store.write { db ->
            val tx = ReplicaTransaction(this, db, admission.store, admission.lane, admission.draft)
            val result = ReplicaTransaction.running(tx) { body(tx) }
            journaled = tx.journaled
            result
        }
        if (journaled) scope.launch { schedulePush() }
        return value
    }

    /** The store, or the closed world's refusal. Every write verb enters here. */
    internal fun writableStore(): ReplicaStateStore {
        currentLocalSessionLocal.get()?.let(::requireLocalSession)
        val store = binding.store ?: throw ReplicaError.NoOwner
        if (sealedFlag) throw ReplicaError.IdentityTransitionInProgress
        return store
    }

    // MARK: - Command import / drafts (ReplicaEngine.swift)

    private val commitIdentity = java.util.UUID.randomUUID()

    public suspend fun captureLocalSession(): ReplicaLocalSession = withContext(engineContext) {
        captureLocalSessionNow()
    }

    /** Capture before an owned queue drops the caller's coroutine/session context. */
    public fun captureLocalSessionNow(): ReplicaLocalSession = liveDocuments.publishing {
        writableStore()
        ReplicaLocalSession(commitIdentity, binding.snapshot().second)
    }

    /** Identity only: an admitted write may finish under a seal, never in a replacement store. */
    public fun isCurrentLocalSession(session: ReplicaLocalSession): Boolean =
        session.engine == commitIdentity && session.binding == binding.snapshot().second

    /**
     * Keep inference/tool work attached to its captured world. Every local write revalidates
     * after any suspension. The scope itself is not a transaction; write/writeAsync
     * capture this admission before entering the transaction writer.
     */
    public suspend fun <T> withLocalSession(session: ReplicaLocalSession, body: suspend () -> T): T {
        withContext(engineContext) { requireLocalSession(session); writableStore() }
        return withContext(currentLocalSessionLocal.asContextElement(session)) { body() }
    }

    internal fun requireLocalSession(session: ReplicaLocalSession) {
        if (!isCurrentLocalSession(session)) throw ReplicaError.StaleLocalSession
    }

    public suspend fun commitSession(): ReplicaCommitSession = withContext(engineContext) {
        writableStore()
        ReplicaCommitSession(commitIdentity, binding.snapshot().second)
    }

    /** Command results arrive through the same pull rounds as ordinary sync. */
    public suspend fun apply(commit: String, session: ReplicaCommitSession) = withContext(engineContext) {
        val store = beginWireOperation()
        try {
            if (session.engine != commitIdentity || session.binding != binding.snapshot().second) throw ReplicaError.StaleCommit
            val decoded = ReplicaCommit(commit, schema, store.read { store.meta(it).dataset })
            pullUntilCaughtUp(decoded.shards)
            writableStore()
            Unit
        } finally {
            endWireOperation()
        }
    }

    public suspend fun <T> beginDraft(body: suspend () -> T): ReplicaDraftResult<T> {
        val draft = ReplicaDraft(ReplicaID.ulid())
        val value = withContext(currentDraftLocal.asContextElement(draft.key)) { body() }
        return ReplicaDraftResult(draft, value)
    }

    /**
     * Idempotent; an unknown key is silence. The draft's writes meet the
     * sync gates now: a row a gate holds moves off the journal into `holds`.
     */
    public suspend fun commitDraft(draft: ReplicaDraft) = withContext(engineContext) {
        val store = writableStore()
        store.write { db ->
            for (entry in store.draftEntries(db, draft.key)) {
                val op = entry.op()
                if (!admit(db, op, entry.preimage, applied = true, store = store)) store.discard(db, entry.id)
            }
            store.releaseDraft(db, draft.key)
        }
        schedulePush()
    }

    public suspend fun discardDraft(draft: ReplicaDraft) = withContext(engineContext) {
        val store = writableStore()
        liveDocuments.publishing {
            val documents = mutableListOf<LiveDocuments.Key>()
            store.write { db ->
                for ((stream, id) in store.draftAddresses(db, draft.key)) {
                    store.deleteSnapshot(db, stream, id)
                    if (schema.lane(stream) == ReplicaStreamSpec.Lane.DOCUMENT) {
                        store.deleteDoc(db, stream, id)
                        documents += LiveDocuments.Key(stream, id)
                    }
                }
                store.dropDraftEntries(db, draft.key)
            }
            for (key in documents) liveDocuments.evict(key)
        }
    }

    private fun sweepDrafts(store: ReplicaStateStore) {
        store.write { db ->
            for (key in store.draftKeys(db)) {
                for ((stream, id) in store.draftAddresses(db, key)) {
                    store.archiveEntity(db, stream, id, "Uncommitted draft recovered after restart")
                    store.deleteSnapshot(db, stream, id)
                    if (schema.lane(stream) == ReplicaStreamSpec.Lane.DOCUMENT) store.deleteDoc(db, stream, id)
                }
                store.dropDraftEntries(db, key)
                Log.logger.info("[open] swept a draft that outlived its process ($key)")
            }
        }
    }

    // MARK: - Pull

    /**
     * One page past the held cursor, behind the drain barrier — the echo of
     * anything owed is in the answer.
     */
    public suspend fun pullOnce(shard: String = "user"): Int {
        if (withContext(engineContext) { binding.store == null || sealedFlag }) return 0
        drainIfWarm()
        return pullPage(shard).first
    }

    /**
     * The named shards (every shard by default) until the server reports
     * nothing further waiting. Warm-up, foreground and reconnect walk every
     * shard; the doorbell asks for the one it rang for — the server rings
     * for the user shard only, so a full walk per ring was a wasted catalog
     * round-trip each time.
     */
    public suspend fun pullUntilCaughtUp(shards: List<String>? = null): Int {
        if (withContext(engineContext) { binding.store == null || sealedFlag }) return 0
        drainIfWarm()
        var total = 0
        for (shard in shards ?: schema.shards) {
            while (true) {
                val page = pullPage(shard)
                total += page.first
                if (!page.second) break
            }
        }
        return total
    }

    public suspend fun currentCursor(shard: String = "user"): String? =
        withContext(engineContext) {
            val store = binding.store ?: return@withContext null
            store.read { store.cursor(it, shard) }
        }

    /**
     * Blow away every shard's read position — the "rebuild the replica"
     * lever; the next pull re-snapshots. Safe BECAUSE the journal survives.
     */
    public suspend fun resetCursors() {
        withContext(engineContext) {
            val store = writableStore()
            store.write { db ->
                for (shard in schema.shards) store.clearCursor(db, shard)
            }
        }
    }

    private suspend fun pullPage(shard: String): Pair<Int, Boolean> = withContext(engineContext) {
        val existing = activePulls[shard]
        if (existing != null) return@withContext awaitPull(existing)
        val flight = scope.async {
            try {
                val store = beginWireOperation()
                try {
                    downloadPage(shard, store, transport)
                } finally {
                    endWireOperation()
                }
            } finally {
                activePulls.remove(shard)
            }
        }
        activePulls[shard] = flight
        awaitPull(flight)
    }

    private suspend fun awaitPull(flight: Deferred<Pair<Int, Boolean>>): Pair<Int, Boolean> {
        try {
            return flight.await()
        } catch (error: CancellationException) {
            // Stop the shared network flight when its caller is cancelled.
            // Every complete staged page remains available for the next pull.
            flight.cancel(error)
            throw error
        }
    }

    /**
     * A row a gate holds keeps what the device wrote — it leaves as that
     * state, whole — and takes from a pulled frame only what the server alone
     * writes: the fields a device may not send. A held row has no journal
     * entry to replay over the frame.
     */
    // MARK: - Row lane (local writes)

    /**
     * The generated `create()`: the row must be ABSENT — a present row
     * answers `RowExists` and nothing is written. Journals `row.create`
     * (all writable fields) plus the complete client snapshot in one
     * transaction. Snapshot-only fields never enter the journal.
     */
    internal suspend fun createRow(
        stream: String,
        id: String,
        type: String?,
        data: Map<String, ReplicaValue>,
        snapshot: Map<String, ReplicaValue>? = null,
    ) {
        writeRow(stream, id, type, data, RowWriteExpectation.ABSENT, snapshot)
    }

    /**
     * The generated `update()`: the row must be PRESENT — a missing row
     * answers `UnknownRow` and nothing is written. Diffs against
     * last-known ⇒ `row.patch` of the changed fields ONLY; an unchanged
     * update owes nothing.
     */
    internal suspend fun updateRow(
        stream: String,
        id: String,
        type: String?,
        data: Map<String, ReplicaValue>,
    ) {
        writeRow(stream, id, type, data, RowWriteExpectation.PRESENT)
    }

    /**
     * The engine's own upsert — absent ⇒ `row.create`, present ⇒ diff
     * into `row.patch`, unchanged ⇒ nothing. Not a client door: the app
     * speaks `create`/`update` ("it was not there" must never be silent);
     * this is the package tests' seeding primitive.
     */
    internal suspend fun saveRow(
        stream: String,
        id: String,
        type: String?,
        data: Map<String, ReplicaValue>,
    ) {
        writeRow(stream, id, type, data, RowWriteExpectation.ANY)
    }

    private suspend fun writeRow(
        stream: String,
        id: String,
        type: String?,
        data: Map<String, ReplicaValue>,
        expectation: RowWriteExpectation,
        snapshot: Map<String, ReplicaValue>? = null,
    ) {
        writeAsync { tx -> tx.writeRaw(stream, id, type, data, expectation, snapshot) }
    }

    /** What a row write asserts about the row it addresses. */
    internal enum class RowWriteExpectation {
        ABSENT,
        PRESENT,
        ANY;

        internal fun violation(stream: String, id: String, present: Boolean): ReplicaError? =
            when {
                this == ABSENT && present -> ReplicaError.RowExists(stream, id)
                this == PRESENT && !present -> ReplicaError.UnknownRow(stream, id)
                else -> null
            }
    }

    /**
     * One row write inside an OPEN transaction — the shared body of
     * transactions and internal fixture helpers. A no-change diff is absorbed (returns
     * false, nothing journaled).
     */
    internal fun applyRowWrite(
        db: SQLiteConnection,
        store: ReplicaStateStore,
        spec: ReplicaStreamSpec,
        stream: String,
        id: String,
        type: String?,
        data: Map<String, ReplicaValue>,
        existing: ReplicaStateStore.SnapshotRow?,
        lane: ReplicaLane,
        draft: String? = currentDraftLocal.get(),
        snapshot: Map<String, ReplicaValue>? = null,
    ): Boolean {
        validateAtomicAddress(db, stream, id, store)
        if (existing != null) {
            // A missing row field and an explicit JSON null are the same
            // nullable-column value. Generated models encode authored nil
            // as `Null` so nonnil → nil remains observable, while this
            // normalization keeps already-empty optionals out of patches.
            val changed = data.filter { (key, value) ->
                (existing.data[key] ?: ReplicaValue.Null) != value
            }.toMutableMap()
            if (changed.isEmpty()) return false
            for (field in spec.preconditions) {
                if (field !in changed) existing.data[field]?.let { changed[field] = it }
            }
            val op = ReplicaOp(
                id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_PATCH,
                stream = stream, rowId = id, data = changed
            )
            val prior = LinkedHashMap<String, ReplicaValue>()
            val missing = mutableListOf<String>()
            for (key in changed.keys) {
                val value = existing.data[key]
                if (value != null) prior[key] = value else missing.add(key)
            }
            val preimage = ReplicaPreimage.Fields(prior, missing.sorted())
            enqueueOp(db, op, store, preimage.encoded(), lane, draft)
            val merged = existing.data + changed
            store.upsertSnapshot(db, stream, id, spec.shard, existing.type ?: type, merged)
        } else {
            val op = ReplicaOp(
                id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_CREATE,
                stream = stream, rowId = id, type = type, data = data
            )
            enqueueOp(db, op, store, ReplicaPreimage.Absent.encoded(), lane, draft)
            // The journal carries only authored fields. The local birth keeps
            // required server-owned values supplied by the generated model.
            store.upsertSnapshot(db, stream, id, spec.shard, type, snapshot.orEmpty() + data)
        }
        return true
    }

    /**
     * The generated `delete()`, both lanes. A row the server never heard of
     * (its create still pending) dies silently — every owed entry discarded,
     * no delete op; anything else journals `row.delete`. Document lane also
     * drops the fold and its superseded delta.
     */
    public suspend fun deleteRow(stream: String, id: String): Boolean {
        val lane = currentLane()
        return withContext(engineContext) {
            val store = writableStore()
            val spec = writableSpec(stream)
            val queuedDelete = liveDocuments.publishing {
                val queued = store.write { db ->
                    applyRowDelete(db, store, spec, stream, id, lane)
                }
                if (spec.lane == ReplicaStreamSpec.Lane.DOCUMENT) {
                    liveDocuments.evict(LiveDocuments.Key(stream, id))
                }
                queued
            }
            if (queuedDelete) schedulePush()
            queuedDelete
        }
    }

    internal fun applyRowDelete(db: SQLiteConnection, store: ReplicaStateStore, spec: ReplicaStreamSpec,
                               stream: String, id: String, lane: ReplicaLane, draft: String? = currentDraftLocal.get()): Boolean {
        validateAtomicAddress(db, stream, id, store)
        val incarnation = store.incarnation(db, stream, id)
        val births = store.entriesAddressing(db, stream, id, ReplicaOp.Verb.ROW_CREATE)
            .filter { it.op().incarnation == incarnation }
        // A lost answer may hide a committed birth; a refusal is definite.
        val heardBirth = births.firstOrNull { it.sent }
        val displaced = store.snapshot(db, stream, id)
        val heldDocument = if (spec.lane == ReplicaStreamSpec.Lane.DOCUMENT) {
            store.doc(db, stream, id)
        } else {
            null
        }

        // Deleting a value that never existed is ordinary CRUD silence.
        if (displaced == null && heldDocument == null && births.isEmpty()) {
            return false
        }

        store.deleteSnapshot(db, stream, id)
        if (spec.lane == ReplicaStreamSpec.Lane.DOCUMENT) {
            store.deleteDoc(db, stream, id)
        }

        if (heardBirth != null) {
            // The create may already commit server-side. Keep it ordered
            // ahead of the delete, but drop every now-irrelevant patch or
            // delta addressed at the value.
            store.discardLifetime(db, stream, id, incarnation, except = heardBirth.id)
        } else if (births.isNotEmpty()) {
            // No transport owns the birth: the server cannot have heard it,
            // so create + dependent work collapse to nothing.
            store.discardLifetime(db, stream, id, incarnation)
            store.cancelUnsentBirth(db, stream, id)
            return false
        } else if (spec.lane == ReplicaStreamSpec.Lane.DOCUMENT) {
            store.discardLifetime(db, stream, id, incarnation)
        }

        val op = ReplicaOp(
            id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_DELETE,
            stream = stream, rowId = id
        )
        val preimage = displaced?.let {
            ReplicaPreimage.Row(spec.shard, it.type, it.data)
        }
        enqueueOp(db, op, store, preimage?.encoded(), lane, draft)
        return true
    }

    // MARK: - Document lane (local writes)

    /**
     * Birth the document: fold = seed, acked = nothing (the server knows
     * nothing until the verdict), and the journaled `row.create` carrying
     * codec + seed. `peer` is the seed's authoring peer — recorded so the
     * app can keep authoring under it.
     */
    public suspend fun createDoc(
        stream: String,
        id: String,
        seed: ByteArray,
        peer: ULong,
        data: Map<String, ReplicaValue> = emptyMap(),
    ): Boolean {
        val lane = currentLane()
        return withContext(engineContext) {
            val store = writableStore()
            val spec = writableSpec(stream)
            if (spec.lane != ReplicaStreamSpec.Lane.DOCUMENT) {
                throw ReplicaError.LaneMismatch(stream)
            }
            val codecName = codecName(spec)
            val rowData = stamped(data, spec.stamp, binding.owner).toMutableMap()
            val codec = codecs[codecName] ?: throw ReplicaError.Codec("no codec registered for $codecName")
            rowData.putAll(codec.merge(null, seed, spec.reflections).reflected)
            val inserted = store.write { db ->
                val existingBirths =
                    store.entriesAddressing(db, stream, id, ReplicaOp.Verb.ROW_CREATE)
                if (store.snapshot(db, stream, id) != null ||
                    store.doc(db, stream, id) != null ||
                    existingBirths.isNotEmpty()
                ) {
                    return@write false
                }

                val op = ReplicaOp(
                    id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_CREATE,
                    stream = stream, rowId = id, codec = codecName, seed = seed
                )
                enqueueOp(db, op, store, ReplicaPreimage.Absent.encoded(), lane)
                store.upsertSnapshot(db, stream, id, spec.shard, null, rowData)
                store.upsertDoc(
                    db, stream, id, spec.shard,
                    codec = codecName, fold = seed, acked = null, peer = peer
                )
                true
            }
            if (inserted) schedulePush()
            inserted
        }
    }

    /**
     * A local edit: merged into the fold, then SUPERSEDED into the one
     * unsent `doc.delta` per document — the intent's payload is always
     * `diff(fold, since = acked)`, so consecutive edits fold into a single
     * op that keeps its id and queue position. A frozen or refused delta is
     * never rewritten: the next edit owes a new one.
     */
    public suspend fun recordDocDelta(stream: String, id: String, payload: ByteArray) {
        val lane = currentLane()
        withContext(engineContext) { recordDocDeltaInScope(stream, id, payload, lane) }
    }

    /** Caller owns the engine context; safe inside the live-document publication lock. */
    internal fun recordDocDeltaInScope(stream: String, id: String, payload: ByteArray, lane: ReplicaLane) {
        val store = writableStore()
        val spec = writableSpec(stream)
        if (spec.lane != ReplicaStreamSpec.Lane.DOCUMENT) throw ReplicaError.LaneMismatch(stream)
        store.write { db ->
            val doc = store.doc(db, stream, id) ?: throw ReplicaError.UnknownDocument(stream, id)
            val codec = codecs[doc.codec] ?: throw ReplicaError.Codec("no codec registered for ${doc.codec}")
            val merged = codec.merge(doc.fold, payload, spec.reflections)
            store.updateDoc(db, stream, id, fold = merged.fold)
            val touched = spec.stamp?.updatedAt?.let { mapOf(it to ReplicaValue.Str(ISO_INSTANT.format(clock()))) }.orEmpty()
            reflect(merged.reflected + touched, db, stream, id, spec.shard, store)
            val owed = codec.diff(merged.fold, doc.acked)
            if (codec.isEmptyDiff(owed)) {
                store.discardUnsentDeltas(db, stream, id)
            } else {
                val op = ReplicaOp(id = ReplicaID.ulid(), verb = ReplicaOp.Verb.DOC_DELTA, stream = stream,
                    rowId = id, codec = doc.codec, payload = owed)
                enqueueOp(db, op, store, null, lane)
            }
        }
        schedulePush()
    }

    internal fun reflect(reflected: Map<String, ReplicaValue>, db: SQLiteConnection, stream: String,
                        id: String, shard: String, store: ReplicaStateStore) {
        if (reflected.isEmpty()) return
        val row = store.snapshot(db, stream, id) ?: return
        val data = row.data + reflected
        if (data != row.data) store.upsertSnapshot(db, stream, id, shard, row.type, data)
    }


    private fun stamped(
        data: Map<String, ReplicaValue>,
        stamp: ReplicaStamp?,
        userId: Long?,
    ): Map<String, ReplicaValue> {
        if (stamp == null || userId == null) return data

        val result = LinkedHashMap(data)
        val timestamp = ISO_INSTANT.format(clock())
        stamp.userId?.let { result[it] = ReplicaValue.signedInteger(userId) }
        stamp.createdAt?.let { result[it] = ReplicaValue.Str(timestamp) }
        stamp.updatedAt?.let { result[it] = ReplicaValue.Str(timestamp) }
        return result
    }

    /**
     * A local op: judged by the sync gates (`admit`), then journaled — or
     * held on the device, or dropped.
     */
    private fun enqueueOp(
        db: SQLiteConnection,
        operation: ReplicaOp,
        store: ReplicaStateStore,
        preimage: ByteArray?,
        requested: ReplicaLane,
        draft: String? = currentDraftLocal.get(),
    ): ReplicaLane {
        val op = store.identify(db, operation, schema,
            birth = operation.verb == ReplicaOp.Verb.ROW_CREATE, preimage = preimage)
        ReplicaTransaction.open(this)?.atomicEntries?.let { captured ->
            validateAtomicAdmission(db, op, preimage, store)
            val lane = journal(db, op, store, preimage, requested, null)
            captured += op.id
            return lane
        }
        val key = draft ?: draftKey(db, op, store)
        // A draft's writes are judged when it commits.
        if (key == null && !admit(db, op, preimage, applied = false, store = store)) return requested
        return journal(db, op, store, preimage, requested, key)
    }

    /**
     * The journal write half of a local op: claim the lane, keep the two
     * invariants that make overtaking safe, then enqueue.
     *
     *   STICKINESS — a row's later ops join the lane its pending ops are on.
     *     Otherwise a bulk patch passes the interactive create of its own
     *     row and the server refuses an update to a row it has never seen.
     *   PROMOTION — an interactive op naming a pending bulk row pulls that
     *     row (and what IT names, transitively) onto the interactive lane.
     *     Otherwise attaching a clip whose element create is still queued
     *     refuses with "unknown element", the entry parks, and the local row
     *     reverts. Ids are unique minted strings, so matching op data values
     *     against pending row ids needs no foreign-key schema; a false match
     *     costs one row shipping sooner.
     */
    private fun journal(
        db: SQLiteConnection,
        operation: ReplicaOp,
        store: ReplicaStateStore,
        preimage: ByteArray?,
        requested: ReplicaLane,
        key: String?,
    ): ReplicaLane {
        val identified = if (operation.incarnation == null) {
            store.identify(db, operation, schema, preimage = preimage)
        } else operation
        val superseded = if (identified.verb == ReplicaOp.Verb.DOC_DELTA) {
            store.unsentDelta(db, identified.stream, identified.rowId, key)
        } else null
        val op = superseded?.let { identified.copy(id = it) } ?: identified
        val queued = store.pendingEntries(db, op.stream, op.rowId)
        var lane = queued.firstOrNull()?.lane ?: requested
        var alsoWalk: List<ReplicaOp> = emptyList()
        if (requested == ReplicaLane.INTERACTIVE && lane == ReplicaLane.BULK) {
            store.promote(db, queued.map { it.id })
            lane = ReplicaLane.INTERACTIVE
            // Those entries were pulled up by their ROW, so nothing has walked
            // what THEY name yet.
            alsoWalk = queued.map { entry -> ReplicaOp.fromValue(ReplicaProtocol.decode(entry.payload)) }
        }
        val payload = ReplicaJSON.encodeToBytes(op.toValue())
        if (superseded == null) {
            store.enqueue(db, op.id, op.verb, op.stream, op.rowId, payload, preimage, lane, key)
        } else {
            store.supersede(db, superseded, payload, preimage, lane)
        }
        if (lane == ReplicaLane.INTERACTIVE) {
            promoteDependencies(db, listOf(op) + alsoWalk, store)
        }
        return lane
    }

    private fun draftKey(db: SQLiteConnection, op: ReplicaOp, store: ReplicaStateStore): String? {
        val named = mutableSetOf(op.rowId)
        for (value in (op.data ?: emptyMap()).values) namedIds(value, named)
        return store.draftKey(db, named.toList())
    }

    /**
     * Every id a value mentions, at any depth — a row reference can sit
     * inside an array or a nested document (`Cook.data`), not only in a
     * top-level string column.
     */
    private fun namedIds(value: ReplicaValue, found: MutableSet<String>) {
        when (value) {
            is ReplicaValue.Str -> found.add(value.value)
            is ReplicaValue.Arr -> for (item in value.values) namedIds(item, found)
            is ReplicaValue.Obj -> for (field in value.fields.values) namedIds(field, found)
            else -> Unit
        }
    }

    private fun promoteDependencies(
        db: SQLiteConnection,
        ops: List<ReplicaOp>,
        store: ReplicaStateStore,
    ) {
        val frontier = ops.toMutableList()
        val promoted = mutableListOf<String>()
        val visited = mutableSetOf<String>()
        while (frontier.isNotEmpty()) {
            val current = frontier.removeAt(frontier.size - 1)
            val named = mutableSetOf<String>()
            for (value in (current.data ?: emptyMap()).values) namedIds(value, named)
            val fresh = named - visited
            if (fresh.isEmpty()) continue
            visited.addAll(fresh)
            for (entry in store.pendingBulkEntries(db, fresh.toList())) {
                // An entry whose bytes will not decode is the drain's problem
                // to park, never a reason to fail the write the user just made.
                val entryOp = entry.op()
                promoted.add(entry.id)
                // Transitive: what the dependency itself names must come with
                // it, or the chain breaks one link further down.
                frontier.add(entryOp)
            }
        }
        store.promote(db, promoted)
    }

    /**
     * Something was written — kick whichever lanes now owe work. The entry
     * may have landed on a lane the caller did NOT request (stickiness joins
     * its row's lane; promotion pulls dependencies up), so the caller's own
     * lane is not a reliable answer to "what needs draining".
     */
    /**
     * Every write's kick, counted — `pushScheduledWrites` reads it to tell a
     * write that arrived behind its back from the leftovers its own drain
     * could not send.
     */
    private var journalWrites = 0L

    private fun schedulePush() {
        journalWrites += 1
        if (!automaticallyPushWrites || sealedFlag) return
        val store = binding.store ?: return
        if (scheduledPushes.size >= ReplicaLane.allCases.size) return
        val owed = try {
            store.read { db -> store.lanesOwed(db) }
        } catch (error: Exception) {
            // The local write already committed. Report scheduling failure while
            // retaining its journal for the next delivery attempt.
            health.record(error, "schedule automatic delivery")
            Log.logger.error("[push] could not read the owed lanes — no push scheduled: $error")
            return
        }
        for (lane in owed) schedulePush(lane)
    }

    private fun schedulePush(lane: ReplicaLane) {
        if (!automaticallyPushWrites || sealedFlag) return
        if (binding.store == null) return
        if (scheduledPushes[lane] != null) return
        scheduledPushes[lane] = scope.launch {
            yield()
            if (!isActive) return@launch
            pushScheduledWrites(lane)
        }
    }

    private suspend fun pushScheduledWrites(lane: ReplicaLane) {
        var transportFailed = false
        var seenWrites = journalWrites
        while (!isCold(lane) && !sealedFlag && binding.store != null) {
            seenWrites = journalWrites
            val hasWork = try {
                hasPendingWork(lane)
            } catch (error: Exception) {
                // Background work has no awaiter. Keep its queue and expose the error.
                health.record(error, "read automatic delivery queue")
                Log.logger.error(
                    "[push] journal read failed — scheduled push exits, entries wait for the " +
                        "next lifecycle event: ${error.message}"
                )
                break
            }
            if (!hasWork) break

            val delivered = try {
                drain(lane)
            } catch (error: Exception) {
                // Delivery remains retryable through the frozen outbox. A health
                // failure is visible even when no explicit drain was awaiting it.
                health.record(error, "automatic push")
                Log.logger.error(
                    "[push] scheduled drain failed — pending entries stay for the next " +
                        "lifecycle: ${error.message}"
                )
                transportFailed = true
                break
            }
            // A drain that delivered nothing has nothing it can send now — its
            // lane's entries are on the other lane's wire. Re-draining here spun
            // the engine flat out (D6). A write that arrived meanwhile is the one
            // thing that changes the answer, and it is counted.
            if (delivered.isEmpty() && journalWrites == seenWrites) break
        }

        // Clear ownership before the trailing check. A write that interleaves
        // here schedules its own task; otherwise this task catches the narrow
        // "arrived after our empty read" race itself.
        scheduledPushes.remove(lane)
        if (transportFailed || isCold(lane) || sealedFlag || binding.store == null) return
        try {
            if (journalWrites != seenWrites && hasPendingWork(lane)) schedulePush(lane)
        } catch (error: Exception) {
            // This final scheduling check follows a completed delivery. The
            // remaining queue stays durable and the failure is exposed to health.
            health.record(error, "read trailing delivery queue")
            Log.logger.error(
                "[push] trailing journal read failed — entries wait for the next lifecycle " +
                    "event: ${error.message}"
            )
        }
    }

    private fun hasPendingWork(lane: ReplicaLane): Boolean {
        val store = binding.store ?: return false
        return store.read { db -> store.owesWork(db, lane) }
    }

    private fun isCold(lane: ReplicaLane): Boolean =
        coldUntil[lane]?.hasNotPassedNow() ?: false

    internal suspend fun isColdForTesting(lane: ReplicaLane): Boolean =
        withContext(engineContext) { isCold(lane) }

    /**
     * Synchronous on purpose (store reads never touch engine state): the doc
     * plane loads folds without a hop.
     */
    public fun docFold(stream: String, id: String): ByteArray? {
        val store = binding.store ?: return null
        return store.read { store.doc(it, stream, id)?.fold }
    }

    public fun docPeer(stream: String, id: String): ULong? {
        val store = binding.store ?: return null
        return store.read { store.doc(it, stream, id)?.peer }
    }

    // MARK: - Journal / drain

    /**
     * The whole pending queue through the transport in order, verdicts
     * applied to the frozen intents by operation id — an edit made
     * mid-flight owes its own intent and waits for its own drain. Transport
     * failure marks the wire cold, applies nothing, and leaves every entry
     * pending (retryable).
     * Both lanes, interactive first — the compatibility path (lifecycle
     * wake-ups, tests, the identity fence) where "everything owed" is meant.
     */
    public suspend fun drain(): List<ReplicaVerdict> {
        if (binding.store == null) return emptyList()
        val verdicts = drain(ReplicaLane.INTERACTIVE).toMutableList()
        verdicts += drain(ReplicaLane.BULK)
        return verdicts
    }

    /**
     * One lane's queue. Single-flight PER LANE: a bulk push already on the
     * wire does not hold this one, which is the entire point.
     */
    public suspend fun drain(lane: ReplicaLane): List<ReplicaVerdict> = withContext(engineContext) {
        if (binding.store == null) return@withContext emptyList()
        if (sealedFlag) throw ReplicaError.IdentityTransitionInProgress

        // Frozen submissions leave oldest first, one answer at a time, whatever
        // their lane. Finish every flight before selecting another lane's
        // intents; never overtake a retry.
        val joined = mutableListOf<ReplicaVerdict>()
        while (activeDrains.isNotEmpty()) {
            val (activeLane, active) = activeDrains.entries.first()
            val verdicts = awaitDrain(active)
            if (activeLane == lane) joined += verdicts
            if (sealedFlag) throw ReplicaError.IdentityTransitionInProgress
        }

        val flight = scope.async { performDrain(lane) }
        activeDrains[lane] = flight
        joined + awaitDrain(flight)
    }

    private suspend fun awaitDrain(flight: Deferred<List<ReplicaVerdict>>): List<ReplicaVerdict> {
        try {
            return flight.await()
        } catch (error: CancellationException) {
            // Cancellation stops the request, never its durable frozen bytes.
            flight.cancel(error)
            throw error
        }
    }

    /**
     * `lane = null` means EVERY lane — the sign-out flush: whatever the journal
     * holds, before the file closes; what stays drains at the next sign-in.
     */
    private suspend fun performDrain(
        lane: ReplicaLane? = ReplicaLane.BULK,
        selectedTransport: ReplicaTransport? = null,
        sealedFlush: Boolean = false,
    ): List<ReplicaVerdict> {
        val store: ReplicaStateStore
        try {
            if (sealedFlush) {
                val bound = binding.store
                if (!sealedFlag || activeWireOperations != 0 || bound == null) {
                    throw ReplicaError.IdentityTransitionRequired
                }
                store = bound
                activeWireOperations += 1
            } else {
                store = beginWireOperation()
            }
        } catch (error: Throwable) {
            // `drain()` claimed the single-flight task before suspending. If a
            // seal won the engine between that claim and this task's first
            // turn, release the claim as well as refusing the wire.
            activeDrains.remove(lane ?: ReplicaLane.BULK)
            throw error
        }
        try {
            val verdicts = transmitSubmissions(store, lane, selectedTransport ?: transport)
            coldUntil.clear()
            return verdicts
        } catch (error: Exception) {
            coldUntil.clear()
            if (error is ReplicaError.Transport) {
                val until = TimeSource.Monotonic.markNow() + coldWindow
                for (priority in ReplicaLane.entries) coldUntil[priority] = until
            }
            throw error
        } finally {
            activeDrains.remove(lane ?: ReplicaLane.BULK)
            endWireOperation()
        }
    }

    /**
     * The barrier variant: skips while the wire is known-cold — offline
     * must not stack timeouts. Network errors reach health; storage, protocol
     * and cancellation failures propagate. Explicit drains never skip.
     */
    public suspend fun drainIfWarm() {
        // Both priorities share the frozen submissions. Once they fail to
        // leave, this barrier must not retry them through another lane.
        for (lane in ReplicaLane.allCases) {
            if (withContext(engineContext) { isCold(lane) }) continue
            try {
                drain(lane)
            } catch (error: Exception) {
                if (error !is ReplicaError.Transport) throw error
                // Receiving remote changes is independent of an unavailable
                // upload connection. Local storage and protocol errors propagate.
                health.record(error, "push before pull")
                Log.logger.warning("[drain] warm drain of $lane failed: $error")
                return
            }
        }
    }

    // MARK: - Sync gate

    /**
     * The GC read seam: string values of `fields` across every journal
     * entry (pending AND parked) of `stream`. With rows' own fields — held
     * rows among them — this is the app's provable keeping-set for staged
     * bytes.
     */
    public suspend fun pendingFieldStrings(stream: String, fields: List<String>): List<String> =
        withContext(engineContext) {
            val store = binding.store ?: return@withContext emptyList()
            val rows = store.read { db -> store.entriesForStream(db, stream) }
            rows.flatMap { entry ->
                val data = entry.op().data ?: return@flatMap emptyList()
                fields.mapNotNull { data[it]?.string }.filter { it.isNotEmpty() }
            }
        }

    /**
     * Row ids on `stream` the device still owes: any journal entry (pending
     * or parked), or a gate's hold — the row half of the app's keeping-set.
     */
    public suspend fun pendingRowIds(stream: String): List<String> = withContext(engineContext) {
        val store = binding.store ?: return@withContext emptyList()
        store.read { db ->
            store.entriesForStream(db, stream).map { it.op().rowId } + store.heldRowIds(db, stream)
        }
    }

    /** Every row a gate holds, in the order the holds began. */
    public fun heldRows(): List<ReplicaStateStore.GateHold> {
        val store = binding.store ?: return emptyList()
        return store.read { db -> store.holds(db, gateId = null) }
    }

    /**
     * Let held rows go unsent — the discard path: a project thrown away
     * before the server heard of it must not come back when its gate opens.
     * A row waiting behind one goes with it: it could only be refused.
     */
    public suspend fun discardHolds(stream: String, rowIds: List<String>) {
        withContext(engineContext) {
            val store = writableStore()
            store.write { db ->
                val dropping = rowIds.map { stream to it }.toMutableList()
                while (dropping.isNotEmpty()) {
                    val (heldStream, rowId) = dropping.removeAt(dropping.size - 1)
                    store.dropHold(db, heldStream, rowId)
                    dropping += store.holds(db, gateId = waitGate(heldStream, rowId)).map { it.stream to it.rowId }
                }
            }
        }
    }

    /** Test seam: what an open does over a store bound by the test constructor. */
    internal suspend fun settle() = refreshSyncGates()

    /** Explicit reevaluation propagates failure and leaves durable holds intact. */
    public suspend fun refreshSyncGates(id: String? = null) {
        withContext(engineContext) { refreshGates(id) }
    }

    private fun refreshGates(id: String?) {
        val store = writableStore()
        val gate = id?.let { gateId -> syncGates.all.firstOrNull { it.id == gateId } }
        val event = id?.let { "$it changed" } ?: "refresh"
        store.write { db ->
            val asked = store.holds(db, gateId = id)
            if (gate != null) holdJournal(db, gate, store)
            releaseHolds(db, asked, event, store)
        }
        schedulePush()
    }

    /**
     * A gate said its answer may have moved — or the store opened (null). A
     * gate that can now hold what it let go takes those rows off the
     * journal; the holds it had are asked again.
     */
    private fun gateChanged(id: String?) {
        if (binding.store == null || sealedFlag) return
        try {
            refreshGates(id)
        } catch (error: Exception) {
            // A gate notification cannot return an error to its sender. Its
            // transaction rolled back; report the failure with the holds intact.
            health.record(error, "reevaluate synchronization gates")
            Log.logger.error("[gate] reevaluation failed; holds remain for retry: $error")
        }
    }

    /**
     * At open, off the opener's path: every hold is asked again — a gate's
     * answer may have moved while the process was gone.
     */
    private fun askHoldsAgain() {
        if (syncGates.isEmpty) return
        scope.launch { gateChanged(null) }
    }

    private fun releaseHolds(db: SQLiteConnection, holds: List<ReplicaStateStore.GateHold>, event: String, store: ReplicaStateStore) {
        if (holds.isEmpty()) return
        var released = 0
        for (hold in holds) if (rejudge(db, hold, store)) released += 1
        Log.logger.info("[gate] $event: released $released, holds ${holds.size - released}")
    }

    /**
     * The gate half of a local op, inside its write transaction: whether the
     * op joins the journal at all.
     * - a held row journals nothing: its state is judged again — with the
     *   op, when the row does not carry it yet (`applied`) — and leaves as
     *   that state when its gates let it go;
     * - a write naming a held row waits behind it — the server refuses a
     *   child whose parent it does not know;
     * - otherwise the change is judged: push journals it, discard drops it,
     *   a hold records the row in `holds`.
     */
    private fun holdBaseline(db: SQLiteConnection, stream: String, rowId: String, images: List<ByteArray>, store: ReplicaStateStore): ByteArray {
        val row = store.snapshot(db, stream, rowId)
        val baseline = row?.let { ReplicaPreimage.Row(schema.spec(stream)?.shard ?: "user", it.type, it.data) } ?: ReplicaPreimage.Absent
        return baseline.undoing(images.map(ReplicaPreimage::require)).encoded()
    }

    private fun admit(
        db: SQLiteConnection, op: ReplicaOp, preimage: ByteArray?, applied: Boolean, store: ReplicaStateStore,
    ): Boolean {
        if (syncGates.isEmpty) return true
        val held = store.hold(db, op.stream, op.rowId)
        if (held != null) {
            rejudge(db, held, store, landing = if (applied) null else op)
            return false
        }
        val knows = op.verb != ReplicaOp.Verb.ROW_CREATE
        val parent = heldParent(db, op.rowId, op.data.orEmpty(), before = null, store = store)
        if (parent != null) {
            holdBehind(db, op.stream, op.rowId, parent, serverKnows = knows, preimage = holdBaseline(db, op.stream, op.rowId, listOfNotNull(preimage), store), store = store)
            return false
        }
        return when (val outcome = syncGates.judge(change(op, preimage))) {
            SyncGates.Outcome.Push -> true
            SyncGates.Outcome.Discard -> if (!knows) {
                Log.logger.error("[gate] a birth was discarded — sent anyway, every later write stands on it: ${op.stream}/${op.rowId}")
                true
            } else {
                false
            }
            is SyncGates.Outcome.Hold -> {
                hold(db, op.stream, op.rowId, outcome.gate, outcome.reason, serverKnows = knows, preimage = holdBaseline(db, op.stream, op.rowId, listOfNotNull(preimage), store), store = store)
                false
            }
        }
    }

    private fun hold(
        db: SQLiteConnection, stream: String, rowId: String, gate: String, reason: String,
        serverKnows: Boolean, seq: Long? = null, preimage: ByteArray, store: ReplicaStateStore,
    ) {
        store.insertHold(db, stream, rowId, gate, reason, serverKnows, seq, preimage)
        Log.logger.info("[gate] hold $stream/$rowId: $reason")
    }

    /**
     * A row naming a held row waits behind it, under that row's own wait
     * gate: it is asked again the moment that row leaves its hold.
     */
    private fun holdBehind(
        db: SQLiteConnection, stream: String, rowId: String, parent: ReplicaStateStore.GateHold,
        serverKnows: Boolean, seq: Long? = null, preimage: ByteArray, store: ReplicaStateStore,
    ) {
        hold(db, stream, rowId, waitGate(parent.stream, parent.rowId), "waits for ${parent.stream}/${parent.rowId}",
             serverKnows, seq, preimage, store)
    }

    /**
     * The earliest held row this row's data names — only one held BEFORE
     * it, so two rows naming each other never wait on each other, and never
     * one a policy over every stream holds.
     */
    private fun heldParent(
        db: SQLiteConnection, rowId: String, data: Map<String, ReplicaValue>, before: Long?, store: ReplicaStateStore,
    ): ReplicaStateStore.GateHold? {
        val named = mutableSetOf<String>()
        for (value in data.values) namedIds(value, named)
        named.remove(rowId)
        for (parent in store.holds(db, named.toList())) {
            if (before != null && parent.seq >= before) return null
            if (syncGates.orders(parent.gateId)) return parent
        }
        return null
    }

    /**
     * Ask a held row's gates again; when it leaves its hold, the rows
     * waiting behind it are asked next. Returns whether it left.
     */
    private fun rejudge(
        db: SQLiteConnection, asked: ReplicaStateStore.GateHold, store: ReplicaStateStore, landing: ReplicaOp? = null,
    ): Boolean {
        if (!askAgain(db, asked, store, landing)) return false
        val behind = store.holds(db, gateId = waitGate(asked.stream, asked.rowId)).toMutableList()
        while (behind.isNotEmpty()) {
            val next = behind.removeAt(0)
            if (askAgain(db, next, store, null)) behind += store.holds(db, gateId = waitGate(next.stream, next.rowId))
        }
        return true
    }

    /**
     * One held row, judged on its CURRENT state — `landing` is the write
     * committing now, which its row does not carry yet. Push releases it:
     * its state joins the journal as one op.
     */
    private fun askAgain(
        db: SQLiteConnection, asked: ReplicaStateStore.GateHold, store: ReplicaStateStore, landing: ReplicaOp?,
    ): Boolean {
        val held = store.hold(db, asked.stream, asked.rowId) ?: return false
        val existing = store.snapshot(db, held.stream, held.rowId)
        var current: Map<String, ReplicaValue>? = existing?.data
        if (landing != null) {
            current = when (landing.verb) {
                ReplicaOp.Verb.ROW_DELETE -> null
                ReplicaOp.Verb.ROW_CREATE -> landing.data.orEmpty()
                ReplicaOp.Verb.ROW_PATCH -> current.orEmpty() + landing.data.orEmpty()
                else -> current
            }
        }
        if (current == null && !held.serverKnows) {
            store.cancelUnsentBirth(db, held.stream, held.rowId)
            store.dropHold(db, held.stream, held.rowId)
            Log.logger.info("[gate] drop ${held.stream}/${held.rowId}: gone before the server heard of it")
            return true
        }
        if (current != null) {
            val parent = heldParent(db, held.rowId, current, before = held.seq, store = store)
            if (parent != null) {
                val gate = waitGate(parent.stream, parent.rowId)
                if (gate != held.gateId) {
                    store.updateHold(db, held.stream, held.rowId, gate, "waits for ${parent.stream}/${parent.rowId}")
                }
                return false
            }
        }
        val document = schema.lane(held.stream) == ReplicaStreamSpec.Lane.DOCUMENT
        val kind = when {
            current == null -> SyncChange.Kind.DELETE
            document && held.serverKnows -> SyncChange.Kind.DOCUMENT
            held.serverKnows -> SyncChange.Kind.PATCH
            else -> SyncChange.Kind.CREATE
        }
        val local = if (document) emptyMap() else current.orEmpty()
        var outcome = syncGates.judge(SyncChange(held.stream, held.rowId, kind, local))
        if (outcome == SyncGates.Outcome.Discard && kind == SyncChange.Kind.CREATE) {
            Log.logger.error("[gate] a birth was discarded — sent anyway, every later write stands on it: ${held.stream}/${held.rowId}")
            outcome = SyncGates.Outcome.Push
        }
        return when (outcome) {
            is SyncGates.Outcome.Hold -> {
                if (outcome.gate != held.gateId || outcome.reason != held.reason) {
                    store.updateHold(db, held.stream, held.rowId, outcome.gate, outcome.reason)
                }
                false
            }
            SyncGates.Outcome.Discard -> {
                store.dropHold(db, held.stream, held.rowId)
                Log.logger.info("[gate] drop ${held.stream}/${held.rowId}: its gate discards it")
                true
            }
            SyncGates.Outcome.Push -> {
                store.dropHold(db, held.stream, held.rowId)
                journalRelease(db, held, current, existing?.type ?: landing?.type, document, store)
                true
            }
        }
    }

    /**
     * A released row leaves as its state, never as the history of its
     * writes: a birth the server never heard, a patch of every field the
     * device may send, a delete, or the document's delta past what the
     * server acked.
     */
    private fun journalRelease(
        db: SQLiteConnection, held: ReplicaStateStore.GateHold, current: Map<String, ReplicaValue>?, type: String?,
        document: Boolean, store: ReplicaStateStore,
    ) {
        val op: ReplicaOp = when {
            current != null && document -> {
                val doc = store.doc(db, held.stream, held.rowId)
                    ?: throw ReplicaError.UnknownDocument(held.stream, held.rowId)
                val codec = codecs[doc.codec] ?: throw ReplicaError.Codec("no codec registered for ${doc.codec}")
                if (held.serverKnows) {
                    val owed = codec.diff(doc.fold, doc.acked)
                    if (codec.isEmptyDiff(owed)) {
                        Log.logger.info("[gate] release ${held.stream}/${held.rowId}: nothing owed")
                        return
                    }
                    ReplicaOp(id = ReplicaID.ulid(), verb = ReplicaOp.Verb.DOC_DELTA,
                        stream = held.stream, rowId = held.rowId, codec = doc.codec, payload = owed)
                } else {
                    ReplicaOp(id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_CREATE,
                        stream = held.stream, rowId = held.rowId, codec = doc.codec, seed = doc.fold)
                }
            }
            current != null -> {
                val pushed = schema.spec(held.stream)?.pushed
                val sendable = if (pushed == null) current else current.filterKeys { it in pushed }
                if (held.serverKnows) {
                    ReplicaOp(id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_PATCH,
                        stream = held.stream, rowId = held.rowId, data = sendable)
                } else {
                    ReplicaOp(id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_CREATE,
                        stream = held.stream, rowId = held.rowId, type = type, data = sendable)
                }
            }
            else -> ReplicaOp(id = ReplicaID.ulid(), verb = ReplicaOp.Verb.ROW_DELETE, stream = held.stream, rowId = held.rowId)
        }
        val preimage = ReplicaPreimage.require(held.preimage).forRelease(op).encoded()
        journal(db, op, store, preimage, ReplicaLane.BULK, key = null)
        Log.logger.info("[gate] release ${held.stream}/${held.rowId} as ${op.verb}")
    }

    /**
     * The owed intents judged again by a gate that fired. An owed intent its
     * row can no longer send (backup turned off) moves into `holds`, with
     * every later intent of that row, and so does an intent naming a row
     * this moved before the server heard of it. A frozen intent stays — the
     * server may already hold it. The rows moved go ahead of every earlier
     * hold, in queue order: their writes are older, and a row held earlier
     * may name one of them.
     */
    private fun holdJournal(db: SQLiteConnection, gate: SyncGate, store: ReplicaStateStore) {
        val entries = store.owed(db, gate.stream)
        val earlier = store.firstHoldSeq(db) ?: 1L
        var next = earlier - entries.size
        var moved = 0
        val operations = entries.map { it.op() }
        for ((index, entry) in entries.withIndex()) {
            val op = operations[index]
            val birth = op.verb == ReplicaOp.Verb.ROW_CREATE
            if (store.hold(db, op.stream, op.rowId) != null) {
                if (birth) store.markServerUnaware(db, op.stream, op.rowId)
                store.discard(db, entry.id)
                continue
            }
            val undo = entries.indices.filter { it >= index && operations[it].stream == op.stream && operations[it].rowId == op.rowId }.mapNotNull { entries[it].preimage }
            val baseline = holdBaseline(db, op.stream, op.rowId, undo, store)
            val outcome = when (val verdict = gate.judge(change(op, entry.preimage))) {
                SyncVerdict.Push -> SyncGates.Outcome.Push
                is SyncVerdict.Gate -> SyncGates.Outcome.Hold(gate.id, verdict.reason)
                SyncVerdict.Discard -> SyncGates.Outcome.Discard
            }
            when (outcome) {
                SyncGates.Outcome.Push -> {
                    val parent = heldParent(db, op.rowId, op.data.orEmpty(), before = earlier, store = store) ?: continue
                    holdBehind(db, op.stream, op.rowId, parent, serverKnows = !birth, seq = next, preimage = baseline, store = store)
                }
                SyncGates.Outcome.Discard -> if (birth) continue
                is SyncGates.Outcome.Hold ->
                    hold(db, op.stream, op.rowId, outcome.gate, outcome.reason, serverKnows = !birth, seq = next, preimage = baseline, store = store)
            }
            next += 1
            moved += 1
            store.discard(db, entry.id)
        }
        if (moved == 0) return
        Log.logger.info("[gate] ${gate.id}: $moved owed entries left the journal")
    }

    /** The change a journaled op carries, as a gate sees it — the displaced row is a delete's `previous`. */
    internal fun change(op: ReplicaOp, preimage: ByteArray?): SyncChange {
        val displaced = preimage?.let { ReplicaPreimage.decode(it) }
        return when (op.verb) {
            ReplicaOp.Verb.ROW_DELETE -> SyncChange(
                op.stream, op.rowId, SyncChange.Kind.DELETE,
                previous = (displaced as? ReplicaPreimage.Row)?.data.orEmpty()
            )
            ReplicaOp.Verb.DOC_DELTA -> SyncChange(op.stream, op.rowId, SyncChange.Kind.DOCUMENT)
            ReplicaOp.Verb.ROW_PATCH -> SyncChange(
                op.stream, op.rowId, SyncChange.Kind.PATCH, op.data.orEmpty(),
                (displaced as? ReplicaPreimage.Fields)?.values.orEmpty()
            )
            else -> SyncChange(op.stream, op.rowId, SyncChange.Kind.CREATE, op.data.orEmpty())
        }
    }

    private fun waitGate(stream: String, rowId: String): String = "row:$stream/$rowId"

    /**
     * Undo later local writes first, refuse this write, then replay the
     * surviving suffix. Otherwise refusing an earlier patch erases a later
     * one, or leaves that later patch holding an obsolete rollback image.
     * Returns the frozen writes a refused birth takes with it: the cascade
     * cannot discard them, so their own refusals leave no second record.
     */
    internal fun rejectRow(db: SQLiteConnection, entry: ReplicaStateStore.JournalRow, op: ReplicaOp, reason: String, store: ReplicaStateStore): List<String> {
        if (op.verb == ReplicaOp.Verb.ROW_CREATE) {
            // Rollback removes dependent edits, so preserve the branch first.
            store.archiveEntity(db, op.stream, op.rowId, reason)
        }
        val owed = store.entriesAddressing(db, op.stream, op.rowId).filter { it.parked == null }
        val index = owed.indexOfFirst { it.id == entry.id }
        val current = owed[index]
        val later = owed.drop(index + 1)
        for (pending in later.asReversed()) {
            val preimage = pending.preimage?.let(ReplicaPreimage::decode) ?: continue
            revert(db, pending.op(), preimage, store, cascading = false)
        }
        store.refuse(db, entry.id, reason)
        current.preimage?.let(ReplicaPreimage::decode)?.let { revert(db, op, it, store) }
        val survivors = later.mapNotNull { store.entry(db, it.id, it.payload) }
        rebaseOwedWrites(db, op.stream, op.rowId, schema.spec(op.stream)?.shard ?: "user", store, survivors)
        return if (op.verb == ReplicaOp.Verb.ROW_CREATE) survivors.filter { it.sent }.map { it.id } else emptyList()
    }

    /** The undo, inside the verdict transaction. */
    private fun revert(
        db: SQLiteConnection,
        op: ReplicaOp,
        preimage: ReplicaPreimage,
        store: ReplicaStateStore,
        cascading: Boolean = true,
    ) {
        when {
            op.verb == ReplicaOp.Verb.ROW_CREATE && preimage == ReplicaPreimage.Absent -> {
                store.deleteSnapshot(db, op.stream, op.rowId)
                if (!cascading) return
                if (schema.lane(op.stream) == ReplicaStreamSpec.Lane.DOCUMENT) {
                    store.deleteDoc(db, op.stream, op.rowId)
                }
                // A refused birth takes every dependent patch/delta/delete with it
                // on either lane, and the hold its later writes wait in. Its own
                // parked entry stays as the evidence.
                store.discardEntries(db, op.stream, op.rowId, except = op.id)
                store.dropHold(db, op.stream, op.rowId)
            }

            op.verb == ReplicaOp.Verb.ROW_PATCH && preimage is ReplicaPreimage.Fields -> {
                val row = store.snapshot(db, op.stream, op.rowId) ?: return
                val data = LinkedHashMap(row.data)
                for ((key, value) in preimage.values) data[key] = value
                for (key in preimage.missing) data.remove(key)
                store.upsertSnapshot(
                    db, op.stream, op.rowId,
                    shard = schema.spec(op.stream)?.shard ?: "user", type = row.type, data = data
                )
            }

            (op.verb == ReplicaOp.Verb.ROW_DELETE || op.verb == ReplicaOp.Verb.ROW_CREATE) && preimage is ReplicaPreimage.Row -> {
                store.upsertSnapshot(
                    db, op.stream, op.rowId, preimage.shard, preimage.type, preimage.data
                )
                if (cascading && schema.lane(op.stream) == ReplicaStreamSpec.Lane.DOCUMENT) {
                    // The row returns but the fold died with the local delete —
                    // only a re-bootstrap can rebuild it.
                    store.clearCursor(db, schema.spec(op.stream)?.shard ?: preimage.shard)
                }
            }

            else -> Unit
        }
    }

    internal fun advanceAcked(db: SQLiteConnection, op: ReplicaOp, store: ReplicaStateStore) {
        val payload = op.payload ?: op.seed ?: return
        // A later local delete may already have removed this authoring fold.
        val doc = store.doc(db, op.stream, op.rowId) ?: return
        val codec = codecs[doc.codec] ?: throw ReplicaError.Codec("No codec registered for ${doc.codec}")
        val acked = codec.mergeVersions(doc.acked, codec.payloadVersion(payload))
        store.updateDoc(db, op.stream, op.rowId, acked = acked)
    }

    public suspend fun pendingOps(): List<ReplicaStateStore.JournalRow> =
        withContext(engineContext) {
            val store = binding.store ?: return@withContext emptyList()
            store.read { store.pending(it) }
        }

    public suspend fun parkedOps(): List<ReplicaStateStore.JournalRow> =
        withContext(engineContext) {
            val store = binding.store ?: return@withContext emptyList()
            store.read { store.parked(it) }
        }

    /**
     * Abandon entries by id whatever their bytes or parked state — the
     * discard path (a row thrown away before the server ever heard its id
     * must stop owing anything). A frozen entry stays for its own verdict.
     */
    public suspend fun discardOps(ids: List<String>) {
        withContext(engineContext) {
            val store = writableStore()
            store.write { db ->
                for (id in ids) store.discard(db, id)
            }
        }
    }

    // MARK: - Watch

    /**
     * Post-commit signal for one stream — the durable `stream_meta.change_seq`
     * re-read after every commit, so consumers hang off committed state only:
     * a rolled-back checkpoint never fires. Change-only is the default;
     * state-owning consumers can request the committed baseline to close the
     * observer-arming race.
     *
     * The signal outlives its store: a watcher armed before anyone owned the
     * process, or held across a foreign switch, re-arms on the owner that
     * arrives instead of observing a store nobody writes to any more.
     */
    public fun watchSignal(stream: String, includeInitial: Boolean = false): Flow<Unit> =
        callbackFlow {
            val worker = launch {
                // The baseline belongs to the OBSERVER, not to the store: a
                // watcher that already reported one world sees the next
                // owner's first picture as a CHANGE, not as another baseline.
                var armed = false
                while (isActive) {
                    val (bound, generation) = binding.snapshot()
                    val store = bound?.store
                    if (store == null) {
                        if (includeInitial && !armed) trySend(Unit)
                        armed = true
                        binding.waitForChange(generation)
                        continue
                    }
                    val wasArmed = armed
                    armed = true
                    observeValue(
                        store = store,
                        binding = binding,
                        generation = generation,
                        fetch = { it.read { db -> it.changeSequence(db, stream) } }
                    ) { _, baseline ->
                        if (!baseline || wasArmed || includeInitial) trySend(Unit)
                    }
                    binding.waitForChange(generation)
                }
            }
            awaitClose { worker.cancel() }
        }.buffer(Channel.UNLIMITED)

    /**
     * Post-commit VALUES of the journal's parked entries — the refusal
     * ledger as a subscription. Same identity-rebinding loop as
     * `watchSignal`, but the yielded list IS the state (baseline included):
     * a consumer owns its picture by assignment, never by re-query, so a
     * drain's park and a re-run's discard both arrive as committed pictures
     * in commit order — nothing to poll, nothing to coalesce.
     */
    public fun watchParkedOps(): Flow<List<ReplicaStateStore.JournalRow>> = callbackFlow {
        val worker = launch {
            while (isActive) {
                val (bound, generation) = binding.snapshot()
                val store = bound?.store
                if (store == null) {
                    // No owner: the committed picture is "nothing parked".
                    trySend(emptyList())
                    binding.waitForChange(generation)
                    continue
                }
                observeValue(
                    store = store,
                    binding = binding,
                    generation = generation,
                    fetch = { it.read { db -> it.parked(db) } }
                ) { parked, _ -> trySend(parked) }
                binding.waitForChange(generation)
            }
        }
        awaitClose { worker.cancel() }
    }.buffer(Channel.UNLIMITED)

    // MARK: - Plumbing

    internal fun writableSpec(stream: String): ReplicaStreamSpec {
        val spec = schema.spec(stream) ?: throw ReplicaError.UnknownStream(stream)
        if (spec.readonly) throw ReplicaError.ReadonlyStream(stream)
        if (documentMode == ReplicaDocumentMode.PROJECTIONS_ONLY && spec.lane == ReplicaStreamSpec.Lane.DOCUMENT) {
            throw ReplicaError.ReadonlyStream(stream)
        }
        return spec
    }

    private fun codecName(spec: ReplicaStreamSpec): String {
        spec.codec?.let { return it }
        if (codecs.size == 1) return codecs.keys.first()
        throw ReplicaError.Codec("stream ${spec.name} declares no codec")
    }

    // MARK: - Test seams

    internal suspend fun setCheckpointFault(fault: (() -> Unit)?) {
        withContext(engineContext) { checkpointFault = fault }
    }

    public companion object {
        /**
         * Owed entries frozen per push, under the protocol's 100.
         * 50, not 500: a chunk must come back in seconds so acks land
         * incrementally — the 500-op request outlived the client timeout on a
         * real device (server applied for minutes, no verdict ever returned,
         * the same backlog re-pushed every cold window, forever).
         */
        internal const val MAX_OPS_PER_PUSH = 50

        /** A runaway reducer must not spin the flight forever. */
        internal const val MAX_DRAIN_PASSES = 16

        private val ISO_INSTANT: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

        /**
         * The lane the current action claims. Swift's `@TaskLocal` becomes a
         * `ThreadLocal` carried as a coroutine context element: a NON-suspend
         * write must capture it at API entry, which a plain context element
         * cannot answer.
         */
        internal val currentDraftLocal: ThreadLocal<String?> = ThreadLocal()
        internal val currentLocalSessionLocal: ThreadLocal<ReplicaLocalSession?> = ThreadLocal()

        internal val currentLaneLocal: ThreadLocal<ReplicaLane?> = ThreadLocal()

        internal fun currentLane(): ReplicaLane = currentLaneLocal.get() ?: ReplicaLane.BULK
    }
}
