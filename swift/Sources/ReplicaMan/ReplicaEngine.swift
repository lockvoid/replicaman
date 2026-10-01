import Foundation
import GRDB

/// The loop. One actor owns the whole replication surface: pull rounds
/// (staged pages → store → cursor, one transaction), the outbound op
/// journal (per-document supersede, verdicts, cold window), and the local
/// write door the generated verbs call.
///
/// ORDER IS THE CONTRACT (ported from Syncer v1):
/// - A pull round publishes ATOMICALLY with its cursor — a crash before
///   its last page leaves the previous round intact and the staged pages
///   to resume from.
/// - Drain-before-pull, so the echo is in the answer; `drainIfWarm` skips
///   inside the cold window so an offline fetch burst doesn't stack
///   timeouts on a wire that just proved dead.
/// - Journal durability and the client snapshot write share one
///   transaction — a crash between them cannot exist.
/// - Rejection is a VERDICT (park, keep the reason), transport failure is a
///   RETRY (entries stay pending) — never conflated.
public actor ReplicaEngine {
    /// Which owner's file this process holds open. Everything below reads it;
    /// only `open`/`close`/`retire` write it.
    nonisolated let binding = ReplicaBinding()
    /// The documents held live — the state cache behind `findDoc`.
    nonisolated let liveDocuments = LiveDocuments()
    public nonisolated let health = ReplicaHealth()
    /// Where the worlds live — `replica-<owner>.sqlite` under it
    /// (`ReplicaMan.Configuration.homePath` unless the caller says otherwise).
    /// The suffix is what a test process adds so two of them never share a
    /// store.
    public nonisolated let home: URL
    private let storeSuffix: String
    public nonisolated let schema: ReplicaSchema
    let transport: any ReplicaTransport
    let codecs: [String: any ReplicaCodec]
    nonisolated let documentMode: ReplicaDocumentMode
    let batchLimit: Int
    private let coldWindow: TimeInterval
    let peerMinter: @Sendable () -> UInt64
    private let clock: @Sendable () -> Date
    private let automaticallyPushWrites: Bool
    /// Per LANE, not per engine: a fat bulk batch is what dies on a slow link
    /// (the server needs ~9s to apply 20 ops), and the one-row message that
    /// would have succeeded must not wait out ITS backoff. A genuinely dead
    /// wire cools each track on that track's own failed attempt.
    private var coldUntil: [ReplicaLane: Date] = [:]
    private var scheduledPushes: [ReplicaLane: Task<Void, Never>] = [:]
    /// A local write arriving during a scheduled drain owes one more pass.
    /// Gated journal entries alone are not a wake event.
    private var scheduledPushReruns: Set<ReplicaLane> = []
    /// One flight per lane: that is what lets an interactive write leave while
    /// a bulk push is still on the wire.
    private var activeDrains: [ReplicaLane: Task<[ReplicaVerdict], Error>] = [:]
    /// One flight per shard, and it leaves this map itself before anyone sees
    /// its page: a caller looping on `more` must never find the flight it just
    /// awaited — awaiting a finished task does not suspend, so the loop would
    /// hold the actor and the flight's owner could never clear it.
    private var activePulls: [String: (id: UUID, task: Task<(applied: Int, more: Bool), Error>)] = [:]
    /// Local transactions run on their caller's thread; this is what a seal
    /// waits on for them, the way it waits on `activeWireOperations`.
    nonisolated let writeGate = ReplicaWriteGate()

    /// The lane the current action claims. A task-local so a helper called
    /// inside `lane { }` — `createElements`, a journal writer — inherits it
    /// without knowing lanes exist; forgetting to thread a parameter is the
    /// failure mode this design exists to remove.
    @TaskLocal public static var currentLane: ReplicaLane = .bulk
    /// The draft the body's writes are held under — see `beginDraft`.
    @TaskLocal static var currentDraft: String?

    /// Every write inside rides one lane, in order. Reads as the fact the
    /// caller actually knows: someone is waiting on this.
    ///
    ///     try await replica.lane(.interactive) {
    ///         try await ProjectMutations.createElements(...)   // inherits
    ///         try await replica.agentMessages.save(message)
    ///     }
    ///
    /// Not a transaction: writes land locally as they happen, the server
    /// applies each op in its own savepoint, and a refusal is per op. It is a
    /// routing and ordering scope, nothing more.
    public nonisolated func lane<T: Sendable>(
        _ lane: ReplicaLane, _ body: @Sendable () async throws -> T
    ) async rethrows -> T {
        try await ReplicaEngine.$currentLane.withValue(lane) { try await body() }
    }

    /// A sealed engine admits no new local authoring and no authenticated
    /// transport. `seal()` returns only once every operation that already
    /// captured the outgoing bearer has finished applying its response
    /// locally — which is what makes the merge barrier and the sign-out
    /// flush safe. Closing seals; opening unseals.
    private(set) var sealed = false
    private var activeWireOperations = 0
    private var sealWaiters: [CheckedContinuation<Void, Never>] = []

    /// Read-only cross-module proof for the app host's pinned-source drain
    /// admission. Mutation remains owned by seal/unseal.
    public var isSealed: Bool { sealed }

    /// Owed entries frozen for one push — a drain chunks its backlog under it.
    // 50, not 500: a chunk must come back in seconds so acks land
    // incrementally — the 500-op request outlived the client timeout on a
    // real device (server applied for minutes, no verdict ever returned,
    // the same backlog re-pushed every cold window, forever).
    static let maxOpsPerPush = 50

    /// Test seam: thrown between frame apply and cursor advance to prove
    /// checkpoint atomicity. Internal on purpose — `@testable` only.
    var checkpointFault: (@Sendable () throws -> Void)?

    /// The rejection seam: fired once per rejected op AFTER the
    /// verdict transaction (revert included) commits. The app hangs its
    /// "changes couldn't sync and were undone" surface off it.
    var onRejected: (@Sendable (ReplicaOp, String) -> Void)?

    /// How many client writes have been undone by rejected verdicts
    /// this session — the debug surface reads it next to the parked count.
    public internal(set) var revertedCount = 0

    public func setRejectionHandler(_ handler: (@Sendable (ReplicaOp, String) -> Void)?) {
        onRejected = handler
    }

    // MARK: - Identity boundary

    /// Freeze local write admissions and authenticated transport, then wait
    /// for every already-started push/pull to settle under the outgoing
    /// credentials. The caller may merge or release the store only after this
    /// returns. Idempotent for the one serialized host transition.
    public func seal() async {
        sealed = true
        await writeGate.close()
        guard activeWireOperations > 0 else { return }
        await withCheckedContinuation { continuation in
            sealWaiters.append(continuation)
        }
    }

    /// Re-admit local authoring and the wire once credentials and the durable
    /// store agree on one identity.
    public func unseal() {
        sealed = false
        writeGate.open()
        // A gate's signal fired while sealed was dropped: ask its holds now.
        askHoldsAgain()
        guard automaticallyPushWrites, let store = binding.store else { return }
        for lane in ReplicaLane.allCases where scheduledPushes[lane] == nil {
            let owed: Bool
            do {
                owed = try store.pool.read { db in try store.owesWork(db, lane: lane) }
            } catch {
                // This lifecycle callback has no awaiting caller. Report the
                // failure and leave the durable queue untouched for retry.
                health.record(error, operation: "resume automatic delivery")
                Log.logger.error("[push] unseal could not read what \(String(describing: lane), privacy: .public) owes — no push scheduled: \(String(describing: error), privacy: .public)")
                owed = false
            }
            if owed { schedulePush(lane) }
        }
    }

    /// Seal, then push the frozen outgoing journal through a caller-pinned
    /// transport. Deliberately narrower than ordinary `drain()`:
    ///
    /// - the engine is sealed for the whole push, so local CRUD stays refused;
    /// - the normal engine transport is never consulted, so a durable sign-out
    ///   marker can keep live/target credentials unavailable while Auth supplies
    ///   only the captured source bearer to this final outgoing exchange.
    ///
    /// Replaying after a crash is idempotent: accepted entries were removed by
    /// their verdict transaction; unacknowledged entries remain pending and are
    /// the only entries selected by the next call.
    @discardableResult
    @_spi(SignOutIdentityTransition)
    public func sealAndDrain(
        using pinnedSourceTransport: any ReplicaTransport
    ) async throws -> [ReplicaVerdict] {
        await seal()
        guard binding.store != nil else { return [] }

        // `drain()` claims its flight before that task begins its wire operation.
        // If the seal won in that narrow window, let the refused flight release
        // its claim before installing the pinned-source flight below.
        for flight in activeDrains.values {
            do {
                _ = try await awaitDrain(flight)
            } catch ReplicaError.identityTransitionRequired {
                // Sealing refused a flight before it entered the wire. The
                // pinned flight below takes responsibility for those same bytes.
            }
        }
        guard sealed, activeWireOperations == 0, activeDrains.isEmpty else {
            throw ReplicaError.identityTransitionRequired
        }

        let flight = Task { [weak self, pinnedSourceTransport] () throws -> [ReplicaVerdict] in
            guard let self else { return [] }
            return try await self.performDrain(
                lane: nil,
                using: pinnedSourceTransport,
                sealedFlush: true
            )
        }
        // The sign-out flush drains EVERY lane (`lane: nil` above): the engine
        // is sealed and nothing can race it. What it leaves behind — owed
        // entries, held rows — stays in the owner's file for their next
        // sign-in, the message typed a second ago included.
        activeDrains[.bulk] = flight
        return try await awaitDrain(flight)
    }

    func beginWireOperation() throws -> ReplicaStateStore {
        let store = try writableStore()
        activeWireOperations += 1
        return store
    }

    func endWireOperation() {
        activeWireOperations = max(0, activeWireOperations - 1)
        guard sealed, activeWireOperations == 0 else { return }
        let waiters = sealWaiters
        sealWaiters.removeAll()
        for waiter in waiters { waiter.resume() }
    }

    /// Deltas intentionally omitted in persisted projection-only mode.
    /// Full replication refuses unsupported or corrupt document checkpoints.
    public private(set) var skippedDeltaCount = 0

    /// The per-doc resync lever: drop the fold and blank its shard's
    /// cursor; the next pull re-bootstraps and the doc is reborn from server
    /// truth (with a fresh peer). The journal is untouched.
    public func resyncDocument(stream: String, id: String) throws {
        let store = try writableStore()
        try liveDocuments.publishing {
            try store.pool.write { db in
                try store.archiveDocument(db, stream: stream, rowId: id, reason: "explicit resync")
                try store.deleteDoc(db, stream: stream, rowId: id)
                try store.clearCursor(db, shard: schema.spec(stream)?.shard ?? "user")
            }
            liveDocuments.evict(LiveDocuments.Key(stream: stream, id: id))
        }
    }

    /// Corrupt-fold recovery: REPLACE an unreadable fold with `fold` under a
    /// NEW peer, and blank the shard's cursor so the re-bootstrap merges the
    /// server's true history back in.
    ///
    /// A replacement rather than a drop, because the row must stay WRITABLE:
    /// an edit made between the recovery and the next pull has to have
    /// somewhere to land. The new peer is persisted here — rotating and then
    /// forgetting would walk the next launch straight back into the reused
    /// counter range (loro dedups by (peer, counter)); `acked` resets to
    /// nothing, so the whole recovered history is owed again.
    public func rebuildDocument(stream: String, id: String, fold: Data, peer: UInt64) throws {
        let store = try writableStore()
        let spec = try writableSpec(stream)
        guard spec.lane == .document else { throw ReplicaError.laneMismatch(stream) }
        let codecName = try codecName(for: spec)
        guard let codec = codecs[codecName] else { throw ReplicaError.codec("no codec registered for \(codecName)") }
        let replacement = try codec.merge(fold: nil, payload: fold, reflecting: spec.reflections)
        let owed = try codec.diff(fold: replacement.fold, since: nil)
        try liveDocuments.publishing {
            try store.pool.write { db in
                if try store.doc(db, stream: stream, rowId: id)?.peer == peer {
                    throw ReplicaError.codec("A rebuilt document requires a fresh authoring peer")
                }
                try store.archiveDocument(db, stream: stream, rowId: id, reason: "explicit rebuild")
                try store.upsertDoc(db, stream: stream, rowId: id, shard: spec.shard,
                                    codec: codecName, fold: replacement.fold, acked: nil, peer: peer)
                try reflect(replacement.reflected, db, stream: stream, id: id, shard: spec.shard, store: store)
                if !codec.isEmptyDiff(owed) {
                    let op = ReplicaOp(id: ReplicaID.ulid(), verb: ReplicaOp.Verb.docDelta,
                                       stream: stream, rowId: id, codec: codecName, payload: owed)
                    try enqueueOp(db, op, store: store, lane: ReplicaEngine.currentLane, draft: ReplicaEngine.currentDraft)
                }
                try store.clearCursor(db, shard: spec.shard)
            }
            liveDocuments.evict(LiveDocuments.Key(stream: stream, id: id))
        }
        schedulePush()
    }

    public init(
        home: URL = ReplicaMan.Configuration.homePath,
        transport: any ReplicaTransport,
        schema: ReplicaSchema,
        codecs: [any ReplicaCodec] = [],
        batchLimit: Int = 500,
        coldWindow: TimeInterval = 10,
        peerMinter: @escaping @Sendable () -> UInt64 = { ReplicaID.peer() },
        clock: @escaping @Sendable () -> Date = Date.init,
        automaticallyPushWrites: Bool = true,
        storeSuffix: String = "",
        syncGates: [any SyncGate] = [],
        documentMode: ReplicaDocumentMode = .replicated
    ) {
        self.home = home
        self.storeSuffix = storeSuffix
        self.transport = transport
        self.schema = schema
        self.codecs = Dictionary(uniqueKeysWithValues: codecs.map { ($0.name, $0) })
        self.documentMode = documentMode
        self.batchLimit = batchLimit
        self.coldWindow = coldWindow
        self.peerMinter = peerMinter
        self.clock = clock
        self.automaticallyPushWrites = automaticallyPushWrites
        // Armed AT BIRTH: the engine self-drains from its first write, so a
        // registration that arrives by a later call can lose the race — a
        // gated field rode the wire that way live.
        self.syncGates = SyncGates(syncGates)
        for gate in self.syncGates.all {
            let id = gate.id
            let changes = gate.changes
            gateWatches.append(Task { [weak self] in
                for await _ in changes { await self?.gateChanged(id) }
            })
        }
    }

    /// Test seam: bind a store the caller already built, without the
    /// filesystem naming. Internal — production has exactly one door, and it
    /// is `open(owner:)`.
    init(
        store: ReplicaStateStore,
        owner: Int,
        transport: any ReplicaTransport,
        schema: ReplicaSchema,
        codecs: [any ReplicaCodec] = [],
        batchLimit: Int = 500,
        coldWindow: TimeInterval = 10,
        peerMinter: @escaping @Sendable () -> UInt64 = { ReplicaID.peer() },
        clock: @escaping @Sendable () -> Date = Date.init,
        automaticallyPushWrites: Bool = true,
        syncGates: [any SyncGate] = [],
        documentMode: ReplicaDocumentMode = .replicated
    ) {
        self.init(
            home: FileManager.default.temporaryDirectory,
            transport: transport,
            schema: schema,
            codecs: codecs,
            batchLimit: batchLimit,
            coldWindow: coldWindow,
            peerMinter: peerMinter,
            clock: clock,
            automaticallyPushWrites: automaticallyPushWrites,
            syncGates: syncGates,
            documentMode: documentMode
        )
        binding.bind(.init(owner: owner, store: store, path: store.path))
    }

    // MARK: - Owner lifecycle

    /// The owner this process is writing for — nil while the engine is closed.
    public nonisolated var owner: Int? { binding.owner }

    /// The bound store — nil while the engine is closed. Reads go straight
    /// through it (the pool serves concurrent readers); only the engine writes.
    public nonisolated var store: ReplicaStateStore? { binding.store }

    /// The raw-query escape hatch (read contract: app code reads, only the
    /// engine writes).
    public nonisolated var database: (any DatabaseReader)? { binding.store?.pool }

    /// The byte plane's reference harvest: the string values of the named
    /// wire fields across every row of a stream. Feeds the staged-blob
    /// reference-watch GC — a staged blob no row and no journal op names is
    /// sweepable (`ReplicaBlobs.reconcile(keeping:)`).
    public nonisolated func rowFieldStrings(stream: String, fields: [String]) throws -> Set<String> {
        guard let store = binding.store else { return [] }
        let harvest: ReplicaStateStore.RowMaterialization<[String]> = try store.materializedRows(stream: stream) { _, _, data in
            fields.compactMap { data[$0]?.string }
        }
        return Set(harvest.rows.flatMap(\.model))
    }

    nonisolated func storeURL(owner: Int) -> URL {
        home.appendingPathComponent("replica-\(owner)\(storeSuffix).sqlite")
    }

    /// Open this owner's file — creating it on first sight. Reopening the
    /// owner already bound is a no-op, so signing back in as the same owner
    /// keeps its world and its live observations.
    public func open(owner: Int) async throws {
        guard binding.owner != owner else {
            sealed = false
            writeGate.open()
            askHoldsAgain()
            return
        }
        await seal()
        try releaseBinding(retiring: false)
        try bindStore(at: storeURL(owner: owner), owner: owner)
        sealed = false
        unseal()
    }

    /// Cold boot: bind the identity the keychain already holds BEFORE the
    /// first read, without an actor hop — a returning user's grid renders
    /// from disk on the first frame, and waiting on an `await` here would
    /// paint them an empty world first.
    ///
    /// It can only bind from NOTHING. Changing owners is a transition and
    /// goes through `open(owner:)`, where the in-flight work is quiesced.
    public nonisolated func openForColdBoot(owner: Int) throws {
        try binding.bindIfUnbound {
            let path = storeURL(owner: owner)
            try FileManager.default.createDirectory(at: home, withIntermediateDirectories: true)
            let store = try ReplicaStateStore(path: path.path, indexes: schema.indexes)
            try healCursors(store)
            try sweepDrafts(store)
            return .init(owner: owner, store: store, path: path)
        }
        askHoldsAgain()
    }

    private func bindStore(at path: URL, owner: Int) throws {
        try FileManager.default.createDirectory(at: home, withIntermediateDirectories: true)
        let store = try ReplicaStateStore(path: path.path, indexes: schema.indexes)
        try healCursors(store)
        try sweepDrafts(store)
        binding.bind(.init(owner: owner, store: store, path: path))
    }

    // MARK: - Drafts

    /// A draft is a scope, not a store: everything the body
    /// writes lands in the one store for every reader — rows, documents,
    /// watches — and is journaled under the draft's key, which the drain never
    /// selects. The key comes back so the caller can `commitDraft` (the
    /// entries become ordinary owed work at their own queue positions) or
    /// `discardDraft` (rows, documents and entries go; the server never hears
    /// of them). A write outside the scope that names a drafted row joins the
    /// draft. A draft never survives the process: the store sweeps every
    /// keyed entry at open.
    public nonisolated func beginDraft<T: Sendable>(
        _ body: @Sendable () async throws -> T
    ) async throws -> (draft: ReplicaDraft, value: T) {
        let draft = ReplicaDraft(key: ReplicaID.ulid())
        let value = try await ReplicaEngine.$currentDraft.withValue(draft.key) { try await body() }
        return (draft, value)
    }

    public nonisolated func beginDraft(_ body: @Sendable () async throws -> Void) async throws -> ReplicaDraft {
        let draft = ReplicaDraft(key: ReplicaID.ulid())
        try await ReplicaEngine.$currentDraft.withValue(draft.key) { try await body() }
        return draft
    }

    /// Idempotent; an unknown key is silence. The draft's writes meet the
    /// sync gates now: a row a gate holds moves off the journal into `holds`.
    public func commitDraft(_ draft: ReplicaDraft) async throws {
        let store = try writableStore()
        try await store.pool.write { db in
            for entry in try store.draftEntries(db, key: draft.key) {
                let op = try entry.op()
                if try !self.admit(db, op, preimage: entry.preimage, applied: true, store: store) {
                    try store.discard(db, id: entry.id)
                }
            }
            try store.releaseDraft(db, key: draft.key)
        }
        schedulePush()
    }

    public func discardDraft(_ draft: ReplicaDraft) async throws {
        let store = try writableStore()
        let documents = try await store.pool.write { db -> [LiveDocuments.Key] in
            var documents: [LiveDocuments.Key] = []
            for address in try store.draftAddresses(db, key: draft.key) {
                try store.deleteSnapshot(db, stream: address.stream, rowId: address.rowId)
                if schema.spec(address.stream)?.lane == .document {
                    try store.deleteDoc(db, stream: address.stream, rowId: address.rowId)
                    documents.append(LiveDocuments.Key(stream: address.stream, id: address.rowId))
                }
            }
            try store.dropDraftEntries(db, key: draft.key)
            return documents
        }
        for key in documents { liveDocuments.evict(key) }
    }

    /// The open sweep: a keyed entry in a store being bound belongs to a
    /// process that died mid-draft. Store-only — nothing is live yet.
    private nonisolated func sweepDrafts(_ store: ReplicaStateStore) throws {
        try store.pool.write { db in
            for key in try store.draftKeys(db) {
                for address in try store.draftAddresses(db, key: key) {
                    try store.archiveEntity(db, stream: address.stream, id: address.rowId,
                        reason: "Uncommitted draft recovered after restart")
                    try store.deleteSnapshot(db, stream: address.stream, rowId: address.rowId)
                    if schema.spec(address.stream)?.lane == .document {
                        try store.deleteDoc(db, stream: address.stream, rowId: address.rowId)
                    }
                }
                try store.dropDraftEntries(db, key: key)
                Log.logger.info("[open] swept a draft that outlived its process (\(key, privacy: .public))")
            }
        }
    }

    /// Where the bound owner's world lives on disk — nil while closed.
    public nonisolated var storePath: URL? { binding.current?.path }

    private nonisolated func healCursors(_ store: ReplicaStateStore) throws {
        try store.requireSchema(schema)
        try store.requireDocumentMode(documentMode)
    }

    /// Let this owner's world go: quiesce, then close the pool. The file
    /// stays — a sign-out that keeps the device's world for a later sign-in
    /// closes, it does not retire.
    public func close() async throws {
        await seal()
        try releaseBinding(retiring: false)
    }

    /// A foreign identity took the device: close AND delete. Wipe is the file
    /// going away, so there is no wiping pass that could miss a table.
    public func retire() async throws {
        await seal()
        try releaseBinding(retiring: true)
    }

    private func releaseBinding(retiring: Bool) throws {
        guard let released = binding.current else { return }
        try released.store.close()
        _ = binding.unbind()
        quiesceInFlight()
        liveDocuments.evictAll()
        if retiring { try ReplicaStateStore.remove(at: released.path) }
    }

    private func quiesceInFlight() {
        for task in scheduledPushes.values { task.cancel() }
        scheduledPushes.removeAll()
        scheduledPushReruns.removeAll()
        activeDrains.removeAll()
        coldUntil.removeAll()
    }

    /// The store, or the closed world's refusal. Every write verb enters here.
    func writableStore() throws -> ReplicaStateStore {
        guard let store = binding.store else { throw ReplicaError.noOwner }
        guard !sealed else { throw ReplicaError.identityTransitionInProgress }
        return store
    }

    // Command refresh hints are bound to the engine and its current owner.
    private let commitIdentity = UUID()

    public func commitSession() throws -> ReplicaCommitSession {
        _ = try writableStore()
        return ReplicaCommitSession(engine: commitIdentity, binding: binding.snapshot().generation)
    }

    /// Refresh after a committed command through an ordinary pull round.
    /// The admission keeps an account transition from changing credentials while
    /// this response is being synchronized.
    public func apply(commit encoded: String, session: ReplicaCommitSession) async throws {
        let store = try beginWireOperation()
        defer { endWireOperation() }
        guard session.engine == commitIdentity, session.binding == binding.snapshot().generation else {
            throw ReplicaError.staleCommit
        }
        let dataset = try await store.pool.read { try store.meta($0).dataset }
        let commit = try ReplicaCommit(encoded, schema: schema, dataset: dataset)
        _ = try await pullUntilCaughtUp(shards: commit.shards)
        _ = try writableStore()
    }

    // MARK: - Pull

    /// One request of the shard's round, behind the drain barrier — the echo
    /// of anything owed is in the answer. Nonzero only when it published the round.
    @discardableResult
    public func pullOnce(shard: String = "user") async throws -> Int {
        guard binding.store != nil, !sealed else { return 0 }
        try await drainIfWarm()
        return try await pullPage(shard: shard).applied
    }

    /// The named shards (every shard by default) until the server reports
    /// nothing further waiting. Warm-up, foreground and reconnect walk every
    /// shard; the doorbell asks for the one it rang for — the server rings
    /// for the user shard only, so a full walk per ring was a wasted catalog
    /// round-trip each time.
    @discardableResult
    public func pullUntilCaughtUp(shards: [String]? = nil) async throws -> Int {
        guard binding.store != nil, !sealed else { return 0 }
        try await drainIfWarm()
        var total = 0
        for shard in shards ?? schema.shards {
            while true {
                let page = try await pullPage(shard: shard)
                total += page.applied
                if !page.more { break }
            }
        }
        return total
    }

    /// The cursor of the shard's last published round.
    public func currentCursor(shard: String = "user") throws -> String? {
        guard let store = binding.store else { return nil }
        return try store.pool.read { try store.cursor($0, shard: shard) }
    }

    /// Blow away every shard's read position — the "rebuild the replica"
    /// lever; the next pull re-snapshots. Safe BECAUSE the journal survives.
    public func resetCursors() throws {
        let store = try writableStore()
        try store.pool.write { db in
            for shard in schema.shards {
                try store.clearCursor(db, shard: shard)
            }
        }
    }

    private func pullPage(shard: String) async throws -> (applied: Int, more: Bool) {
        if let flight = activePulls[shard] { return try await awaitPull(flight.task) }
        let id = UUID()
        let flight = Task { () throws -> (applied: Int, more: Bool) in
            do {
                let page = try await self.downloadPage(shard: shard)
                await self.landPull(shard: shard, id: id)
                return page
            } catch {
                await self.landPull(shard: shard, id: id)
                throw error
            }
        }
        activePulls[shard] = (id, flight)
        return try await awaitPull(flight)
    }

    private func landPull(shard: String, id: UUID) {
        guard activePulls[shard]?.id == id else { return }
        activePulls[shard] = nil
    }

    private func downloadPage(shard: String) async throws -> (applied: Int, more: Bool) {
        let store = try beginWireOperation()
        defer { endWireOperation() }
        return try await downloadPage(shard: shard, store: store,
                                      connection: ReplicaConnection(transport: transport, schema: schema))
    }

    private func awaitPull(_ flight: Task<(applied: Int, more: Bool), Error>) async throws -> (applied: Int, more: Bool) {
        try await withTaskCancellationHandler {
            let result = try await flight.value
            try Task.checkCancellation()
            return result
        } onCancel: {
            // A cancelled caller cancels the shared network flight. Durable
            // staging lets every waiter resume it on a later explicit pull.
            flight.cancel()
        }
    }


    // MARK: - Row lane (local writes)

    /// Row writes by stream name, each its own transaction — the package
    /// suites' seeding verbs; a client writes inside `write`. `createRow`
    /// wants the row ABSENT (`rowExists`), `updateRow` PRESENT (`unknownRow`)
    /// and owes a `row.patch` of the changed fields only.
    func createRow(stream: String, id: String, type: String?, data: [String: ReplicaValue]) throws {
        try writeRow(stream: stream, id: id, type: type, data: data, expecting: .absent)
    }

    func updateRow(stream: String, id: String, type: String?, data: [String: ReplicaValue]) throws {
        try writeRow(stream: stream, id: id, type: type, data: data, expecting: .present)
    }

    /// Absent ⇒ `row.create`, present ⇒ diff into `row.patch`, unchanged ⇒
    /// nothing.
    func saveRow(stream: String, id: String, type: String?, data: [String: ReplicaValue]) throws {
        try writeRow(stream: stream, id: id, type: type, data: data, expecting: .any)
    }

    private func writeRow(
        stream: String, id: String, type: String?, data: [String: ReplicaValue],
        expecting expectation: RowWriteExpectation
    ) throws {
        let store = try writableStore()
        let spec = try writableSpec(stream)
        guard spec.lane == .row else { throw ReplicaError.laneMismatch(stream) }
        let lane = ReplicaEngine.currentLane
        let draft = ReplicaEngine.currentDraft
        try store.pool.write { db in
            let existing = try store.snapshot(db, stream: stream, rowId: id)
            if let violation = expectation.violation(stream: stream, id: id, present: existing != nil) {
                throw violation
            }
            _ = try applyRowWrite(
                db, store: store, spec: spec,
                stream: stream, id: id, type: type, data: data, existing: existing,
                lane: lane, draft: draft
            )
        }
        schedulePush()
    }

    /// What a row write asserts about the row it addresses.
    enum RowWriteExpectation: Sendable {
        case absent
        case present
        case any

        func violation(stream: String, id: String, present: Bool) -> ReplicaError? {
            switch self {
            case .absent where present: return .rowExists(stream: stream, id: id)
            case .present where !present: return .unknownRow(stream: stream, id: id)
            default: return nil
            }
        }
    }

    /// One row write inside an OPEN transaction — the shared body of
    /// `saveRow` and `write`. A no-change diff is absorbed (returns false,
    /// nothing journaled).
    nonisolated func applyRowWrite(
        _ db: Database, store: ReplicaStateStore, spec: ReplicaStreamSpec,
        stream: String, id: String, type: String?,
        data: [String: ReplicaValue],
        existing: ReplicaStateStore.SnapshotRow?,
        lane: ReplicaLane, draft: String?,
        snapshot: [String: ReplicaValue]? = nil
    ) throws -> Bool {
        try validateAtomicAddress(db, stream: stream, id: id, store: store)
        if let existing {
            // A missing row field and an explicit JSON null are the same
            // nullable-column value. Generated models encode authored nil
            // as `.null` so nonnil → nil remains observable, while this
            // normalization keeps already-empty optionals out of patches.
            var changed = data.filter { (existing.data[$0.key] ?? .null) != $0.value }
            guard !changed.isEmpty else { return false }
            for field in spec.preconditions where changed[field] == nil {
                changed[field] = existing.data[field]
            }
            let op = ReplicaOp(
                id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowPatch,
                stream: stream, rowId: id, data: changed
            )
            var prior: [String: ReplicaValue] = [:]
            var missing: [String] = []
            for key in changed.keys {
                if let value = existing.data[key] {
                    prior[key] = value
                } else {
                    missing.append(key)
                }
            }
            let preimage = ReplicaPreimage.fields(values: prior, missing: missing.sorted())
            try enqueueOp(db, op, store: store, preimage: preimage.encoded(), lane: lane, draft: draft)
            var merged = existing.data
            for (key, value) in changed { merged[key] = value }
            try store.upsertSnapshot(
                db, stream: stream, rowId: id, shard: spec.shard,
                type: existing.type ?? type, data: merged
            )
        } else {
            let op = ReplicaOp(
                id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowCreate,
                stream: stream, rowId: id, type: type, data: data
            )
            try enqueueOp(db, op, store: store, preimage: ReplicaPreimage.absent.encoded(), lane: lane, draft: draft)
            // The journal carries only authored fields. The local birth keeps
            // required server-owned values supplied by the generated model.
            let birth = (snapshot ?? [:]).merging(data) { _, authored in authored }
            try store.upsertSnapshot(db, stream: stream, rowId: id, shard: spec.shard, type: type, data: birth)
        }
        return true
    }

    /// A delete in its own transaction, both lanes. A row the server never heard of
    /// (its create still pending) dies silently — every owed entry discarded,
    /// no delete op; anything else journals `row.delete`. Document lane also
    /// drops the fold and its superseded delta.
    @discardableResult
    public func deleteRow(stream: String, id: String) throws -> Bool {
        let store = try writableStore()
        let spec = try writableSpec(stream)
        let lane = ReplicaEngine.currentLane
        let draft = ReplicaEngine.currentDraft
        let queuedDelete = try store.pool.write { db in
            try applyRowDelete(db, store: store, spec: spec, stream: stream, id: id, lane: lane, draft: draft)
        }
        if spec.lane == .document {
            liveDocuments.evict(LiveDocuments.Key(stream: stream, id: id))
        }
        if queuedDelete { schedulePush() }
        return queuedDelete
    }

    /// Row snapshots and their journal changes commit together, or all roll back.
    func deleteRows(stream: String, ids: [String]) throws {
        let store = try writableStore()
        let spec = try writableSpec(stream)
        guard spec.lane == .row else { throw ReplicaError.laneMismatch(stream) }
        let lane = ReplicaEngine.currentLane
        let draft = ReplicaEngine.currentDraft
        let queuedDelete = try store.pool.write { db in
            var queued = false
            for id in ids {
                if try applyRowDelete(db, store: store, spec: spec, stream: stream, id: id, lane: lane, draft: draft) {
                    queued = true
                }
            }
            return queued
        }
        if queuedDelete { schedulePush() }
    }

    nonisolated func applyRowDelete(
        _ db: Database, store: ReplicaStateStore, spec: ReplicaStreamSpec,
        stream: String, id: String, lane: ReplicaLane, draft: String?
    ) throws -> Bool {
        try validateAtomicAddress(db, stream: stream, id: id, store: store)
        let incarnation = try store.incarnation(db, stream: stream, id: id)
        let births = try store.entriesAddressing(
            db, stream: stream, rowId: id, verb: ReplicaOp.Verb.rowCreate
        ).filter { try $0.op().incarnation == incarnation }
        // A refused birth is a definite answer; a frozen one may already be
        // committed server-side.
        let heardBirth = births.first { $0.parked == nil && $0.sent }
        let displaced = try store.snapshot(db, stream: stream, rowId: id)
        let heldDocument = spec.lane == .document
            ? try store.doc(db, stream: stream, rowId: id)
            : nil

        // Deleting a value that never existed is ordinary CRUD silence.
        guard displaced != nil || heldDocument != nil || !births.isEmpty else {
            return false
        }

        try store.deleteSnapshot(db, stream: stream, rowId: id)
        if spec.lane == .document {
            try store.deleteDoc(db, stream: stream, rowId: id)
        }

        if let heardBirth {
            // The create may already commit server-side. Keep it ordered
            // ahead of the delete, but drop every now-irrelevant patch or
            // delta addressed at the value.
            try store.discardLifetime(
                db, stream: stream, rowId: id, incarnation: incarnation, except: heardBirth.id
            )
        } else if !births.isEmpty {
            // The server never heard the birth: create + dependent work
            // collapse to nothing.
            try store.discardLifetime(db, stream: stream, rowId: id, incarnation: incarnation)
            try store.cancelUnsentBirth(db, stream: stream, id: id)
            return false
        } else if spec.lane == .document {
            try store.discardLifetime(db, stream: stream, rowId: id, incarnation: incarnation)
        }

        let op = ReplicaOp(
            id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowDelete,
            stream: stream, rowId: id
        )
        let preimage = displaced.map {
            ReplicaPreimage.row(shard: spec.shard, type: $0.type, data: $0.data)
        }
        try enqueueOp(db, op, store: store, preimage: try preimage?.encoded(), lane: lane, draft: draft)
        return true
    }

    // MARK: - Document lane (local writes)

    /// Birth the document: fold = seed, acked = nothing (the server knows
    /// nothing until the verdict), and the journaled `row.create` carrying
    /// codec + seed. `peer` is the seed's authoring peer — recorded so the
    /// app can keep authoring under it. The fields the document owns are
    /// read off the seed.
    @discardableResult
    public func createDoc(
        stream: String,
        id: String,
        seed: Data,
        peer: UInt64,
        data: [String: ReplicaValue] = [:]
    ) throws -> Bool {
        let store = try writableStore()
        let spec = try writableSpec(stream)
        guard spec.lane == .document else { throw ReplicaError.laneMismatch(stream) }
        let codecName = try codecName(for: spec)
        var rowData = stamped(data, stamp: spec.stamp, userId: binding.owner)
        guard let codec = codecs[codecName] else { throw ReplicaError.codec("no codec registered for \(codecName)") }
        let born = try codec.merge(fold: nil, payload: seed, reflecting: spec.reflections)
        rowData.merge(born.reflected) { _, reflected in reflected }
        let lane = ReplicaEngine.currentLane
        let draft = ReplicaEngine.currentDraft
        let inserted = try store.pool.write { db -> Bool in
            let existingBirths = try store.entriesAddressing(
                db, stream: stream, rowId: id, verb: ReplicaOp.Verb.rowCreate
            )
            guard try store.snapshot(db, stream: stream, rowId: id) == nil,
                  try store.doc(db, stream: stream, rowId: id) == nil,
                  existingBirths.isEmpty
            else {
                return false
            }

            let op = ReplicaOp(
                id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowCreate,
                stream: stream, rowId: id, codec: codecName, seed: seed
            )
            try enqueueOp(db, op, store: store, preimage: ReplicaPreimage.absent.encoded(), lane: lane, draft: draft)
            try store.upsertSnapshot(db, stream: stream, rowId: id, shard: spec.shard, type: nil, data: rowData)
            try store.upsertDoc(
                db, stream: stream, rowId: id, shard: spec.shard,
                codec: codecName, fold: seed, acked: nil, peer: peer
            )
            return true
        }
        if inserted { schedulePush() }
        return inserted
    }

    /// A local edit: merged into the fold, then SUPERSEDED into the one
    /// editable `doc.delta` per document — the entry's payload is always
    /// `diff(fold, since: acked)`, so consecutive edits fold into a single
    /// intent (its id and queue place stable). A frozen or refused delta is
    /// never rewritten: the new bytes become the next editable one.
    public func recordDocDelta(stream: String, id: String, payload: Data) throws {
        let store = try writableStore()
        let spec = try writableSpec(stream)
        guard spec.lane == .document else { throw ReplicaError.laneMismatch(stream) }
        let lane = ReplicaEngine.currentLane
        let draft = ReplicaEngine.currentDraft
        try store.pool.write { db in
            guard let doc = try store.doc(db, stream: stream, rowId: id) else {
                throw ReplicaError.unknownDocument(stream: stream, id: id)
            }
            guard let codec = codecs[doc.codec] else {
                throw ReplicaError.codec("no codec registered for \(doc.codec)")
            }
            let merged = try codec.merge(fold: doc.fold, payload: payload, reflecting: spec.reflections)
            try store.updateDoc(db, stream: stream, rowId: id, fold: merged.fold)
            let moved = merged.reflected.merging(touched(spec)) { _, clock in clock }
            try reflect(moved, db, stream: stream, id: id, shard: spec.shard, store: store)

            let owed = try codec.diff(fold: merged.fold, since: doc.acked)
            if codec.isEmptyDiff(owed) {
                try store.discardEditableDelta(db, stream: stream, rowId: id)
            } else {
                let op = ReplicaOp(
                    id: ReplicaID.ulid(), verb: ReplicaOp.Verb.docDelta,
                    stream: stream, rowId: id, codec: doc.codec, payload: owed
                )
                try enqueueOp(db, op, store: store, lane: lane, draft: draft)
            }
        }
        schedulePush()
    }

    /// The row's clock for a document that moved on this device — the
    /// engine's own provenance, like a create's.
    private func touched(_ spec: ReplicaStreamSpec) -> [String: ReplicaValue] {
        guard let field = spec.stamp?.updatedAt else { return [:] }
        return [field: .string(ISO8601DateFormatter().string(from: clock()))]
    }

    /// The fields its document owns, written into the row when they moved —
    /// in the transaction that moved the fold.
    func reflect(
        _ reflected: [String: ReplicaValue], _ db: Database,
        stream: String, id: String, shard: String, store: ReplicaStateStore
    ) throws {
        guard !reflected.isEmpty, let row = try store.snapshot(db, stream: stream, rowId: id) else { return }
        let data = row.data.merging(reflected) { _, reflected in reflected }
        guard data != row.data else { return }
        try store.upsertSnapshot(db, stream: stream, rowId: id, shard: shard, type: row.type, data: data)
    }


    private func stamped(
        _ data: [String: ReplicaValue],
        stamp: ReplicaStamp?,
        userId: Int?
    ) -> [String: ReplicaValue] {
        guard let stamp, let userId else { return data }

        var result = data
        let timestamp = ISO8601DateFormatter().string(from: clock())
        if let field = stamp.userId { result[field] = .signedInteger(Int64(userId)) }
        if let field = stamp.createdAt { result[field] = .string(timestamp) }
        if let field = stamp.updatedAt { result[field] = .string(timestamp) }
        return result
    }

    /// A local op: judged by the sync gates (`admit`), then journaled — or
    /// held on the device, or dropped.
    @discardableResult
    nonisolated func enqueueOp(
        _ db: Database, _ op: ReplicaOp, store: ReplicaStateStore, preimage: Data? = nil,
        lane requested: ReplicaLane, draft: String?
    ) throws -> ReplicaLane {
        let op = try store.identify(db, operation: op, schema: schema,
            birth: op.verb == ReplicaOp.Verb.rowCreate, preimage: preimage)
        if let tx = ReplicaTransaction.open(on: self), tx.atomicEntries != nil {
            try validateAtomicAdmission(db, op: op, preimage: preimage, store: store)
            let lane = try journal(db, op, store: store, preimage: preimage, lane: requested, draft: nil)
            tx.atomicEntries?.append(op.id)
            return lane
        }
        let key = try draftKey(db, for: op, store: store, scoped: draft)
        // A draft's writes are judged when it commits.
        guard try key != nil || admit(db, op, preimage: preimage, applied: false, store: store) else { return requested }
        return try journal(db, op, store: store, preimage: preimage, lane: requested, draft: key)
    }

    /// The journal write half of a local op: claim the lane, keep the two
    /// invariants that make overtaking safe, then enqueue.
    ///
    ///   STICKINESS — a row's later ops join the lane its pending ops are on.
    ///     Otherwise a bulk patch passes the interactive create of its own
    ///     row and the server refuses an update to a row it has never seen.
    ///   PROMOTION — an interactive op naming a pending bulk row pulls that
    ///     row (and what IT names, transitively) onto the interactive lane.
    ///     Otherwise attaching a clip whose element create is still queued
    ///     refuses with "unknown element", the entry parks, and the local row
    ///     reverts. Ids are unique minted strings, so matching op data values
    ///     against pending row ids needs no foreign-key schema; a false match
    ///     costs one row shipping sooner.
    ///
    /// The lane and the draft are the WRITER's, captured before it entered
    /// the transaction: the body runs on GRDB's writer, where the task-locals
    /// that name them may not be visible.
    @discardableResult
    nonisolated func journal(
        _ db: Database, _ op: ReplicaOp, store: ReplicaStateStore, preimage: Data?,
        lane requested: ReplicaLane, draft key: String?
    ) throws -> ReplicaLane {
        var op = try op.incarnation == nil
            ? store.identify(db, operation: op, schema: schema, preimage: preimage)
            : op
        let queued = try store.pendingEntries(db, stream: op.stream, rowId: op.rowId)
        var lane = queued.first?.lane ?? requested
        var alsoWalk: [ReplicaOp] = []
        if requested == .interactive, lane == .bulk {
            try store.promote(db, entryIds: queued.map(\.id))
            lane = .interactive
            // Those entries were pulled up by their ROW, so nothing has walked
            // what THEY name yet.
            alsoWalk = try queued.map { try ReplicaJSON.decoder().decode(ReplicaOp.self, from: $0.payload) }
        }
        if op.verb == ReplicaOp.Verb.docDelta, let editable = try store.editableDelta(db, stream: op.stream, rowId: op.rowId) {
            op.id = editable
            try store.supersede(db, id: editable, payload: Self.encode(op), preimage: preimage, lane: lane, draft: key)
        } else {
            try store.enqueue(db, id: op.id, verb: op.verb, stream: op.stream, rowId: op.rowId,
                              payload: Self.encode(op), preimage: preimage, lane: lane, draft: key)
        }
        if lane == .interactive { try promoteDependencies(db, of: [op] + alsoWalk, store: store) }
        return lane
    }

    /// The key this op is held under: the enclosing `beginDraft` scope, or —
    /// outside any scope — the draft of a row it addresses or NAMES (the
    /// chat message for a draft project, the delta of a draft document): a
    /// dependent never ships ahead of its parent's birth, and dies with it.
    private nonisolated func draftKey(
        _ db: Database, for op: ReplicaOp, store: ReplicaStateStore, scoped: String?
    ) throws -> String? {
        if let scoped { return scoped }
        var named: Set<String> = [op.rowId]
        for value in (op.data ?? [:]).values { Self.namedIds(in: value, into: &named) }
        return try store.draftKey(db, rowIds: Array(named))
    }

    /// Every id a value mentions, at any depth — a row reference can sit
    /// inside an array or a nested document (`Cook.data`), not only in a
    /// top-level string column.
    private nonisolated static func namedIds(in value: ReplicaValue, into found: inout Set<String>) {
        switch value {
        case .string(let text): found.insert(text)
        case .array(let items): for item in items { namedIds(in: item, into: &found) }
        case .object(let fields): for field in fields.values { namedIds(in: field, into: &found) }
        default: break
        }
    }

    private nonisolated func promoteDependencies(
        _ db: Database, of ops: [ReplicaOp], store: ReplicaStateStore
    ) throws {
        var frontier = ops
        var promoted: [String] = []
        var visited: Set<String> = []
        while let current = frontier.popLast() {
            var named: Set<String> = []
            for value in (current.data ?? [:]).values {
                Self.namedIds(in: value, into: &named)
            }
            let fresh = named.subtracting(visited)
            guard !fresh.isEmpty else { continue }
            visited.formUnion(fresh)
            for entry in try store.pendingBulkEntries(db, rowIds: Array(fresh)) {
                let entryOp = try entry.op()
                promoted.append(entry.id)
                // Transitive: what the dependency itself names must come with
                // it, or the chain breaks one link further down.
                frontier.append(entryOp)
            }
        }
        try store.promote(db, entryIds: promoted)
    }

    /// CRUD commits schedule their own delivery, PER LANE — a bulk backlog
    /// stuck on the wire must not hold an interactive write behind it. One
    /// task owns each lane's queue and drains its sendable work. A gate
    /// waits for a new write or lifecycle wake; a transport failure leaves
    /// the durable entries for the foreground/reconnect lifecycle.
    /// Something was written — kick whichever lanes now owe work. The entry
    /// may have landed on a lane the caller did NOT request (stickiness joins
    /// its row's lane; promotion pulls dependencies up), so the caller's own
    /// lane is not a reliable answer to "what needs draining".
    func schedulePush() {
        guard automaticallyPushWrites, !sealed,
              let store = binding.store
        else { return }
        let owed: Set<ReplicaLane>
        do {
            owed = try store.pool.read { db in try store.lanesOwed(db) }
        } catch {
            // Scheduling is a notification after the local commit. A failed
            // queue read must be observable without undoing that saved write.
            health.record(error, operation: "schedule automatic delivery")
            Log.logger.error("[push] could not read the owed lanes — no push scheduled: \(String(describing: error), privacy: .public)")
            return
        }
        for lane in owed { schedulePush(lane) }
    }

    private func schedulePush(_ lane: ReplicaLane) {
        guard automaticallyPushWrites,
              !sealed,
              binding.store != nil
        else { return }
        if scheduledPushes[lane] != nil {
            scheduledPushReruns.insert(lane)
            return
        }
        scheduledPushes[lane] = Task { [weak self] in
            await Task.yield()
            guard !Task.isCancelled else { return }
            await self?.pushScheduledWrites(lane)
        }
    }

    private func pushScheduledWrites(_ lane: ReplicaLane) async {
        defer {
            // Identity teardown already canceled and forgot this task. Its
            // late completion must not clear a new owner's scheduled task.
            if !Task.isCancelled {
                scheduledPushes[lane] = nil
                scheduledPushReruns.remove(lane)
            }
        }
        repeat {
            scheduledPushReruns.remove(lane)
            guard !Task.isCancelled, !isCold(lane), !sealed, binding.store != nil else { return }
            do {
                _ = try await drain(lane)
            } catch {
                // Background delivery has no awaiter. Health reports the error;
                // the frozen outbox remains durable for an explicit retry.
                health.record(error, operation: "automatic push")
                Log.logger.error("[push] scheduled drain failed — pending entries stay for the next lifecycle: \(error.localizedDescription, privacy: .public)")
                return
            }
            // This check and ownership release have no suspension between
            // them: an interleaving write either set this bit or will own a
            // fresh scheduled task. A gated row stays durable without spinning.
        } while scheduledPushReruns.contains(lane)
    }

    private func isCold(_ lane: ReplicaLane) -> Bool {
        coldUntil[lane].map { clock() < $0 } ?? false
    }

    func isColdForTesting(_ lane: ReplicaLane) -> Bool { isCold(lane) }

    /// Synchronous on purpose (pool reads never touch actor state): the doc
    /// plane loads folds on the main actor without a hop.
    public nonisolated func docFold(stream: String, id: String) throws -> Data? {
        guard let store = binding.store else { return nil }
        return try store.pool.read { try store.doc($0, stream: stream, rowId: id)?.fold }
    }

    public nonisolated func docPeer(stream: String, id: String) throws -> UInt64? {
        guard let store = binding.store else { return nil }
        return try store.pool.read { try store.doc($0, stream: stream, rowId: id)?.peer }
    }

    static let maxDrainPasses = 16

    // MARK: - Journal / drain

    /// The whole pending queue through the transport in order, verdicts
    /// applied to the frozen intents by operation — bytes that superseded
    /// one mid-flight are an intent of their own and wait for their own
    /// drain. Transport failure marks the wire cold, applies nothing, and
    /// leaves every entry pending (retryable).
    /// Both lanes, interactive first — the compatibility path (lifecycle
    /// wake-ups, tests, the identity fence) where "everything owed" is meant.
    @discardableResult
    public func drain() async throws -> [ReplicaVerdict] {
        guard binding.store != nil else { return [] }
        var verdicts = try await drain(.interactive)
        verdicts += try await drain(.bulk)
        return verdicts
    }

    /// Choose this lane's unfrozen work after the frozen prefix.
    @discardableResult
    public func drain(_ lane: ReplicaLane) async throws -> [ReplicaVerdict] {
        guard binding.store != nil else { return [] }
        guard !sealed else { throw ReplicaError.identityTransitionInProgress }
        var joined: [ReplicaVerdict] = []
        while let (activeLane, flight) = activeDrains.first {
            let verdicts = try await awaitDrain(flight)
            if activeLane == lane { joined += verdicts }
            guard !sealed else { throw ReplicaError.identityTransitionInProgress }
        }

        // The flight is claimed BEFORE any suspension — a second drain
        // arriving while this one reads the journal must find `activeDrain`
        // set and await it, or the same entry goes to the wire twice (and a
        // double-sent create comes back as a collision that reverts the row).
        let flight = Task { [weak self] () throws -> [ReplicaVerdict] in
            guard let self else { return [] }
            return try await self.performDrain(lane: lane)
        }
        activeDrains[lane] = flight
        return joined + (try await awaitDrain(flight))
    }

    private func awaitDrain(_ flight: Task<[ReplicaVerdict], Error>) async throws -> [ReplicaVerdict] {
        try await withTaskCancellationHandler {
            let verdicts = try await flight.value
            try Task.checkCancellation()
            return verdicts
        } onCancel: {
            // Frozen submissions remain durable when the shared request stops.
            flight.cancel()
        }
    }

    /// `lane: nil` means EVERY lane — the sign-out flush: whatever the journal
    /// holds, before the file closes; what stays drains at the next sign-in.
    private func performDrain(
        lane: ReplicaLane? = .bulk,
        using selectedTransport: (any ReplicaTransport)? = nil,
        sealedFlush: Bool = false
    ) async throws -> [ReplicaVerdict] {
        let store: ReplicaStateStore
        do {
            if sealedFlush {
                guard sealed, activeWireOperations == 0, let bound = binding.store else {
                    throw ReplicaError.identityTransitionRequired
                }
                store = bound
                activeWireOperations += 1
            } else {
                store = try beginWireOperation()
            }
        } catch {
            // `drain()` claimed the single-flight task before suspending. If a
            // seal won the actor between that claim and this task's first
            // turn, release the claim as well as refusing the wire.
            activeDrains[lane ?? .bulk] = nil
            throw error
        }
        defer {
            activeDrains[lane ?? .bulk] = nil
            endWireOperation()
        }
        do {
            let verdicts = try await transmitSubmissions(
                store: store, lane: lane, transport: selectedTransport ?? transport
            )
            coldUntil.removeAll()
            return verdicts
        } catch {
            coldUntil.removeAll()
            if Self.connectionFailed(error) {
                let until = clock().addingTimeInterval(coldWindow)
                for priority in ReplicaLane.allCases { coldUntil[priority] = until }
            }
            throw error
        }
    }

    private static func connectionFailed(_ error: any Error) -> Bool {
        if case ReplicaError.transport = error { return true }
        if let url = error as? URLError { return url.code != .cancelled }
        return false
    }

    /// The barrier variant: skips while the wire is known-cold — offline
    /// must not stack timeouts. Network failures are reported through health;
    /// storage, protocol and cancellation failures propagate. Explicit `drain()` never skips.
    public func drainIfWarm() async throws {
        // Both priorities share the frozen prefix. Once that
        // prefix fails, this barrier must not retry it through another lane.
        for lane in ReplicaLane.allCases where !isCold(lane) {
            do {
                _ = try await drain(lane)
            } catch {
                guard Self.connectionFailed(error), !Task.isCancelled else { throw error }

                // A failed connection does not prevent independently receiving
                // remote changes. Local bytes stay queued and health reports it.
                health.record(error, operation: "push before pull")
                Log.logger.warning("[drain] warm drain of \(String(describing: lane), privacy: .public) failed: \(String(describing: error), privacy: .public)")
                return
            }
        }
    }

    // MARK: - Sync gate

    /// Host-supplied at INIT — like a cache normalizer. Asked when a row is
    /// WRITTEN, never on a drain: a held row waits in `holds` until its gate
    /// fires `changes`, the row is written again, or the store opens.
    /// Birth-only by design: the engine self-drains from its first write, so
    /// a registration door would race the first judge.
    nonisolated let syncGates: SyncGates

    /// The GC read seam: string values of `fields` across every journal
    /// entry (pending AND parked) of `stream`. With rows' own fields — held
    /// rows among them — this is the app's provable keeping-set for staged
    /// bytes.
    public func pendingFieldStrings(stream: String, fields: [String]) throws -> [String] {
        guard let store = binding.store else { return [] }
        let rows = try store.pool.read { db in
            try store.entriesForStream(db, stream: stream)
        }
        return try rows.flatMap { entry -> [String] in
            guard let data = try entry.op().data else { return [] }
            return fields.compactMap { data[$0]?.string }.filter { !$0.isEmpty }
        }
    }

    /// Row ids on `stream` the device still owes: any journal entry (pending
    /// or parked), or a gate's hold — the row half of the app's keeping-set.
    public func pendingRowIds(stream: String) throws -> [String] {
        guard let store = binding.store else { return [] }
        return try store.pool.read { db in
            try store.entriesForStream(db, stream: stream).map { try $0.op().rowId }
                + store.heldRowIds(db, stream: stream)
        }
    }

    /// Every row a gate holds, in the order the holds began.
    public nonisolated func heldRows() throws -> [ReplicaStateStore.GateHold] {
        guard let store = binding.store else { return [] }
        return try store.pool.read { db in try store.holds(db, gateId: nil) }
    }

    /// The row as application reads see it — the base with local work over
    /// it — of any stream; nil when the store holds no row at the address.
    public nonisolated func snapshotRow(stream: String, id: String) throws -> ReplicaStateStore.SnapshotRow? {
        guard let store = binding.store else { return nil }
        return try store.pool.read { db in try store.snapshot(db, stream: stream, rowId: id) }
    }

    /// The current lifetime of each address; an address the store has never
    /// seen is absent.
    public nonisolated func incarnations(stream: String, ids: [String]) throws -> [String: String] {
        guard let store = binding.store else { return [:] }
        return try store.pool.read { db in
            var lifetimes: [String: String] = [:]
            for id in ids {
                lifetimes[id] = try store.incarnation(db, stream: stream, id: id)
            }
            return lifetimes
        }
    }

    /// The parent lifetimes the row's local writes name — a held row's too.
    public nonisolated func references(stream: String, id: String) throws -> [ReplicaReference] {
        guard let store = binding.store else { return [] }
        return try store.pool.read { db in try store.references(db, stream: stream, id: id) }
    }

    /// Let held rows go unsent — the discard path: a project thrown away
    /// before the server heard of it must not come back when its gate opens.
    /// A row waiting behind one goes with it: it could only be refused.
    public func discardHolds(stream: String, rowIds: [String]) throws {
        let store = try writableStore()
        try store.pool.write { db in
            var dropping = rowIds.map { (stream: stream, rowId: $0) }
            while let row = dropping.popLast() {
                try store.dropHold(db, stream: row.stream, rowId: row.rowId)
                dropping += try store.holds(db, gateId: Self.waitGate(row.stream, row.rowId))
                    .map { (stream: $0.stream, rowId: $0.rowId) }
            }
        }
    }

    /// One listener per gate for the life of the engine.
    private nonisolated let gateWatches = GateWatches()

    final class GateWatches: @unchecked Sendable {
        private let lock = NSLock()
        private var tasks: [Task<Void, Never>] = []

        func append(_ task: Task<Void, Never>) { lock.withLock { tasks.append(task) } }

        func cancelAll() { lock.withLock { tasks }.forEach { $0.cancel() } }
    }

    deinit { gateWatches.cancelAll() }

    /// Test seam: what an open does over a store bound by the test init —
    /// every hold asked again.
    func settle() throws {
        try refreshSyncGates()
    }

    /// Explicit reevaluation propagates failure and leaves durable holds intact.
    public func refreshSyncGates(_ id: String? = nil) throws {
        let store = try writableStore()
        let gate = id.flatMap { id in syncGates.all.first { $0.id == id } }
        let event = id.map { "\($0) changed" } ?? "refresh"
        try store.pool.write { db in
            let asked = try store.holds(db, gateId: id)
            if let gate { try self.holdJournal(db, judgedBy: gate, store: store) }
            try self.release(db, asked, event: event, store: store)
        }
        schedulePush()
    }

    /// A gate said its answer may have moved — or the store opened (nil). A
    /// gate that can now hold what it let go takes those rows off the
    /// journal; the holds it had are asked again.
    private func gateChanged(_ id: String?) {
        guard binding.store != nil, !sealed else { return }
        do {
            try refreshSyncGates(id)
        } catch {
            // Gate notifications cannot return an error. The transaction rolled
            // back, so holds remain intact; surface the failure to the host.
            health.record(error, operation: "reevaluate synchronization gates")
            Log.logger.error("[gate] reevaluation failed; holds remain for retry: \(String(describing: error), privacy: .public)")
        }
    }

    /// At open, off the opener's path: every hold is asked again — a gate's
    /// answer may have moved while the process was gone.
    private nonisolated func askHoldsAgain() {
        guard !syncGates.isEmpty else { return }
        Task { [weak self] in await self?.gateChanged(nil) }
    }

    private nonisolated func release(
        _ db: Database, _ holds: [ReplicaStateStore.GateHold], event: String, store: ReplicaStateStore
    ) throws {
        guard !holds.isEmpty else { return }
        var released = 0
        for hold in holds where try rejudge(db, hold, store: store) { released += 1 }
        Log.logger.info("[gate] \(event, privacy: .public): released \(released, privacy: .public), holds \(holds.count - released, privacy: .public)")
    }

    /// The gate half of a local op, inside its write transaction: whether the
    /// op joins the journal at all.
    /// - a held row journals nothing: its state is judged again — with the
    ///   op, when the row does not carry it yet (`applied`) — and leaves as
    ///   that state when its gates let it go;
    /// - a write naming a held row waits behind it — the server refuses a
    ///   child whose parent it does not know;
    /// - otherwise the change is judged: push journals it, discard drops it,
    ///   a hold records the row in `holds`.
    private nonisolated func holdBaseline(
        _ db: Database, stream: String, rowId: String, undoing images: [Data], store: ReplicaStateStore
    ) throws -> Data {
        let row = try store.snapshot(db, stream: stream, rowId: rowId)
        let baseline: ReplicaPreimage = row.map { .row(shard: schema.spec(stream)?.shard ?? "user", type: $0.type, data: $0.data) } ?? .absent
        return try baseline.undoing(images.map(ReplicaPreimage.decode)).encoded()
    }

    nonisolated func admit(
        _ db: Database, _ op: ReplicaOp, preimage: Data?, applied: Bool, store: ReplicaStateStore
    ) throws -> Bool {
        guard !syncGates.isEmpty else { return true }
        if let held = try store.hold(db, stream: op.stream, rowId: op.rowId) {
            try rejudge(db, held, store: store, landing: applied ? nil : op)
            return false
        }
        let knows = op.verb != ReplicaOp.Verb.rowCreate
        if let parent = try heldParent(db, of: op.rowId, data: op.data ?? [:], before: nil, store: store) {
            try hold(db, op.stream, op.rowId, behind: parent, serverKnows: knows, preimage: holdBaseline(db, stream: op.stream, rowId: op.rowId, undoing: preimage.map { [$0] } ?? [], store: store), store: store)
            return false
        }
        switch syncGates.judge(try Self.change(op, preimage: preimage)) {
        case .push:
            return true
        case .discard where !knows:
            Log.logger.error("[gate] a birth was discarded — sent anyway, every later write stands on it: \(op.stream, privacy: .public)/\(op.rowId, privacy: .public)")
            return true
        case .discard:
            return false
        case .hold(let gate, let reason):
            try hold(db, op.stream, op.rowId, gate: gate, reason: reason, serverKnows: knows, preimage: holdBaseline(db, stream: op.stream, rowId: op.rowId, undoing: preimage.map { [$0] } ?? [], store: store), store: store)
            return false
        }
    }

    private nonisolated func hold(
        _ db: Database, _ stream: String, _ rowId: String, gate: String, reason: String,
        serverKnows: Bool, seq: Int64? = nil, preimage: Data, store: ReplicaStateStore
    ) throws {
        try store.insertHold(db, stream: stream, rowId: rowId, gateId: gate, reason: reason,
                             serverKnows: serverKnows, seq: seq, preimage: preimage)
        Log.logger.info("[gate] hold \(stream, privacy: .public)/\(rowId, privacy: .public): \(reason, privacy: .public)")
    }

    /// A row naming a held row waits behind it, under that row's own wait
    /// gate: it is asked again the moment that row leaves its hold.
    private nonisolated func hold(
        _ db: Database, _ stream: String, _ rowId: String, behind parent: ReplicaStateStore.GateHold,
        serverKnows: Bool, seq: Int64? = nil, preimage: Data, store: ReplicaStateStore
    ) throws {
        try hold(db, stream, rowId, gate: Self.waitGate(parent.stream, parent.rowId),
                 reason: "waits for \(parent.stream)/\(parent.rowId)", serverKnows: serverKnows, seq: seq, preimage: preimage, store: store)
    }

    nonisolated static func waitGate(_ stream: String, _ rowId: String) -> String {
        "row:\(stream)/\(rowId)"
    }

    /// The earliest held row this row's data names — only one held BEFORE
    /// it, so two rows naming each other never wait on each other, and never
    /// one a policy over every stream holds.
    private nonisolated func heldParent(
        _ db: Database, of rowId: String, data: [String: ReplicaValue], before seq: Int64?, store: ReplicaStateStore
    ) throws -> ReplicaStateStore.GateHold? {
        var named: Set<String> = []
        for value in data.values { Self.namedIds(in: value, into: &named) }
        named.remove(rowId)
        for parent in try store.holds(db, rowIds: Array(named)) {
            if let seq, parent.seq >= seq { return nil }
            if syncGates.orders(parent.gateId) { return parent }
        }
        return nil
    }

    /// Ask a held row's gates again; when it leaves its hold, the rows
    /// waiting behind it are asked next. Returns whether it left.
    @discardableResult
    private nonisolated func rejudge(
        _ db: Database, _ asked: ReplicaStateStore.GateHold, store: ReplicaStateStore, landing op: ReplicaOp? = nil
    ) throws -> Bool {
        guard try askAgain(db, asked, store: store, landing: op) else { return false }
        var behind = try store.holds(db, gateId: Self.waitGate(asked.stream, asked.rowId))
        while !behind.isEmpty {
            let next = behind.removeFirst()
            if try askAgain(db, next, store: store, landing: nil) {
                behind += try store.holds(db, gateId: Self.waitGate(next.stream, next.rowId))
            }
        }
        return true
    }

    /// One held row, judged on its CURRENT state — `landing` is the write
    /// committing now, which its row does not carry yet. Push releases it:
    /// its state joins the journal as one op.
    private nonisolated func askAgain(
        _ db: Database, _ asked: ReplicaStateStore.GateHold, store: ReplicaStateStore, landing op: ReplicaOp?
    ) throws -> Bool {
        guard let held = try store.hold(db, stream: asked.stream, rowId: asked.rowId) else { return false }
        let existing = try store.snapshot(db, stream: held.stream, rowId: held.rowId)
        var current = existing?.data
        if let op {
            switch op.verb {
            case ReplicaOp.Verb.rowDelete: current = nil
            case ReplicaOp.Verb.rowCreate: current = op.data ?? [:]
            case ReplicaOp.Verb.rowPatch: current = (current ?? [:]).merging(op.data ?? [:]) { _, landed in landed }
            default: break
            }
        }
        guard current != nil || held.serverKnows else {
            try store.cancelUnsentBirth(db, stream: held.stream, id: held.rowId)
            try store.dropHold(db, stream: held.stream, rowId: held.rowId)
            Log.logger.info("[gate] drop \(held.stream, privacy: .public)/\(held.rowId, privacy: .public): gone before the server heard of it")
            return true
        }
        if let current, let parent = try heldParent(db, of: held.rowId, data: current, before: held.seq, store: store) {
            let gate = Self.waitGate(parent.stream, parent.rowId)
            if gate != held.gateId {
                try store.updateHold(db, stream: held.stream, rowId: held.rowId, gateId: gate,
                                     reason: "waits for \(parent.stream)/\(parent.rowId)")
            }
            return false
        }
        let document = schema.spec(held.stream)?.lane == .document
        let kind: SyncChange.Kind = switch (current, document, held.serverKnows) {
        case (nil, _, _): .delete
        case (_, true, true): .document
        case (_, _, true): .patch
        default: .create
        }
        let local = document ? [:] : current ?? [:]
        var outcome = syncGates.judge(SyncChange(stream: held.stream, rowId: held.rowId, kind: kind, local: local))
        if outcome == .discard, kind == .create {
            Log.logger.error("[gate] a birth was discarded — sent anyway, every later write stands on it: \(held.stream, privacy: .public)/\(held.rowId, privacy: .public)")
            outcome = .push
        }
        switch outcome {
        case .hold(let gate, let reason):
            if gate != held.gateId || reason != held.reason {
                try store.updateHold(db, stream: held.stream, rowId: held.rowId, gateId: gate, reason: reason)
            }
            return false
        case .discard:
            try store.dropHold(db, stream: held.stream, rowId: held.rowId)
            Log.logger.info("[gate] drop \(held.stream, privacy: .public)/\(held.rowId, privacy: .public): its gate discards it")
            return true
        case .push:
            try store.dropHold(db, stream: held.stream, rowId: held.rowId)
            try journalRelease(db, held, current: current, type: existing?.type ?? op?.type, document: document, store: store)
            return true
        }
    }

    /// A released row leaves as its state, never as the history of its
    /// writes: a birth the server never heard, a patch of every field the
    /// device may send, a delete, or the document's delta past what the
    /// server acked.
    private nonisolated func journalRelease(
        _ db: Database, _ held: ReplicaStateStore.GateHold, current: [String: ReplicaValue]?, type: String?,
        document: Bool, store: ReplicaStateStore
    ) throws {
        let op: ReplicaOp
        if current != nil, document {
            guard let doc = try store.doc(db, stream: held.stream, rowId: held.rowId) else {
                throw ReplicaError.unknownDocument(stream: held.stream, id: held.rowId)
            }
            guard let codec = codecs[doc.codec] else { throw ReplicaError.codec("no codec registered for \(doc.codec)") }
            if held.serverKnows {
                let owed = try codec.diff(fold: doc.fold, since: doc.acked)
                guard !codec.isEmptyDiff(owed) else {
                    Log.logger.info("[gate] release \(held.stream, privacy: .public)/\(held.rowId, privacy: .public): nothing owed")
                    return
                }
                op = ReplicaOp(id: ReplicaID.ulid(), verb: ReplicaOp.Verb.docDelta,
                               stream: held.stream, rowId: held.rowId, codec: doc.codec, payload: owed)
            } else {
                op = ReplicaOp(id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowCreate,
                               stream: held.stream, rowId: held.rowId, codec: doc.codec, seed: doc.fold)
            }
        } else if let current {
            let sendable = schema.spec(held.stream)?.pushed.map { pushed in current.filter { pushed.contains($0.key) } } ?? current
            op = held.serverKnows
                ? ReplicaOp(id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowPatch,
                            stream: held.stream, rowId: held.rowId, data: sendable)
                : ReplicaOp(id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowCreate,
                            stream: held.stream, rowId: held.rowId, type: type, data: sendable)
        } else {
            op = ReplicaOp(id: ReplicaID.ulid(), verb: ReplicaOp.Verb.rowDelete, stream: held.stream, rowId: held.rowId)
        }
        let preimage = try ReplicaPreimage.decode(held.preimage).forRelease(op).encoded()
        try journal(db, op, store: store, preimage: preimage, lane: .bulk, draft: nil)
        Log.logger.info("[gate] release \(held.stream, privacy: .public)/\(held.rowId, privacy: .public) as \(op.verb, privacy: .public)")
    }

    /// The journal judged again by a gate that fired. An owed entry its row
    /// can no longer send (backup turned off) moves into `holds`, with every
    /// later entry of that row, and so does an entry naming a row this moved
    /// before the server heard of it. A frozen intent stays — the server may
    /// already hold it. The rows moved go ahead of every earlier hold, in
    /// journal order: their writes are older, and a row held earlier may name
    /// one of them.
    private nonisolated func holdJournal(
        _ db: Database, judgedBy gate: any SyncGate, store: ReplicaStateStore
    ) throws {
        let entries = try store.owed(db, stream: gate.stream)
        let earlier = try store.firstHoldSeq(db) ?? 1
        var next = earlier - Int64(entries.count)
        var moved = 0
        let operations = try entries.map { try $0.op() }
        for (index, entry) in entries.enumerated() {
            let op = operations[index]
            let birth = op.verb == ReplicaOp.Verb.rowCreate
            if try store.hold(db, stream: op.stream, rowId: op.rowId) != nil {
                if birth { try store.markServerUnaware(db, stream: op.stream, rowId: op.rowId) }
                try store.discard(db, id: entry.id)
                continue
            }
            let undo = entries.indices.filter { $0 >= index && operations[$0].stream == op.stream && operations[$0].rowId == op.rowId }
                .compactMap { entries[$0].preimage }
            let baseline = try holdBaseline(db, stream: op.stream, rowId: op.rowId, undoing: undo, store: store)
            switch gate.judge(try Self.change(op, preimage: entry.preimage)) {
            case .push:
                guard let parent = try heldParent(db, of: op.rowId, data: op.data ?? [:], before: earlier, store: store)
                else { continue }
                try hold(db, op.stream, op.rowId, behind: parent, serverKnows: !birth, seq: next, preimage: baseline, store: store)
            case .discard:
                guard !birth else { continue }
            case .gate(let reason):
                try hold(db, op.stream, op.rowId, gate: gate.id, reason: reason, serverKnows: !birth, seq: next, preimage: baseline, store: store)
            }
            next += 1
            moved += 1
            try store.discard(db, id: entry.id)
        }
        guard moved > 0 else { return }
        Log.logger.info("[gate] \(gate.id, privacy: .public): \(moved, privacy: .public) owed entries left the journal")
    }

    /// The change a journaled op carries, as a gate sees it — the displaced
    /// row is a delete's `previous`.
    nonisolated static func change(_ op: ReplicaOp, preimage: Data?) throws -> SyncChange {
        let displaced = try preimage.map(ReplicaPreimage.decode)
        switch op.verb {
        case ReplicaOp.Verb.rowDelete:
            guard case .row(_, _, let data)? = displaced else {
                return SyncChange(stream: op.stream, rowId: op.rowId, kind: .delete)
            }
            return SyncChange(stream: op.stream, rowId: op.rowId, kind: .delete, previous: data)
        case ReplicaOp.Verb.docDelta:
            return SyncChange(stream: op.stream, rowId: op.rowId, kind: .document)
        case ReplicaOp.Verb.rowPatch:
            guard case .fields(let values, _)? = displaced else {
                return SyncChange(stream: op.stream, rowId: op.rowId, kind: .patch, local: op.data ?? [:])
            }
            return SyncChange(stream: op.stream, rowId: op.rowId, kind: .patch, local: op.data ?? [:], previous: values)
        default:
            return SyncChange(stream: op.stream, rowId: op.rowId, kind: .create, local: op.data ?? [:])
        }
    }

    /// Undo later local writes first, refuse this write, then replay the
    /// surviving suffix. Otherwise refusing an earlier patch erases a later
    /// one, or leaves that later patch holding an obsolete rollback image.
    func rejectRow(
        _ db: Database, entry: ReplicaStateStore.JournalRow, op: ReplicaOp, reason: String,
        store: ReplicaStateStore
    ) throws -> Int {
        if op.verb == ReplicaOp.Verb.rowCreate {
            // A refused birth also invalidates its later edits. Preserve the
            // whole local branch before rollback removes those dependents.
            try store.archiveEntity(db, stream: op.stream, id: op.rowId, reason: reason)
        }
        let owed = try store.entriesAddressing(db, stream: op.stream, rowId: op.rowId).filter { $0.parked == nil }
        guard let index = owed.firstIndex(where: { $0.id == entry.id && $0.payload == entry.payload }) else {
            throw ReplicaError.storage("A frozen intent is missing from its address")
        }
        let current = owed[index]
        let later = Array(owed[(index + 1)...])
        for pending in later.reversed() {
            if let raw = pending.preimage {
                let preimage = try ReplicaPreimage.decode(raw)
                try revert(db, op: pending.op(), preimage: preimage, store: store, cascading: false)
            }
        }
        try store.refuse(db, id: entry.id, reason: reason)
        if let raw = current.preimage {
            let preimage = try ReplicaPreimage.decode(raw)
            try revert(db, op: op, preimage: preimage, store: store)
        }
        // A refused unborn create discarded its dependent entries. Re-read
        // survivors so that replay cannot bring those children back.
        let survivors = try later.compactMap { try store.entry(db, id: $0.id, payload: $0.payload) }
        try rebaseOwedWrites(db, stream: op.stream, rowId: op.rowId,
                             shard: schema.spec(op.stream)?.shard ?? "user", store: store,
                             entries: survivors)
        return current.preimage == nil ? 0 : 1
    }

    /// The undo, inside the verdict transaction.
    private func revert(
        _ db: Database, op: ReplicaOp, preimage: ReplicaPreimage, store: ReplicaStateStore,
        cascading: Bool = true
    ) throws {
        switch (op.verb, preimage) {
        case (ReplicaOp.Verb.rowCreate, .absent):
            try store.deleteSnapshot(db, stream: op.stream, rowId: op.rowId)
            guard cascading else { break }
            if schema.lane(of: op.stream) == .document {
                try store.deleteDoc(db, stream: op.stream, rowId: op.rowId)
            }
            // A refused birth takes every dependent patch/delta/delete with it
            // on either lane, and the hold its later writes wait in. Its own
            // parked entry stays as the evidence.
            try store.discardEntries(
                db, stream: op.stream, rowId: op.rowId, except: op.id
            )
            try store.dropHold(db, stream: op.stream, rowId: op.rowId)
        case (ReplicaOp.Verb.rowPatch, .fields(let values, let missing)):
            guard let row = try store.snapshot(db, stream: op.stream, rowId: op.rowId) else { return }
            var data = row.data
            for (key, value) in values { data[key] = value }
            for key in missing { data.removeValue(forKey: key) }
            try store.upsertSnapshot(
                db, stream: op.stream, rowId: op.rowId,
                shard: schema.spec(op.stream)?.shard ?? "user", type: row.type, data: data
            )
        case (ReplicaOp.Verb.rowDelete, .row(let shard, let type, let data)),
             (ReplicaOp.Verb.rowCreate, .row(let shard, let type, let data)):
            try store.upsertSnapshot(db, stream: op.stream, rowId: op.rowId, shard: shard, type: type, data: data)
            if cascading, schema.lane(of: op.stream) == .document {
                // The row returns but the fold died with the local delete —
                // only a re-bootstrap can rebuild it.
                try store.clearCursor(db, shard: schema.spec(op.stream)?.shard ?? shard)
            }
        default:
            break
        }
    }

    func advanceAcked(_ db: Database, op: ReplicaOp, store: ReplicaStateStore) throws {
        guard let payload = op.payload ?? op.seed else { return }
        // A later local delete may already have removed this authoring fold.
        guard let doc = try store.doc(db, stream: op.stream, rowId: op.rowId) else { return }
        guard let codec = codecs[doc.codec] else {
            throw ReplicaError.codec("No codec registered for \(doc.codec)")
        }
        let acked = try codec.mergeVersions(doc.acked, codec.payloadVersion(payload))
        try store.updateDoc(db, stream: op.stream, rowId: op.rowId, acked: acked)
    }

    public func pendingOps() throws -> [ReplicaStateStore.JournalRow] {
        guard let store = binding.store else { return [] }
        return try store.pool.read { try store.pending($0) }
    }

    public func parkedOps() throws -> [ReplicaStateStore.JournalRow] {
        guard let store = binding.store else { return [] }
        return try store.pool.read { try store.parked($0) }
    }

    /// Abandon entries by id whatever their bytes or parked state — the
    /// discard path (a row thrown away before the server ever heard its id
    /// must stop owing anything). A frozen entry stays: the server may
    /// already hold its bytes, so only its own verdict settles it.
    public func discardOps(ids: [String]) throws {
        let store = try writableStore()
        try store.pool.write { db in
            for id in ids {
                try store.discard(db, id: id)
            }
        }
    }

    // MARK: - Watch

    /// Post-commit signal for one stream — ValueObservation over its durable
    /// `stream_meta.change_seq`, so consumers hang off committed state only:
    /// a rolled-back checkpoint never fires. Change-only is the default;
    /// state-owning consumers can request the committed baseline to close the
    /// observer-arming race.
    ///
    /// The signal outlives its store: a watcher armed before anyone owned the
    /// process, or held across a foreign switch, re-arms on the owner that
    /// arrives instead of observing a pool nobody writes to any more.
    public nonisolated func watchSignal(
        stream: String,
        includeInitial: Bool = false
    ) -> AsyncStream<Void> {
        let binding = binding
        return AsyncStream { continuation in
            let task = Task {
                // The baseline belongs to the OBSERVER, not to the store: a
                // watcher that already reported one world sees the next
                // owner's first picture as a CHANGE, not as another baseline.
                var armed = false
                while !Task.isCancelled {
                    let (bound, generation) = binding.snapshot()
                    guard let store = bound?.store else {
                        if includeInitial, !armed { continuation.yield(()) }
                        armed = true
                        await binding.waitForChange(after: generation)
                        continue
                    }
                    let observation = ValueObservation
                        .tracking { db in
                            try store.changeSequence(db, stream: stream)
                        }
                        .removeDuplicates()
                    let wasArmed = armed
                    armed = true
                    // RACED, not awaited: closing a pool does not always end
                    // its observations promptly, and a watcher left inside a
                    // dead one would signal for the outgoing owner forever.
                    await withTaskGroup(of: Void.self) { group in
                        group.addTask {
                            var baseline = true
                            do {
                                for try await _ in observation.values(in: store.pool) {
                                    guard baseline else {
                                        continuation.yield(())
                                        continue
                                    }
                                    baseline = false
                                    if wasArmed || includeInitial { continuation.yield(()) }
                                }
                            } catch {
                                // Cancellation and a replaced binding end this
                                // subscription normally. Other errors are health
                                // failures; do not pretend an empty stream is valid.
                                if !Task.isCancelled, binding.snapshot().generation == generation {
                                    self.health.record(error, operation: "observe \(stream)")
                                }
                                Log.logger.info("[watch] stream=\(stream, privacy: .public) observation ended — waiting for the next owner: \(error.localizedDescription, privacy: .public)")
                            }
                        }
                        group.addTask { await binding.waitForChange(after: generation) }
                        await group.next()
                        group.cancelAll()
                        await group.waitForAll()
                    }
                    // Whichever side won, the next arming waits for a store
                    // that is actually different: re-arming on the same dead
                    // pool would spin, and re-arming on a live one would
                    // duplicate the picture it is already delivering.
                    await binding.waitForChange(after: generation)
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// Post-commit VALUES of the journal's parked entries — the refusal
    /// ledger as a subscription. Same identity-rebinding loop as
    /// `watchSignal`, but the yielded array IS the state (baseline included):
    /// a consumer owns its dictionary by assignment, never by re-query, so a
    /// drain's park and a Re-run's discard both arrive as committed pictures
    /// in commit order — nothing to poll, nothing to coalesce.
    /// `removeDuplicates` rides `JournalRow: Equatable`.
    public nonisolated func watchParkedOps() -> AsyncStream<[ReplicaStateStore.JournalRow]> {
        let binding = binding
        return AsyncStream { continuation in
            let task = Task {
                while !Task.isCancelled {
                    let (bound, generation) = binding.snapshot()
                    guard let store = bound?.store else {
                        // No owner: the committed picture is "nothing parked".
                        continuation.yield([])
                        await binding.waitForChange(after: generation)
                        continue
                    }
                    let observation = ValueObservation
                        .tracking { db in try store.parked(db) }
                        .removeDuplicates()
                    await withTaskGroup(of: Void.self) { group in
                        group.addTask {
                            do {
                                for try await parked in observation.values(in: store.pool) {
                                    continuation.yield(parked)
                                }
                            } catch {
                                // Owner changes cancel the old subscription. A
                                // live owner's query error must reach health.
                                if !Task.isCancelled, binding.snapshot().generation == generation {
                                    self.health.record(error, operation: "observe refused mutations")
                                }
                                Log.logger.info("[watch] parked observation ended — waiting for the next owner: \(error.localizedDescription, privacy: .public)")
                            }
                        }
                        group.addTask { await binding.waitForChange(after: generation) }
                        await group.next()
                        group.cancelAll()
                        await group.waitForAll()
                    }
                    await binding.waitForChange(after: generation)
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    // MARK: - Plumbing

    nonisolated func writableSpec(_ stream: String) throws -> ReplicaStreamSpec {
        guard let spec = schema.spec(stream) else { throw ReplicaError.unknownStream(stream) }
        guard !spec.readonly else { throw ReplicaError.readonlyStream(stream) }
        guard documentMode != .projectionsOnly || spec.lane != .document else { throw ReplicaError.readonlyStream(stream) }
        return spec
    }

    nonisolated func codecName(for spec: ReplicaStreamSpec) throws -> String {
        if let name = spec.codec { return name }
        if codecs.count == 1, let only = codecs.keys.first { return only }
        throw ReplicaError.codec("stream \(spec.name) declares no codec")
    }

    private static func encode(_ op: ReplicaOp) throws -> Data {
        try ReplicaJSON.encoder().encode(op)
    }

    // MARK: - Test seams

    func setCheckpointFault(_ fault: (@Sendable () throws -> Void)?) {
        checkpointFault = fault
    }
}
