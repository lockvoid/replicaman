import ReplicaManTestProtocol
import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

// MARK: - Stub transport

/// Canned wire: scripted pull responses per shard, scripted push verdicts,
/// injectable failures, and a full event log — matrix assertions about
/// ORDER (drain-before-pull) and COUNT (cold window, knock coalescing) read
/// the log.
actor StubTransport: FixtureTransport {
    let protocolFixture = ProtocolFixture()
    enum Event: Equatable {
        case pull(shard: String, cursor: String?)
        case push(ids: [String])
    }

    private(set) var events: [Event] = []
    private(set) var pushedBatches: [[ReplicaOp]] = []
    private var pullQueues: [String: [ReplicaPullResponse]] = [:]
    private var pushScript: (@Sendable ([ReplicaOp]) -> [ReplicaVerdict])?
    private var pullFails = false
    private var pushFails = false
    private var pushFailure: ReplicaError?
    private var pushSuccessBudget: Int?
    private var pullDelayNanos: UInt64 = 0
    private var pushDelayNanos: UInt64 = 0

    func queuePull(shard: String, _ response: ReplicaPullResponse) {
        pullQueues[shard, default: []].append(response)
    }

    func scriptPush(_ script: @escaping @Sendable ([ReplicaOp]) -> [ReplicaVerdict]) {
        pushScript = script
    }

    func failPulls(_ fail: Bool) { pullFails = fail }
    func failPushes(_ fail: Bool) { pushFails = fail }
    /// Every push fails with `error` — the server refusing the request (HTTP
    /// 400 for one operation of the batch) or a 5xx, as the HTTP transport reports them.
    func failPushes(throwing error: ReplicaError?) { pushFailure = error }
    /// Succeed the first `calls` pushes, then fail — the chunked-drain
    /// mid-flight transport death.
    func failPushesAfter(calls: Int) { pushSuccessBudget = calls }
    /// Awaited inside `push` BEFORE verdicts return — the observation seam
    /// for "what had already happened when this chunk went on the wire"
    /// (the incremental-ack contract: earlier chunks' entries must be gone
    /// from the journal by now).
    func onPush(_ hook: @escaping @Sendable ([ReplicaOp]) async -> Void) { pushHook = hook }
    private var pushHook: (@Sendable ([ReplicaOp]) async -> Void)?
    /// Awaited inside `pull` after the request is observable but before its
    /// response is returned. Identity-boundary tests use it to hold one
    /// outgoing-bearer request on the wire deterministically.
    func onPull(_ hook: @escaping @Sendable (String) async -> Void) { pullHook = hook }
    private var pullHook: (@Sendable (String) async -> Void)?
    func delayPulls(nanos: UInt64) { pullDelayNanos = nanos }
    func delayPushes(nanos: UInt64) { pushDelayNanos = nanos }

    var pullCount: Int { events.filter { if case .pull = $0 { return true } else { return false } }.count }
    var pushCount: Int { events.filter { if case .push = $0 { return true } else { return false } }.count }

    func pull(shard: String, cursor: String?, limit: Int) async throws -> ReplicaPullResponse {
        events.append(.pull(shard: shard, cursor: cursor))
        if pullFails { throw ReplicaError.transport("pull refused (stub)") }
        await pullHook?(shard)
        if pullDelayNanos > 0 { try await Task.sleep(nanoseconds: pullDelayNanos) }
        if var queue = pullQueues[shard], !queue.isEmpty {
            let response = queue.removeFirst()
            pullQueues[shard] = queue
            return response
        }
        return ReplicaPullResponse(frames: [], cursor: cursor ?? "0:", more: false)
    }

    /// Every op that reached the wire, in push order.
    func pushedOps() -> [ReplicaOp] { pushedBatches.flatMap { $0 } }

    func push(_ ops: [ReplicaOp]) async throws -> [ReplicaVerdict] {
        events.append(.push(ids: ops.map(\.id)))
        if pushFails { throw ReplicaError.transport("push refused (stub)") }
        if let pushFailure { throw pushFailure }
        if let budget = pushSuccessBudget {
            guard budget > 0 else { throw ReplicaError.transport("push budget exhausted (stub)") }
            pushSuccessBudget = budget - 1
        }
        await pushHook?(ops)
        pushedBatches.append(ops)
        if pushDelayNanos > 0 { try await Task.sleep(nanoseconds: pushDelayNanos) }
        if let pushScript { return pushScript(ops) }
        return ops.map { ReplicaVerdict(id: $0.id, outcome: .accepted) }
    }
}

// MARK: - Stub codec (loro-free doc plumbing)

/// Enough codec to exercise the ENGINE's document plumbing without loro:
/// fold = concatenated payloads, version = byte count. The real merge
/// semantics live in `ReplicaManLoroTests`; this one keeps the core suite
/// rows-only (matrix 12).
struct StubCodec: ReplicaCodec {
    let name = "stub@1"

    func merge(fold: Data?, payload: Data, reflecting reflections: [ReplicaReflection]) throws -> ReplicaMerge {
        ReplicaMerge(fold: (fold ?? Data()) + payload, reflected: [:])
    }

    func diff(fold: Data, since version: Data?) throws -> Data {
        let skip = version.map { Int(String(decoding: $0, as: UTF8.self)) ?? 0 } ?? 0
        return Data(fold.dropFirst(skip))
    }

    func version(fold: Data) throws -> Data {
        Data("\(fold.count)".utf8)
    }

    func payloadVersion(_ payload: Data) throws -> Data {
        Data("\(payload.count)".utf8)
    }

    func mergeVersions(_ a: Data?, _ b: Data) throws -> Data {
        let x = a.map { Int(String(decoding: $0, as: UTF8.self)) ?? 0 } ?? 0
        let y = Int(String(decoding: b, as: UTF8.self)) ?? 0
        return Data("\(max(x, y))".utf8)
    }

    func isEmptyDiff(_ payload: Data) -> Bool {
        payload.isEmpty
    }
}

// MARK: - Causal codec (loro's causal refusal, in miniature)

/// Folds and payloads are sets of tokens `dN`, and `dN` depends on `d(N-1)`
/// the way one Loro peer's consecutive changes do. A merge whose result lacks
/// a dependency refuses with `missingCausalDeps`, as `LoroReplicaCodec` does.
struct CausalCodec: ReplicaCodec {
    let name = "causal@1"

    static func tokens(_ data: Data?) -> Set<Int> {
        Set(String(decoding: data ?? Data(), as: UTF8.self).split(separator: ",").compactMap { Int($0.dropFirst()) })
    }

    static func encode(_ tokens: Set<Int>) -> Data {
        Data(tokens.sorted().map { "d\($0)" }.joined(separator: ",").utf8)
    }

    static func payload(_ range: ClosedRange<Int>) -> Data {
        encode(Set(range))
    }

    func merge(fold: Data?, payload: Data, reflecting reflections: [ReplicaReflection]) throws -> ReplicaMerge {
        let union = Self.tokens(fold).union(Self.tokens(payload))
        for token in Self.tokens(payload) where token > 1 && !union.contains(token - 1) {
            throw ReplicaError.missingCausalDeps
        }
        return ReplicaMerge(fold: Self.encode(union), reflected: [:])
    }

    func diff(fold: Data, since version: Data?) throws -> Data {
        Self.encode(Self.tokens(fold).subtracting(Self.tokens(version)))
    }

    func version(fold: Data) throws -> Data { fold }
    func payloadVersion(_ payload: Data) throws -> Data { payload }
    func mergeVersions(_ a: Data?, _ b: Data) throws -> Data { Self.encode(Self.tokens(a).union(Self.tokens(b))) }
    func isEmptyDiff(_ payload: Data) -> Bool { Self.tokens(payload).isEmpty }
}

/// Opened by sync code, awaited by async code — nothing ever holds a thread of
/// the cooperative pool waiting on it (a held one starves the engine and the
/// transport the test is interleaving).
final class Latch: @unchecked Sendable {
    private let lock = NSLock()
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    var opened: Bool { lock.withLock { isOpen } }

    func open() {
        lock.lock()
        isOpen = true
        let woken = waiters
        waiters.removeAll()
        lock.unlock()
        for waiter in woken { waiter.resume() }
    }

    func wait() async {
        await withCheckedContinuation { continuation in
            lock.lock()
            guard !isOpen else {
                lock.unlock()
                continuation.resume()
                return
            }
            waiters.append(continuation)
            lock.unlock()
        }
    }
}

// MARK: - Typed test model (hand-written; codegen's shape in miniature)

struct TestNote: ReplicaWritableRowModel, Equatable {
    static let streamName = "notes"

    var id: String
    var title: String?
    var rank: String?

    init(id: String, title: String? = nil, rank: String? = nil) {
        self.id = id
        self.title = title
        self.rank = rank
    }

    init?(id: String, type: String?, data: [String: ReplicaValue]) {
        guard type == nil || type == "Note" else { return nil }
        self.id = id
        self.title = data["title"]?.string
        self.rank = data["rank"]?.string
    }

    var typeName: String? { nil }

    func encode() -> [String: ReplicaValue] {
        var data: [String: ReplicaValue] = [:]
        if let title { data["title"] = .string(title) }
        if let rank { data["rank"] = .string(rank) }
        return data
    }
}

// MARK: - Fixtures

enum Fixture {
    /// notes: writable row · boards: document (stub codec) · jobs: readonly
    /// row · assets: row on the global shard.
    static func schema(boardStamp: ReplicaStamp? = nil) -> ReplicaSchema {
        ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "notes", lane: .row, shard: "user"),
            ReplicaStreamSpec(name: "boards", lane: .document, shard: "user", codec: "stub@1", stamp: boardStamp),
            ReplicaStreamSpec(name: "jobs", lane: .row, readonly: true, shard: "user"),
            ReplicaStreamSpec(name: "assets", lane: .row, shard: "global"),
        ])
    }

    /// notes: writable row · boards: document under the causal codec.
    static let causalSchema = ReplicaSchema(streams: [
        ReplicaStreamSpec(name: "notes", lane: .row, shard: "user"),
        ReplicaStreamSpec(name: "boards", lane: .document, shard: "user", codec: "causal@1"),
    ])

    static func store(_ name: String = #function, indexes: [ReplicaIndexSpec] = []) throws -> ReplicaStateStore {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("replica-man-tests", isDirectory: true)
            .appendingPathComponent("\(name)-\(UUID().uuidString).sqlite").path
        return try ReplicaStateStore(path: path, indexes: indexes)
    }

    /// A per-test home for per-owner files — the shape the app host uses.
    static func directory(_ name: String = #function) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("replica-man-tests", isDirectory: true)
            .appendingPathComponent("\(name)-\(UUID().uuidString)", isDirectory: true)
    }

    /// An engine that owns its own files: nothing is bound until `open(owner:)`.
    static func unopenedEngine(
        in directory: URL,
        transport: StubTransport,
        schema: ReplicaSchema = schema(),
        codecs: [any ReplicaCodec] = [StubCodec()],
        coldWindow: TimeInterval = 10,
        automaticallyPushWrites: Bool = false,
        syncGates: [any SyncGate] = [],
        documentMode: ReplicaDocumentMode = .replicated
    ) -> ReplicaEngine {
        ReplicaEngine(
            home: directory,
            transport: transport,
            schema: schema,
            codecs: codecs,
            coldWindow: coldWindow,
            peerMinter: sequentialMinter(),
            automaticallyPushWrites: automaticallyPushWrites,
            syncGates: syncGates,
            documentMode: documentMode
        )
    }

    /// Deterministic peer minter: 100, 101, 102…
    static func sequentialMinter(from start: UInt64 = 100) -> @Sendable () -> UInt64 {
        let counter = Counter(start)
        return { counter.next() }
    }

    final class Counter: @unchecked Sendable {
        private var value: UInt64
        private let lock = NSLock()

        init(_ start: UInt64) { value = start }

        func next() -> UInt64 {
            lock.lock()
            defer { lock.unlock() }
            defer { value += 1 }
            return value
        }
    }

    /// The suite's owner: every fixture engine is bound to user 42, which is
    /// what a create stamps.
    static let owner = 42

    /// `automaticallyPushWrites` is PRODUCTION-`true` (`ReplicaEngine.swift:429`).
    /// This fixture defaulted it to `false` unconditionally, which hid the
    /// production default from every test in the package — no suite ever
    /// judged a self-scheduled push. It is a
    /// parameter now: suites that grade an explicit `drain()` keep `false` so
    /// their pending counts stay deterministic; the suites that grade the
    /// engine's OWN delivery (`GateWakeTests`, `ColdBootDrainTests`) pass
    /// `true` and never call `drain()` at all.
    static func engine(
        store: ReplicaStateStore,
        owner: Int = Fixture.owner,
        transport: StubTransport,
        schema: ReplicaSchema = schema(),
        codecs: [any ReplicaCodec] = [StubCodec()],
        coldWindow: TimeInterval = 10,
        minter: (@Sendable () -> UInt64)? = nil,
        automaticallyPushWrites: Bool = false,
        syncGates: [any SyncGate] = [],
        documentMode: ReplicaDocumentMode = .replicated
    ) -> ReplicaEngine {
        ReplicaEngine(
            store: store,
            owner: owner,
            transport: transport,
            schema: schema,
            codecs: codecs,
            coldWindow: coldWindow,
            peerMinter: minter ?? sequentialMinter(),
            automaticallyPushWrites: automaticallyPushWrites,
            syncGates: syncGates,
            documentMode: documentMode
        )
    }

    static func note(_ id: String, title: String, rank: String? = nil) -> ReplicaFrame {
        var data: [String: ReplicaValue] = ["title": .string(title)]
        if let rank { data["rank"] = .string(rank) }
        return .rowSet(stream: "notes", id: id, type: nil, data: data)
    }
}

/// A thread-safe event tally for observation tests.
final class Tally: @unchecked Sendable {
    private var value = 0
    private let lock = NSLock()

    func bump() {
        lock.lock()
        value += 1
        lock.unlock()
    }

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}

// MARK: - Store peeks (raw truth assertions)

extension ReplicaStateStore {
    func allSnapshots() throws -> [SnapshotRow] {
        try pool.read { db in
            try Row.fetchAll(db, sql: "SELECT stream, row_id, type, data FROM snapshots ORDER BY stream, row_id").map {
                SnapshotRow(stream: $0["stream"], rowId: $0["row_id"], type: $0["type"], data: try Self.decodeData($0["data"]))
            }
        }
    }

    func peekSnapshot(_ stream: String, _ rowId: String) throws -> SnapshotRow? {
        try pool.read { try self.snapshot($0, stream: stream, rowId: rowId) }
    }

    func peekDoc(_ stream: String, _ rowId: String) throws -> DocRow? {
        try pool.read { try self.doc($0, stream: stream, rowId: rowId) }
    }

    func peekPending() throws -> [JournalRow] {
        try pool.read { try self.pending($0) }
    }

    func peekParked() throws -> [JournalRow] {
        try pool.read { try self.parked($0) }
    }

    /// Entries held back as a draft — never on the wire until released.
    func peekDrafted() throws -> [JournalRow] {
        try pool.read { try self.drafted($0) }
    }
}

// MARK: - Async polling

/// Bounded wait for an async condition — the suite's hang defense: every
/// wait has a deadline, a missed one fails the test instead of wedging the
/// runner.
func eventually(
    timeout: TimeInterval = 3,
    _ message: @autoclosure () -> String = "condition not met in time",
    file: StaticString = #filePath,
    line: UInt = #line,
    _ condition: () async throws -> Bool
) async rethrows {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if try await condition() { return }
        do {
            try await Task.sleep(nanoseconds: 20_000_000)
        } catch {
            // Cancellation terminates this bounded test wait. Record a failure
            // so cancellation cannot turn an unmet condition into a green test.
            XCTFail("Polling was interrupted: \(error)", file: file, line: line)
            return
        }
    }
    XCTFail(message(), file: file, line: line)
}

// MARK: - Bounded polling for Swift Testing suites

/// A bounded wait that FAILS BY THROWING instead of by `XCTFail`, so Swift
/// Testing suites can use it. FLEET_LAW ban #5 allows "bounded polls with a
/// named reason"; `reason` is that name and it is required, not optional.
///
/// This is never a substitute for a deterministic seam. Where the engine
/// offers one — `StubTransport.onPush` / `onPull`, a watch baseline, GRDB's
/// `afterNextTransaction` — the seam is used and this helper is not.
struct PollTimeout: Error, CustomStringConvertible {
    let reason: String
    var description: String { "condition never held: \(reason)" }
}

func until(
    _ reason: String,
    timeout: TimeInterval = 5,
    _ condition: () async throws -> Bool
) async throws {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if try await condition() { return }
        try await Task.sleep(nanoseconds: 5_000_000)
    }
    throw PollTimeout(reason: reason)
}

// MARK: - Deterministic release ledger (the byte plane's predicate, in miniature)

/// The mutable set a sync gate consults — `BlobManager.uploadState(forKey:)`
/// in miniature. Landing a key fires the gate's signal, the way
/// `markUploadLanded` does: the engine asks the gate's holds again.
final class ReleaseLedger: @unchecked Sendable {
    private let lock = NSLock()
    private var keys: Set<String> = []
    let signal = SyncGateSignal()

    init(released: [String] = []) { keys = Set(released) }

    func land(_ key: String) {
        lock.withLock { _ = keys.insert(key) }
        signal.fire()
    }

    /// Lands without the signal — what a process that died mid-landing
    /// leaves behind for the next open.
    func landQuietly(_ key: String) {
        lock.withLock { _ = keys.insert(key) }
    }

    func contains(_ key: String) -> Bool {
        lock.withLock { keys.contains(key) }
    }
}

/// A gate written as a closure — what a test needs, nothing more.
struct TestGate: SyncGate {
    let id: String
    let stream: String?
    let signal: SyncGateSignal?
    let verdict: @Sendable (SyncChange) -> SyncVerdict

    init(_ stream: String? = "notes", id: String = "test", signal: SyncGateSignal? = nil,
         _ verdict: @escaping @Sendable (SyncChange) -> SyncVerdict) {
        self.id = id
        self.stream = stream
        self.signal = signal
        self.verdict = verdict
    }

    func judge(_ change: SyncChange) -> SyncVerdict { verdict(change) }

    var changes: AsyncStream<Void> { signal?.stream ?? AsyncStream { $0.finish() } }
}

/// Hold the row while its `blob` names bytes that have not landed — the
/// `brand_kits` / `project_speakers` / `agent_attachments` shape.
func blobGate(_ released: ReleaseLedger) -> TestGate {
    TestGate(id: "blob", signal: released.signal) { change in
        if let key = change.local["blob"]?.string, !key.isEmpty, !released.contains(key) {
            return .gate("blob \(key) in flight")
        }
        return .push
    }
}

/// Hold the WHOLE row until its key has landed — the `cooks` shape.
func gateWholeRowGate(_ released: ReleaseLedger) -> TestGate {
    TestGate(id: "cook", signal: released.signal) { change in
        released.contains(change.rowId) ? .push : .gate("row \(change.rowId) waits for its bytes")
    }
}

/// The Cloud Backup shape: every stream, one flag, flipped by the host.
final class BackupFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var on: Bool
    let signal = SyncGateSignal()

    init(on: Bool) { self.on = on }

    var isOn: Bool { lock.withLock { on } }

    func set(_ value: Bool) {
        lock.withLock { on = value }
        signal.fire()
    }

    var gate: TestGate {
        TestGate(nil, id: "backup", signal: signal) { [self] _ in isOn ? .push : .gate("cloud backup off") }
    }
}

/// Counts every judge — the proof a drain asks no gate.
final class JudgeCount: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    var value: Int { lock.withLock { count } }

    func tick() { lock.withLock { count += 1 } }
}

/// Captures an error raised inside a NON-throwing transport hook so the test
/// can assert it never happened. Swallowing it with `try?` is a hidden green
/// (FLEET_LAW ban #4), and a hook is the one place a test is tempted to.
actor HookOutcome {
    private(set) var failure: String?

    func record(_ error: Error) { failure = "\(error)" }
}
