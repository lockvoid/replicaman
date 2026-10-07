import Foundation
import GRDB
import os

/// ReplicaMan's durable state — one sqlite file via GRDB (`DatabasePool`,
/// WAL). This is the app's PRIMARY database: `snapshots` is the client's raw
/// truth for both lanes; any native projection (SwiftData, phase 4) is
/// rebuildable FROM it and never written by app code (the write-ban
/// contract). Losing the journal loses the user's unsent work; losing the
/// cursor forces a re-snapshot — which is why they live here together and
/// why `reset` wipes never touch the journal.
///
/// The pool is deliberately public: GRDB is the raw-query escape hatch below
/// the generated verbs (codegen guarantees table/column names), and
/// `ValueObservation` over it backs `watch()`. The WRITE side stays
/// engine-private by contract: app code reads; only the engine writes.
public final class ReplicaStateStore: Sendable {
    let pool: DatabasePool
    public var reader: any DatabaseReader { pool }
    /// Where this store was opened. The merge renames the file under a live
    /// pool, so the CURRENT path is the binding's to track — this is only the
    /// starting point.
    public let path: URL
    private let rowCache = DecodedRowCache()
    private let authoringLease: ReplicaStoreLease

    /// Release the file. Live `ValueObservation`s over this pool end with an
    /// error, which is the point: an observation must never keep serving a
    /// world whose owner is gone.
    /// SQLite's own advice: refresh planner statistics on the way out, so
    /// the partial indexes are chosen on stats, not on default estimates.
    public func close() throws {
        try pool.close()
        try authoringLease.release()
    }

    /// Everything sqlite keeps for one store: the database and its WAL
    /// sidecars. A retired owner leaves none of the three behind.
    public static func remove(at path: URL) throws {
        for suffix in ["", "-wal", "-shm"] {
            do {
                try FileManager.default.removeItem(atPath: path.path + suffix)
            } catch CocoaError.fileNoSuchFile {
                // A WAL or shared-memory sidecar may already have been removed
                // by SQLite during close. The requested absence is satisfied.
                continue
            }
        }
    }

    /// The read structures this store keeps over `snapshots.data` — the
    /// manifest's `indexes:`, converged at open (`reconcileIndexes`).
    public let indexes: [ReplicaIndexSpec]

    public init(path: String, indexes: [ReplicaIndexSpec] = []) throws {
        try FileManager.default.createDirectory(
            at: URL(fileURLWithPath: path).deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        self.path = URL(fileURLWithPath: path)
        self.indexes = indexes
        authoringLease = try ReplicaStoreLease(path: path)
        pool = try DatabasePool(path: path)
        // GRDB's WAL setup selects NORMAL. Local writes are primary data, so
        // commit must sync the WAL before we report them as saved.
        try pool.writeWithoutTransaction { db in
            try db.execute(sql: "PRAGMA synchronous = FULL; PRAGMA fullfsync = ON")
        }
        try pool.write { db in
            try Self.prepareSynchronization(db)
            try Self.reconcileIndexes(db, declared: indexes)
            // A reopened or copied store starts a new authoring lease. Existing
            // history remains, but new edits use a fresh CRDT peer counter range.
            try db.execute(sql: "UPDATE docs SET peer = ?", arguments: [Int64(bitPattern: ReplicaID.peer())])
        }
    }

    // MARK: - Index reconcile

    /// Declared ↔ actual convergence for the read structures, at every open,
    /// in the open transaction, before the first reader: a missing generated
    /// column / btree / fts5 table is created (fts rebuilt from the rows on
    /// disk), a stale one dropped. Idempotent — a converged open executes no
    /// DDL and logs nothing. There are no migrations and no versions: the
    /// manifest is the schema, and an older build simply ignores structures
    /// it did not declare. Every owned name carries its prefix (`ix_`,
    /// `idx_`, `fts_`), so the shared layout's own indexes are never in scope.
    private static func reconcileIndexes(_ db: Database, declared: [ReplicaIndexSpec]) throws {
        let started = Date()
        var log: [String] = []
        let escape = { (name: String) in name.replacingOccurrences(of: "'", with: "''") }

        let wantedColumns = Dictionary(declared.map { ($0.column, $0.field) }, uniquingKeysWith: { first, _ in first })
        let wantedBtree = Set(declared.filter { $0.kind == .btree }.map(\.btreeIndex))
        let wantedFts = Set(declared.filter { $0.kind == .fts5 }.map(\.ftsTable))
        let wantedTriggers = Set(wantedFts.flatMap { [$0 + "_ai", $0 + "_ad", $0 + "_au"] })

        let existingColumns = Set(try String.fetchAll(
            db, sql: "SELECT name FROM pragma_table_xinfo('snapshots') WHERE name LIKE 'ix\\_%' ESCAPE '\\'"
        ))
        let existingBtree = Set(try String.fetchAll(
            db, sql: "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx\\_%' ESCAPE '\\'"
        ))
        let existingFts = Set(try String.fetchAll(
            db, sql: "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'fts\\_%' ESCAPE '\\' AND sql LIKE 'CREATE VIRTUAL TABLE%'"
        ))
        let existingTriggers = Set(try String.fetchAll(
            db, sql: "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'fts\\_%' ESCAPE '\\'"
        ))

        // Stale first, dependents before what they depend on.
        for trigger in existingTriggers.subtracting(wantedTriggers).sorted() {
            try db.execute(sql: "DROP TRIGGER \"\(trigger)\"")
        }
        for table in existingFts.subtracting(wantedFts).sorted() {
            try db.execute(sql: "DROP TABLE \"\(table)\"")
            log.append("-\(table)")
        }
        for index in existingBtree.subtracting(wantedBtree).sorted() {
            try db.execute(sql: "DROP INDEX \"\(index)\"")
            log.append("-\(index)")
        }
        for column in existingColumns.subtracting(wantedColumns.keys).sorted() {
            try db.execute(sql: "ALTER TABLE snapshots DROP COLUMN \"\(column)\"")
            log.append("-\(column)")
        }

        // Then the missing ones.
        for (column, field) in wantedColumns.sorted(by: { $0.key < $1.key }) where !existingColumns.contains(column) {
            try db.execute(sql: """
                ALTER TABLE snapshots ADD COLUMN "\(column)"
                GENERATED ALWAYS AS (json_extract(data, '$.\(escape(field))')) VIRTUAL
                """)
            log.append("+\(column)")
        }
        var createdBtree: Set<String> = []
        for spec in declared where spec.kind == .btree && !existingBtree.contains(spec.btreeIndex) && createdBtree.insert(spec.btreeIndex).inserted {
            try db.execute(sql: "CREATE INDEX \"\(spec.btreeIndex)\" ON snapshots(stream, \"\(spec.column)\")")
            log.append("+\(spec.btreeIndex)")
        }
        for spec in declared where spec.kind == .fts5 {
            let table = spec.ftsTable
            let stream = escape(spec.stream)
            let value = "json_extract(new.data, '$.\(escape(spec.field))')"
            if !existingFts.contains(table) {
                try db.execute(sql: """
                    CREATE VIRTUAL TABLE "\(table)"
                    USING fts5(row_id UNINDEXED, value, tokenize = 'unicode61 remove_diacritics 2')
                    """)
                try db.execute(sql: """
                    INSERT INTO "\(table)"(row_id, value)
                    SELECT row_id, json_extract(data, '$.\(escape(spec.field))') FROM snapshots WHERE stream = '\(stream)'
                    """)
                log.append("+\(table) (\(db.changesCount) rows)")
            }
            try db.execute(sql: """
                CREATE TRIGGER IF NOT EXISTS "\(table)_ai" AFTER INSERT ON snapshots WHEN new.stream = '\(stream)'
                BEGIN INSERT INTO "\(table)"(row_id, value) VALUES (new.row_id, \(value)); END
                """)
            try db.execute(sql: """
                CREATE TRIGGER IF NOT EXISTS "\(table)_ad" AFTER DELETE ON snapshots WHEN old.stream = '\(stream)'
                BEGIN DELETE FROM "\(table)" WHERE row_id = old.row_id; END
                """)
            try db.execute(sql: """
                CREATE TRIGGER IF NOT EXISTS "\(table)_au" AFTER UPDATE OF data ON snapshots WHEN new.stream = '\(stream)'
                BEGIN
                    DELETE FROM "\(table)" WHERE row_id = old.row_id;
                    INSERT INTO "\(table)"(row_id, value) VALUES (new.row_id, \(value));
                END
                """)
        }

        guard !log.isEmpty else { return }
        let ms = Int(Date().timeIntervalSince(started) * 1000)
        Log.logger.info("[index] reconcile \(log.joined(separator: " "), privacy: .public) in \(ms)ms")
    }

    // MARK: - Document mode

    func requireDocumentMode(_ mode: ReplicaDocumentMode) throws {
        let requested = mode == .replicated ? "replicated" : "projections"
        try pool.write { db in
            try db.execute(sql: "UPDATE meta SET document_mode = ? WHERE id = 1 AND document_mode IS NULL", arguments: [requested])
            let actual = try String.fetchOne(db, sql: "SELECT document_mode FROM meta WHERE id = 1")
            guard actual == requested else {
                throw ReplicaError.storage("Document mode belongs to the store. Use a separate store for projection-only replicas.")
            }
        }
    }

    // MARK: - Snapshots (raw truth, both lanes)

    public struct SnapshotRow: Sendable, Equatable {
        public var stream: String
        public var rowId: String
        public var type: String?
        public var data: [String: ReplicaValue]

        public init(stream: String, rowId: String, type: String?, data: [String: ReplicaValue]) {
            self.stream = stream
            self.rowId = rowId
            self.type = type
            self.data = data
        }
    }

    struct RowRecord: Sendable, Equatable {
        var id: String
        var type: String?
        /// The snapshot's `data` TEXT exactly as stored — the per-row reuse
        /// key. Comparing it is a memcmp; re-decoding it is the cost the
        /// donor mechanism exists to avoid.
        var raw: String?
        var fields: [String: ReplicaValue]
    }

    struct MaterializedRow<Model: Sendable>: Sendable {
        var record: RowRecord
        var model: Model
    }

    struct RowMaterialization<Model: Sendable>: Sendable {
        var rows: [MaterializedRow<Model>]
        var byKey: [String: Model]
    }

    static func decodeData(_ json: String?) throws -> [String: ReplicaValue] {
        guard let json else { throw ReplicaError.storage("Missing snapshot data") }
        return try ReplicaValueJSON.decodeObject(json)
    }

    static func encodeData(_ data: [String: ReplicaValue]) throws -> String {
        String(decoding: try ReplicaJSON.encoder().encode(data), as: UTF8.self)
    }

    func materializedRows<Model: Sendable>(
        stream: String,
        minimumSequence: Int64? = nil,
        decode: @escaping @Sendable (String, String?, [String: ReplicaValue]) -> Model?
    ) throws -> RowMaterialization<Model> {
        while true {
            if let materialization: RowMaterialization<Model> = rowCache.materialization(
                stream: stream,
                model: Model.self,
                minimumSequence: minimumSequence
            ) {
                return materialization
            }

            if rowCache.rawRows(stream: stream, minimumSequence: minimumSequence) != nil {
                let materialization: RowMaterialization<Model>? = rowCache.withMaterializationLock(
                    stream: stream,
                    model: Model.self
                ) {
                    if let existing: RowMaterialization<Model> = rowCache.materialization(
                        stream: stream,
                        model: Model.self,
                        minimumSequence: minimumSequence
                    ) {
                        return existing
                    }
                    guard let raw = rowCache.rawRows(
                        stream: stream,
                        minimumSequence: minimumSequence
                    ) else { return nil }
                    // Per-row model reuse: a row whose raw text + type match
                    // the donor's decodes nothing — its model carries over.
                    // The check is self-validating (compared against the
                    // donor that HOLDS the model), so a donor superseded
                    // mid-flight merely reduces reuse, never poisons it.
                    let donor: (records: [String: RowRecord], models: [String: Model])? =
                        rowCache.donorSnapshot(stream: stream, model: Model.self)
                    let models = raw.records.compactMap { record -> MaterializedRow<Model>? in
                        if let prior = donor?.records[record.id],
                           prior.raw == record.raw, prior.type == record.type,
                           let model = donor?.models[record.id] {
                            return MaterializedRow(record: record, model: model)
                        }
                        return decode(record.id, record.type, record.fields).map {
                            MaterializedRow(record: record, model: $0)
                        }
                    }
                    let built = RowMaterialization(
                        rows: models,
                        byKey: Dictionary(uniqueKeysWithValues: models.map { ($0.record.id, $0.model) })
                    )
                    return rowCache.installMaterialization(
                        built,
                        stream: stream,
                        model: Model.self,
                        generation: raw.generation,
                        sequence: raw.sequence
                    )
                }
                if let materialization { return materialization }
                continue
            }

            let generation = rowCache.generation(stream: stream)
            // Field-tree reuse on the cold load itself: unchanged rows keep
            // their decoded `fields` from the donor — only changed raw text
            // pays `decodeData`.
            let donorRecords = rowCache.donorRecords(stream: stream)
            let loaded = try pool.read { db -> (Int64, [RowRecord]) in
                let sequence = try changeSequence(db, stream: stream)
                let records = try Row.fetchAll(
                    db,
                    sql: "SELECT row_id, type, data FROM snapshots WHERE stream = ? ORDER BY row_id",
                    arguments: [stream]
                ).map { row -> RowRecord in
                    let id: String = row["row_id"]
                    let type: String? = row["type"]
                    let raw: String? = row["data"]
                    if let prior = donorRecords?[id], prior.raw == raw, prior.type == type {
                        return prior
                    }
                    return RowRecord(
                        id: id,
                        type: type,
                        raw: raw,
                        fields: try Self.decodeData(raw)
                    )
                }
                return (sequence, records)
            }
            _ = rowCache.installRawRows(
                stream: stream,
                generation: generation,
                sequence: loaded.0,
                records: loaded.1
            )
        }
    }

    /// The warm model for `id` when this type is materialized at the live
    /// sequence; nil otherwise — never builds.
    func warmModel<Model: Sendable>(stream: String, id: String) -> Model? {
        let materialization: RowMaterialization<Model>? =
            rowCache.materialization(stream: stream, model: Model.self, minimumSequence: nil)
        return materialization?.byKey[id]
    }

    /// The indexed read: only the rows the WHERE fragment admits are
    /// fetched; each reuses the stream's live decoded record — and the
    /// model, when this type is materialized — whenever its raw text and
    /// type are unchanged, so a scoped read after a whole-stream one decodes
    /// nothing. Not cached as a picture: the subset is small by construction
    /// and the SQL is index-served.
    func scopedRows<Model: Sendable>(
        stream: String,
        where condition: String,
        arguments: StatementArguments,
        orderBy: String = "row_id",
        limit: Int? = nil,
        decode: @escaping @Sendable (String, String?, [String: ReplicaValue]) -> Model?
    ) throws -> [Model] {
        let sources: (records: [String: RowRecord], models: [String: Model]) =
            rowCache.reuseSources(stream: stream, model: Model.self)
        var bound = StatementArguments([stream])
        bound += arguments
        let limited = limit.map { " LIMIT \($0)" } ?? ""
        return try pool.read { db in
            try Row.fetchAll(
                db,
                sql: "SELECT row_id, type, data FROM snapshots WHERE stream = ? AND (\(condition)) ORDER BY \(orderBy)\(limited)",
                arguments: bound
            ).compactMap { row -> Model? in
                let id: String = row["row_id"]
                let type: String? = row["type"]
                let raw: String? = row["data"]
                if let prior = sources.records[id], prior.raw == raw, prior.type == type {
                    if let model = sources.models[id] { return model }
                    return decode(id, type, prior.fields)
                }
                return decode(id, type, try Self.decodeData(raw))
            }
        }
    }

    /// The scoped read inside an open transaction: the rows as this
    /// transaction sees them, its own writes included. No cache — a decision
    /// reads the store, never a picture.
    func snapshots(
        _ db: Database, stream: String, where condition: String, arguments: StatementArguments,
        orderBy: String = "row_id", limit: Int? = nil
    ) throws -> [SnapshotRow] {
        var bound = StatementArguments([stream])
        bound += arguments
        let limited = limit.map { " LIMIT \($0)" } ?? ""
        return try Row.fetchAll(
            db,
            sql: "SELECT row_id, type, data FROM snapshots WHERE stream = ? AND (\(condition)) ORDER BY \(orderBy)\(limited)",
            arguments: bound
        ).map { SnapshotRow(stream: stream, rowId: $0["row_id"], type: $0["type"], data: try Self.decodeData($0["data"])) }
    }

    func snapshot(_ db: Database, stream: String, rowId: String) throws -> SnapshotRow? {
        guard let row = try Row.fetchOne(
            db, sql: "SELECT type, data FROM snapshots WHERE stream = ? AND row_id = ?",
            arguments: [stream, rowId]
        ) else { return nil }
        return SnapshotRow(stream: stream, rowId: rowId, type: row["type"], data: try Self.decodeData(row["data"]))
    }

    func upsertSnapshot(_ db: Database, stream: String, rowId: String, shard: String, type: String?, data: [String: ReplicaValue]) throws {
        try db.execute(
            sql: """
                INSERT INTO snapshots (stream, row_id, shard, type, data) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard, type = excluded.type, data = excluded.data
                WHERE snapshots.shard IS NOT excluded.shard OR snapshots.type IS NOT excluded.type OR snapshots.data IS NOT excluded.data
                """,
            arguments: [stream, rowId, shard, type, try Self.encodeData(data)]
        )
        if db.changesCount > 0 { try bumpChangeSequence(db, stream: stream) }
    }

    func deleteSnapshot(_ db: Database, stream: String, rowId: String) throws {
        try db.execute(sql: "DELETE FROM snapshots WHERE stream = ? AND row_id = ?", arguments: [stream, rowId])
        if db.changesCount > 0 {
            try bumpChangeSequence(db, stream: stream)
        }
    }

    func docAddresses(_ db: Database, shard: String? = nil) throws -> [(stream: String, rowId: String)] {
        try Row.fetchAll(db, sql: "SELECT stream, row_id FROM docs WHERE ? IS NULL OR shard = ?", arguments: [shard, shard])
            .map { (stream: $0["stream"], rowId: $0["row_id"]) }
    }

    // MARK: - Docs (fold + acked version vector + peer)

    public struct DocRow: Sendable, Equatable {
        public var stream: String
        public var rowId: String
        public var codec: String
        public var fold: Data
        public var acked: Data?
        public var peer: UInt64

        public init(stream: String, rowId: String, codec: String, fold: Data, acked: Data?, peer: UInt64) {
            self.stream = stream
            self.rowId = rowId
            self.codec = codec
            self.fold = fold
            self.acked = acked
            self.peer = peer
        }
    }

    func doc(_ db: Database, stream: String, rowId: String) throws -> DocRow? {
        guard let row = try Row.fetchOne(
            db, sql: "SELECT codec, fold, acked, peer FROM docs WHERE stream = ? AND row_id = ?",
            arguments: [stream, rowId]
        ) else { return nil }
        let peer: Int64 = row["peer"]
        return DocRow(
            stream: stream, rowId: rowId, codec: row["codec"],
            fold: row["fold"], acked: row["acked"], peer: UInt64(bitPattern: peer)
        )
    }

    func holdsDoc(_ db: Database, stream: String, rowId: String) throws -> Bool {
        try Bool.fetchOne(
            db, sql: "SELECT EXISTS(SELECT 1 FROM docs WHERE stream = ? AND row_id = ?)",
            arguments: [stream, rowId]
        ) ?? false
    }

    /// What a reader can key a memo on without opening the document: the
    /// row's revision, stamped by every write of its fold or its acked
    /// version. One query for a whole grid.
    func docFingerprints(_ db: Database, stream: String, rowIds: [String]) throws -> [String: Data] {
        guard !rowIds.isEmpty else { return [:] }
        let marks = Array(repeating: "?", count: rowIds.count).joined(separator: ", ")
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT row_id, revision FROM docs WHERE stream = ? AND row_id IN (\(marks))",
            arguments: StatementArguments([stream] + rowIds)
        )
        var fingerprints: [String: Data] = [:]
        for row in rows {
            let revision: Int64 = row["revision"]
            fingerprints[row["row_id"]] = withUnsafeBytes(of: revision.bigEndian) { Data($0) }
        }
        return fingerprints
    }

    func upsertDoc(_ db: Database, stream: String, rowId: String, shard: String, codec: String, fold: Data, acked: Data?, peer: UInt64) throws {
        try db.execute(
            sql: "INSERT OR REPLACE INTO docs (stream, row_id, shard, codec, fold, acked, peer) VALUES (?, ?, ?, ?, ?, ?, ?)",
            arguments: [stream, rowId, shard, codec, fold, acked, Int64(bitPattern: peer)]
        )
        try stampDoc(db, stream: stream, rowId: rowId)
    }

    /// The stream's new change sequence, written onto the row it moved for.
    /// Monotonic per stream, so a fingerprint is never reused — not by a fold
    /// exactly as long as the last one, not by a document deleted and born
    /// again under the same id.
    private func stampDoc(_ db: Database, stream: String, rowId: String) throws {
        let revision = try bumpChangeSequence(db, stream: stream)
        try db.execute(
            sql: "UPDATE docs SET revision = ? WHERE stream = ? AND row_id = ?",
            arguments: [revision, stream, rowId]
        )
    }

    func deleteDoc(_ db: Database, stream: String, rowId: String) throws {
        try db.execute(sql: "DELETE FROM docs WHERE stream = ? AND row_id = ?", arguments: [stream, rowId])
        if db.changesCount > 0 {
            try bumpChangeSequence(db, stream: stream)
        }
    }

    /// Partial update for a live doc row — fold on merges, acked on
    /// accept/import — without disturbing shard or peer.
    func updateDoc(_ db: Database, stream: String, rowId: String, fold: Data? = nil, acked: Data? = nil) throws {
        var changed = false
        if let fold {
            try db.execute(
                sql: "UPDATE docs SET fold = ? WHERE stream = ? AND row_id = ?",
                arguments: [fold, stream, rowId]
            )
            changed = changed || db.changesCount > 0
        }
        if let acked {
            try db.execute(
                sql: "UPDATE docs SET acked = ? WHERE stream = ? AND row_id = ?",
                arguments: [acked, stream, rowId]
            )
            changed = changed || db.changesCount > 0
        }
        if changed {
            try stampDoc(db, stream: stream, rowId: rowId)
        }
    }

    func changeSequence(_ db: Database, stream: String) throws -> Int64 {
        try Int64.fetchOne(
            db,
            sql: "SELECT change_seq FROM stream_meta WHERE stream = ?",
            arguments: [stream]
        ) ?? 0
    }

    @discardableResult
    private func bumpChangeSequence(_ db: Database, stream: String) throws -> Int64 {
        try db.execute(
            sql: """
                INSERT INTO stream_meta (stream, change_seq) VALUES (?, 1)
                ON CONFLICT(stream) DO UPDATE SET change_seq = stream_meta.change_seq + 1
            """,
            arguments: [stream]
        )
        let sequence = try changeSequence(db, stream: stream)
        if rowCache.prepareMutation(stream: stream, sequence: sequence) {
            // One observer per transaction. An observer per changed row makes
            // GRDB walk a growing observer list for every subsequent statement.
            db.afterNextTransaction(
                onCommit: { [rowCache] _ in rowCache.finishTransaction(committed: true) },
                onRollback: { [rowCache] _ in rowCache.finishTransaction(committed: false) }
            )
        }
        return sequence
    }

    // MARK: - Journal

    public struct JournalRow: Sendable, Equatable {
        public var id: String
        public var verb: String
        /// Encoded `ReplicaOp` JSON, byte-stable — what the wire carries and
        /// what verdict payload-matching compares.
        public var payload: Data
        /// Revert record (client-owned JSON) — what this op's client
        /// write displaced.
        public var preimage: Data?
        /// The server's refusal, kept until the application dismisses it.
        public var parked: String?
        /// Frozen: its exact bytes go to the server until answered, which may
        /// have committed them even if no answer came back.
        public var sent: Bool

        public init(id: String, verb: String, payload: Data, preimage: Data? = nil, parked: String? = nil, sent: Bool = false) {
            self.id = id
            self.verb = verb
            self.payload = payload
            self.preimage = preimage
            self.parked = parked
            self.sent = sent
        }

        public func op() throws -> ReplicaOp {
            try Self.decodeOperation(payload)
        }

        static func decodeOperation(_ payload: Data) throws -> ReplicaOp {
            do {
                return try ReplicaJSON.decoder().decode(ReplicaOp.self, from: payload)
            } catch let error as DecodingError {
                throw ReplicaError.storage("Invalid journal operation: \(error)")
            }
        }

    }

    static let journalColumns = "id, op, payload, preimage, reason, state"

    static func journalRow(_ row: Row) -> JournalRow {
        JournalRow(
            id: row["id"], verb: row["op"],
            payload: Data((row["payload"] as String).utf8),
            preimage: (row["preimage"] as String?).map { Data($0.utf8) },
            parked: row["reason"],
            sent: (row["state"] as String) == "frozen"
        )
    }

    /// An intent that could never leave would stop every intent behind it:
    /// bytes over the request limit are refused before they are owed.
    static func admit(_ payload: Data, stream: String, rowId: String) throws {
        guard payload.count <= ReplicaProtocol.operationBytes else {
            throw ReplicaError.oversizedWrite(stream: stream, id: rowId, bytes: payload.count)
        }
    }

    /// A new intent at the end of the queue — owed, or held by its draft.
    func enqueue(_ db: Database, id: String, verb: String, stream: String, rowId: String,
                 payload: Data, preimage: Data? = nil, lane: ReplicaLane = .bulk, draft: String? = nil) throws {
        try Self.admit(payload, stream: stream, rowId: rowId)
        try db.execute(
            sql: """
                INSERT INTO intents (id, stream, row_id, state, op, payload, preimage, lane, draft, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            arguments: [
                id, stream, rowId, draft == nil ? "owed" : "draft", verb,
                String(decoding: payload, as: UTF8.self), preimage.map { String(decoding: $0, as: UTF8.self) },
                lane.rawValue, draft, Date().timeIntervalSince1970,
            ]
        )
    }

    /// The draft that bore the row — the key of its birth still held as a
    /// draft — or nil when the row was born outside any draft, or sent.
    func draftBirth(_ db: Database, stream: String, rowId: String) throws -> String? {
        try String.fetchOne(db, sql: """
            SELECT draft FROM intents WHERE row_id = ? AND stream = ? AND op = ? AND state = 'draft'
            """, arguments: [rowId, stream, ReplicaOp.Verb.rowCreate])
    }

    /// The document's one editable delta — owed, or held by its draft — the
    /// intent a newer delta supersedes.
    func editableDelta(_ db: Database, stream: String, rowId: String) throws -> String? {
        try String.fetchOne(db, sql: """
            SELECT id FROM intents
            WHERE row_id = ? AND stream = ? AND op = ? AND state IN ('owed', 'draft')
            """, arguments: [rowId, stream, ReplicaOp.Verb.docDelta])
    }

    /// Newer bytes for an editable intent: its rowid (queue place) and
    /// created_at stay, and newer bytes for a draft are still the draft's.
    func supersede(_ db: Database, id: String, payload: Data, preimage: Data?, lane: ReplicaLane, draft: String?) throws {
        if let address = try Row.fetchOne(db, sql: "SELECT stream, row_id FROM intents WHERE id = ?", arguments: [id]) {
            try Self.admit(payload, stream: address["stream"], rowId: address["row_id"])
        }
        try db.execute(
            sql: """
                UPDATE intents SET payload = ?, preimage = ?, lane = ?,
                    state = CASE WHEN ? IS NULL THEN state ELSE 'draft' END, draft = COALESCE(?, draft)
                WHERE id = ?
                """,
            arguments: [
                String(decoding: payload, as: UTF8.self), preimage.map { String(decoding: $0, as: UTF8.self) },
                lane.rawValue, draft, draft, id,
            ]
        )
    }

    func discardEditableDelta(_ db: Database, stream: String, rowId: String) throws {
        try db.execute(sql: """
            DELETE FROM intents WHERE row_id = ? AND stream = ? AND op = ? AND state IN ('owed', 'draft')
            """, arguments: [rowId, stream, ReplicaOp.Verb.docDelta])
    }

    // MARK: Drafts

    /// The key an entry addressing any of these rows is held under, if one
    /// is: a write naming a drafted row joins that draft.
    func draftKey(_ db: Database, rowIds: [String]) throws -> String? {
        guard !rowIds.isEmpty else { return nil }
        let placeholders = databaseQuestionMarks(count: rowIds.count)
        return try String.fetchOne(
            db,
            sql: "SELECT draft FROM intents WHERE state = 'draft' AND row_id IN (\(placeholders)) ORDER BY rowid LIMIT 1",
            arguments: StatementArguments(rowIds)
        )
    }

    func draftKeys(_ db: Database) throws -> [String] {
        try String.fetchAll(db, sql: "SELECT DISTINCT draft FROM intents WHERE state = 'draft'")
    }

    /// Every (stream, row) a draft holds, in journal order.
    func draftAddresses(_ db: Database, key: String) throws -> [(stream: String, rowId: String)] {
        try Row.fetchAll(
            db,
            sql: "SELECT DISTINCT stream, row_id FROM intents WHERE draft = ? ORDER BY rowid",
            arguments: [key]
        ).map { (stream: $0["stream"], rowId: $0["row_id"]) }
    }

    /// Strip the key: the entries become ordinary owed work at their own
    /// queue positions.
    func releaseDraft(_ db: Database, key: String) throws {
        try db.execute(sql: "UPDATE intents SET state = 'owed', draft = NULL WHERE draft = ?", arguments: [key])
    }

    func dropDraftEntries(_ db: Database, key: String) throws {
        try db.execute(sql: "DELETE FROM intents WHERE draft = ?", arguments: [key])
    }

    func drafted(_ db: Database) throws -> [JournalRow] {
        try entries(db, where: "state = 'draft'")
    }

    /// A draft's own entries, in journal order — what `commitDraft` judges.
    func draftEntries(_ db: Database, key: String) throws -> [JournalRow] {
        try Row.fetchAll(
            db,
            sql: "SELECT \(Self.journalColumns) FROM intents WHERE draft = ? ORDER BY rowid",
            arguments: [key]
        ).map(Self.journalRow)
    }

    // MARK: Holds

    /// A row a sync gate holds on the device.
    public struct GateHold: Sendable, Equatable {
        public let stream: String
        public let rowId: String
        public let gateId: String
        public let reason: String
        public let seq: Int64
        public let serverKnows: Bool
        let preimage: Data
    }

    private static func gateHold(_ row: Row) -> GateHold {
        GateHold(stream: row["stream"], rowId: row["row_id"], gateId: row["gate_id"], reason: row["reason"],
                 seq: row["seq"], serverKnows: row["server_knows"], preimage: row["preimage"])
    }

    func hold(_ db: Database, stream: String, rowId: String) throws -> GateHold? {
        try Row.fetchOne(db, sql: "SELECT * FROM holds WHERE stream = ? AND row_id = ?", arguments: [stream, rowId])
            .map(Self.gateHold)
    }

    /// The holds among these rows, in the order they began — a write naming a
    /// held row waits behind it.
    func holds(_ db: Database, rowIds: [String]) throws -> [GateHold] {
        guard !rowIds.isEmpty else { return [] }
        return try Row.fetchAll(
            db,
            sql: "SELECT * FROM holds WHERE row_id IN (\(databaseQuestionMarks(count: rowIds.count))) ORDER BY seq",
            arguments: StatementArguments(rowIds)
        ).map(Self.gateHold)
    }

    /// A new hold, after every other — or at `seq`.
    func insertHold(_ db: Database, stream: String, rowId: String, gateId: String, reason: String,
                    serverKnows: Bool, seq: Int64? = nil, preimage: Data) throws {
        try db.execute(
            sql: """
                INSERT INTO holds (stream, row_id, gate_id, reason, seq, server_knows, preimage)
                VALUES (?, ?, ?, ?, COALESCE(?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM holds)), ?, ?)
                """,
            arguments: [stream, rowId, gateId, reason, seq, serverKnows, preimage]
        )
    }

    /// The earliest hold's `seq` — nil when nothing is held.
    func firstHoldSeq(_ db: Database) throws -> Int64? {
        try Int64.fetchOne(db, sql: "SELECT MIN(seq) FROM holds")
    }

    /// The row's birth joined its hold: it leaves as a create.
    func markServerUnaware(_ db: Database, stream: String, rowId: String) throws {
        try db.execute(sql: "UPDATE holds SET server_knows = 0, preimage = ? WHERE stream = ? AND row_id = ?",
                       arguments: [try ReplicaPreimage.absent.encoded(), stream, rowId])
    }

    func setHoldPreimage(_ db: Database, stream: String, rowId: String, preimage: Data) throws {
        try db.execute(sql: "UPDATE holds SET preimage = ? WHERE stream = ? AND row_id = ?", arguments: [preimage, stream, rowId])
    }

    func updateHold(_ db: Database, stream: String, rowId: String, gateId: String, reason: String) throws {
        try db.execute(sql: "UPDATE holds SET gate_id = ?, reason = ? WHERE stream = ? AND row_id = ?",
                       arguments: [gateId, reason, stream, rowId])
    }

    func dropHold(_ db: Database, stream: String, rowId: String) throws {
        try db.execute(sql: "DELETE FROM holds WHERE stream = ? AND row_id = ?", arguments: [stream, rowId])
    }

    /// One gate's holds, or every hold, in the order they began.
    func holds(_ db: Database, gateId: String?) throws -> [GateHold] {
        guard let gateId else {
            return try Row.fetchAll(db, sql: "SELECT * FROM holds ORDER BY seq").map(Self.gateHold)
        }
        return try Row.fetchAll(db, sql: "SELECT * FROM holds WHERE gate_id = ? ORDER BY seq", arguments: [gateId])
            .map(Self.gateHold)
    }

    func heldRowIds(_ db: Database, stream: String) throws -> [String] {
        try String.fetchAll(db, sql: "SELECT row_id FROM holds WHERE stream = ? ORDER BY seq", arguments: [stream])
    }

    func lane(_ db: Database, entryId: String) throws -> ReplicaLane {
        let raw = try String.fetchOne(db, sql: "SELECT lane FROM intents WHERE id = ?", arguments: [entryId])
        return raw.flatMap(ReplicaLane.init(rawValue:)) ?? .bulk
    }

    /// Entries still owed for a row, whatever lane they sit on — the input to
    /// stickiness. Carries the payload so a caller that promotes them can walk
    /// what THEY name without reading the journal a second time.
    func pendingEntries(_ db: Database, stream: String, rowId: String) throws
        -> [(id: String, lane: ReplicaLane, payload: Data)] {
        try Row.fetchAll(
            db,
            sql: """
                SELECT id, lane, payload FROM intents
                WHERE row_id = ? AND stream = ? AND state IN ('draft', 'owed', 'frozen')
                ORDER BY rowid
                """,
            arguments: [rowId, stream]
        ).map {
            (id: $0["id"],
             lane: ReplicaLane(rawValue: $0["lane"] ?? "") ?? .bulk,
             payload: Data(($0["payload"] as String).utf8))
        }
    }

    /// Pending BULK entries addressing any of these rows — the promotion
    /// lookup. One index seek per named id, so an interactive write costs the
    /// same whether the bulk backlog holds ten ops or ten thousand.
    func pendingBulkEntries(_ db: Database, rowIds: [String]) throws -> [JournalRow] {
        guard !rowIds.isEmpty else { return [] }
        let placeholders = databaseQuestionMarks(count: rowIds.count)
        return try Row.fetchAll(
            db,
            sql: """
                SELECT \(Self.journalColumns) FROM intents
                WHERE row_id IN (\(placeholders)) AND state IN ('draft', 'owed', 'frozen') AND lane = ?
                ORDER BY rowid
                """,
            arguments: StatementArguments(rowIds + [ReplicaLane.bulk.rawValue])
        ).map(Self.journalRow)
    }

    /// The lanes whose drain has work: owed intents, and frozen ones awaiting
    /// their answer.
    func lanesOwed(_ db: Database) throws -> Set<ReplicaLane> {
        let raw = try String.fetchAll(db, sql: "SELECT DISTINCT lane FROM intents WHERE state IN ('owed', 'frozen')")
        return Set(raw.compactMap(ReplicaLane.init(rawValue:)))
    }

    func owesWork(_ db: Database, lane: ReplicaLane) throws -> Bool {
        try Bool.fetchOne(
            db,
            sql: "SELECT EXISTS(SELECT 1 FROM intents WHERE state IN ('owed', 'frozen') AND lane = ?)",
            arguments: [lane.rawValue]
        ) ?? false
    }

    func promote(_ db: Database, entryIds: [String]) throws {
        guard !entryIds.isEmpty else { return }
        let placeholders = databaseQuestionMarks(count: entryIds.count)
        try db.execute(
            sql: "UPDATE intents SET lane = ? WHERE id IN (\(placeholders))",
            arguments: StatementArguments([ReplicaLane.interactive.rawValue] + entryIds)
        )
    }

    /// Read the current rollback image only for the exact bytes judged by
    /// the server; an entry superseded during HTTP keeps its own fate.
    func entry(_ db: Database, id: String, payload: Data) throws -> JournalRow? {
        try Row.fetchOne(db, sql: "SELECT \(Self.journalColumns) FROM intents WHERE id = ? AND payload = ?",
                         arguments: [id, String(decoding: payload, as: UTF8.self)]).map(Self.journalRow)
    }

    func updatePreimage(_ db: Database, id: String, preimage: Data) throws {
        try db.execute(sql: "UPDATE intents SET preimage = ? WHERE id = ?",
                       arguments: [String(decoding: preimage, as: UTF8.self), id])
    }

    private func entries(_ db: Database, where condition: String) throws -> [JournalRow] {
        try Row.fetchAll(
            db,
            sql: "SELECT \(Self.journalColumns) FROM intents WHERE \(condition) ORDER BY rowid"
        ).map(Self.journalRow)
    }

    /// Owed to the server: editable, or frozen and awaiting its answer.
    func pending(_ db: Database) throws -> [JournalRow] {
        try entries(db, where: "state IN ('owed', 'frozen')")
    }

    /// Owed and still editable, of one stream — of every stream when nil.
    func owed(_ db: Database, stream: String?) throws -> [JournalRow] {
        try Row.fetchAll(
            db,
            sql: "SELECT \(Self.journalColumns) FROM intents WHERE state = 'owed' AND (? IS NULL OR stream = ?) ORDER BY rowid",
            arguments: [stream, stream]
        ).map(Self.journalRow)
    }

    /// Every intent of a stream the server has not accepted — refused ones
    /// included.
    func entriesForStream(_ db: Database, stream: String) throws -> [JournalRow] {
        try Row.fetchAll(
            db,
            sql: "SELECT \(Self.journalColumns) FROM intents WHERE stream = ? AND state <> 'accepted' ORDER BY rowid",
            arguments: [stream]
        ).map(Self.journalRow)
    }

    func parked(_ db: Database) throws -> [JournalRow] {
        try entries(db, where: "state = 'refused'")
    }

    /// Abandon an entry WHATEVER its bytes or refusal — the discard path: a
    /// row thrown away before the server ever heard its id must stop owing
    /// anything, or the next drain resurrects it. A frozen intent stays: only
    /// its own verdict settles it.
    func discard(_ db: Database, id: String) throws {
        try db.execute(sql: "DELETE FROM intents WHERE id = ? AND state IN ('draft', 'owed', 'refused')", arguments: [id])
    }

    /// Discard an address's entries, frozen ones excepted. `except` preserves
    /// a rejected create's own intent during its revert cascade.
    func discardEntries(_ db: Database, stream: String, rowId: String, except id: String? = nil) throws {
        try db.execute(
            sql: """
                DELETE FROM intents
                WHERE row_id = ? AND stream = ? AND state IN ('draft', 'owed', 'refused')
                  AND (? IS NULL OR id != ?)
                """,
            arguments: [rowId, stream, id, id]
        )
    }

    func discardLifetime(_ db: Database, stream: String, rowId: String, incarnation: String?, except id: String? = nil) throws {
        for entry in try entriesAddressing(db, stream: stream, rowId: rowId) {
            if entry.id != id, try entry.op().incarnation == incarnation {
                try discard(db, id: entry.id)
            }
        }
    }

    // MARK: - Cold-boot reads
    //
    // A store opened over an existing file answers these without an engine —
    // what a relaunched process (or a test standing in for one) sees.

    public func pendingOps() throws -> [JournalRow] {
        try pool.read { try pending($0) }
    }

    public func parkedOps() throws -> [JournalRow] {
        try pool.read { try parked($0) }
    }

    public func fold(stream: String, rowId: String) throws -> Data? {
        try pool.read { try doc($0, stream: stream, rowId: rowId)?.fold }
    }

    /// Entries addressing one row — every intent the server has not accepted,
    /// refused ones INCLUDED: an "unborn" check must see a rejected create too
    /// (the server refused the birth; the row still never existed server-side).
    /// Callers replaying local writes must exclude refused evidence.
    func entriesAddressing(_ db: Database, stream: String, rowId: String) throws -> [JournalRow] {
        try Row.fetchAll(
            db,
            sql: """
                SELECT \(Self.journalColumns) FROM intents
                WHERE row_id = ? AND stream = ? AND state <> 'accepted'
                ORDER BY rowid
                """,
            arguments: [rowId, stream]
        ).map(Self.journalRow)
    }

    func entriesAddressing(_ db: Database, stream: String, rowId: String, verb: String) throws -> [JournalRow] {
        try Row.fetchAll(
            db,
            sql: """
                SELECT \(Self.journalColumns) FROM intents
                WHERE row_id = ? AND stream = ? AND op = ? AND state <> 'accepted'
                ORDER BY rowid
                """,
            arguments: [rowId, stream, verb]
        ).map(Self.journalRow)
    }

    /// A lifetime's writes still awaiting their verdict, other than `id`.
    func frozenIntents(_ db: Database, stream: String, rowId: String, incarnation: String?, except id: String) throws -> Set<String> {
        Set(try entriesAddressing(db, stream: stream, rowId: rowId)
            .filter { try $0.sent && $0.id != id && $0.op().incarnation == incarnation }
            .map(\.id))
    }
}

private final class DecodedRowCache: @unchecked Sendable {
    struct RawRows: Sendable {
        var generation: UInt64
        var sequence: Int64
        var records: [ReplicaStateStore.RowRecord]
    }

    private final class MaterializationBox: @unchecked Sendable {
        let value: Any

        init(_ value: Any) {
            self.value = value
        }
    }

    private final class MaterializationLock: @unchecked Sendable {
        let value = NSLock()
    }

    private struct MaterializationKey: Hashable, Sendable {
        var stream: String
        var model: ObjectIdentifier
    }

    private struct Entry: Sendable {
        var sequence: Int64
        var records: [ReplicaStateStore.RowRecord]
        var byKey: [String: ReplicaStateStore.RowRecord]
        var materializations: [ObjectIdentifier: MaterializationBox] = [:]
    }

    private struct State: Sendable {
        var generations: [String: UInt64] = [:]
        var pendingSequences: [String: Int64] = [:]
        var entries: [String: Entry] = [:]
        /// The superseded entry, kept as a per-row reuse DONOR: rows whose
        /// raw text is unchanged carry their decoded fields and models into
        /// the next materialization. One per stream — replaced, never
        /// accumulated. Never SERVED: readers only see `entries`.
        var donors: [String: Entry] = [:]
        var materializationLocks: [MaterializationKey: MaterializationLock] = [:]
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func generation(stream: String) -> UInt64 {
        state.withLock { $0.generations[stream, default: 0] }
    }

    func rawRows(stream: String, minimumSequence: Int64?) -> RawRows? {
        state.withLock { state in
            guard let entry = state.entries[stream],
                  minimumSequence.map({ entry.sequence >= $0 }) ?? true
            else { return nil }
            return RawRows(
                generation: state.generations[stream, default: 0],
                sequence: entry.sequence,
                records: entry.records
            )
        }
    }

    func materialization<Model: Sendable>(
        stream: String,
        model: Model.Type,
        minimumSequence: Int64?
    ) -> ReplicaStateStore.RowMaterialization<Model>? {
        state.withLock { state in
            guard let entry = state.entries[stream],
                  minimumSequence.map({ entry.sequence >= $0 }) ?? true
            else { return nil }
            return entry.materializations[ObjectIdentifier(model)]?
                .value as? ReplicaStateStore.RowMaterialization<Model>
        }
    }

    func withMaterializationLock<Model: Sendable, Value>(
        stream: String,
        model: Model.Type,
        _ body: () -> Value
    ) -> Value {
        let key = MaterializationKey(stream: stream, model: ObjectIdentifier(model))
        let materializationLock = state.withLock { state in
            if let existing = state.materializationLocks[key] { return existing }
            let created = MaterializationLock()
            state.materializationLocks[key] = created
            return created
        }
        return materializationLock.value.withLock(body)
    }

    func installRawRows(
        stream: String,
        generation: UInt64,
        sequence: Int64,
        records: [ReplicaStateStore.RowRecord]
    ) -> Bool {
        state.withLock { state in
            guard state.generations[stream, default: 0] == generation else { return false }
            if let existing = state.entries[stream], existing.sequence >= sequence {
                return true
            } else {
                state.entries[stream] = Entry(
                    sequence: sequence,
                    records: records,
                    byKey: Dictionary(uniqueKeysWithValues: records.map { ($0.id, $0) })
                )
            }
            return true
        }
    }

    func installMaterialization<Model: Sendable>(
        _ materialization: ReplicaStateStore.RowMaterialization<Model>,
        stream: String,
        model: Model.Type,
        generation: UInt64,
        sequence: Int64
    ) -> ReplicaStateStore.RowMaterialization<Model>? {
        state.withLock { state in
            guard state.generations[stream, default: 0] == generation,
                  var entry = state.entries[stream],
                  entry.sequence == sequence
            else { return nil }

            let key = ObjectIdentifier(model)
            if let existing = entry.materializations[key]?.value
                as? ReplicaStateStore.RowMaterialization<Model> {
                return existing
            }
            entry.materializations[key] = MaterializationBox(materialization)
            state.entries[stream] = entry
            return materialization
        }
    }

    /// Per-row reuse for a SCOPED read: the live entry's decoded records
    /// and this type's models, the superseded donor when nothing is live.
    /// The caller validates each row against raw text + type, so a stale
    /// entry only reduces reuse, never poisons it.
    func reuseSources<Model: Sendable>(
        stream: String,
        model: Model.Type
    ) -> (records: [String: ReplicaStateStore.RowRecord], models: [String: Model]) {
        state.withLock { state in
            guard let entry = state.entries[stream] ?? state.donors[stream] else { return ([:], [:]) }
            let models = (entry.materializations[ObjectIdentifier(model)]?.value
                as? ReplicaStateStore.RowMaterialization<Model>)?.byKey ?? [:]
            return (entry.byKey, models)
        }
    }

    /// Returns true only for the first mutation in the writer's transaction.
    func prepareMutation(stream: String, sequence: Int64) -> Bool {
        state.withLock { state in
            let first = state.pendingSequences.isEmpty
            state.pendingSequences[stream] = sequence
            state.generations[stream, default: 0] &+= 1
            return first
        }
    }

    func finishTransaction(committed: Bool) {
        state.withLock { state in
            let pending = state.pendingSequences
            state.pendingSequences.removeAll()
            guard committed else { return }

            for (stream, sequence) in pending {
                // A reader may already have loaded the committed sequence.
                if let entry = state.entries[stream], entry.sequence >= sequence {
                    continue
                }
                state.generations[stream, default: 0] &+= 1
                if let superseded = state.entries.removeValue(forKey: stream) {
                    state.donors[stream] = superseded
                }
            }
        }
    }

    func donorRecords(stream: String) -> [String: ReplicaStateStore.RowRecord]? {
        state.withLock { $0.donors[stream]?.byKey }
    }

    /// The donor's records and its materialized models for `Model`, read
    /// atomically — a record/model pair from two different donor generations
    /// could otherwise pair a stale model with a matching-looking record.
    func donorSnapshot<Model: Sendable>(
        stream: String,
        model: Model.Type
    ) -> (records: [String: ReplicaStateStore.RowRecord], models: [String: Model])? {
        state.withLock { state in
            guard let donor = state.donors[stream] else { return nil }
            let models = donor.materializations[ObjectIdentifier(model)]?
                .value as? ReplicaStateStore.RowMaterialization<Model>
            return (records: donor.byKey, models: models?.byKey ?? [:])
        }
    }
}
