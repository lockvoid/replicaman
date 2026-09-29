package io.replicaman

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READONLY
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import java.io.File
import java.nio.file.Files
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass

/**
 * ReplicaMan's durable state — one sqlite file via `androidx.sqlite`'s
 * bundled driver, WAL. This is the app's PRIMARY database: `snapshots` is the
 * client's raw truth for both lanes; any native projection is rebuildable
 * FROM it and never written by app code (the write-ban contract). Losing the
 * journal loses the user's unsent work; losing the cursor forces a
 * re-snapshot — which is why they live here together and why `reset` wipes
 * never touch the journal.
 *
 * The connections are deliberately reachable: raw SQL is the escape hatch
 * below the generated verbs (codegen guarantees table/column names), and the
 * post-commit [commits] signal backs `watch()`. The WRITE side stays
 * internal to the core: app code reads; only engine transactions write.
 */
public class ReplicaStateStore(
    path: String,
    /**
     * The read structures this store keeps over `snapshots.data` — the
     * manifest's `indexes:`, converged at open (`reconcileIndexes`).
     */
    public val indexes: List<ReplicaIndexSpec> = emptyList(),
) {
    /**
     * Where this store was opened. The merge renames the file under a live
     * pool, so the CURRENT path is the binding's to track — this is only the
     * starting point.
     */
    public val path: File = File(path)
    private val authoringLease = ReplicaStoreLease(this.path)

    private val driver = BundledSQLiteDriver()
    private val writeConnection: SQLiteConnection
    private val writeLock = ReentrantLock()
    private val readers: ArrayBlockingQueue<SQLiteConnection>
    private val allConnections = mutableListOf<SQLiteConnection>()
    private val rowCache = DecodedRowCache()

    @Volatile
    private var closed = false

    private var transactionDepth = 0
    private var afterCommit = mutableListOf<() -> Unit>()

    /** GRDB's `currentReader` check, in the shape a JVM thread gives. */
    private val reading = ThreadLocal.withInitial { false }

    private val commitTick = AtomicLong(0)
    private val commitSignal = MutableStateFlow(0L)

    /**
     * Post-commit ticks — GRDB's `ValueObservation` seat. Every committed
     * write that bumped a stream's sequence raises it; a watcher re-reads
     * `changeSequence` and compares. Conflated on purpose: the value only
     * grows and the reader consults the durable truth, so a coalesced burst
     * behaves exactly like GRDB's.
     */
    public val commits: StateFlow<Long> get() = commitSignal

    init {
        try {
        this.path.parentFile?.mkdirs()
        writeConnection = driver.open(path)
        allConnections.add(writeConnection)
        writeConnection.exec("PRAGMA busy_timeout=5000")
        requireStoreFormat(writeConnection)
        writeConnection.exec("PRAGMA journal_mode=WAL")
        writeConnection.exec("PRAGMA synchronous=FULL")
        writeConnection.exec("PRAGMA fullfsync=ON")
        writeConnection.exec("PRAGMA foreign_keys=ON")

        readers = ArrayBlockingQueue(READER_COUNT)
        repeat(READER_COUNT) {
            val reader = driver.open(path, SQLITE_OPEN_READONLY)
            reader.exec("PRAGMA busy_timeout=5000")
            allConnections.add(reader)
            readers.put(reader)
        }

        write { db ->
            prepareSynchronization(db)
            reconcileIndexes(db, indexes)
            // Every authoring lease receives a fresh CRDT counter range. Existing
            // history stays intact when a store is reopened or copied.
            db.exec("UPDATE docs SET peer = ?", listOf(ReplicaID.peer().toLong()))
        }
        } catch (error: Throwable) {
            for (connection in allConnections) {
                try { connection.close() } catch (closing: Throwable) { error.addSuppressed(closing) }
            }
            try { authoringLease.close() } catch (closing: Throwable) { error.addSuppressed(closing) }
            throw error
        }
    }

    // MARK: - Connections

    /**
     * One write connection, one transaction, `BEGIN IMMEDIATE` … `COMMIT` —
     * GRDB's `write` contract kept. Post-commit callbacks run after the
     * COMMIT, never inside it.
     */
    internal fun <T> write(body: (SQLiteConnection) -> T): T {
        var committed: Long? = null
        val result = writeLock.withLock {
            if (closed) throw ReplicaError.Storage("store is closed")
            if (transactionDepth > 0) return@withLock body(writeConnection)

            val callbacks = mutableListOf<() -> Unit>()
            afterCommit = callbacks
            writeConnection.exec("BEGIN IMMEDIATE")
            transactionDepth = 1
            // COMMIT is guarded with the body: a deferred constraint fails AT
            // commit and leaves the transaction OPEN.
            val value = try {
                val value = body(writeConnection)
                writeConnection.exec("COMMIT")
                value
            } catch (error: Throwable) {
                rollback(writeConnection, error)
                transactionDepth = 0
                throw error
            }
            transactionDepth = 0
            // Cache invalidation belongs to the committed transaction. User
            // observers, unlike these internal callbacks, must run unlocked.
            for (callback in callbacks) callback()
            committed = commitTick.incrementAndGet()
            value
        }
        // EVERY commit raises the tick, exactly as GRDB re-evaluates every
        // observation after any write to a tracked table: a journal-only
        // transaction (park, discard, promote) moves no stream sequence but
        // is the whole state `watchParkedOps` exists to serve. Watchers
        // de-duplicate on the value they fetched, so an extra tick costs one
        // read and yields nothing.
        // An immediate collector may open/recover a document. Publishing under
        // writeLock would invert that reader's publication -> SQLite order.
        // Writers can reach this line out of order, so an older notification
        // can never lower the signal. A nested write has no commit of its own.
        committed?.let { tick -> commitSignal.update { current -> maxOf(current, tick) } }
        return result
    }

    /**
     * GRDB's `Database.rollback`: sqlite rolls some failures back on its own
     * (SQLITE_FULL, SQLITE_IOERR, SQLITE_BUSY, SQLITE_NOMEM), so ask the
     * connection what is still open rather than guessing which ROLLBACK
     * errors to ignore. A rollback that still fails rides along on the error
     * that caused it — the first error stays the thrown one.
     */
    private fun rollback(connection: SQLiteConnection, cause: Throwable) {
        try {
            if (connection.inTransaction()) connection.exec("ROLLBACK")
        } catch (error: Throwable) {
            cause.addSuppressed(error)
        }
    }

    /**
     * A borrowed read connection — read-only, and inside a DEFERRED
     * transaction so the block sees ONE snapshot whatever commits while it
     * runs (GRDB's `pool.read` is `readonly` + `db.isolated`). WAL, so it
     * never blocks the writer.
     *
     * Not reentrant, for the same reason GRDB's is not: a nested read takes
     * a SECOND connection at a SECOND snapshot, and past the pool's size it
     * blocks forever. GRDB answers that with a precondition; so does this.
     */
    public fun <T> read(body: (SQLiteConnection) -> T): T {
        if (reading.get()) throw ReplicaError.Storage("reads are not reentrant")
        if (closed) throw ReplicaError.Storage("store is closed")
        val connection = readers.take()
        reading.set(true)
        try {
            // `close` hands its drained connections back so a reader parked
            // here wakes; what it wakes to is a closed store.
            if (closed) throw ReplicaError.Storage("store is closed")
            connection.exec("BEGIN DEFERRED")
            val result = try {
                body(connection)
            } catch (error: Throwable) {
                rollback(connection, error)
                throw error
            }
            connection.exec("COMMIT")
            return result
        } finally {
            reading.set(false)
            readers.put(connection)
        }
    }

    /**
     * Release the file. Live observations over this store end, which is the
     * point: an observation must never keep serving a world whose owner is
     * gone.
     * SQLite's own advice: refresh planner statistics on the way out, so
     * the partial indexes are chosen on stats, not on default estimates.
     * Waits for the readers it is closing, so it cannot be called from
     * inside `read` — GRDB's barrier has the same rule.
     */
    public fun close() {
        writeLock.withLock {
            if (closed && allConnections.isEmpty()) {
                authoringLease.close()
                return
            }
            closed = true
            // GRDB closes behind `Pool.barrier`: every borrowed reader is
            // back before the first handle goes. The bundled driver builds
            // sqlite in MULTI-THREAD mode, so closing a connection another
            // thread is stepping is not an error — it is undefined.
            val borrowed = List(READER_COUNT) { readers.take() }
            var closeFailure: Throwable? = null
            for (connection in allConnections.toList()) {
                try {
                    connection.close()
                    allConnections.remove(connection)
                } catch (error: Throwable) {
                    val first = closeFailure
                    if (first == null) closeFailure = error
                    else if (first !== error) first.addSuppressed(error)
                }
            }
            // Hand the (closed) connections back so a reader parked in
            // `take()` wakes and sees the closed store instead of hanging.
            for (connection in borrowed) readers.put(connection)
            closeFailure?.let { throw it }
            authoringLease.close()
        }
    }

    public companion object {
        private const val READER_COUNT = 4

        /**
         * Everything sqlite keeps for one store: the database and its WAL
         * sidecars. A retired owner leaves none of the three behind.
         */
        public fun remove(path: File) {
            for (suffix in listOf("", "-wal", "-shm")) {
                val retired = File(path.path + suffix)
                Files.deleteIfExists(retired.toPath())
            }
        }

        internal fun decodeData(json: String?): Map<String, ReplicaValue> =
            ReplicaValueJSON.decodeObject(json)

        internal fun encodeData(data: Map<String, ReplicaValue>): String =
            ReplicaJSON.encodeToString(ReplicaValue.Obj(data))

        /**
         * Declared ↔ actual convergence for the read structures, at every open,
         * in the open transaction, before the first reader: a missing generated
         * column / btree / fts5 table is created (fts rebuilt from the rows on
         * disk), a stale one dropped. Idempotent — a converged open executes no
         * DDL and logs nothing. There are no migrations and no versions: the
         * manifest is the schema, and an older build simply ignores structures
         * it did not declare. Every owned name carries its prefix (`ix_`,
         * `idx_`, `fts_`), so the shared layout's indexes are never in scope.
         */
        private fun reconcileIndexes(db: SQLiteConnection, declared: List<ReplicaIndexSpec>) {
            val started = System.nanoTime()
            val log = mutableListOf<String>()
            val escape = { name: String -> name.replace("'", "''") }

            val wantedColumns = LinkedHashMap<String, String>()
            for (spec in declared) wantedColumns.putIfAbsent(spec.column, spec.field)
            val wantedBtree =
                declared.filter { it.kind == ReplicaIndexKind.BTREE }.map { it.btreeIndex }.toSet()
            val wantedFts =
                declared.filter { it.kind == ReplicaIndexKind.FTS5 }.map { it.ftsTable }.toSet()
            val wantedTriggers =
                wantedFts.flatMap { listOf("${it}_ai", "${it}_ad", "${it}_au") }.toSet()

            val existingColumns = db.queryStrings(
                "SELECT name FROM pragma_table_xinfo('snapshots') WHERE name LIKE 'ix\\_%' ESCAPE '\\'"
            ).toSet()
            val existingBtree = db.queryStrings(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx\\_%' ESCAPE '\\'"
            ).toSet()
            val existingFts = db.queryStrings(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'fts\\_%' ESCAPE '\\' " +
                    "AND sql LIKE 'CREATE VIRTUAL TABLE%'"
            ).toSet()
            val existingTriggers = db.queryStrings(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'fts\\_%' ESCAPE '\\'"
            ).toSet()

            // Stale first, dependents before what they depend on.
            for (trigger in (existingTriggers - wantedTriggers).sorted()) {
                db.exec("DROP TRIGGER \"$trigger\"")
            }
            for (table in (existingFts - wantedFts).sorted()) {
                db.exec("DROP TABLE \"$table\"")
                log.add("-$table")
            }
            for (index in (existingBtree - wantedBtree).sorted()) {
                db.exec("DROP INDEX \"$index\"")
                log.add("-$index")
            }
            for (column in (existingColumns - wantedColumns.keys).sorted()) {
                db.exec("ALTER TABLE snapshots DROP COLUMN \"$column\"")
                log.add("-$column")
            }

            // Then the missing ones.
            for ((column, field) in wantedColumns.entries.sortedBy { it.key }) {
                if (existingColumns.contains(column)) continue
                db.exec(
                    "ALTER TABLE snapshots ADD COLUMN \"$column\" " +
                        "GENERATED ALWAYS AS (json_extract(data, '${'$'}.${escape(field)}')) VIRTUAL"
                )
                log.add("+$column")
            }
            val createdBtree = mutableSetOf<String>()
            for (spec in declared) {
                if (spec.kind != ReplicaIndexKind.BTREE) continue
                if (existingBtree.contains(spec.btreeIndex)) continue
                if (!createdBtree.add(spec.btreeIndex)) continue
                db.exec("CREATE INDEX \"${spec.btreeIndex}\" ON snapshots(stream, \"${spec.column}\")")
                log.add("+${spec.btreeIndex}")
            }
            for (spec in declared) {
                if (spec.kind != ReplicaIndexKind.FTS5) continue
                val table = spec.ftsTable
                val stream = escape(spec.stream)
                val value = "json_extract(new.data, '${'$'}.${escape(spec.field)}')"
                if (!existingFts.contains(table)) {
                    db.exec(
                        "CREATE VIRTUAL TABLE \"$table\" " +
                            "USING fts5(row_id UNINDEXED, value, tokenize = 'unicode61 remove_diacritics 2')"
                    )
                    db.exec(
                        "INSERT INTO \"$table\"(row_id, value) " +
                            "SELECT row_id, json_extract(data, '${'$'}.${escape(spec.field)}') " +
                            "FROM snapshots WHERE stream = '$stream'"
                    )
                    log.add("+$table (${db.changes()} rows)")
                }
                db.exec(
                    "CREATE TRIGGER IF NOT EXISTS \"${table}_ai\" AFTER INSERT ON snapshots " +
                        "WHEN new.stream = '$stream' " +
                        "BEGIN INSERT INTO \"$table\"(row_id, value) VALUES (new.row_id, $value); END"
                )
                db.exec(
                    "CREATE TRIGGER IF NOT EXISTS \"${table}_ad\" AFTER DELETE ON snapshots " +
                        "WHEN old.stream = '$stream' " +
                        "BEGIN DELETE FROM \"$table\" WHERE row_id = old.row_id; END"
                )
                db.exec(
                    "CREATE TRIGGER IF NOT EXISTS \"${table}_au\" AFTER UPDATE OF data ON snapshots " +
                        "WHEN new.stream = '$stream' BEGIN " +
                        "DELETE FROM \"$table\" WHERE row_id = old.row_id; " +
                        "INSERT INTO \"$table\"(row_id, value) VALUES (new.row_id, $value); END"
                )
            }

            if (log.isEmpty()) return
            val ms = (System.nanoTime() - started) / 1_000_000
            Log.logger.info("[index] reconcile ${log.joinToString(" ")} in ${ms}ms")
        }
    }

    // MARK: - Document mode

    internal fun requireDocumentMode(mode: ReplicaDocumentMode) {
        val requested = if (mode == ReplicaDocumentMode.REPLICATED) "replicated" else "projections"
        write { db ->
            db.exec("UPDATE meta SET document_mode = ? WHERE id = 1 AND document_mode IS NULL", listOf(requested))
            if (db.queryString("SELECT document_mode FROM meta WHERE id = 1") != requested) {
                throw ReplicaError.Storage("Document mode belongs to the store. Use a separate store for projection-only replicas.")
            }
        }
    }

    // MARK: - Snapshots (raw truth, both lanes)

    public data class SnapshotRow(
        val stream: String,
        val rowId: String,
        val type: String?,
        val data: Map<String, ReplicaValue>,
    )

    internal data class RowRecord(
        val id: String,
        val type: String?,
        /**
         * The snapshot's `data` TEXT exactly as stored — the per-row reuse
         * key. Comparing it is a memcmp; re-decoding it is the cost the
         * donor mechanism exists to avoid.
         */
        val raw: String?,
        val fields: Map<String, ReplicaValue>,
    )

    internal data class MaterializedRow<Model : Any>(
        val record: RowRecord,
        val model: Model,
    )

    internal data class RowMaterialization<Model : Any>(
        val rows: List<MaterializedRow<Model>>,
        val byKey: Map<String, Model>,
    )

    internal fun <Model : Any> materializedRows(
        stream: String,
        modelKey: KClass<*>,
        minimumSequence: Long? = null,
        decode: (String, String?, Map<String, ReplicaValue>) -> Model?,
    ): RowMaterialization<Model> {
        while (true) {
            rowCache.materialization<Model>(stream, modelKey, minimumSequence)?.let { return it }

            if (rowCache.rawRows(stream, minimumSequence) != null) {
                val materialization = rowCache.withMaterializationLock(stream, modelKey) {
                    rowCache.materialization<Model>(stream, modelKey, minimumSequence)
                        ?: run {
                            val raw = rowCache.rawRows(stream, minimumSequence)
                                ?: return@run null
                            // Per-row model reuse: a row whose raw text + type
                            // match the donor's decodes nothing — its model
                            // carries over. The check is self-validating
                            // (compared against the donor that HOLDS the
                            // model), so a donor superseded mid-flight merely
                            // reduces reuse, never poisons it.
                            val donor = rowCache.donorSnapshot<Model>(stream, modelKey)
                            val models = raw.records.mapNotNull { record ->
                                val prior = donor?.first?.get(record.id)
                                val model = if (prior != null && prior.raw == record.raw &&
                                    prior.type == record.type
                                ) {
                                    donor.second[record.id]
                                } else {
                                    null
                                }
                                if (model != null) {
                                    MaterializedRow(record, model)
                                } else {
                                    decode(record.id, record.type, record.fields)
                                        ?.let { MaterializedRow(record, it) }
                                }
                            }
                            val built = RowMaterialization(
                                rows = models,
                                byKey = models.associate { it.record.id to it.model }
                            )
                            rowCache.installMaterialization(
                                built, stream, modelKey, raw.generation, raw.sequence
                            )
                        }
                }
                if (materialization != null) return materialization
                continue
            }

            val generation = rowCache.generation(stream)
            // Field-tree reuse on the cold load itself: unchanged rows keep
            // their decoded `fields` from the donor — only changed raw text
            // pays `decodeData`.
            val donorRecords = rowCache.donorRecords(stream)
            val loaded = read { db ->
                val sequence = changeSequence(db, stream)
                val records = db.query(
                    "SELECT row_id, type, data FROM snapshots WHERE stream = ? ORDER BY row_id",
                    listOf(stream)
                ) { statement ->
                    val id = statement.getText(0)
                    val type = statement.textOrNull(1)
                    val raw = statement.textOrNull(2)
                    val prior = donorRecords?.get(id)
                    if (prior != null && prior.raw == raw && prior.type == type) {
                        prior
                    } else {
                        RowRecord(id = id, type = type, raw = raw, fields = decodeData(raw))
                    }
                }
                sequence to records
            }
            rowCache.installRawRows(stream, generation, loaded.first, loaded.second)
        }
    }

    /**
     * The warm model for `id` when this type is materialized at the live
     * sequence; null otherwise — never builds.
     */
    internal fun <Model : Any> warmModel(stream: String, modelKey: KClass<*>, id: String): Model? =
        rowCache.materialization<Model>(stream, modelKey, null)?.byKey?.get(id)

    /**
     * The indexed read: only the rows the WHERE fragment admits are
     * fetched; each reuses the stream's live decoded record — and the
     * model, when this type is materialized — whenever its raw text and
     * type are unchanged, so a scoped read after a whole-stream one decodes
     * nothing. Not cached as a picture: the subset is small by construction
     * and the SQL is index-served.
     */
    internal fun <Model : Any> scopedRows(
        stream: String,
        modelKey: KClass<*>,
        condition: String,
        arguments: List<Any?>,
        decode: (String, String?, Map<String, ReplicaValue>) -> Model?,
        orderBy: String = "row_id",
        limit: Int? = null,
    ): List<Model> {
        val sources = rowCache.reuseSources<Model>(stream, modelKey)
        val bound = listOf<Any?>(stream) + arguments
        return read { db ->
            db.query(
                "SELECT row_id, type, data FROM snapshots WHERE stream = ? AND ($condition) ORDER BY $orderBy${limit?.let { " LIMIT ${it.coerceAtLeast(0)}" }.orEmpty()}",
                bound
            ) { statement ->
                Triple(
                    statement.getText(0),
                    statement.textOrNull(1),
                    statement.textOrNull(2)
                )
            }.mapNotNull { (id, type, raw) ->
                val prior = sources.first[id]
                if (prior != null && prior.raw == raw && prior.type == type) {
                    sources.second[id] ?: decode(id, type, prior.fields)
                } else {
                    decode(id, type, decodeData(raw))
                }
            }
        }
    }

    internal fun snapshot(db: SQLiteConnection, stream: String, rowId: String): SnapshotRow? =
        db.queryOne(
            "SELECT type, data FROM snapshots WHERE stream = ? AND row_id = ?",
            listOf(stream, rowId)
        ) { statement ->
            SnapshotRow(
                stream = stream,
                rowId = rowId,
                type = statement.textOrNull(0),
                data = decodeData(statement.textOrNull(1))
            )
        }

    internal fun upsertSnapshot(
        db: SQLiteConnection,
        stream: String,
        rowId: String,
        shard: String,
        type: String?,
        data: Map<String, ReplicaValue>,
    ) {
        db.exec(
            """
            INSERT INTO snapshots (stream, row_id, shard, type, data) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(stream, row_id) DO UPDATE SET shard = excluded.shard, type = excluded.type, data = excluded.data
            WHERE snapshots.shard IS NOT excluded.shard OR snapshots.type IS NOT excluded.type OR snapshots.data IS NOT excluded.data
            """.trimIndent(),
            listOf(stream, rowId, shard, type, encodeData(data))
        )
        if (db.changes() > 0) bumpChangeSequence(db, stream)
    }

    internal fun deleteSnapshot(db: SQLiteConnection, stream: String, rowId: String) {
        db.exec("DELETE FROM snapshots WHERE stream = ? AND row_id = ?", listOf(stream, rowId))
        if (db.changes() > 0) bumpChangeSequence(db, stream)
    }

    internal fun docAddresses(db: SQLiteConnection): List<Pair<String, String>> =
        db.query("SELECT stream, row_id FROM docs") {
            it.getText(0) to it.getText(1)
        }

    // MARK: - Docs (fold + acked version vector + peer)

    public data class DocRow(
        val stream: String,
        val rowId: String,
        val codec: String,
        val fold: ByteArray,
        val acked: ByteArray?,
        val peer: ULong,
    ) {
        override fun equals(other: Any?): Boolean =
            other is DocRow && stream == other.stream && rowId == other.rowId &&
                codec == other.codec && fold.contentEquals(other.fold) &&
                acked.contentEquals(other.acked) && peer == other.peer

        override fun hashCode(): Int {
            var result = stream.hashCode()
            result = 31 * result + rowId.hashCode()
            result = 31 * result + codec.hashCode()
            result = 31 * result + fold.contentHashCode()
            result = 31 * result + (acked?.contentHashCode() ?: 0)
            result = 31 * result + peer.hashCode()
            return result
        }
    }

    internal fun doc(db: SQLiteConnection, stream: String, rowId: String): DocRow? =
        db.queryOne(
            "SELECT codec, fold, acked, peer FROM docs WHERE stream = ? AND row_id = ?",
            listOf(stream, rowId)
        ) { statement ->
            DocRow(
                stream = stream,
                rowId = rowId,
                codec = statement.getText(0),
                fold = statement.getBlob(1),
                acked = statement.blobOrNull(2),
                peer = statement.getLong(3).toULong()
            )
        }

    internal fun holdsDoc(db: SQLiteConnection, stream: String, rowId: String): Boolean =
        db.queryLong("SELECT EXISTS(SELECT 1 FROM docs WHERE stream = ? AND row_id = ?)", listOf(stream, rowId)) == 1L

    internal fun docFingerprints(db: SQLiteConnection, stream: String, rowIds: List<String>): Map<String, ByteArray> {
        if (rowIds.isEmpty()) return emptyMap()
        return db.query("SELECT row_id, revision FROM docs WHERE stream = ? AND row_id IN (${questionMarks(rowIds.size)})", listOf<Any?>(stream) + rowIds) {
            it.getText(0) to java.nio.ByteBuffer.allocate(8).putLong(it.getLong(1)).array()
        }.toMap()
    }

    internal fun upsertDoc(
        db: SQLiteConnection,
        stream: String,
        rowId: String,
        shard: String,
        codec: String,
        fold: ByteArray,
        acked: ByteArray?,
        peer: ULong,
    ) {
        db.exec(
            "INSERT OR REPLACE INTO docs (stream, row_id, shard, codec, fold, acked, peer) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
            listOf(stream, rowId, shard, codec, fold, acked, peer.toLong())
        )
        stampDoc(db, stream, rowId)
    }

    internal fun deleteDoc(db: SQLiteConnection, stream: String, rowId: String) {
        db.exec("DELETE FROM docs WHERE stream = ? AND row_id = ?", listOf(stream, rowId))
        if (db.changes() > 0) bumpChangeSequence(db, stream)
    }

    /**
     * Partial update for a live doc row — fold on merges, acked on
     * accept/import — without disturbing shard or peer.
     */
    internal fun updateDoc(
        db: SQLiteConnection,
        stream: String,
        rowId: String,
        fold: ByteArray? = null,
        acked: ByteArray? = null,
    ) {
        var changed = false
        if (fold != null) {
            db.exec(
                "UPDATE docs SET fold = ? WHERE stream = ? AND row_id = ?",
                listOf(fold, stream, rowId)
            )
            changed = changed || db.changes() > 0
        }
        if (acked != null) {
            db.exec(
                "UPDATE docs SET acked = ? WHERE stream = ? AND row_id = ?",
                listOf(acked, stream, rowId)
            )
            changed = changed || db.changes() > 0
        }
        if (changed) stampDoc(db, stream, rowId)
    }

    private fun stampDoc(db: SQLiteConnection, stream: String, rowId: String) {
        val revision = bumpChangeSequence(db, stream)
        db.exec("UPDATE docs SET revision = ? WHERE stream = ? AND row_id = ?", listOf(revision, stream, rowId))
    }

    internal fun changeSequence(db: SQLiteConnection, stream: String): Long =
        db.queryLong("SELECT change_seq FROM stream_meta WHERE stream = ?", listOf(stream)) ?: 0L

    private fun bumpChangeSequence(db: SQLiteConnection, stream: String): Long {
        db.exec(
            """
            INSERT INTO stream_meta (stream, change_seq) VALUES (?, 1)
            ON CONFLICT(stream) DO UPDATE SET change_seq = stream_meta.change_seq + 1
            """.trimIndent(),
            listOf(stream)
        )
        val sequence = changeSequence(db, stream)
        rowCache.prepareMutation(stream)
        afterCommit.add { rowCache.commitMutation(stream, sequence) }
        return sequence
    }

    // MARK: - Intents

    public data class JournalRow(
        val id: String,
        val verb: String,
        /**
         * Encoded `ReplicaOp` JSON, byte-stable — what a submission freezes
         * and what a verdict is matched against.
         */
        val payload: ByteArray,
        /**
         * Revert record (client-owned JSON) — what this op's client
         * write displaced.
         */
        val preimage: ByteArray? = null,
        /** The server's refusal, kept until the application dismisses the intent. */
        val parked: String? = null,
        /** Frozen into a submission; its answer may have been lost. */
        val sent: Boolean = false,
    ) {
        public fun op(): ReplicaOp = decodeOperation(payload)

        internal companion object {
            fun decodeOperation(payload: ByteArray): ReplicaOp = try {
                ReplicaOp.fromValue(ReplicaProtocol.decode(payload))
            } catch (error: kotlinx.serialization.SerializationException) {
                throw ReplicaError.Storage("Invalid journal operation JSON").also { it.initCause(error) }
            } catch (error: java.nio.charset.CharacterCodingException) {
                throw ReplicaError.Storage("Invalid journal operation UTF-8").also { it.initCause(error) }
            }
        }

        override fun equals(other: Any?): Boolean =
            other is JournalRow && id == other.id && verb == other.verb &&
                payload.contentEquals(other.payload) &&
                preimage.contentEquals(other.preimage) &&
                parked == other.parked && sent == other.sent

        override fun hashCode(): Int {
            var result = id.hashCode()
            result = 31 * result + verb.hashCode()
            result = 31 * result + payload.contentHashCode()
            result = 31 * result + (preimage?.contentHashCode() ?: 0)
            result = 31 * result + (parked?.hashCode() ?: 0)
            result = 31 * result + sent.hashCode()
            return result
        }

        override fun toString(): String =
            "JournalRow(id=$id, op=$verb, payload=${payload.toString(Charsets.UTF_8)}, parked=$parked)"
    }

    private val journalColumns = "id, op, payload, preimage, reason, state = 'frozen'"

    private fun journalRow(statement: SQLiteStatement): JournalRow = JournalRow(
        id = statement.getText(0),
        verb = statement.getText(1),
        payload = statement.getText(2).toByteArray(Charsets.UTF_8),
        preimage = statement.textOrNull(3)?.toByteArray(Charsets.UTF_8),
        parked = statement.textOrNull(4),
        sent = statement.getLong(5) != 0L,
    )

    private fun entries(db: SQLiteConnection, condition: String, arguments: List<Any?> = emptyList()): List<JournalRow> =
        db.query("SELECT $journalColumns FROM intents WHERE $condition ORDER BY rowid", arguments) { journalRow(it) }

    /** A new intent at the end of the queue: owed, or held back by its draft. */
    internal fun enqueue(
        db: SQLiteConnection,
        id: String,
        verb: String,
        stream: String,
        rowId: String,
        payload: ByteArray,
        preimage: ByteArray? = null,
        lane: ReplicaLane = ReplicaLane.BULK,
        draft: String? = null,
    ) {
        db.exec(
            """
            INSERT INTO intents (id, stream, row_id, state, op, payload, preimage, lane, draft, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            listOf(
                id, stream, rowId, if (draft == null) "owed" else "draft", verb,
                payload.toString(Charsets.UTF_8), preimage?.toString(Charsets.UTF_8), lane.rawValue, draft,
                System.currentTimeMillis() / 1000.0
            )
        )
    }

    /**
     * The document's delta that has not left the device — owed, or in the same
     * draft: a newer delta replaces its bytes and keeps its queue position.
     */
    internal fun unsentDelta(db: SQLiteConnection, stream: String, rowId: String, draft: String?): String? =
        db.queryString(
            """
            SELECT id FROM intents
            WHERE row_id = ? AND stream = ? AND op = ? AND state IN ('owed', 'draft') AND draft IS ?
            ORDER BY rowid LIMIT 1
            """.trimIndent(),
            listOf(rowId, stream, ReplicaOp.Verb.DOC_DELTA, draft)
        )

    internal fun supersede(db: SQLiteConnection, id: String, payload: ByteArray, preimage: ByteArray?, lane: ReplicaLane) {
        db.exec(
            "UPDATE intents SET payload = ?, preimage = ?, lane = ? WHERE id = ?",
            listOf(payload.toString(Charsets.UTF_8), preimage?.toString(Charsets.UTF_8), lane.rawValue, id)
        )
    }

    /** The document owes nothing: its deltas that have not left the device go. */
    internal fun discardUnsentDeltas(db: SQLiteConnection, stream: String, rowId: String) {
        db.exec(
            "DELETE FROM intents WHERE row_id = ? AND stream = ? AND op = ? AND state IN ('owed', 'draft')",
            listOf(rowId, stream, ReplicaOp.Verb.DOC_DELTA)
        )
    }

    internal fun draftKey(db: SQLiteConnection, rowIds: List<String>): String? {
        if (rowIds.isEmpty()) return null
        return db.queryString("SELECT draft FROM intents WHERE state = 'draft' AND row_id IN (${questionMarks(rowIds.size)}) ORDER BY rowid LIMIT 1", rowIds)
    }

    internal fun draftKeys(db: SQLiteConnection): List<String> = db.queryStrings("SELECT DISTINCT draft FROM intents WHERE state = 'draft'")

    internal fun draftAddresses(db: SQLiteConnection, key: String): List<Pair<String, String>> =
        db.query("SELECT DISTINCT stream, row_id FROM intents WHERE state = 'draft' AND draft = ? ORDER BY rowid", listOf(key)) { it.getText(0) to it.getText(1) }

    internal fun releaseDraft(db: SQLiteConnection, key: String) { db.exec("UPDATE intents SET state = 'owed', draft = NULL WHERE state = 'draft' AND draft = ?", listOf(key)) }
    internal fun dropDraftEntries(db: SQLiteConnection, key: String) { db.exec("DELETE FROM intents WHERE state = 'draft' AND draft = ?", listOf(key)) }
    internal fun drafted(db: SQLiteConnection): List<JournalRow> = entries(db, "state = 'draft'")

    /** A draft's own entries, in queue order — what `commitDraft` judges. */
    internal fun draftEntries(db: SQLiteConnection, key: String): List<JournalRow> = entries(db, "state = 'draft' AND draft = ?", listOf(key))

    // MARK: Holds

    /** A row a sync gate holds on the device. */
    public data class GateHold(
        val stream: String,
        val rowId: String,
        val gateId: String,
        val reason: String,
        val seq: Long,
        val serverKnows: Boolean,
        internal val preimage: ByteArray,
    ) {
        override fun equals(other: Any?): Boolean = other is GateHold &&
            stream == other.stream && rowId == other.rowId && gateId == other.gateId &&
            reason == other.reason && seq == other.seq && serverKnows == other.serverKnows &&
            preimage.contentEquals(other.preimage)

        override fun hashCode(): Int =
            31 * listOf(stream, rowId, gateId, reason, seq, serverKnows).hashCode() + preimage.contentHashCode()
    }

    private fun gateHold(statement: SQLiteStatement): GateHold = GateHold(
        stream = statement.getText(0),
        rowId = statement.getText(1),
        gateId = statement.getText(2),
        reason = statement.getText(3),
        seq = statement.getLong(4),
        serverKnows = statement.getLong(5) != 0L,
        preimage = statement.getBlob(6),
    )

    private val gateColumns = "stream, row_id, gate_id, reason, seq, server_knows, preimage"

    internal fun hold(db: SQLiteConnection, stream: String, rowId: String): GateHold? =
        db.queryOne("SELECT $gateColumns FROM holds WHERE stream = ? AND row_id = ?", listOf(stream, rowId)) { gateHold(it) }

    /** The holds among these rows, in the order they began — a write naming a held row waits behind it. */
    internal fun holds(db: SQLiteConnection, rowIds: List<String>): List<GateHold> {
        if (rowIds.isEmpty()) return emptyList()
        return db.query(
            "SELECT $gateColumns FROM holds WHERE row_id IN (${questionMarks(rowIds.size)}) ORDER BY seq", rowIds
        ) { gateHold(it) }
    }

    /** A new hold, after every other — or at `seq`. */
    internal fun insertHold(
        db: SQLiteConnection, stream: String, rowId: String, gateId: String, reason: String,
        serverKnows: Boolean, seq: Long? = null, preimage: ByteArray,
    ) {
        db.exec(
            """
            INSERT INTO holds (stream, row_id, gate_id, reason, seq, server_knows, preimage)
            VALUES (?, ?, ?, ?, COALESCE(?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM holds)), ?, ?)
            """.trimIndent(),
            listOf(stream, rowId, gateId, reason, seq, serverKnows, preimage)
        )
    }

    /** The earliest hold's `seq` — null when nothing is held. */
    internal fun firstHoldSeq(db: SQLiteConnection): Long? = db.queryLong("SELECT MIN(seq) FROM holds")

    /** The row's birth joined its hold: it leaves as a create. */
    internal fun markServerUnaware(db: SQLiteConnection, stream: String, rowId: String) {
        db.exec("UPDATE holds SET server_knows = 0, preimage = ? WHERE stream = ? AND row_id = ?", listOf(ReplicaPreimage.Absent.encoded(), stream, rowId))
    }

    internal fun setHoldPreimage(db: SQLiteConnection, stream: String, rowId: String, preimage: ByteArray) {
        db.exec("UPDATE holds SET preimage = ? WHERE stream = ? AND row_id = ?", listOf(preimage, stream, rowId))
    }

    internal fun updateHold(db: SQLiteConnection, stream: String, rowId: String, gateId: String, reason: String) {
        db.exec("UPDATE holds SET gate_id = ?, reason = ? WHERE stream = ? AND row_id = ?", listOf(gateId, reason, stream, rowId))
    }

    internal fun dropHold(db: SQLiteConnection, stream: String, rowId: String) {
        db.exec("DELETE FROM holds WHERE stream = ? AND row_id = ?", listOf(stream, rowId))
    }

    /** One gate's holds, or every hold, in the order they began. */
    internal fun holds(db: SQLiteConnection, gateId: String?): List<GateHold> =
        if (gateId == null) {
            db.query("SELECT $gateColumns FROM holds ORDER BY seq") { gateHold(it) }
        } else {
            db.query("SELECT $gateColumns FROM holds WHERE gate_id = ? ORDER BY seq", listOf(gateId)) { gateHold(it) }
        }

    internal fun heldRowIds(db: SQLiteConnection, stream: String): List<String> =
        db.queryStrings("SELECT row_id FROM holds WHERE stream = ? ORDER BY seq", listOf(stream))

    /**
     * The rows a sync gate holds that belong to [ownerId] — the row itself on
     * [ownerStreams], every other row by the [field] naming it — by stream:
     * the app's "does this project still owe the server".
     */
    public fun heldRowIds(ownerId: String, ownerStreams: List<String>, field: String): Map<String, List<String>> = read { db ->
        db.query(
            """
            SELECT holds.stream, holds.row_id FROM holds
            LEFT JOIN snapshots ON snapshots.stream = holds.stream AND snapshots.row_id = holds.row_id
            WHERE (holds.stream IN (${questionMarks(ownerStreams.size)}) AND holds.row_id = ?)
               OR json_extract(snapshots.data, ?) = ?
               OR EXISTS (
                   SELECT 1 FROM entity_references ref
                   JOIN snapshots target ON target.stream = ref.target_stream AND target.row_id = ref.target_id
                   JOIN entities entity ON entity.stream = ref.target_stream AND entity.row_id = ref.target_id
                       AND entity.incarnation = ref.target_incarnation
                   WHERE ref.stream = holds.stream AND ref.row_id = holds.row_id
                     AND json_extract(target.data, ?) = ?
               )
            """.trimIndent(),
            ownerStreams + listOf(ownerId, "$.$field", ownerId, "$.$field", ownerId)
        ) { it.getText(0) to it.getText(1) }.groupBy({ it.first }, { it.second })
    }

    /**
     * The current lifetime of every row of [stream] whose [field] is [value],
     * by row id: what an operation's reference incarnation is matched against.
     */
    public fun rowLifetimes(stream: String, field: String, value: String): Map<String, String> = read { db ->
        db.query(
            """
            SELECT snapshots.row_id, entities.incarnation FROM snapshots
            LEFT JOIN entities ON entities.stream = snapshots.stream AND entities.row_id = snapshots.row_id
            WHERE snapshots.stream = ? AND json_extract(snapshots.data, ?) = ?
            """.trimIndent(),
            listOf(stream, "$.$field", value)
        ) { row ->
            row.getText(0) to (row.textOrNull(1) ?: throw ReplicaError.Storage("$stream/${row.getText(0)} has no lifetime"))
        }.toMap()
    }

    // MARK: Lanes

    internal fun lane(db: SQLiteConnection, entryId: String): ReplicaLane {
        val raw = db.queryString("SELECT lane FROM intents WHERE id = ?", listOf(entryId))
        return ReplicaLane.fromRaw(raw) ?: ReplicaLane.BULK
    }

    internal data class PendingEntry(
        val id: String,
        val lane: ReplicaLane,
        val payload: ByteArray,
    )

    /**
     * Entries still owed for a row, whatever lane they sit on — the input to
     * stickiness. Carries the payload so a caller that promotes them can walk
     * what THEY name without reading the queue a second time.
     */
    internal fun pendingEntries(
        db: SQLiteConnection,
        stream: String,
        rowId: String,
    ): List<PendingEntry> = db.query(
        """
        SELECT id, lane, payload FROM intents
        WHERE state IN ('draft', 'owed', 'frozen') AND row_id = ? AND stream = ?
        ORDER BY rowid
        """.trimIndent(),
        listOf(rowId, stream)
    ) { statement ->
        PendingEntry(
            id = statement.getText(0),
            lane = ReplicaLane.fromRaw(statement.textOrNull(1)) ?: ReplicaLane.BULK,
            payload = statement.getText(2).toByteArray(Charsets.UTF_8)
        )
    }

    /**
     * Pending BULK entries addressing any of these rows — the promotion
     * lookup. One index seek per named id, so an interactive write costs the
     * same whether the bulk backlog holds ten ops or ten thousand.
     */
    internal fun pendingBulkEntries(db: SQLiteConnection, rowIds: List<String>): List<JournalRow> {
        if (rowIds.isEmpty()) return emptyList()
        return entries(
            db, "state IN ('draft', 'owed', 'frozen') AND lane = ? AND row_id IN (${questionMarks(rowIds.size)})",
            listOf<Any?>(ReplicaLane.BULK.rawValue) + rowIds
        )
    }

    /** The lanes a drain still has to send: owed intents, and intents of a submission awaiting its answer. */
    internal fun lanesOwed(db: SQLiteConnection): Set<ReplicaLane> =
        db.queryStrings(
            "SELECT lane FROM intents WHERE state = 'owed' " +
                "UNION SELECT lane FROM intents WHERE sequence IN (SELECT sequence FROM submissions)"
        )
            .mapNotNull { ReplicaLane.fromRaw(it) }
            .toSet()

    internal fun owesWork(db: SQLiteConnection, lane: ReplicaLane): Boolean = db.queryBool(
        "SELECT EXISTS(SELECT 1 FROM intents WHERE lane = ? " +
            "AND (state = 'owed' OR sequence IN (SELECT sequence FROM submissions)))",
        listOf(lane.rawValue)
    )

    internal fun promote(db: SQLiteConnection, entryIds: List<String>) {
        if (entryIds.isEmpty()) return
        db.exec(
            "UPDATE intents SET lane = ? WHERE id IN (${questionMarks(entryIds.size)})",
            listOf<Any?>(ReplicaLane.INTERACTIVE.rawValue) + entryIds
        )
    }

    // MARK: Queue

    internal fun entry(db: SQLiteConnection, id: String, payload: ByteArray): JournalRow? =
        entries(db, "id = ? AND payload = ?", listOf(id, payload.toString(Charsets.UTF_8))).firstOrNull()

    internal fun updatePreimage(db: SQLiteConnection, id: String, preimage: ByteArray) {
        db.exec("UPDATE intents SET preimage = ? WHERE id = ?", listOf(preimage.toString(Charsets.UTF_8), id))
    }

    /** Owed and frozen: every intent that has not been answered, drafts excepted. */
    internal fun pending(db: SQLiteConnection): List<JournalRow> = entries(db, "state IN ('owed', 'frozen')")

    /** Owed intents of one stream — of every stream when null. */
    internal fun owed(db: SQLiteConnection, stream: String?): List<JournalRow> =
        if (stream == null) entries(db, "state = 'owed'") else entries(db, "state = 'owed' AND stream = ?", listOf(stream))

    internal fun entriesForStream(db: SQLiteConnection, stream: String): List<JournalRow> =
        entries(db, "state <> 'accepted' AND stream = ?", listOf(stream))

    internal fun parked(db: SQLiteConnection): List<JournalRow> = entries(db, "state = 'refused'")

    /**
     * Abandon an intent whatever its bytes or refusal — the discard path: a
     * row thrown away before the server ever heard its id must stop owing
     * anything, or the next drain resurrects it. A frozen intent stays for
     * its own verdict.
     */
    internal fun discard(db: SQLiteConnection, id: String) {
        db.exec("DELETE FROM intents WHERE id = ? AND state IN ('draft', 'owed', 'refused')", listOf(id))
    }

    /**
     * Discard an address's intents, frozen ones excepted. `except` preserves a
     * rejected create's refusal during its revert cascade.
     */
    internal fun discardEntries(
        db: SQLiteConnection,
        stream: String,
        rowId: String,
        except: String? = null,
    ) {
        db.exec(
            """
            DELETE FROM intents
            WHERE row_id = ? AND stream = ? AND state IN ('draft', 'owed', 'refused')
              AND (? IS NULL OR id != ?)
            """.trimIndent(),
            listOf(rowId, stream, except, except)
        )
    }

    internal fun discardLifetime(db: SQLiteConnection, stream: String, rowId: String, incarnation: String?, except: String? = null) {
        for (entry in entriesAddressing(db, stream, rowId)) {
            if (entry.id != except && entry.op().incarnation == incarnation) discard(db, entry.id)
        }
    }

    // MARK: - Cold-boot reads
    //
    // A store opened over an existing file answers these without an engine —
    // what a relaunched process (or a test standing in for one) sees.

    public fun pendingOps(): List<JournalRow> = read { pending(it) }

    public fun parkedOps(): List<JournalRow> = read { parked(it) }

    public fun fold(stream: String, rowId: String): ByteArray? =
        read { doc(it, stream, rowId)?.fold }

    /**
     * Every intent a row still has, refused ones INCLUDED — an "unborn" check
     * must see a rejected create too (the server refused the birth; the row
     * still never existed server-side) — in queue order: what a pulled
     * snapshot has to be rebased under. Accepted intents are overlays.
     */
    internal fun entriesAddressing(
        db: SQLiteConnection,
        stream: String,
        rowId: String,
    ): List<JournalRow> = entries(db, "row_id = ? AND stream = ? AND state <> 'accepted'", listOf(rowId, stream))

    internal fun entriesAddressing(
        db: SQLiteConnection,
        stream: String,
        rowId: String,
        verb: String,
    ): List<JournalRow> = entries(db, "row_id = ? AND stream = ? AND op = ? AND state <> 'accepted'", listOf(rowId, stream, verb))
}

private class DecodedRowCache {
    data class RawRows(
        val generation: ULong,
        val sequence: Long,
        val records: List<ReplicaStateStore.RowRecord>,
    )

    private class Entry(
        val sequence: Long,
        val records: List<ReplicaStateStore.RowRecord>,
        val byKey: Map<String, ReplicaStateStore.RowRecord>,
        val materializations: MutableMap<KClass<*>, Any> = mutableMapOf(),
    )

    private data class MaterializationKey(val stream: String, val model: KClass<*>)

    private val lock = ReentrantLock()
    private val generations = mutableMapOf<String, ULong>()
    private val entries = mutableMapOf<String, Entry>()

    /**
     * The superseded entry, kept as a per-row reuse DONOR: rows whose raw
     * text is unchanged carry their decoded fields and models into the next
     * materialization. One per stream — replaced, never accumulated. Never
     * SERVED: readers only see `entries`.
     */
    private val donors = mutableMapOf<String, Entry>()
    private val materializationLocks = mutableMapOf<MaterializationKey, ReentrantLock>()

    fun generation(stream: String): ULong = lock.withLock { generations[stream] ?: 0uL }

    fun rawRows(stream: String, minimumSequence: Long?): RawRows? = lock.withLock {
        val entry = entries[stream] ?: return@withLock null
        if (minimumSequence != null && entry.sequence < minimumSequence) return@withLock null
        RawRows(
            generation = generations[stream] ?: 0uL,
            sequence = entry.sequence,
            records = entry.records
        )
    }

    @Suppress("UNCHECKED_CAST")
    fun <Model : Any> materialization(
        stream: String,
        model: KClass<*>,
        minimumSequence: Long?,
    ): ReplicaStateStore.RowMaterialization<Model>? = lock.withLock {
        val entry = entries[stream] ?: return@withLock null
        if (minimumSequence != null && entry.sequence < minimumSequence) return@withLock null
        entry.materializations[model] as? ReplicaStateStore.RowMaterialization<Model>
    }

    fun <Value> withMaterializationLock(
        stream: String,
        model: KClass<*>,
        body: () -> Value,
    ): Value {
        val key = MaterializationKey(stream, model)
        val materializationLock = lock.withLock {
            materializationLocks.getOrPut(key) { ReentrantLock() }
        }
        return materializationLock.withLock(body)
    }

    fun installRawRows(
        stream: String,
        generation: ULong,
        sequence: Long,
        records: List<ReplicaStateStore.RowRecord>,
    ): Boolean = lock.withLock {
        if ((generations[stream] ?: 0uL) != generation) return@withLock false
        val existing = entries[stream]
        if (existing != null && existing.sequence >= sequence) return@withLock true
        entries[stream] = Entry(
            sequence = sequence,
            records = records,
            byKey = records.associateBy { it.id }
        )
        true
    }

    @Suppress("UNCHECKED_CAST")
    fun <Model : Any> installMaterialization(
        materialization: ReplicaStateStore.RowMaterialization<Model>,
        stream: String,
        model: KClass<*>,
        generation: ULong,
        sequence: Long,
    ): ReplicaStateStore.RowMaterialization<Model>? = lock.withLock {
        if ((generations[stream] ?: 0uL) != generation) return@withLock null
        val entry = entries[stream] ?: return@withLock null
        if (entry.sequence != sequence) return@withLock null

        val existing = entry.materializations[model]
            as? ReplicaStateStore.RowMaterialization<Model>
        if (existing != null) return@withLock existing
        entry.materializations[model] = materialization
        materialization
    }

    /**
     * Per-row reuse for a SCOPED read: the live entry's decoded records
     * and this type's models, the superseded donor when nothing is live.
     * The caller validates each row against raw text + type, so a stale
     * entry only reduces reuse, never poisons it.
     */
    @Suppress("UNCHECKED_CAST")
    fun <Model : Any> reuseSources(
        stream: String,
        model: KClass<*>,
    ): Pair<Map<String, ReplicaStateStore.RowRecord>, Map<String, Model>> = lock.withLock {
        val entry = entries[stream] ?: donors[stream]
            ?: return@withLock emptyMap<String, ReplicaStateStore.RowRecord>() to emptyMap()
        val models = (entry.materializations[model]
            as? ReplicaStateStore.RowMaterialization<Model>)?.byKey ?: emptyMap()
        entry.byKey to models
    }

    fun prepareMutation(stream: String) = lock.withLock {
        generations[stream] = (generations[stream] ?: 0uL) + 1uL
        Unit
    }

    fun commitMutation(stream: String, through: Long) = lock.withLock {
        val entry = entries[stream]
        if (entry != null && entry.sequence >= through) return@withLock
        generations[stream] = (generations[stream] ?: 0uL) + 1uL
        entries.remove(stream)?.let { donors[stream] = it }
        Unit
    }

    fun donorRecords(stream: String): Map<String, ReplicaStateStore.RowRecord>? =
        lock.withLock { donors[stream]?.byKey }

    /**
     * The donor's records and its materialized models for `Model`, read
     * atomically — a record/model pair from two different donor generations
     * could otherwise pair a stale model with a matching-looking record.
     */
    @Suppress("UNCHECKED_CAST")
    fun <Model : Any> donorSnapshot(
        stream: String,
        model: KClass<*>,
    ): Pair<Map<String, ReplicaStateStore.RowRecord>, Map<String, Model>>? = lock.withLock {
        val donor = donors[stream] ?: return@withLock null
        val models = (donor.materializations[model]
            as? ReplicaStateStore.RowMaterialization<Model>)?.byKey ?: emptyMap()
        donor.byKey to models
    }
}
