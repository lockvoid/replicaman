package io.replicaman

import androidx.sqlite.SQLiteConnection

/**
 * ReplicaTransaction.swift: reads and row edits share one SQLite transaction.
 * Models are decoded from this transaction's bytes, never borrowed from the read cache.
 */
public class ReplicaTransaction internal constructor(
    internal val engine: ReplicaEngine,
    internal val db: SQLiteConnection,
    internal val store: ReplicaStateStore,
    internal val lane: ReplicaLane,
    internal val draft: String?,
) {
    internal var atomicEntries: MutableList<String>? = null

    internal var journaled: Boolean = false
        private set

    public fun <M : ReplicaRowModel, F : ReplicaIndexedField> find(
        type: ReplicaRowModelType<M, F>, id: String,
    ): M? {
        val row = store.snapshot(db, type.streamName, id) ?: return null
        return type.from(id, row.type, row.data) ?: throw ReplicaError.UndecodableRow(type.streamName, id)
    }

    public fun <M : ReplicaRowModel, F : ReplicaIndexedField> list(
        type: ReplicaRowModelType<M, F>,
        predicate: ReplicaPredicate<F>? = null,
        order: List<ReplicaOrder<F>> = emptyList(),
        limit: Int? = null,
    ): List<M> {
        val compiled = predicate?.compile(type.streamName, store.indexes)
        val orderBy = ReplicaOrder.clause(order, type.streamName, store.indexes)
        val rows = db.query(
            "SELECT row_id, type, data FROM snapshots WHERE stream = ? AND (${compiled?.sql ?: "1"}) " +
                "ORDER BY $orderBy${limit?.let { " LIMIT ${it.coerceAtLeast(0)}" }.orEmpty()}",
            listOf<Any?>(type.streamName) + compiled?.arguments.orEmpty(),
        ) { statement ->
            type.from(statement.getText(0), statement.textOrNull(1), ReplicaStateStore.decodeData(statement.textOrNull(2)))
        }
        return rows.filterNotNull()
    }

    public fun <M : ReplicaWritableRowModel, F : ReplicaIndexedField> create(
        type: ReplicaWritableRowModelType<M, F>, model: M,
    ) {
        val spec = rowSpec(type.streamName)
        if (store.snapshot(db, type.streamName, model.id) != null) throw ReplicaError.RowExists(type.streamName, model.id)
        val wrote = engine.applyRowWrite(
            db, store, spec, type.streamName, model.id, model.typeName, model.encode(),
            existing = null, lane = lane, draft = draft, snapshot = model.encodeSnapshot(),
        )
        journaled = journaled || wrote
    }

    /** Only fields changed by the edit are owed; unreadable and undeclared fields stay intact. */
    public fun <M : ReplicaWritableRowModel, F : ReplicaIndexedField> update(
        type: ReplicaWritableRowModelType<M, F>, id: String, edit: (M) -> M,
    ): M {
        val spec = rowSpec(type.streamName)
        val existing = store.snapshot(db, type.streamName, id) ?: throw ReplicaError.UnknownRow(type.streamName, id)
        val model = type.from(id, existing.type, existing.data) ?: throw ReplicaError.UndecodableRow(type.streamName, id)
        val before = model.encode()
        val edited = edit(model)
        val changed = edited.encode().filter { (key, value) -> (before[key] ?: ReplicaValue.Null) != value }
        val wrote = engine.applyRowWrite(
            db, store, spec, type.streamName, id, existing.type ?: edited.typeName,
            changed, existing, lane, draft,
        )
        journaled = journaled || wrote
        return edited
    }

    public fun <M : ReplicaWritableRowModel, F : ReplicaIndexedField> delete(
        type: ReplicaWritableRowModelType<M, F>, id: String,
    ): Boolean {
        val queued = engine.applyRowDelete(db, store, rowSpec(type.streamName), type.streamName, id, lane, draft)
        journaled = journaled || queued
        return queued
    }

    public fun <M : ReplicaWritableRowModel, F : ReplicaIndexedField> delete(
        type: ReplicaWritableRowModelType<M, F>, ids: List<String>,
    ) {
        for (id in ids) delete(type, id)
    }

    public fun <M : ReplicaWritableRowModel, F : ReplicaIndexedField> rows(
        type: ReplicaWritableRowModelType<M, F>,
    ): TransactionRows<M, F> = TransactionRows(this, type)

    public fun <M : ReplicaRowModel, F : ReplicaIndexedField> readonlyRows(
        type: ReplicaRowModelType<M, F>,
    ): TransactionReadonlyRows<M, F> = TransactionReadonlyRows(this, type)

    internal fun writeRaw(
        stream: String, id: String, type: String?, data: Map<String, ReplicaValue>,
        expectation: ReplicaEngine.RowWriteExpectation, snapshot: Map<String, ReplicaValue>?,
    ) {
        val spec = rowSpec(stream)
        val existing = store.snapshot(db, stream, id)
        expectation.violation(stream, id, existing != null)?.let { throw it }
        val wrote = engine.applyRowWrite(db, store, spec, stream, id, type, data, existing, lane, draft, snapshot)
        journaled = journaled || wrote
    }

    private fun rowSpec(stream: String): ReplicaStreamSpec = engine.writableSpec(stream).also {
        if (it.lane != ReplicaStreamSpec.Lane.ROW) throw ReplicaError.LaneMismatch(stream)
    }

    internal companion object {
        private val current = ThreadLocal<ReplicaTransaction?>()

        fun open(engine: ReplicaEngine): ReplicaTransaction? = current.get()?.takeIf { it.engine === engine }

        fun <T> running(tx: ReplicaTransaction, body: () -> T): T {
            val previous = current.get()
            current.set(tx)
            return try { body() } finally { current.set(previous) }
        }
    }
}

public class TransactionRows<M : ReplicaWritableRowModel, F : ReplicaIndexedField> internal constructor(
    private val tx: ReplicaTransaction,
    private val type: ReplicaWritableRowModelType<M, F>,
) {
    public fun find(id: String): M? = tx.find(type, id)
    public fun list(predicate: ReplicaPredicate<F>? = null, order: List<ReplicaOrder<F>> = emptyList(), limit: Int? = null): List<M> =
        tx.list(type, predicate, order, limit)
    public fun create(model: M) { tx.create(type, model) }
    public fun update(id: String, edit: (M) -> M): M = tx.update(type, id, edit)
    public fun delete(id: String): Boolean = tx.delete(type, id)
    public fun delete(ids: List<String>) { tx.delete(type, ids) }
}

public class TransactionReadonlyRows<M : ReplicaRowModel, F : ReplicaIndexedField> internal constructor(
    private val tx: ReplicaTransaction,
    private val type: ReplicaRowModelType<M, F>,
) {
    public fun find(id: String): M? = tx.find(type, id)
    public fun list(predicate: ReplicaPredicate<F>? = null, order: List<ReplicaOrder<F>> = emptyList(), limit: Int? = null): List<M> =
        tx.list(type, predicate, order, limit)
}
