package io.replicaman

/** One durable SQLite snapshot. Timestamps are Unix seconds; bytes include retained copies. */
public data class ReplicaSyncStatus(
    val queuedOperations: Long,
    val heldEntities: Long,
    val submittedGroups: Long,
    val rejectedOperations: Long,
    val acceptedOperations: Long,
    val recoveryBranches: Long,
    val oldestIntentAt: Double?,
    val oldestDownloadAt: Double?,
    val journalBytes: Long,
    val submittedBytes: Long,
    val acceptedBytes: Long,
    val downloadBytes: Long,
    val recoveryBytes: Long,
    val documentBytes: Long,
) {
    public val hasUnsettledWork: Boolean get() =
        queuedOperations + heldEntities + submittedGroups + rejectedOperations +
        acceptedOperations + recoveryBranches > 0
}

public fun ReplicaStateStore.syncStatus(): ReplicaSyncStatus = read { db ->
    db.queryOne(ReplicaStatusSQL.sql) { row ->
        ReplicaSyncStatus(
            queuedOperations = row.getLong(0),
            heldEntities = row.getLong(1),
            submittedGroups = row.getLong(2),
            rejectedOperations = row.getLong(3),
            acceptedOperations = row.getLong(4),
            recoveryBranches = row.getLong(5),
            oldestIntentAt = if (row.isNull(6)) null else row.getDouble(6),
            oldestDownloadAt = if (row.isNull(7)) null else row.getDouble(7),
            journalBytes = row.getLong(8),
            submittedBytes = row.getLong(9),
            acceptedBytes = row.getLong(10),
            downloadBytes = row.getLong(11),
            recoveryBytes = row.getLong(12),
            documentBytes = row.getLong(13),
        )
    } ?: throw ReplicaError.Storage("Missing synchronization status")
}

/** Inspect all delivery stages without materializing the backlog.
 * The predicate must be read-only. Corrupt bytes throw; they cannot prove safe eviction.
 */
public fun ReplicaStateStore.containsUnsettledOperation(predicate: (ReplicaOp) -> Boolean): Boolean = read { db ->
    db.prepare(ReplicaUnsettledSQL.sql).use { rows ->
        while (rows.step()) {
            val operation = ReplicaStateStore.JournalRow.decodeOperation(rows.getBlob(0))
            if (predicate(operation)) return@read true
        }
    }
    false
}
