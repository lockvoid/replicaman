import Foundation
import GRDB

/// Durable state from one SQLite snapshot. Counts describe distinct stages;
/// byte counts measure stored payloads, including copies retained for recovery.
public struct ReplicaSyncStatus: Sendable, Equatable {
    public let queuedOperations: Int64
    public let heldEntities: Int64
    public let submittedGroups: Int64
    public let rejectedOperations: Int64
    public let acceptedOperations: Int64
    public let recoveryBranches: Int64
    public let oldestIntentAt: Date?
    public let oldestDownloadAt: Date?
    public let journalBytes: Int64
    public let submittedBytes: Int64
    public let acceptedBytes: Int64
    public let downloadBytes: Int64
    public let recoveryBytes: Int64
    public let documentBytes: Int64

    public var hasUnsettledWork: Bool {
        queuedOperations + heldEntities + submittedGroups + rejectedOperations +
        acceptedOperations + recoveryBranches > 0
    }
}

extension ReplicaStateStore {
    public func syncStatus() throws -> ReplicaSyncStatus {
        try pool.read { db in
            guard let row = try Row.fetchOne(db, sql: ReplicaStatusSQL.sql) else {
                throw ReplicaError.storage("Missing synchronization status")
            }
            return ReplicaSyncStatus(
                queuedOperations: row["queued"],
                heldEntities: row["held"],
                submittedGroups: row["submitted"],
                rejectedOperations: row["rejected"],
                acceptedOperations: row["accepted"],
                recoveryBranches: row["recovery"],
                oldestIntentAt: (row["oldest_intent"] as Double?).map(Date.init(timeIntervalSince1970:)),
                oldestDownloadAt: (row["oldest_download"] as Double?).map(Date.init(timeIntervalSince1970:)),
                journalBytes: row["journal_bytes"],
                submittedBytes: row["submitted_bytes"],
                acceptedBytes: row["accepted_bytes"],
                downloadBytes: row["download_bytes"],
                recoveryBytes: row["recovery_bytes"],
                documentBytes: row["document_bytes"]
            )
        }
    }

    /// Inspect pending, refused, in-flight and accepted-but-not-visible work.
    /// The predicate must be read-only. Corrupt bytes throw; never treat them as
    /// proof that media or other dependencies are safe to delete.
    public func containsUnsettledOperation(where predicate: (ReplicaOp) throws -> Bool) throws -> Bool {
        try pool.read { db in
            let rows = try Data.fetchCursor(db, sql: ReplicaUnsettledSQL.sql)
            while let payload = try rows.next() {
                if try predicate(JournalRow.decodeOperation(payload)) { return true }
            }
            return false
        }
    }
}
