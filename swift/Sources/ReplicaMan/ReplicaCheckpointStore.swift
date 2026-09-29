import Foundation
import GRDB

/// A pull round in progress: the cursor it continues from (nil until a
/// baseline's first page), whether it replaces the shard's base, and the
/// highest accepted submission when it started.
struct ReplicaRound: Equatable, Sendable {
    let cursor: String?
    let reset: Bool
    let visible: Int64
}

extension ReplicaStateStore {
    /// The cursor of the shard's last published round.
    func cursor(_ db: Database, shard: String) throws -> String? {
        try String.fetchOne(db, sql: "SELECT cursor FROM checkpoints WHERE shard = ?", arguments: [shard])
    }

    func setCursor(_ db: Database, _ cursor: String, shard: String) throws {
        try db.execute(sql: """
            INSERT INTO checkpoints (shard, cursor) VALUES (?, ?)
            ON CONFLICT(shard) DO UPDATE SET cursor = excluded.cursor
            """, arguments: [shard, cursor])
    }

    /// Forget the read position and the round in progress: the next round is
    /// a baseline. The new generation keeps a pull already on the wire from
    /// staging its answer into the abandoned round.
    func clearCursor(_ db: Database, shard: String) throws {
        try db.execute(sql: """
            INSERT INTO checkpoints (shard, cursor, generation) VALUES (?, NULL, 1)
            ON CONFLICT(shard) DO UPDATE SET cursor = NULL, generation = generation + 1
            """, arguments: [shard])
        try discardDownload(db, shard: shard)
    }

    func readGeneration(_ db: Database, shard: String) throws -> Int64 {
        try Int64.fetchOne(db, sql: "SELECT generation FROM checkpoints WHERE shard = ?", arguments: [shard]) ?? 0
    }

    func round(_ db: Database, shard: String) throws -> ReplicaRound? {
        try Row.fetchOne(db, sql: "SELECT cursor, reset, visible FROM downloads WHERE shard = ?", arguments: [shard])
            .map { ReplicaRound(cursor: $0["cursor"], reset: $0["reset"], visible: $0["visible"]) }
    }

    /// The shard's round in progress, or a new one from the published cursor.
    func beginRound(_ db: Database, shard: String) throws -> ReplicaRound {
        if let round = try round(db, shard: shard) { return round }
        return try startRound(db, shard: shard, cursor: cursor(db, shard: shard))
    }

    /// Intents accepted after this moment carry higher sequences, so the
    /// round removes only what it can prove the server already shows.
    func startRound(_ db: Database, shard: String, cursor: String?) throws -> ReplicaRound {
        let visible = try Int64.fetchOne(db, sql: "SELECT COALESCE(MAX(sequence), 0) FROM intents WHERE state = 'accepted'") ?? 0
        let round = ReplicaRound(cursor: cursor, reset: cursor == nil, visible: visible)
        try db.execute(sql: "INSERT INTO downloads (shard, cursor, reset, visible, started_at) VALUES (?, ?, ?, ?, ?)",
            arguments: [shard, cursor, round.reset, visible, Date().timeIntervalSince1970])
        return round
    }

    /// Keep one answer of the round and continue from its cursor.
    func stage(_ db: Database, shard: String, content: Data, cursor: String) throws {
        try db.execute(sql: """
            INSERT INTO download_pages (shard, page, content)
            VALUES (?, (SELECT COALESCE(MAX(page) + 1, 0) FROM download_pages WHERE shard = ?), ?)
            """, arguments: [shard, shard, content])
        try db.execute(sql: "UPDATE downloads SET cursor = ? WHERE shard = ?", arguments: [cursor, shard])
    }

    /// The round's pages in order, one decoded at a time.
    func forEachStagedPage(_ db: Database, shard: String, _ body: (ReplicaPullPage) throws -> Void) throws {
        let pages = try Data.fetchCursor(db, sql: "SELECT content FROM download_pages WHERE shard = ? ORDER BY page",
                                         arguments: [shard])
        while let content = try pages.next() {
            try body(ReplicaPullPage.decode(content))
        }
    }

    func discardDownload(_ db: Database, shard: String) throws {
        try db.execute(sql: "DELETE FROM download_pages WHERE shard = ?", arguments: [shard])
        try db.execute(sql: "DELETE FROM downloads WHERE shard = ?", arguments: [shard])
    }
}
