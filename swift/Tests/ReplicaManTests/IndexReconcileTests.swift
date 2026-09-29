import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// Read structures are DERIVATIVES of
/// `snapshots.data`, converged at every open — declared ↔ actual, idempotent,
/// rebuilt from the rows already on disk, stale ones dropped. No migration
/// files, no versions: the manifest is the schema.
final class IndexReconcileTests: XCTestCase {
    private let kind = ReplicaIndexSpec(stream: "notes", field: "kind")
    private let title = ReplicaIndexSpec(stream: "notes", field: "title", kind: .fts5)

    private func generatedColumns(_ store: ReplicaStateStore) throws -> Set<String> {
        try store.pool.read { db in
            Set(try String.fetchAll(db, sql: "SELECT name FROM pragma_table_xinfo('snapshots') WHERE hidden IN (2, 3)"))
        }
    }

    private func owned(_ store: ReplicaStateStore, type: String) throws -> Set<String> {
        try store.pool.read { db in
            Set(try String.fetchAll(
                db,
                sql: """
                    SELECT name FROM sqlite_master WHERE type = ?
                    AND (name LIKE 'idx\\_%' ESCAPE '\\' OR name LIKE 'fts\\_%' ESCAPE '\\')
                    AND sql NOT LIKE 'CREATE TABLE ''fts\\_%' ESCAPE '\\'
                    """,
                arguments: [type]
            ))
        }
    }

    private func ftsRows(_ store: ReplicaStateStore) throws -> Int {
        try store.pool.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM fts_notes_title") ?? -1
        }
    }

    private func seed(_ store: ReplicaStateStore, _ rows: [(String, String, String)]) throws {
        try store.pool.write { db in
            for (id, kind, title) in rows {
                try store.upsertSnapshot(
                    db, stream: "notes", rowId: id, shard: "user", type: nil,
                    data: ["kind": .string(kind), "title": .string(title)]
                )
            }
        }
    }

    func testDeclaredStructuresExistAfterOpen() throws {
        let store = try Fixture.store(indexes: [kind, title])

        XCTAssertEqual(try generatedColumns(store), ["ix_kind", "ix_title"], "one generated column per indexed FIELD")
        XCTAssertEqual(try owned(store, type: "index"), ["idx_kind"])
        XCTAssertEqual(try owned(store, type: "table"), ["fts_notes_title"])
        XCTAssertEqual(try owned(store, type: "trigger"), ["fts_notes_title_ai", "fts_notes_title_ad", "fts_notes_title_au"])
        // The intents' own indexes are outside the reconcile's scope.
        let intents = try store.pool.read { db in
            try String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'intents' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'")
        }
        XCTAssertEqual(Set(intents), ["intents_owed", "intents_address", "intents_draft", "intents_sequence"])
    }

    func testReopenWithFewerDeclarationsDropsTheStale() throws {
        let first = try Fixture.store(indexes: [kind, title])
        try seed(first, [("n1", "clip", "дача")])
        let path = first.pool.path
        try first.close()

        let second = try ReplicaStateStore(path: path, indexes: [kind])

        XCTAssertEqual(try generatedColumns(second), ["ix_kind"], "an undeclared derivative is dropped, column included")
        XCTAssertEqual(try owned(second, type: "index"), ["idx_kind"])
        XCTAssertEqual(try owned(second, type: "table"), [])
        XCTAssertEqual(try owned(second, type: "trigger"), [])
        let survivors = try second.pool.read { db in try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM snapshots") }
        XCTAssertEqual(survivors, 1, "dropping a derivative never touches data")
    }

    func testReconcileIsIdempotent() throws {
        let first = try Fixture.store(indexes: [kind, title])
        try seed(first, [("n1", "clip", "дача"), ("n2", "still", "сад")])
        let path = first.pool.path
        let schema = try first.pool.read { db in
            try Row.fetchAll(db, sql: "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY type, name")
        }
        try first.close()

        let second = try ReplicaStateStore(path: path, indexes: [kind, title])

        let again = try second.pool.read { db in
            try Row.fetchAll(db, sql: "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY type, name")
        }
        XCTAssertEqual(schema, again, "a converged open executes no DDL")
        XCTAssertEqual(try ftsRows(second), 2, "a converged open does not rebuild (and never double-inserts) the fts rows")
    }

    func testStructuresBuildFromRowsAlreadyOnDisk() throws {
        let bare = try Fixture.store()
        try seed(bare, [("n1", "clip", "дача"), ("n2", "clip", "дачный участок"), ("n3", "still", "сад")])
        try bare.pool.write { db in
            try bare.upsertSnapshot(db, stream: "jobs", rowId: "j1", shard: "user", type: nil, data: ["title": .string("дача")])
        }
        let path = bare.pool.path
        try bare.close()

        let indexed = try ReplicaStateStore(path: path, indexes: [kind, title])

        XCTAssertEqual(try ftsRows(indexed), 3, "the fts table is rebuilt from the stream's rows on disk — another stream's rows stay out")
        let plan = try indexed.pool.read { db in
            try Row.fetchAll(
                db,
                sql: "EXPLAIN QUERY PLAN SELECT row_id FROM snapshots WHERE stream = 'notes' AND \"ix_kind\" = 'clip'"
            ).map { $0["detail"] as String }.joined(separator: " | ")
        }
        XCTAssertTrue(plan.contains("idx_kind"), "eq over the generated column is index-served: \(plan)")
    }

    func testFtsFollowsTheRows() throws {
        let store = try Fixture.store(indexes: [title])
        try seed(store, [("n1", "clip", "Дача"), ("n2", "clip", "дачный участок"), ("n3", "still", "сад")])

        func matching(_ query: String) throws -> [String] {
            try store.pool.read { db in
                try String.fetchAll(
                    db, sql: "SELECT row_id FROM fts_notes_title WHERE fts_notes_title MATCH ? ORDER BY row_id", arguments: [query]
                )
            }
        }

        XCTAssertEqual(try matching("\"дач\"*"), ["n1", "n2"], "unicode61 folds case — Дача matches дач")
        try seed(store, [("n2", "clip", "сарай")])
        XCTAssertEqual(try matching("\"дач\"*"), ["n1"], "a replaced row's old terms are gone")
        XCTAssertEqual(try matching("\"сар\"*"), ["n2"])
        try store.pool.write { db in try store.deleteSnapshot(db, stream: "notes", rowId: "n1") }
        XCTAssertEqual(try matching("\"дач\"*"), [], "a deleted row leaves the index")
        XCTAssertEqual(try ftsRows(store), 2)
    }
}
