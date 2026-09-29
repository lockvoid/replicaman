import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// `list` is a sync indexed read over
/// a generated `Field` enum with stable operators; `watch` delivers CHANGES
/// of the scoped picture only, on the main actor. A commit outside the
/// scope is silent; a scoped read after a whole-stream one decodes nothing.
final class ScopedReadTests: XCTestCase {

    private struct ScopedNote: ReplicaWritableRowModel, Equatable {
        enum Field: String, ReplicaIndexedField {
            case kind, title, score
        }

        static let streamName = "notes"
        nonisolated(unsafe) static var decodes = Tally()

        var id: String
        var kind: String?
        var title: String?
        var score: Double?

        init?(id: String, type: String?, data: [String: ReplicaValue]) {
            Self.decodes.bump()
            self.id = id
            kind = data["kind"]?.string
            title = data["title"]?.string
            score = data["score"]?.number
        }

        var typeName: String? { nil }

        func encode() -> [String: ReplicaValue] {
            var out: [String: ReplicaValue] = [:]
            if let kind { out["kind"] = .string(kind) }
            if let title { out["title"] = .string(title) }
            if let score { out["score"] = .number(score) }
            return out
        }
    }

    private static let indexes: [ReplicaIndexSpec] = [
        ReplicaIndexSpec(stream: "notes", field: "kind"),
        ReplicaIndexSpec(stream: "notes", field: "score"),
        ReplicaIndexSpec(stream: "notes", field: "title"),
        ReplicaIndexSpec(stream: "notes", field: "title", kind: .fts5),
    ]

    private func world() throws -> (ReplicaStateStore, ReplicaEngine, RowStream<ScopedNote>) {
        let schema = ReplicaSchema(streams: Fixture.schema().specs, indexes: Self.indexes)
        let store = try Fixture.store(indexes: Self.indexes)
        let engine = Fixture.engine(store: store, transport: StubTransport(), schema: schema)
        return (store, engine, RowStream<ScopedNote>(engine: engine))
    }

    private func seed(_ engine: ReplicaEngine) async throws {
        let rows: [(String, [String: ReplicaValue])] = [
            ("n1", ["kind": .string("clip"), "title": .string("дача"), "score": .number(1)]),
            ("n2", ["kind": .string("clip"), "title": .string("дачный участок"), "score": .number(2.5)]),
            ("n3", ["kind": .string("still"), "title": .string("Дача зимой"), "score": .number(1)]),
            ("n4", ["title": .string("café")]),
            ("n5", ["kind": .string("clip"), "title": .string("сад")]),
        ]
        for (id, data) in rows {
            try await engine.saveRow(stream: "notes", id: id, type: nil, data: data)
        }
    }

    override func setUp() {
        super.setUp()
        ScopedNote.decodes = Tally()
    }

    func testEqListsOnlyTheScope() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)

        XCTAssertEqual(try notes.list(.eq(.kind, "clip")).map(\.id), ["n1", "n2", "n5"])
        XCTAssertEqual(try notes.list(.eq(.kind, "still")).map(\.id), ["n3"])
        XCTAssertEqual(try notes.list(.eq(.score, 1)).map(\.id), ["n1", "n3"], "a JSON integer and a bound Double compare numerically")
        XCTAssertEqual(try notes.list(.eq(.score, 2.5)).map(\.id), ["n2"])
        XCTAssertEqual(try notes.list(.eq(.score, "1")).map(\.id), [], "a string never equals a number — json_extract typing holds at the edge")
        XCTAssertEqual(try notes.list().map(\.id), ["n1", "n2", "n3", "n4", "n5"], "bare list is the whole stream")
    }

    func testPrefixIsBinaryStringStart() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)

        XCTAssertEqual(try notes.list(.hasPrefix(.title, "дач")).map(\.id), ["n1", "n2"], "BINARY: Дача (capital) is not a дач prefix — that is match's job")
        XCTAssertEqual(try notes.list(.hasPrefix(.title, "дача")).map(\.id), ["n1"])
        XCTAssertEqual(try notes.list(.hasPrefix(.title, "")).map(\.id), ["n1", "n2", "n3", "n4", "n5"])
    }

    func testMatchFoldsCaseAndDiacriticsOverTheRawQuery() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)

        XCTAssertEqual(try notes.list(.match(.title, "дач")).map(\.id), ["n1", "n2", "n3"], "unicode61 folds case")
        XCTAssertEqual(try notes.list(.match(.title, "cafe")).map(\.id), ["n4"], "remove_diacritics 2: café ~ cafe")
        XCTAssertEqual(try notes.list(.match(.title, "дач уч")).map(\.id), ["n2"], "every word must start-match (AND)")
        XCTAssertEqual(try notes.list(.match(.title, "   ")).map(\.id), ["n1", "n2", "n3", "n4", "n5"], "an empty query is a cleared search box")
        XCTAssertEqual(try notes.list(.match(.title, "\"дач* OR (x:y)")).map(\.id), [], "FTS syntax in the raw query is neutralized, never executed")
    }

    func testCombinatorsNarrowInSQL() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)

        XCTAssertEqual(try notes.list(.oneOf(.kind, ["clip", "still"])).map(\.id), ["n1", "n2", "n3", "n5"])
        XCTAssertEqual(try notes.list(.oneOf(.kind, [])).map(\.id), [], "one of nothing is nothing")
        XCTAssertEqual(try notes.list(.isNull(.kind)).map(\.id), ["n4"], "an absent field is null")
        XCTAssertEqual(try notes.list(.and([.eq(.kind, "clip"), .eq(.score, 1)])).map(\.id), ["n1"])
        XCTAssertEqual(try notes.list(.and([.eq(.kind, "clip"), .match(.title, "дач")])).map(\.id), ["n1", "n2"])
        XCTAssertEqual(try notes.list(.and([])).map(\.id), ["n1", "n2", "n3", "n4", "n5"], "an empty conjunction holds for every row")
        XCTAssertEqual(try notes.list(.not(.eq(.kind, "clip"))).map(\.id), ["n3"], "SQL NOT: n4's null kind is in neither side")
    }

    private enum Photo: ReplicaVariant {
        static let wireType = "Photo"
    }

    func testKindScopesAnSTIStreamByItsWireType() async throws {
        let (_, engine, notes) = try world()
        try await engine.saveRow(stream: "notes", id: "k1", type: "Photo", data: ["kind": .string("clip")])
        try await engine.saveRow(stream: "notes", id: "k2", type: "Text", data: ["kind": .string("clip")])

        XCTAssertEqual(try notes.list(.kind(Photo.self)).map(\.id), ["k1"])
        XCTAssertEqual(try notes.list(.and([.kind("Text"), .eq(.kind, "clip")])).map(\.id), ["k2"])
    }

    func testOrderAndLimitRideTheIndexedColumns() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)

        XCTAssertEqual(try notes.list(order: [.ascending(.score)]).map(\.id), ["n4", "n5", "n1", "n3", "n2"],
                       "nulls first ascending; the tie at 1 breaks by row id")
        XCTAssertEqual(try notes.list(order: [.descending(.score)]).map(\.id), ["n2", "n3", "n1", "n5", "n4"],
                       "descending: the tiebreak follows the last key's direction, nulls last")
        XCTAssertEqual(try notes.list(.eq(.kind, "clip"), order: [.ascending(.title)], limit: 2).map(\.id), ["n1", "n2"])
        XCTAssertEqual(try notes.list(limit: 1).map(\.id), ["n1"], "a bare limit takes the first rows by id")
    }

    func testAnOrderedWatchDeliversTheOrderedPicture() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)
        let pictures = Pictures()

        let watch = notes.watch(.eq(.kind, "clip"), order: [.descending(.score)], limit: 1, includeInitial: true) { rows in
            pictures.append(rows.map(\.id))
        }
        defer { watch.cancel() }
        await eventually(timeout: 3, "the baseline is the top of the order") { pictures.all.count == 1 }
        XCTAssertEqual(pictures.all, [["n2"]])

        try await engine.saveRow(stream: "notes", id: "n6", type: nil, data: ["kind": .string("clip"), "score": .number(9)])
        await eventually(timeout: 3, "a new top delivers") { pictures.all.count == 2 }
        XCTAssertEqual(pictures.all.last, ["n6"])
    }

    func testHoldKeepsTheWatchForTheCallingTask() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)
        let pictures = Pictures()

        let holder = Task { await notes.watch(.eq(.kind, "clip")) { rows in pictures.append(rows.map(\.id)) }.hold() }
        try await Task.sleep(for: .milliseconds(200))
        try await engine.saveRow(stream: "notes", id: "n6", type: nil, data: ["kind": .string("clip")])
        await eventually(timeout: 3, "a held watch delivers") { pictures.all.count == 1 }

        holder.cancel()
        await holder.value
        try await engine.saveRow(stream: "notes", id: "n7", type: nil, data: ["kind": .string("clip")])
        try await Task.sleep(for: .milliseconds(300))
        XCTAssertEqual(pictures.all.count, 1, "cancelling the holder cancels the watch")
    }

    func testIdentityPredicatesScopeByAddress() async throws {
        let (_, engine, notes) = try world()
        let keys = ["pmck/Project/p1/plan", "pmck/Element/e1/original", "pmck/Element/e1/poster", "pmck/Element/e2/original", "pmck/Element/e10/original", "loose"]
        for key in keys {
            try await engine.saveRow(stream: "notes", id: key, type: nil, data: ["kind": .string("cook")])
        }

        XCTAssertEqual(try notes.list(.id("pmck/Element/e1/poster")).map(\.id), ["pmck/Element/e1/poster"])
        XCTAssertEqual(
            try notes.list(.idPrefixes(["pmck/Project/p1/", "pmck/Element/e1/"])).map(\.id),
            ["pmck/Element/e1/original", "pmck/Element/e1/poster", "pmck/Project/p1/plan"],
            "a project's cooks are the union of its records' address prefixes; e10 is not e1"
        )
        XCTAssertEqual(try notes.list(.idPrefixes([])).map(\.id), [], "an empty union is nobody's scope")
    }

    func testScopedReadsAreIndexServed() async throws {
        let (store, engine, _) = try world()
        try await seed(engine)
        func plan(_ predicate: ReplicaPredicate<ScopedNote.Field>) throws -> String {
            let compiled = predicate.compile(stream: "notes", indexes: store.indexes)
            return try store.pool.read { db in
                var arguments = StatementArguments(["notes"])
                arguments += compiled.arguments
                return try Row.fetchAll(
                    db,
                    sql: "EXPLAIN QUERY PLAN SELECT row_id FROM snapshots WHERE stream = ? AND (\(compiled.sql))",
                    arguments: arguments
                ).map { $0["detail"] as String }.joined(separator: " | ")
            }
        }

        let eq = try plan(.eq(.kind, "clip"))
        XCTAssertTrue(eq.contains("idx_kind"), eq)
        let prefix = try plan(.hasPrefix(.title, "да"))
        XCTAssertTrue(prefix.contains("idx_title"), prefix)
        let match = try plan(.match(.title, "да"))
        XCTAssertTrue(match.contains("fts_notes_title"), match)
        let identity = try plan(.idPrefixes(["pmck/Project/p1/", "pmck/Element/e1/", "pmck/Element/e2/"]))
        XCTAssertEqual(
            identity.components(separatedBy: "row_id>?").count - 1, 3,
            "an address union is one primary-key RANGE SEEK per prefix (MULTI-INDEX OR), never a filtered walk of the stream: \(identity)"
        )
    }

    func testScopedListReusesTheStreamMaterialization() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)
        ScopedNote.decodes = Tally()

        XCTAssertEqual(try notes.list().count, 5)
        XCTAssertEqual(ScopedNote.decodes.count, 5, "the whole-stream read decodes once per row")
        XCTAssertEqual(try notes.list(.eq(.kind, "clip")).count, 3)
        XCTAssertEqual(ScopedNote.decodes.count, 5, "a scoped read over unchanged rows decodes nothing")

        try await engine.saveRow(stream: "notes", id: "n1", type: nil, data: ["kind": .string("clip"), "title": .string("дача 2")])
        XCTAssertEqual(try notes.list(.eq(.kind, "clip")).map(\.title), ["дача 2", "дачный участок", "сад"])
        XCTAssertEqual(ScopedNote.decodes.count, 6, "only the changed row pays a decode")
    }

    func testScopedWatchDeliversOnlyScopeChanges() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)
        let pictures = Pictures()

        let watch = notes.watch(.eq(.kind, "clip")) { rows in pictures.append(rows.map(\.id)) }
        defer { watch.cancel() }
        try await Task.sleep(for: .milliseconds(200))
        XCTAssertEqual(pictures.all, [], "no includeInitial: the sync list gave the first picture")

        try await engine.saveRow(stream: "notes", id: "n3", type: nil, data: ["kind": .string("still"), "title": .string("Дача весной")])
        try await Task.sleep(for: .milliseconds(300))
        XCTAssertEqual(pictures.all, [], "a commit outside the scope is silent")

        try await engine.saveRow(stream: "notes", id: "n6", type: nil, data: ["kind": .string("clip"), "title": .string("лес")])
        await eventually(timeout: 3, "an in-scope commit delivers the new picture") { pictures.all.count == 1 }
        XCTAssertEqual(pictures.all, [["n1", "n2", "n5", "n6"]])

        try await engine.saveRow(stream: "notes", id: "n6", type: nil, data: ["kind": .string("clip"), "title": .string("лес")])
        try await Task.sleep(for: .milliseconds(300))
        XCTAssertEqual(pictures.all.count, 1, "an identical re-save changes nothing and delivers nothing")

        try await engine.saveRow(stream: "notes", id: "n6", type: nil, data: ["kind": .string("still"), "title": .string("лес")])
        await eventually(timeout: 3, "leaving the scope delivers the shrunken picture") { pictures.all.count == 2 }
        XCTAssertEqual(pictures.all.last, ["n1", "n2", "n5"])
    }

    func testAddressScopedWatchDeliversOnlyItsRecords() async throws {
        let (_, engine, notes) = try world()
        try await engine.saveRow(stream: "notes", id: "pmck/Element/e1/original", type: nil, data: ["kind": .string("cook"), "title": .string("v1")])
        let pictures = Pictures()

        let watch = notes.watch(.idPrefixes(["pmck/Project/p1/", "pmck/Element/e1/"])) { rows in pictures.append(rows.map(\.id)) }
        defer { watch.cancel() }
        try await Task.sleep(for: .milliseconds(200))

        try await engine.saveRow(stream: "notes", id: "pmck/Element/e2/original", type: nil, data: ["kind": .string("cook")])
        try await Task.sleep(for: .milliseconds(300))
        XCTAssertEqual(pictures.all, [], "another record's cook is silent")

        try await engine.saveRow(stream: "notes", id: "pmck/Project/p1/plan", type: nil, data: ["kind": .string("cook")])
        await eventually(timeout: 3, "a cook of one of the scope's records delivers") { pictures.all.count == 1 }
        XCTAssertEqual(pictures.all, [["pmck/Element/e1/original", "pmck/Project/p1/plan"]])
    }

    func testScopedWatchCanIncludeTheBaseline() async throws {
        let (_, engine, notes) = try world()
        try await seed(engine)
        let pictures = Pictures()

        let watch = notes.watch(.match(.title, "дач"), includeInitial: true) { rows in pictures.append(rows.map(\.id)) }
        defer { watch.cancel() }
        await eventually(timeout: 3, "includeInitial delivers the committed baseline") { pictures.all.count == 1 }
        XCTAssertEqual(pictures.all, [["n1", "n2", "n3"]])
    }

    private final class Pictures: @unchecked Sendable {
        private let lock = NSLock()
        private var stored: [[String]] = []

        var all: [[String]] { lock.withLock { stored } }

        func append(_ picture: [String]) {
            lock.withLock { stored.append(picture) }
        }
    }
}
