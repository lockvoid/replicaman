import Foundation
import GRDB

/// The read predicates a stream handle accepts — STABLE operators over a
/// generated `Field` enum (the stream's INDEXED fields, per the manifest's
/// `indexes:`). Nothing here can express a whole-table filter: a field the
/// manifest did not index is not a `Field` case, so it does not compile.
///
/// Each operator rides one physical structure: `eq`/`oneOf`/`isNull`/
/// `hasPrefix` the btree over the generated column, `match` the fts5 shadow
/// table. Asking for an operator whose structure the manifest did not
/// declare is a precondition failure at first use — the manifest is
/// committed, a dev run catches it.
public enum ReplicaPredicate<Field: ReplicaIndexedField>: Sendable, Equatable {
    case equals(Field, ReplicaIndexValue)
    /// Equal to any of the values; none matches nothing.
    case oneOf(Field, [ReplicaIndexValue])
    /// Absent from the row, or JSON null.
    case isNull(Field)
    /// String start, BINARY collation (SQLite's NOCASE folds ASCII only —
    /// Cyrillic case-insensitivity is `match`'s job).
    case hasPrefix(Field, String)
    /// Word-start full text over the RAW user query: no wildcard or FTS
    /// syntax crosses the door — the engine tokenizes and sanitizes. An
    /// empty query matches everything (a cleared search box).
    case match(Field, String)
    /// The row IDENTITY — always served by the store's primary key, so it
    /// needs no manifest declaration. Address-scoped streams live here: a
    /// cook is `pmck/<record>/<id>/<op>`, and a project's cooks are the
    /// union of its records' prefixes (`idPrefixes`). An empty union
    /// matches nothing.
    case id(String)
    case idPrefixes([String])
    /// The row's STI kind — the wire type its decode switches on.
    case kind(String)
    /// Every predicate holds; none holds for every row.
    case and([ReplicaPredicate])
    /// SQL's NOT: a row whose field is null is in neither `p` nor `not(p)`.
    indirect case not(ReplicaPredicate)

    public static func eq(_ field: Field, _ value: String) -> Self { .equals(field, .string(value)) }
    public static func eq(_ field: Field, _ value: Int) -> Self { .equals(field, .integer(Int64(value))) }
    public static func eq(_ field: Field, _ value: Double) -> Self { .equals(field, .number(value)) }
    public static func eq(_ field: Field, _ value: Bool) -> Self { .equals(field, .bool(value)) }
    public static func oneOf(_ field: Field, _ values: [String]) -> Self { .oneOf(field, values.map { .string($0) }) }
    public static func kind<Variant: ReplicaVariant>(_: Variant.Type) -> Self { .kind(Variant.wireType) }

    struct Compiled {
        var sql: String
        var arguments: StatementArguments
    }

    /// The WHERE fragment over `snapshots`, bound to the declared structures.
    func compile(stream: String, indexes: [ReplicaIndexSpec]) -> Compiled {
        switch self {
        case .equals(let field, let value):
            let column = Self.column(stream: stream, field: field, kind: .btree, indexes: indexes)
            return Compiled(sql: "\(column) = ?", arguments: [value.databaseValue])
        case .oneOf(let field, let values):
            guard !values.isEmpty else { return Compiled(sql: "0", arguments: []) }
            let column = Self.column(stream: stream, field: field, kind: .btree, indexes: indexes)
            let marks = Array(repeating: "?", count: values.count).joined(separator: ", ")
            return Compiled(sql: "\(column) IN (\(marks))", arguments: StatementArguments(values.map(\.databaseValue)))
        case .isNull(let field):
            let column = Self.column(stream: stream, field: field, kind: .btree, indexes: indexes)
            return Compiled(sql: "\(column) IS NULL", arguments: [])
        case .kind(let type):
            return Compiled(sql: "type = ?", arguments: [type])
        case .and(let predicates):
            guard !predicates.isEmpty else { return Compiled(sql: "1", arguments: []) }
            let parts = predicates.map { $0.compile(stream: stream, indexes: indexes) }
            var arguments = StatementArguments()
            for part in parts { arguments += part.arguments }
            return Compiled(sql: parts.map { "(\($0.sql))" }.joined(separator: " AND "), arguments: arguments)
        case .not(let predicate):
            let inner = predicate.compile(stream: stream, indexes: indexes)
            return Compiled(sql: "NOT (\(inner.sql))", arguments: inner.arguments)
        case .hasPrefix(let field, let prefix):
            let column = Self.column(stream: stream, field: field, kind: .btree, indexes: indexes)
            return Compiled(sql: "\(column) >= ? AND \(column) < ?", arguments: [prefix, prefix + "\u{10FFFF}"])
        case .id(let value):
            return Compiled(sql: "row_id = ?", arguments: [value])
        case .idPrefixes(let prefixes):
            guard !prefixes.isEmpty else { return Compiled(sql: "0", arguments: []) }
            // One primary-key RANGE SEEK per prefix, by construction: a
            // UNION ALL of covering subselects feeding an IN. An OR of ranges
            // is only seeked when the planner's cost model feels like it —
            // on a small table it walks the stream's index entries and
            // filters (10k rows: 2.3 ms for six hits).
            let literal = stream.replacingOccurrences(of: "'", with: "''")
            var arguments = StatementArguments()
            for prefix in prefixes {
                arguments += [prefix, prefix + "\u{10FFFF}"]
            }
            let seeks = Array(
                repeating: "SELECT row_id FROM snapshots WHERE stream = '\(literal)' AND row_id >= ? AND row_id < ?",
                count: prefixes.count
            )
            return Compiled(sql: "row_id IN (\(seeks.joined(separator: " UNION ALL ")))", arguments: arguments)
        case .match(let field, let query):
            let table = ReplicaIndexSpec(stream: stream, field: field.rawValue, kind: .fts5).ftsTable
            precondition(
                indexes.contains(ReplicaIndexSpec(stream: stream, field: field.rawValue, kind: .fts5)),
                "replica: \(stream).\(field.rawValue) has no fts5 index — declare `index :\(field.rawValue), kind: :fts5`"
            )
            guard let compiled = Self.ftsQuery(query) else { return Compiled(sql: "1", arguments: []) }
            return Compiled(
                sql: "row_id IN (SELECT row_id FROM \"\(table)\" WHERE \"\(table)\" MATCH ?)",
                arguments: [compiled]
            )
        }
    }

    static func column(stream: String, field: Field, kind: ReplicaIndexKind, indexes: [ReplicaIndexSpec]) -> String {
        let spec = ReplicaIndexSpec(stream: stream, field: field.rawValue, kind: kind)
        precondition(
            indexes.contains(spec),
            "replica: \(stream).\(field.rawValue) has no \(kind.rawValue) index — declare `index :\(field.rawValue)`"
        )
        return "\"\(spec.column)\""
    }

    /// Whitespace-split tokens, each a quoted prefix term; the characters
    /// that carry FTS5 syntax never reach the engine.
    static func ftsQuery(_ raw: String) -> String? {
        let tokens = raw.split { $0.isWhitespace || "\"*():^-+".contains($0) }
        guard !tokens.isEmpty else { return nil }
        return tokens.map { "\"\($0)\"*" }.joined(separator: " ")
    }
}

/// One ORDER BY key over an indexed field. `row_id` breaks every tie, in
/// the last key's direction — the `(field, id)` order a list sorts by.
public struct ReplicaOrder<Field: ReplicaIndexedField>: Sendable, Equatable {
    public let field: Field
    public let descending: Bool

    public static func ascending(_ field: Field) -> Self { Self(field: field, descending: false) }
    public static func descending(_ field: Field) -> Self { Self(field: field, descending: true) }

    static func clause(_ order: [Self], stream: String, indexes: [ReplicaIndexSpec]) -> String {
        let keys = order.map { key in
            let column = ReplicaPredicate<Field>.column(stream: stream, field: key.field, kind: .btree, indexes: indexes)
            return "\(column) \(key.descending ? "DESC" : "ASC")"
        }
        return (keys + ["row_id \(order.last?.descending == true ? "DESC" : "ASC")"]).joined(separator: ", ")
    }
}

/// A generated STI variant: `wireType` is the `type` its rows carry.
public protocol ReplicaVariant {
    static var wireType: String { get }
}

/// The scalar a btree predicate binds — typed at the SQL edge the way
/// `json_extract` types the generated column, so a string never compares
/// equal to a number by accident.
public enum ReplicaIndexValue: Sendable, Equatable {
    case string(String)
    case number(Double)
    case integer(Int64)
    case bool(Bool)

    var databaseValue: DatabaseValue {
        switch self {
        case .string(let value): return value.databaseValue
        case .number(let value): return value.databaseValue
        case .integer(let value): return value.databaseValue
        case .bool(let value): return (value ? 1 : 0).databaseValue
        }
    }
}

/// A live scoped query. Cancel explicitly or let it go — dropping the last
/// reference ends the observation.
public final class ReplicaWatch: Sendable {
    private let task: Task<Void, Never>

    init(task: Task<Void, Never>) {
        self.task = task
    }

    public func cancel() {
        task.cancel()
    }

    /// Keeps the watch for as long as the calling task runs — a view's
    /// `.task` — and cancels it with that task.
    public func hold() async {
        await withTaskCancellationHandler {
            await task.value
        } onCancel: {
            task.cancel()
        }
    }

    deinit {
        task.cancel()
    }
}
