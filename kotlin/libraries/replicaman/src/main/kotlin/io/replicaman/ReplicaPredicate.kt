package io.replicaman

import kotlinx.coroutines.Job

/**
 * The read predicates a stream handle accepts — STABLE operators over a
 * generated `Field` enum (the stream's INDEXED fields, per the manifest's
 * `indexes:`). Nothing here can express a whole-table filter: a field the
 * manifest did not index is not a `Field` case, so it does not compile.
 *
 * Each operator rides one physical structure: `equals`/`hasPrefix` the btree
 * over the generated column, `match` the fts5 shadow table. Asking for an
 * operator whose structure the manifest did not declare is a precondition
 * failure at first use — the manifest is committed, a dev run catches it.
 */
public sealed interface ReplicaPredicate<Field : ReplicaIndexedField> {
    public data class Equals<Field : ReplicaIndexedField>(
        val field: Field,
        val value: ReplicaIndexValue,
    ) : ReplicaPredicate<Field>

    public data class OneOf<Field : ReplicaIndexedField>(val field: Field, val values: List<ReplicaIndexValue>) : ReplicaPredicate<Field>
    public data class IsNull<Field : ReplicaIndexedField>(val field: Field) : ReplicaPredicate<Field>
    public data class Kind<Field : ReplicaIndexedField>(val wireType: String) : ReplicaPredicate<Field>
    public data class And<Field : ReplicaIndexedField>(val predicates: List<ReplicaPredicate<Field>>) : ReplicaPredicate<Field>
    public data class Not<Field : ReplicaIndexedField>(val predicate: ReplicaPredicate<Field>) : ReplicaPredicate<Field>

    /**
     * String start, BINARY collation (SQLite's NOCASE folds ASCII only —
     * Cyrillic case-insensitivity is `match`'s job).
     */
    public data class HasPrefix<Field : ReplicaIndexedField>(
        val field: Field,
        val prefix: String,
    ) : ReplicaPredicate<Field>

    /**
     * Word-start full text over the RAW user query: no wildcard or FTS
     * syntax crosses the door — the engine tokenizes and sanitizes. An
     * empty query matches everything (a cleared search box).
     */
    public data class Match<Field : ReplicaIndexedField>(
        val field: Field,
        val query: String,
    ) : ReplicaPredicate<Field>

    /**
     * The row IDENTITY — always served by the store's primary key, so it
     * needs no manifest declaration. Address-scoped streams live here: a
     * cook is `pmck/<record>/<id>/<op>`, and a project's cooks are the
     * union of its records' prefixes (`IdPrefixes`). An empty union
     * matches nothing.
     */
    public data class Id<Field : ReplicaIndexedField>(val value: String) : ReplicaPredicate<Field>

    public data class IdPrefixes<Field : ReplicaIndexedField>(val prefixes: List<String>) :
        ReplicaPredicate<Field>

    public companion object {
        public fun <F : ReplicaIndexedField> oneOf(field: F, values: List<String>): ReplicaPredicate<F> = OneOf(field, values.map { ReplicaIndexValue.Text(it) })
        public fun <F : ReplicaIndexedField> isNull(field: F): ReplicaPredicate<F> = IsNull(field)
        public fun <F : ReplicaIndexedField> kind(variant: ReplicaVariant): ReplicaPredicate<F> = Kind(variant.wireType)
        public fun <F : ReplicaIndexedField> and(predicates: List<ReplicaPredicate<F>>): ReplicaPredicate<F> = And(predicates)
        public fun <F : ReplicaIndexedField> not(predicate: ReplicaPredicate<F>): ReplicaPredicate<F> = Not(predicate)

        public fun <F : ReplicaIndexedField> eq(field: F, value: String): ReplicaPredicate<F> =
            Equals(field, ReplicaIndexValue.Text(value))

        public fun <F : ReplicaIndexedField> eq(field: F, value: Int): ReplicaPredicate<F> =
            Equals(field, ReplicaIndexValue.Integer(value.toLong()))

        public fun <F : ReplicaIndexedField> eq(field: F, value: Long): ReplicaPredicate<F> =
            Equals(field, ReplicaIndexValue.Integer(value))

        public fun <F : ReplicaIndexedField> eq(field: F, value: Double): ReplicaPredicate<F> =
            Equals(field, ReplicaIndexValue.Number(value))

        public fun <F : ReplicaIndexedField> eq(field: F, value: Boolean): ReplicaPredicate<F> =
            Equals(field, ReplicaIndexValue.Flag(value))

        public fun <F : ReplicaIndexedField> hasPrefix(field: F, prefix: String): ReplicaPredicate<F> =
            HasPrefix(field, prefix)

        public fun <F : ReplicaIndexedField> match(field: F, query: String): ReplicaPredicate<F> =
            Match(field, query)

        public fun <F : ReplicaIndexedField> id(value: String): ReplicaPredicate<F> = Id(value)

        public fun <F : ReplicaIndexedField> idPrefixes(prefixes: List<String>): ReplicaPredicate<F> =
            IdPrefixes(prefixes)

        /**
         * Whitespace-split tokens, each a quoted prefix term; the characters
         * that carry FTS5 syntax never reach the engine.
         */
        internal fun ftsQuery(raw: String): String? {
            val tokens = mutableListOf<String>()
            val current = StringBuilder()
            for (character in raw) {
                if (character.isWhitespace() || FTS_SYNTAX.contains(character)) {
                    if (current.isNotEmpty()) {
                        tokens.add(current.toString())
                        current.clear()
                    }
                } else {
                    current.append(character)
                }
            }
            if (current.isNotEmpty()) tokens.add(current.toString())
            if (tokens.isEmpty()) return null
            return tokens.joinToString(" ") { "\"$it\"*" }
        }

        private const val FTS_SYNTAX: String = "\"*():^-+"

        /** U+10FFFF - the last code point, so a prefix range covers every continuation. */
        internal const val PREFIX_CEILING: String = "\uDBFF\uDFFF"
    }
}

internal data class CompiledPredicate(
    val sql: String,
    val arguments: List<Any?>,
)

/** The WHERE fragment over `snapshots`, bound to the declared structures. */
internal fun <Field : ReplicaIndexedField> ReplicaPredicate<Field>.compile(
    stream: String,
    indexes: List<ReplicaIndexSpec>,
): CompiledPredicate = when (this) {
    is ReplicaPredicate.Equals -> {
        val column = predicateColumn(stream, field, ReplicaIndexKind.BTREE, indexes)
        CompiledPredicate("$column = ?", listOf(value.databaseValue))
    }

    is ReplicaPredicate.OneOf -> if (values.isEmpty()) CompiledPredicate("0", emptyList()) else {
        val column = predicateColumn(stream, field, ReplicaIndexKind.BTREE, indexes)
        CompiledPredicate("$column IN (${values.joinToString(", ") { "?" }})", values.map { it.databaseValue })
    }
    is ReplicaPredicate.IsNull -> CompiledPredicate("${predicateColumn(stream, field, ReplicaIndexKind.BTREE, indexes)} IS NULL", emptyList())
    is ReplicaPredicate.Kind -> CompiledPredicate("type = ?", listOf(wireType))
    is ReplicaPredicate.And -> {
        val parts = predicates.map { it.compile(stream, indexes) }
        CompiledPredicate(if (parts.isEmpty()) "1" else parts.joinToString(" AND ") { "(${it.sql})" }, parts.flatMap { it.arguments })
    }
    is ReplicaPredicate.Not -> predicate.compile(stream, indexes).let { CompiledPredicate("NOT (${it.sql})", it.arguments) }

    is ReplicaPredicate.HasPrefix -> {
        val column = predicateColumn(stream, field, ReplicaIndexKind.BTREE, indexes)
        CompiledPredicate(
            "$column >= ? AND $column < ?",
            listOf(prefix, prefix + ReplicaPredicate.PREFIX_CEILING)
        )
    }

    is ReplicaPredicate.Id -> CompiledPredicate("row_id = ?", listOf(value))

    is ReplicaPredicate.IdPrefixes -> {
        if (prefixes.isEmpty()) {
            CompiledPredicate("0", emptyList())
        } else {
            // One primary-key RANGE SEEK per prefix, by construction: a
            // UNION ALL of covering subselects feeding an IN. An OR of ranges
            // is only seeked when the planner's cost model feels like it —
            // on a small table it walks the stream's index entries and
            // filters (10k rows: 2.3 ms for six hits).
            val literal = stream.replace("'", "''")
            val arguments = mutableListOf<Any?>()
            for (prefix in prefixes) {
                arguments.add(prefix)
                arguments.add(prefix + ReplicaPredicate.PREFIX_CEILING)
            }
            val seeks = List(prefixes.size) {
                "SELECT row_id FROM snapshots WHERE stream = '$literal' AND row_id >= ? AND row_id < ?"
            }
            CompiledPredicate(
                "row_id IN (${seeks.joinToString(" UNION ALL ")})",
                arguments
            )
        }
    }

    is ReplicaPredicate.Match -> {
        val spec = ReplicaIndexSpec(stream, field.rawValue, ReplicaIndexKind.FTS5)
        require(indexes.contains(spec)) {
            "replica: $stream.${field.rawValue} has no fts5 index — declare `index :${field.rawValue}, kind: :fts5`"
        }
        val table = spec.ftsTable
        val compiled = ReplicaPredicate.ftsQuery(query)
        if (compiled == null) {
            CompiledPredicate("1", emptyList())
        } else {
            CompiledPredicate(
                "row_id IN (SELECT row_id FROM \"$table\" WHERE \"$table\" MATCH ?)",
                listOf(compiled)
            )
        }
    }
}

internal fun predicateColumn(
    stream: String,
    field: ReplicaIndexedField,
    kind: ReplicaIndexKind,
    indexes: List<ReplicaIndexSpec>,
): String {
    val spec = ReplicaIndexSpec(stream, field.rawValue, kind)
    require(indexes.contains(spec)) {
        "replica: $stream.${field.rawValue} has no ${kind.rawValue} index — declare `index :${field.rawValue}`"
    }
    return "\"${spec.column}\""
}

/**
 * The scalar a btree predicate binds — typed at the SQL edge the way
 * `json_extract` types the generated column, so a string never compares
 * equal to a number by accident.
 */
public sealed interface ReplicaIndexValue {
    public data class Text(val value: String) : ReplicaIndexValue

    public data class Number(val value: Double) : ReplicaIndexValue

    public data class Integer(val value: Long) : ReplicaIndexValue

    public data class Flag(val value: Boolean) : ReplicaIndexValue

}

internal val ReplicaIndexValue.databaseValue: Any
    get() = when (this) {
        is ReplicaIndexValue.Text -> value
        is ReplicaIndexValue.Number -> value
        is ReplicaIndexValue.Integer -> value
        is ReplicaIndexValue.Flag -> if (value) 1L else 0L
    }

/**
 * A live scoped query. Cancel explicitly or let the owning scope end —
 * dropping the job ends the observation.
 */
public class ReplicaWatch internal constructor(private val job: Job) {
    public fun cancel() {
        job.cancel()
    }

    public suspend fun hold() {
        try { job.join() } finally { job.cancel() }
    }
}

/** Indexed sort keys, with row identity breaking ties in the last key's direction. */
public data class ReplicaOrder<Field : ReplicaIndexedField>(val field: Field, val descending: Boolean = false) {
    public companion object {
        public fun <F : ReplicaIndexedField> ascending(field: F): ReplicaOrder<F> = ReplicaOrder(field)
        public fun <F : ReplicaIndexedField> descending(field: F): ReplicaOrder<F> = ReplicaOrder(field, true)
        internal fun <F : ReplicaIndexedField> clause(order: List<ReplicaOrder<F>>, stream: String, indexes: List<ReplicaIndexSpec>): String =
            (order.map { "${predicateColumn(stream, it.field, ReplicaIndexKind.BTREE, indexes)} ${if (it.descending) "DESC" else "ASC"}" } +
                "row_id ${if (order.lastOrNull()?.descending == true) "DESC" else "ASC"}").joinToString(", ")
    }
}
