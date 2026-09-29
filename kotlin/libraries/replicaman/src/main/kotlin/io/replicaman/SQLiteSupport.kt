package io.replicaman

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement

/**
 * GRDB's row/value helpers, in the shape `androidx.sqlite` leaves room for:
 * a prepared statement, positional binds, one mapping closure. Every SQL
 * string in the store goes through here so binding stays in one place.
 */
internal fun SQLiteStatement.bindAll(arguments: List<Any?>) {
    arguments.forEachIndexed { index, argument ->
        val position = index + 1
        when (argument) {
            null -> bindNull(position)
            is String -> bindText(position, argument)
            is Long -> bindLong(position, argument)
            is Int -> bindLong(position, argument.toLong())
            is Double -> bindDouble(position, argument)
            is Boolean -> bindLong(position, if (argument) 1L else 0L)
            is ByteArray -> bindBlob(position, argument)
            else -> throw ReplicaError.Storage("cannot bind ${argument::class}")
        }
    }
}

internal fun <T> SQLiteConnection.query(
    sql: String,
    arguments: List<Any?> = emptyList(),
    map: (SQLiteStatement) -> T,
): List<T> = prepare(sql).use { statement ->
    ReplicaSQLReadTrace.didPrepare(sql, arguments)
    statement.bindAll(arguments)
    val rows = mutableListOf<T>()
    while (statement.step()) rows.add(map(statement))
    rows
}

internal fun <T> SQLiteConnection.queryOne(
    sql: String,
    arguments: List<Any?> = emptyList(),
    map: (SQLiteStatement) -> T,
): T? = prepare(sql).use { statement ->
    ReplicaSQLReadTrace.didPrepare(sql, arguments)
    statement.bindAll(arguments)
    if (statement.step()) map(statement) else null
}

/**
 * ONE statement. `androidx.sqlite` prepares to the first `;` and gives no
 * tail offset, so — unlike GRDB's `execute(sql:)` — a second statement in
 * the same string is silently not run.
 */
internal fun SQLiteConnection.exec(sql: String, arguments: List<Any?> = emptyList()) {
    prepare(sql).use { statement ->
        statement.bindAll(arguments)
        statement.step()
    }
}

internal fun SQLiteConnection.queryStrings(
    sql: String,
    arguments: List<Any?> = emptyList(),
): List<String> = query(sql, arguments) { if (it.isNull(0)) "" else it.getText(0) }

internal fun SQLiteConnection.queryString(
    sql: String,
    arguments: List<Any?> = emptyList(),
): String? = queryOne(sql, arguments) { if (it.isNull(0)) null else it.getText(0) }

internal fun SQLiteConnection.queryLong(
    sql: String,
    arguments: List<Any?> = emptyList(),
): Long? = queryOne(sql, arguments) { if (it.isNull(0)) null else it.getLong(0) }

internal fun SQLiteConnection.queryBool(
    sql: String,
    arguments: List<Any?> = emptyList(),
): Boolean = (queryLong(sql, arguments) ?: 0L) != 0L

/** `db.changesCount` — rows touched by the most recent statement. */
internal fun SQLiteConnection.changes(): Int = (queryLong("SELECT changes()") ?: 0L).toInt()

internal fun SQLiteStatement.textOrNull(index: Int): String? =
    if (isNull(index)) null else getText(index)

internal fun SQLiteStatement.blobOrNull(index: Int): ByteArray? =
    if (isNull(index)) null else getBlob(index)

/** `?, ?, ?` for an `IN` list. */
internal fun questionMarks(count: Int): String = List(count) { "?" }.joinToString(", ")
