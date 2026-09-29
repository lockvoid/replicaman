package io.replicaman

/**
 * Opt-in diagnostics at the real SQLite prepared-read boundary. No logging or
 * collection occurs unless a caller installs [observer]. Tests must restore it
 * in teardown; callbacks can run on concurrent database reader threads.
 */
object ReplicaSQLReadTrace {
    data class Read(val sql: String, val arguments: List<Any?>)

    @Volatile var observer: ((Read) -> Unit)? = null

    internal fun didPrepare(sql: String, arguments: List<Any?>) {
        val callback = observer ?: return
        // A diagnostic cannot mutate the caller's binds or change query success.
        // Callbacks should record evidence; assertions belong after the read.
        val snapshot = arguments.map { if (it is ByteArray) it.copyOf() else it }
        runCatching { callback(Read(sql, snapshot)) }
    }
}
