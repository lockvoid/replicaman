package io.replicaman

/** Optional diagnostics for typed projections that cannot decode a retained raw row. */
public object ReplicaDiagnostics {
    @Volatile public var onError: ((String, Throwable) -> Unit)? = null
    public fun report(operation: String, error: Throwable) {
        val handler = onError
        if (handler != null) handler(operation, error)
        else System.err.println("[ReplicaMan] $operation: ${error.message}")
    }
}
