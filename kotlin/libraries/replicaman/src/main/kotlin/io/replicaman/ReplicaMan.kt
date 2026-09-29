package io.replicaman

/**
 * The log sink ReplicaMan writes through. The host app installs one; the
 * default swallows everything (there is no `os.Logger` on this side).
 */
public interface ReplicaLogger {
    public fun info(message: String)
    public fun debug(message: String)
    public fun warning(message: String)
    public fun error(message: String)
}

/** The no-op default — a package with no host writes nowhere. */
public object NoopReplicaLogger : ReplicaLogger {
    override fun info(message: String) {}
    override fun debug(message: String) {}
    override fun warning(message: String) {}
    override fun error(message: String) {}
}

/**
 * ReplicaMan — row + document replication engine.
 *
 * ```kotlin
 * // In App.onCreate()
 * ReplicaMan.logger = Log.replica
 * ```
 */
public object ReplicaMan {
    public object Configuration {
        @Volatile public var homePath: java.io.File = java.io.File(System.getProperty("user.home"), "ReplicaMan")
    }

    // MARK: - Logger

    /** Logger instance. Set once by the host app at startup. */
    @Volatile
    public var logger: ReplicaLogger = NoopReplicaLogger
}

// Internal alias for logging
internal typealias Log = ReplicaMan
