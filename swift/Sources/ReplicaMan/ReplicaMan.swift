import Foundation
import os.log

/// ReplicaMan — row + document replication engine.
///
/// ```swift
/// // In App.init()
/// ReplicaMan.logger = Log.replica
/// ```
public struct ReplicaMan {
    private init() {}

    // MARK: - Configuration

    /// What the host app decides for the package — set before the first
    /// engine is built:
    ///
    /// ```swift
    /// ReplicaMan.Configuration.homePath = appSupport.appendingPathComponent("replica@1")
    /// ```
    public enum Configuration {
        /// Where the worlds live — one `replica-<owner>.sqlite` per owner.
        /// The host versions the path: a world an older build wrote is
        /// whatever the host's migration makes of it, never this package's.
        nonisolated(unsafe) public static var homePath: URL = FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("ReplicaMan", isDirectory: true)
    }

    // MARK: - Logger

    /// Logger instance. Set by host app, defaults to default Logger.
    nonisolated(unsafe) public static var logger = Logger()
}

// Internal alias for logging
typealias Log = ReplicaMan
