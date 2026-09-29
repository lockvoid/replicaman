import Foundation
import OSLog

/// Diagnostics for typed projections that cannot decode retained raw values.
public enum ReplicaDiagnostics {
    private final class State: @unchecked Sendable {
        let lock = NSLock()
        var handler: (@Sendable (String, any Error) -> Void)?
        var reported: Set<String> = []
    }
    private static let state = State()
    private static let logger = Logger(subsystem: "io.replicaman", category: "decoding")

    public static var onError: (@Sendable (String, any Error) -> Void)? {
        get { state.lock.withLock { state.handler } }
        set { state.lock.withLock { state.handler = newValue; state.reported.removeAll() } }
    }

    public static func attemptOnce<Value>(_ operation: String, _ body: () throws -> Value) -> Value? {
        do { return try body() }
        catch {
            let report = state.lock.withLock { () -> (Bool, (@Sendable (String, any Error) -> Void)?) in
                (state.reported.insert(operation).inserted, state.handler)
            }
            if report.0 {
                if let handler = report.1 { handler(operation, error) }
                else { logger.error("\(operation, privacy: .public): \(String(describing: error), privacy: .private)") }
            }
            return nil
        }
    }
}
