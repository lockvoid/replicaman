import Foundation

/// Server backpressure applies to every endpoint sharing this transport.
actor ReplicaRetryDelay {
    private var deadline: ContinuousClock.Instant?

    func wait() async throws {
        while let deadline, deadline > .now {
            try await Task.sleep(until: deadline, clock: .continuous)
        }
        try Task.checkCancellation()
    }

    func record(_ value: String?) {
        guard let seconds = Self.seconds(value), seconds > 0 else { return }
        let delay = min(seconds, 86_400) + Double.random(in: 0...0.255)
        let next = ContinuousClock.now.advanced(by: .seconds(delay))
        deadline = max(deadline ?? next, next)
    }

    static func seconds(_ value: String?, now: Date = Date()) -> TimeInterval? {
        guard let value else { return nil }
        let text = value.trimmingCharacters(in: .whitespaces)
        if !text.isEmpty, text.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }),
           let seconds = Double(text), seconds.isFinite {
            return seconds
        }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss 'GMT'"
        formatter.isLenient = false
        // Retry-After is optional advice. Invalid advice leaves the normal
        // failure policy in force; the HTTP failure itself still propagates.
        return formatter.date(from: text).map { max(0, $0.timeIntervalSince(now)) }
    }
}
