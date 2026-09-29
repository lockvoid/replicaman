import Foundation
import XCTest
@testable import ReplicaMan

final class RetryDelayTests: XCTestCase {
    func testRetryAfterSecondsDatesAndInvalidAdvice() {
        let now = Date(timeIntervalSince1970: 0)
        XCTAssertEqual(ReplicaRetryDelay.seconds("12", now: now), 12)
        XCTAssertEqual(ReplicaRetryDelay.seconds("Thu, 01 Jan 1970 00:00:12 GMT", now: now), 12)
        XCTAssertEqual(ReplicaRetryDelay.seconds("Thu, 01 Jan 1970 00:00:00 GMT", now: now.addingTimeInterval(1)), 0)
        for invalid in ["-1", "1.5", "NaN", "Infinity", "tomorrow", ""] {
            XCTAssertNil(ReplicaRetryDelay.seconds(invalid, now: now), invalid)
        }
    }

    func testCancellationDoesNotWaitForServerDeadline() async throws {
        let delay = ReplicaRetryDelay()
        await delay.record("86400")
        let waiting = Task { try await delay.wait() }
        waiting.cancel()
        do {
            try await waiting.value
            XCTFail("Cancellation completed as success")
        } catch is CancellationError {
            // Cancellation is the expected outcome; no HTTP request may follow.
        }
    }
}
