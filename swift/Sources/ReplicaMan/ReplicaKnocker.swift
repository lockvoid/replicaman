import Foundation

/// The doorbell's landing pad, ported verbatim from Syncer v1. Every "server
/// state moved" signal — a realtime event, app foregrounding, a drained
/// create — calls `knock()`; the knocker coalesces them into AT MOST one
/// in-flight sync (drain + pull-until-caught-up) plus one queued re-run.
/// Data never rides the signal itself; the pull is always the carrier, so a
/// lost doorbell costs latency, never correctness. Callers never await.
public actor ReplicaKnocker {
    private let sync: @Sendable () async -> Void
    private let interval: TimeInterval
    private var inFlight = false
    private var queued = false

    /// `interval` is the trailing-edge throttle window: doorbell storms (a
    /// render signals ~5/s per cook) collapse to at most one sync per
    /// window, and the LAST doorbell always lands. 0 = pure coalescer.
    public init(interval: TimeInterval = 0, sync: @escaping @Sendable () async -> Void) {
        self.sync = sync
        self.interval = interval
    }

    /// `immediate` skips the throttle window for this run. The window exists to
    /// absorb machine chatter; a doorbell rung BY a tap is the opposite — the
    /// person is watching the affordance that the pulled row will flip, and a
    /// second of politeness reads as a dead button.
    public func knock(immediate: Bool = false) {
        if immediate { skipWindow = true }
        if inFlight {
            queued = true
            return
        }
        inFlight = true
        Task { await run() }
    }

    private var lastStart: Date?
    private var skipWindow = false

    private func run() async {
        repeat {
            // Trailing edge: never start a sync inside the window of the
            // previous one; doorbells arriving during the wait fold into
            // this run.
            if interval > 0, !skipWindow, let lastStart {
                let wait = interval - Date().timeIntervalSince(lastStart)
                if wait > 0 {
                    // swiftlint:disable:next no_try_optional - a cut-short window syncs at once; inFlight must clear either way
                    try? await Task.sleep(nanoseconds: UInt64(wait * 1_000_000_000))
                }
            }
            skipWindow = false
            queued = false
            lastStart = Date()
            await sync()
        } while queued
        inFlight = false
    }
}
