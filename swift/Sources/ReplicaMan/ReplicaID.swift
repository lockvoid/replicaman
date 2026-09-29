import Foundation

/// Client-generated ids: ids are ALWAYS minted client-side —
/// offline-first non-negotiable (ARCHITECTURE §3.7). Journal entries take
/// ULIDs; a frozen operation takes the UUIDv7 its verdict answers.
public enum ReplicaID {
    private static let alphabet = Array("0123456789ABCDEFGHJKMNPQRSTVWXYZ")

    public static func ulid(now: Date = Date()) -> String {
        var characters: [Character] = []
        characters.reserveCapacity(26)

        var milliseconds = UInt64(now.timeIntervalSince1970 * 1000)
        var time: [Character] = []
        for _ in 0..<10 {
            time.append(alphabet[Int(milliseconds & 0x1F)])
            milliseconds >>= 5
        }
        characters.append(contentsOf: time.reversed())

        for _ in 0..<16 {
            characters.append(alphabet[Int.random(in: 0..<32)])
        }
        return String(characters)
    }

    /// A lowercase UUIDv7: 48 bits of Unix milliseconds over a random UUID —
    /// the id a frozen operation keeps on every retry.
    static func uuidV7(now: Date = Date()) -> String {
        var uuid = UUID().uuid
        let milliseconds = UInt64(now.timeIntervalSince1970 * 1000)
        withUnsafeMutableBytes(of: &uuid) { bytes in
            for index in 0..<6 {
                bytes[index] = UInt8(truncatingIfNeeded: milliseconds >> (8 * (5 - index)))
            }
            bytes[6] = bytes[6] & 0x0F | 0x70
        }
        return UUID(uuid: uuid).uuidString.lowercased()
    }

    /// A fresh loro peer id, clear of the reserved actors (server = 1,
    /// agent = 2) and of 0. Minted whenever a doc fold is created or
    /// recreated — a reborn doc reusing its peer would have its edits
    /// silently discarded (loro dedups by (peer, counter) — the v1 lesson).
    public static func peer() -> UInt64 {
        UInt64.random(in: 16 ..< UInt64.max)
    }
}
