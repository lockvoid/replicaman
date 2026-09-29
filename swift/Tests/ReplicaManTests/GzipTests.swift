import Foundation
import XCTest
@testable import ReplicaMan

/// The push body's framing: real gzip (RFC 1952 — magic bytes, inflatable
/// by any gzip reader), round-trips, and actually shrinks the JSON rows it
/// exists for.
final class GzipTests: XCTestCase {

    func testRoundTripsAndCarriesGzipMagic() throws {
        let text = Data(String(repeating: #"{"op":"row.set","stream":"projects","data":{"name":"x"}}"#, count: 200).utf8)
        let zipped = try Gzip.compress(text)
        XCTAssertEqual(Array(zipped.prefix(2)), [0x1f, 0x8b])
        XCTAssertLessThan(zipped.count, text.count / 5)
        XCTAssertEqual(try Gzip.decompress(zipped), text)

        // Empty input still produces a well-formed member, not an empty Data:
        // a pair of identity functions would satisfy the round trip alone,
        // which is why this rides the magic-byte assertion instead of standing
        // as its own test.
        let empty = try Gzip.compress(Data())
        XCTAssertEqual(Array(empty.prefix(2)), [0x1f, 0x8b])
        XCTAssertEqual(try Gzip.decompress(empty), Data())
    }
    func testGarbageDoesNotInflate() {
        XCTAssertThrowsError(try Gzip.decompress(Data([1, 2, 3, 4, 5])))
    }
    func testRejectsTruncationCorruptionAndExpansionPastTheLimit() throws {
        let input = Data(repeating: 120, count: 262_145)
        let zipped = try Gzip.compress(input)
        XCTAssertEqual(try Gzip.decompress(zipped, limit: input.count), input)
        XCTAssertThrowsError(try Gzip.decompress(zipped, limit: input.count - 1))
        XCTAssertThrowsError(try Gzip.decompress(zipped.dropLast()))
        var corrupt = zipped
        corrupt[corrupt.count - 8] ^= 1
        XCTAssertThrowsError(try Gzip.decompress(corrupt))
    }
}
