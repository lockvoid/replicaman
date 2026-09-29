import Foundation
import GRDB
import XCTest
@testable import ReplicaMan

/// A reader keys a memo over CLOSED documents on the fingerprint, so it has to
/// move whenever the fold does. The fold's length does not: a rename re-encodes
/// to exactly as many bytes often enough to leave a project tile on its old
/// name.
final class DocumentFingerprintTests: XCTestCase {
    private func store(_ name: String = #function) throws -> ReplicaStateStore {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("replica-man-tests", isDirectory: true)
            .appendingPathComponent("\(name)-\(UUID().uuidString).sqlite").path
        try FileManager.default.createDirectory(
            atPath: (path as NSString).deletingLastPathComponent, withIntermediateDirectories: true)
        return try ReplicaStateStore(path: path)
    }

    private func fingerprint(_ store: ReplicaStateStore) throws -> Data? {
        try store.pool.read { try store.docFingerprints($0, stream: "boards", rowIds: ["b1"])["b1"] }
    }

    func testAFoldOfTheSameLengthStillMovesTheFingerprint() throws {
        let store = try store()
        try store.pool.write {
            try store.upsertDoc($0, stream: "boards", rowId: "b1", shard: "user", codec: "loro",
                                fold: Data("Before".utf8), acked: nil, peer: 7)
        }
        let before = try fingerprint(store)

        try store.pool.write {
            try store.updateDoc($0, stream: "boards", rowId: "b1", fold: Data("After!".utf8))
        }

        XCTAssertNotEqual(before, try fingerprint(store))
    }

    func testAReplacedDocumentNeverReusesAFingerprint() throws {
        let store = try store()
        var seen: Set<Data> = []
        for fold in ["one", "two", "six"] {
            try store.pool.write {
                try store.upsertDoc($0, stream: "boards", rowId: "b1", shard: "user", codec: "loro",
                                    fold: Data(fold.utf8), acked: nil, peer: 7)
            }
            seen.insert(try XCTUnwrap(try fingerprint(store)))
            try store.pool.write { try store.deleteDoc($0, stream: "boards", rowId: "b1") }
        }

        XCTAssertEqual(seen.count, 3)
    }
}
