import Foundation
import Loro
@testable import ReplicaManLoro
import XCTest
@testable import ReplicaMan

/// The document adapter the app writes through: paths in, values out, Loro
/// never above this package — the cases that need Loro itself to grade.
final class LoroDocumentAdapterTests: XCTestCase {
    let codec = LoroReplicaCodec()

    private func open(_ peer: UInt64 = 10, fold: Data? = nil) throws -> LoroDocument {
        try codec.open(fold: fold, peer: peer)
    }

    /// The peers that authored ops in `document`, read off its raw version
    /// vector rather than anything the adapter says about itself.
    private func authors(_ document: LoroDocument) throws -> Set<UInt64> {
        Set(try VersionVector.decode(bytes: codec.version(document)).toHashmap().keys)
    }

    /// Two peers who both write the first field of one entry must merge
    /// field-wise: an op-id child forks, and one peer's whole entry is lost.
    func testTwoFirstWritesOfOneEntryMergeFieldWise() throws {
        let mine = try open(11)
        let theirs = try open(22)
        try mine.write(.number(2), at: ["clips", "a", "start"])
        try theirs.write(.number(0.5), at: ["clips", "a", "volume"])

        try codec.importDeltas(mine, [try codec.snapshot(theirs)])

        XCTAssertEqual(mine.value["clips"]?["a"], .object(["start": .number(2), "volume": .number(0.5)]))
    }

    /// A path through a plain value — a peer put it there — is refused, and
    /// the value is left exactly as the peer left it.
    func testAPathThroughAPlainValueRefusesAndLeavesItAlone() throws {
        let peer = try open(77)
        try peer.write(.string("not-a-clip"), at: ["clips", "clip-a"])
        let document = try open(10, fold: try codec.snapshot(peer))

        XCTAssertThrowsError(try document.write(.number(12), at: ["clips", "clip-a", "start"])) { error in
            XCTAssertEqual(error as? LoroDocumentError, .notAMap(path: ["clips", "clip-a"]))
        }
        XCTAssertEqual(document.value["clips"], .object(["clip-a": .string("not-a-clip")]))
    }

    /// An idle save stays off the wire: a value the key holds, and a null where
    /// the key never was, write no op.
    func testAnEqualValueAndANullOverNothingWriteNothing() throws {
        let document = try open()
        try document.write(.number(2), at: ["clips", "a", "start"])
        let before = codec.version(document)

        try document.write(.number(2), at: ["clips", "a", "start"])
        try document.write(.null, at: ["clips", "a", "volume"])

        XCTAssertEqual(codec.version(document), before)
    }

    /// Clearing a present value is a real write: an explicit null.
    func testANullOverAValueClearsIt() throws {
        let document = try open()
        try document.write(.number(0.5), at: ["clips", "a", "volume"])

        try document.write(.null, at: ["clips", "a", "volume"])

        XCTAssertEqual(document.value["clips"]?["a"], .object(["volume": .null]))
    }

    /// A birth names every field it has: its nulls stand.
    func testASeedKeepsItsNulls() throws {
        let document = try open()

        try document.seed(.null, at: ["settings", "watermark"])

        XCTAssertEqual(document.value["settings"], .object(["watermark": .null]))
    }

    func testADeleteTakesTheKey() throws {
        let document = try open()
        try document.write(.number(2), at: ["clips", "a", "start"])
        try document.write(.number(4), at: ["clips", "b", "start"])

        try document.delete(at: ["clips", "a"])

        XCTAssertEqual(document.keys(at: ["clips"]), ["b"])
    }

    func testKeysReadWithoutMakingAnything() throws {
        let document = try open()
        let before = codec.version(document)

        XCTAssertEqual(document.keys(at: ["clips", "absent"]), [])
        XCTAssertEqual(codec.version(document), before)
    }

    /// Kill: make `asPeer` a no-op on the id — the op log is the only place
    /// attribution shows, so this reads it.
    func testAsPeerAttributesItsWritesThenRestoresTheDocumentsOwn() throws {
        let document = try open(10)

        try document.asPeer(2) {
            try document.write(.number(0), at: ["clips", "a", "start"])
        }

        XCTAssertEqual(document.peer, 10)
        XCTAssertEqual(try authors(document), [2], "the agent's write must be the agent's")
        try document.write(.number(5), at: ["clips", "a", "start"])
        XCTAssertEqual(try authors(document), [2, 10], "the next edit is the document's own")
    }

    func testAPeerLoroRefusesFailsAsPeerBeforeItsBodyWrites() throws {
        let document = try open(10)
        var ran = false

        XCTAssertThrowsError(try document.asPeer(.max) {
            ran = true
            try document.write(.integer(90), at: ["settings", "bpm"])
        })
        XCTAssertFalse(ran)
        XCTAssertEqual(document.peer, 10)
        XCTAssertNil(document.value["settings"]?["bpm"])
    }

    func testAPeerLoroRefusesFailsTheDocumentsBirth() {
        XCTAssertThrowsError(try codec.open(fold: nil, peer: .max))
    }

    /// A write the document refuses throws — reported and skipped, the rest of
    /// an edit landed without it and the caller heard "saved". A detached
    /// document is the refusal on demand: Loro edits only at the head.
    func testWritesTheDocumentRefusesThrow() throws {
        let document = try open()
        try document.write(.integer(100), at: ["settings", "bpm"])
        try document.write(.number(0), at: ["clips", "a", "start"])
        _ = codec.version(document)
        document.doc.detach()
        XCTAssertTrue(document.doc.isDetached())

        XCTAssertThrowsError(try document.write(.integer(120), at: ["settings", "bpm"]))
        XCTAssertThrowsError(try document.write(.number(3), at: ["clips", "a", "start"]))
        XCTAssertThrowsError(try document.seed(.number(4), at: ["clips", "b", "start"]))
        XCTAssertThrowsError(try document.delete(at: ["clips", "a"]))
        XCTAssertThrowsError(try document.writeRegistry("clips", ["a": ["start": .number(5)]], base: ["a": ["start": .number(0)]]),
                             "a refused entry write is not a poisoned slot — it fails the write")
    }

    func testMalformedHistoryAddressIsAnErrorRatherThanAnAbsentVersion() throws {
        let document = try open()
        XCTAssertThrowsError(try document.value(at: Data([0xff])))
    }

    func testUnexpectedNativeHistoryFailurePropagates() throws {
        final class BrokenFork: LoroDoc, @unchecked Sendable {
            override func forkAt(frontiers: Frontiers) throws -> LoroDoc {
                throw LoroError.LockError(message: "injected native lock failure")
            }
        }
        let document = LoroDocument(doc: BrokenFork())
        try document.write(.string("saved"), at: ["meta", "name"])
        let empty = Frontiers().encode()

        for read in [
            { _ = try document.value(at: empty) },
            { _ = try document.firstMatch(among: [empty]) },
            { _ = try document.differingRoots(from: empty) }
        ] {
            XCTAssertThrowsError(try read()) { error in
                XCTAssertEqual(error as? LoroError, .LockError(message: "injected native lock failure"))
            }
        }
    }

    /// The read door's reading of a past version: as it was then, this
    /// document untouched; a point outside its history reads nothing.
    func testAPastVersionReadsAsItWas() throws {
        let document = try open()
        try document.write(.string("First"), at: ["meta", "name"])
        let then = document.frontiers
        try document.write(.string("Second"), at: ["meta", "name"])
        _ = codec.version(document)
        let now = codec.version(document)

        XCTAssertEqual(try document.value(at: then)?["meta"], .object(["name": .string("First")]))
        XCTAssertEqual(document.value["meta"], .object(["name": .string("Second")]))
        XCTAssertEqual(codec.version(document), now)

        let stranger = try open(99)
        try stranger.write(.string("Elsewhere"), at: ["meta", "name"])
        XCTAssertNil(try document.value(at: stranger.frontiers), "a point outside this history read as something")
    }
}
