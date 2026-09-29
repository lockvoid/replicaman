import Foundation
import Loro
@testable import ReplicaManLoro
import XCTest
@testable import ReplicaMan

/// The codec seam itself: merge refuses missing causal deps (the server
/// codec's refusal, mirrored), version arithmetic reads blob metadata
/// without applying, and empty diffs are recognizably empty.
final class LoroCodecTests: XCTestCase {
    let codec = LoroReplicaCodec()

    func testMergeRefusesAPayloadWithUnseenDeps() throws {
        let author = try LoroFixture.doc(peer: 9)
        let first = try LoroFixture.editPayload(author, "a", "1")
        let second = try LoroFixture.editPayload(author, "b", "2")

        do {
            _ = try codec.merge(fold: nil, payload: second, reflecting: [])
            XCTFail("a delta depending on unseen changes must be refused, not parked silently")
        } catch ReplicaError.missingCausalDeps {}

        let fold = try codec.merge(fold: try codec.merge(fold: nil, payload: first, reflecting: []).fold, payload: second, reflecting: []).fold
        XCTAssertEqual(try LoroFixture.meta(fold: fold, "a"), "1")
        XCTAssertEqual(try LoroFixture.meta(fold: fold, "b"), "2")
    }

    func testMergeIsIdempotentAcrossReplays() throws {
        let author = try LoroFixture.doc(peer: 9)
        let payload = try LoroFixture.editPayload(author, "a", "1")

        let once = try codec.merge(fold: nil, payload: payload, reflecting: []).fold
        let twice = try codec.merge(fold: once, payload: payload, reflecting: []).fold
        XCTAssertEqual(try LoroFixture.meta(fold: twice, "a"), "1", "loro re-apply is harmless on retry")
        XCTAssertEqual(try codec.version(fold: twice), try codec.version(fold: once))
        XCTAssertEqual(twice, once)
    }

    /// The fields a row takes from its document are read off the merged
    /// document while it is open: a path walks maps, an absent one reads
    /// `.null` (the server's reflection reads nil), an integer is a number.
    func testMergeReadsTheReflectedPathsOffTheMergedDocument() throws {
        let author = try LoroFixture.doc(peer: 9)
        try LoroFixture.setMeta(author, "name", "Plans")
        let export = try author.getMap(id: "settings").insertContainer(key: "export", child: LoroMap())
        try export.insert(key: "fps", v: Int64(30))
        author.commit()

        let merged = try codec.merge(fold: nil, payload: try author.export(mode: .snapshot), reflecting: [
            ReplicaReflection(field: "name", path: ["meta", "name"]),
            ReplicaReflection(field: "fps", path: ["settings", "export", "fps"]),
            ReplicaReflection(field: "cover", path: ["meta", "cover"]),
        ])

        XCTAssertEqual(merged.reflected, ["name": .string("Plans"), "fps": .number(30), "cover": .null])
        XCTAssertEqual(try LoroFixture.meta(fold: merged.fold, "name"), "Plans")
    }

    /// A write lands where the reflection reads it back, the maps on the way
    /// made when absent.
    func testAWriteReadsBackThroughItsPath() throws {
        let document = try codec.open(fold: nil, peer: 9)
        try codec.write(.string("Plans"), at: ["meta", "name"], in: document)
        try codec.write(.number(30), at: ["settings", "export", "fps"], in: document)

        let merged = try codec.merge(fold: nil, payload: try codec.snapshot(document), reflecting: [
            ReplicaReflection(field: "name", path: ["meta", "name"]),
            ReplicaReflection(field: "fps", path: ["settings", "export", "fps"]),
        ])

        XCTAssertEqual(merged.reflected, ["name": .string("Plans"), "fps": .number(30)])
    }

    /// The server's binding is type-faithful (Integer → i64,
    /// Float → double) and so is the document; the adapter wrote every whole
    /// number as i64, so a clip's `start = 2.0` came back an Integer.
    func testAWholeDoubleKeepsItsTypeAndAnIntegerKeepsItsOwn() throws {
        let document = try codec.open(fold: nil, peer: 9)
        try codec.write(.number(2), at: ["settings", "start"], in: document)
        try codec.write(.integer(30), at: ["settings", "fps"], in: document)

        let reopened = LoroDoc()
        _ = try reopened.import(bytes: try codec.snapshot(document))
        XCTAssertEqual(reopened.getMap(id: "settings").get(key: "start")?.asValue(), .double(value: 2))
        XCTAssertEqual(reopened.getMap(id: "settings").get(key: "fps")?.asValue(), .i64(value: 30))
    }

    func testPayloadVersionAndMergeVersionsUnion() throws {
        let alice = try LoroFixture.doc(peer: 9)
        let payloadA = try LoroFixture.editPayload(alice, "a", "1")
        let bob = try LoroFixture.doc(peer: 3)
        let payloadB = try LoroFixture.editPayload(bob, "b", "2")

        let union = try codec.mergeVersions(try codec.payloadVersion(payloadA), try codec.payloadVersion(payloadB))
        let decoded = try VersionVector.decode(bytes: union)
        XCTAssertTrue(decoded.includesVv(other: try VersionVector.decode(bytes: try codec.payloadVersion(payloadA))))
        XCTAssertTrue(decoded.includesVv(other: try VersionVector.decode(bytes: try codec.payloadVersion(payloadB))))
    }

    /// The `since` version comes from the AUTHORING doc's own `oplogVv()`, not
    /// from `codec.version(fold:)` — otherwise the codec supplies both the
    /// input and the expectation and a matching pair of bugs is invisible.
    /// This is also the shape the drain actually uses: `advanceAcked`
    /// (`ReplicaEngine.swift:1703`) advances by a version derived from the
    /// bytes the SERVER acknowledged, never by re-reading our own fold.
    func testEmptyDiffIsRecognized() throws {
        let author = try LoroFixture.doc(peer: 9)
        try LoroFixture.setMeta(author, "a", "1")
        let fold = try author.export(mode: .snapshot)
        let serverHasEverything = author.oplogVv().encode()

        let nothing = try codec.diff(fold: fold, since: serverHasEverything)
        XCTAssertTrue(codec.isEmptyDiff(nothing), "a diff since everything must carry no changes")

        let everything = try codec.diff(fold: fold, since: nil)
        XCTAssertFalse(codec.isEmptyDiff(everything))

        // And the discriminator: an edit made AFTER that version is owed.
        let payload = try LoroFixture.editPayload(author, "b", "2")
        let owed = try codec.diff(fold: try codec.merge(fold: fold, payload: payload, reflecting: []).fold, since: serverHasEverything)
        XCTAssertFalse(codec.isEmptyDiff(owed), "a new edit must be owed against the version that predates it")
    }

    /// A step the document refuses throws. Answered `false`, it read as
    /// "nothing to undo" and the button silently did nothing. A detached
    /// document is the refusal on demand: Loro edits only at the head.
    func testARefusedUndoThrows() throws {
        let document = try codec.open(fold: nil, peer: 9)
        try codec.write(.string("a"), at: ["meta", "name"], in: document)
        _ = codec.version(document)
        try codec.write(.string("b"), at: ["meta", "name"], in: document)
        _ = codec.version(document)
        document.doc.detach()
        XCTAssertTrue(document.doc.isDetached())
        XCTAssertTrue(codec.canUndo(document))

        XCTAssertThrowsError(try codec.undo(document))
    }

    func testARefusedRedoThrows() throws {
        let document = try codec.open(fold: nil, peer: 9)
        try codec.write(.string("a"), at: ["meta", "name"], in: document)
        _ = codec.version(document)
        try codec.write(.string("b"), at: ["meta", "name"], in: document)
        _ = codec.version(document)
        XCTAssertTrue(try codec.undo(document))
        document.doc.detach()
        XCTAssertTrue(document.doc.isDetached())
        XCTAssertTrue(codec.canRedo(document))

        XCTAssertThrowsError(try codec.redo(document))
    }
}
