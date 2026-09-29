import Foundation
import Loro
import Testing
@testable import ReplicaManLoro
@testable import ReplicaMan

/// The WRITER half of the document. `DocumentCorpusTests` proves the two
/// platforms MERGE the same way from committed bytes; this proves iOS EMITS
/// the ops the server would, which is the half a fixture can't see.
///
/// The base-relative contract, exactly the cases that
/// silently destroy another device's work when a writer gets them wrong.
@Suite("Document registry writes")
struct DocumentRegistryTests {

    /// A document holding two clips, plus the registry a writer would have
    /// read from it.
    private func seeded() throws -> (HeldDocument, [String: [String: ReplicaValue]]) {
        let document = try HeldDocument()
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0), "b": clipFields(start: 4)], base: nil) }
        return (document, document.registry("clips"))
    }

    // MARK: Sparse axes

    @Test("a root the writer omitted is left completely alone")
    func omittedRootUntouched() throws {
        let (document, _) = try seeded()

        // A rename touches `meta` and nothing else.
        try document.edit { try $0.writeFields("meta", ["name": .string("Renamed")], base: nil) }

        #expect(document.live.value["meta"]?["name"] == .string("Renamed"))
        #expect(document.registry("clips").keys.sorted() == ["a", "b"])
    }

    @Test("an EMPTY registry from an authoritative writer clears it")
    func emptyRegistryClears() throws {
        let (document, _) = try seeded()

        // An authoritative writer that genuinely sees no clips is deleting them.
        try document.edit { try $0.writeRegistry("clips", [:], base: nil) }

        #expect(document.registry("clips").isEmpty)
    }

    // MARK: Base-relative writes

    @Test("a writer does not delete an entry that appeared after it read")
    func doesNotDeleteUnseen() throws {
        let (document, base) = try seeded()

        // A peer adds `c` while our writer is mid-edit...
        try document.edit { try $0.writeEntry("clips", "c", clipFields(start: 8), base: nil) }
        // ...and the writer, which never saw `c`, saves its own two clips.
        try document.edit { try $0.writeRegistry("clips", base, base: base) }

        #expect(document.registry("clips").keys.sorted() == ["a", "b", "c"])
    }

    @Test("a writer does not resurrect an entry deleted after it read")
    func doesNotResurrectDeleted() throws {
        let (document, base) = try seeded()

        try document.edit { try $0.delete(at: ["clips", "b"]) }
        // The writer still holds `b` from its read and echoes it back. That is
        // not intent to re-create — it simply has not heard about the delete.
        try document.edit { try $0.writeRegistry("clips", base, base: base) }

        #expect(document.registry("clips").keys.sorted() == ["a"])
    }

    @Test("a writer still deletes an entry it DID see and dropped")
    func deletesSeenAndDropped() throws {
        let (document, base) = try seeded()

        try document.edit { try $0.writeRegistry("clips", base.filter { $0.key == "a" }, base: base) }

        #expect(document.registry("clips").keys.sorted() == ["a"])
    }

    @Test("a writer still changes a field it DID touch, even against a concurrent edit")
    func writesTouchedField() throws {
        let (document, base) = try seeded()

        try document.edit { try $0.writeEntry("clips", "a", ["start": .number(99)], base: nil) }
        // Our writer read start=0 and moved it to 2 — a real decision, which
        // must land even though someone else moved it meanwhile.
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 2), "b": clipFields(start: 4)], base: base) }

        #expect(document.entry("clips", "a")?["start"] == .number(2))
    }

    @Test("a writer still adds an entry of its own")
    func addsOwnEntry() throws {
        let (document, base) = try seeded()

        try document.edit { try $0.writeRegistry("clips", base.merging(["c": clipFields(start: 8)]) { old, _ in old }, base: base) }

        #expect(document.registry("clips").keys.sorted() == ["a", "b", "c"])
    }

    @Test("an authoritative writer (no base) can still delete")
    func authoritativeDeletes() throws {
        let (document, base) = try seeded()

        // The normalizer's seam: its input IS the document it writes back, so
        // absence genuinely is intent.
        try document.edit { try $0.writeRegistry("clips", base.filter { $0.key == "b" }, base: nil) }

        #expect(document.registry("clips").keys.sorted() == ["b"])
    }

    @Test("a writer does not clobber a field it read and left alone")
    func doesNotClobberUntouchedField() throws {
        let document = try HeldDocument()
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0, volume: 0.5)], base: nil) }
        let base = document.registry("clips")

        // A peer turns the volume up while our writer is dragging the clip.
        try document.edit { try $0.writeEntry("clips", "a", ["volume": .number(0.9)], base: nil) }
        // The writer saves: `start` is its decision, `volume` is just the value
        // it happened to read. Echoing the read back must not out-vote the peer.
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 2, volume: 0.5)], base: base) }

        #expect(document.entry("clips", "a")?["volume"] == .number(0.9))
        #expect(document.entry("clips", "a")?["start"] == .number(2))
    }

    @Test("re-saving an unchanged document emits no ops")
    func unchangedSaveIsSilent() throws {
        let (document, base) = try seeded()
        let before = document.version

        try document.edit { try $0.writeRegistry("clips", base, base: base) }
        try document.edit { try $0.writeFields("settings", [:], base: [:]) }

        // An LWW write is an op, so without this an idle autosave would fork
        // history on every tick.
        #expect(document.version == before)
    }

    // MARK: Field semantics

    @Test("a registry write never strips fields the writer did not mention")
    func leavesUnmentionedFields() throws {
        let document = try HeldDocument()
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0, volume: 0.5)], base: nil) }
        let base = document.registry("clips")

        // A writer that models fewer fields than the document holds (an older
        // client) must not blank what it doesn't know about.
        try document.edit { try $0.writeRegistry("clips", ["a": ["start": .number(3)]], base: base) }

        #expect(document.entry("clips", "a")?["volume"] == .number(0.5))
        #expect(document.entry("clips", "a")?["start"] == .number(3))
    }

    @Test("clearing a present field writes an explicit null")
    func clearWritesNull() throws {
        let document = try HeldDocument()
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0, volume: 0.5)], base: nil) }
        let base = document.registry("clips")

        try document.edit {
            try $0.writeRegistry("clips", ["a": clipFields(start: 0).merging(["volume": .null]) { _, new in new }], base: base)
        }

        #expect(document.entry("clips", "a")?["volume"] == .null)
    }

    // MARK: Poisoned slots

    /// A peer's snapshot in which `registry[key]` holds a plain value rather
    /// than a mergeable child. Our own writers only ever put mergeable
    /// children in a registry, so the poisoned shape arrives in a PEER's bytes,
    /// and reads back without complaint until the first write.
    private func poisoned(_ root: String, _ key: String, _ value: ReplicaValue) throws -> HeldDocument {
        let peer = try HeldDocument(peer: 77)
        try peer.edit { try $0.write(value, at: [root, key]) }
        return try HeldDocument(peer: 10, fold: try peer.snapshot())
    }

    @Test("a registry write reports a refused entry to its owner")
    func registryRefusesPoisonedEntry() throws {
        let document = try poisoned("clips", "clip-a", .string("not-a-clip"))

        #expect(throws: LoroDocumentError.self) {
            try document.edit {
                try $0.writeRegistry("clips", [
                    "clip-a": ["start": .number(0), "duration": .number(4)],
                    "clip-b": ["start": .number(4), "duration": .number(4)],
                ], base: nil)
            }
        }
        #expect(document.live.value["clips"]?["clip-a"] == .string("not-a-clip"))
    }

    // MARK: Birth

    @Test("a birth seeds registries as mergeable entries and keeps its nulls")
    func birthSeedsRegistriesAndNulls() throws {
        let document = try HeldDocument(peer: 77)
        try document.edit {
            try $0.seed([
                "tracks": .object(["video": .object(["visual_rank": .integer(1000), "tint_color": .null])]),
                "settings": .object(["bpm": .integer(0), "watermark": .null]),
            ], registries: ["tracks"])
        }

        #expect(document.entry("tracks", "video") == ["visual_rank": .integer(1000), "tint_color": .null])
        #expect(document.live.value["settings"] == .object(["bpm": .integer(0), "watermark": .null]))

        // The seeded entry is a mergeable child: a peer's field lands beside it.
        let peer = try HeldDocument(peer: 78)
        try peer.edit { try $0.writeEntry("tracks", "video", ["content_muted": .bool(true)], base: nil) }
        try peer.sync(to: document)
        #expect(document.entry("tracks", "video")?["visual_rank"] == .integer(1000))
        #expect(document.entry("tracks", "video")?["content_muted"] == .bool(true))
    }

    // MARK: Transport

    @Test("a delta whose causal history is missing is refused, not silently parked")
    func refusesParkedDelta() throws {
        let source = try HeldDocument(peer: 20)
        try source.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0)], base: nil) }
        let midway = source.version

        try source.edit { try $0.writeEntry("clips", "a", ["start": .number(5)], base: nil) }
        let second = try source.delta(since: midway)

        // Importing the SECOND delta without the first leaves Loro holding it
        // unapplied. Reporting success there is how an edit vanishes.
        let target = try HeldDocument(peer: 22)
        #expect(throws: ReplicaError.missingCausalDeps) { try target.importing([second]) }
    }

    @Test("importing the same delta twice is a no-op")
    func idempotentImport() throws {
        let source = try HeldDocument(peer: 20)
        try source.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0)], base: nil) }
        let blob = try source.snapshot()

        let target = try HeldDocument(peer: 22)
        try target.importing([blob])
        let after = target.version
        try target.importing([blob])

        #expect(target.version == after)
        #expect(target.registry("clips").keys.sorted() == ["a"])
    }

    @Test("a delta since a peer's version carries only what it is missing")
    func incrementalExport() throws {
        let source = try HeldDocument(peer: 20)
        try source.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0)], base: nil) }
        let caughtUp = try HeldDocument(peer: 21)
        try caughtUp.importing([try source.snapshot()])

        try source.edit {
            try $0.writeRegistry("clips", ["a": clipFields(start: 0), "b": clipFields(start: 4)], base: source.registry("clips"))
        }

        let delta = try source.delta(since: caughtUp.version)
        #expect(throws: ReplicaError.missingCausalDeps) { try HeldDocument(peer: 23).importing([delta]) }
        #expect(delta.count < (try source.snapshot()).count)
        try caughtUp.importing([delta])
        #expect(caughtUp.live.value == source.live.value)
    }

    /// Kill: drop a field in the snapshot or the import — this reads the
    /// restored document's RAW Loro tree with a bare reader, not the adapter.
    @Test("a snapshot round-trips into a fresh document, checked against the raw tree")
    func snapshotRoundTrip() throws {
        let source = try HeldDocument(peer: 20)
        try source.edit {
            try $0.writeRegistry("clips", ["a": clipFields(start: 0, volume: 0.25)], base: nil)
            try $0.writeFields("meta", ["name": .string("Trip")], base: nil)
        }

        let restored = try HeldDocument(peer: 21, fold: try source.snapshot())

        let reader = LoroDoc()
        _ = try reader.import(bytes: try restored.snapshot())
        guard case let .map(root) = reader.getDeepValue(),
              case let .map(clips) = root["clips"],
              case let .map(a) = clips["a"],
              case let .map(meta) = root["meta"]
        else {
            Issue.record("the restored snapshot is not the document shape")
            return
        }
        #expect(a["start"] == .double(value: 0))
        #expect(a["duration"] == .double(value: 4))
        #expect(a["volume"] == .double(value: 0.25))
        #expect(a["track_key"] == .string(value: "video"))
        #expect(meta["name"] == .string(value: "Trip"))
        #expect(restored.live.value == source.live.value)
    }
}
