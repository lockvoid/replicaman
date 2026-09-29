import CryptoKit
import Foundation
import Testing
@testable import ReplicaManLoro
@testable import ReplicaMan

/// The cross-platform merge corpus, merged by the ENGINE's codec — the one
/// that holds every document the app edits. These are the SAME committed bytes
/// the Rails suite replays (`tools/generate_crdt_corpus.rb` generates them; the
/// gem's `DocumentCorpusTest` asserts the identical expectations). If the
/// two Loro bindings ever disagree on a merge — including across the version
/// skew, since the server builds crate 1.13.6 and loro-swift ships 1.13.3 —
/// one of the two suites goes red instead of a user losing an edit.
///
/// Provenance: the `.bin` trio is INPUT and `expected.json` is
/// the OTHER implementation's answer. Nothing here writes either, so from here
/// the corpus is frozen. `INDEX.json` pins WHICH cases and WHICH bytes are
/// graded, so a half-finished re-copy fails loudly instead of leaving iOS
/// quietly asserting a stale contract.
struct DocumentCorpusTests {
    struct Fixture {
        let name: String
        let description: String
        let blobs: [Data]
        /// The server's projection, as the document holds it: registries as
        /// `key → fields`, plain roots as maps.
        let expected: [String: ReplicaValue]
    }

    /// Loads the corpus WITHOUT swallowing anything: every unreadable file,
    /// malformed manifest or unparseable projection is recorded by name and
    /// asserted on below — a swallowed load is how parameterized cases
    /// silently become zero.
    struct Corpus {
        var fixtures: [Fixture] = []
        var problems: [String] = []
        /// case name → file name → sha256, over the bytes actually on disk.
        var digests: [String: [String: String]] = [:]
    }

    static let corpus: Corpus = load()

    static var all: [Fixture] { corpus.fixtures }

    private static func load() -> Corpus {
        var corpus = Corpus()
        guard let root = Bundle.module.url(forResource: "crdt_convergence", withExtension: nil) else {
            corpus.problems.append("the crdt_convergence resource is not in the test bundle")
            return corpus
        }
        let entries: [URL]
        do {
            entries = try FileManager.default.contentsOfDirectory(at: root, includingPropertiesForKeys: nil)
        } catch {
            corpus.problems.append("cannot list the corpus: \(error)")
            return corpus
        }

        for dir in entries.filter(\.hasDirectoryPath).sorted(by: { $0.lastPathComponent < $1.lastPathComponent }) {
            let name = dir.lastPathComponent
            do {
                var digests: [String: String] = [:]
                func read(_ file: String) throws -> Data {
                    let bytes = try Data(contentsOf: dir.appendingPathComponent(file))
                    digests[file] = SHA256.hash(data: bytes).map { String(format: "%02x", $0) }.joined()
                    return bytes
                }

                let manifestData = try read("manifest.json")
                guard let manifest = try JSONSerialization.jsonObject(with: manifestData) as? [String: Any],
                      let names = manifest["blobs"] as? [String]
                else {
                    corpus.problems.append("\(name): manifest.json has no `blobs`")
                    continue
                }
                let blobs = try names.map(read)
                guard let expected = document(projection: try read("expected.json")) else {
                    corpus.problems.append("\(name): expected.json is not a projection")
                    continue
                }

                corpus.digests[name] = digests
                corpus.fixtures.append(
                    Fixture(
                        name: manifest["name"] as? String ?? name,
                        description: manifest["description"] as? String ?? "",
                        blobs: blobs,
                        expected: expected
                    )
                )
            } catch {
                corpus.problems.append("\(name): \(error)")
            }
        }
        return corpus
    }

    /// The server's `expected.json` — the generator's projection, registries
    /// as arrays of entries carrying their key — in the document's own shape.
    private static func document(projection bytes: Data) -> [String: ReplicaValue]? {
        guard let root = (try? JSONSerialization.jsonObject(with: bytes)) as? [String: Any] else { return nil }
        var document: [String: ReplicaValue] = [:]
        for (name, value) in root {
            if let entries = value as? [[String: Any]] {
                var registry: [String: ReplicaValue] = [:]
                for raw in entries {
                    guard let key = raw["key"] as? String else { return nil }
                    var fields = raw
                    fields.removeValue(forKey: "key")
                    registry[key] = .object(fields.compactMapValues(ReplicaValue.init(documentJSON:)))
                }
                document[name] = .object(registry)
            } else if let fields = value as? [String: Any] {
                document[name] = .object(fields.compactMapValues(ReplicaValue.init(documentJSON:)))
            } else {
                return nil
            }
        }
        return document
    }

    /// The merged document root by root — an absent root reads as the empty
    /// map the server projects for it.
    private func merged(_ document: HeldDocument, like expected: [String: ReplicaValue]) -> [String: ReplicaValue] {
        let held = document.live.value.object ?? [:]
        return Dictionary(uniqueKeysWithValues: Set(expected.keys).union(held.keys).map { root in
            (root, held[root] ?? .object([:]))
        })
    }

    /// The anti-drift gate, and the only thing standing between a corrupt
    /// fixture and vacuously-green parameterized cases.
    ///
    /// Kill: delete a `.bin`, truncate an `expected.json`, or land a
    /// regenerated case here without its `INDEX.json`.
    @Test("the corpus is exactly the frozen index, byte for byte")
    func corpusMatchesTheFrozenIndex() throws {
        #expect(Self.corpus.problems.isEmpty, "corpus load problems: \(Self.corpus.problems)")

        let url = try #require(
            Bundle.module.url(forResource: "crdt_convergence", withExtension: nil)?.appendingPathComponent("INDEX.json")
        )
        let index = try #require(JSONSerialization.jsonObject(with: try Data(contentsOf: url)) as? [String: Any])
        let frozen = try #require(index["cases"] as? [String: [String: String]])

        #expect(Set(Self.corpus.digests.keys) == Set(frozen.keys),
                "case set drifted from INDEX.json — re-copy from protocol/fixtures/crdt_convergence and re-freeze")
        for (name, files) in frozen {
            #expect(Self.corpus.digests[name] == files, "\(name): bytes differ from the frozen index")
        }
        #expect(Self.all.count == frozen.count)
    }

    @Test("every fixture merges to the projection the server produced", arguments: Self.all)
    func mergesToExpected(fixture: Fixture) throws {
        let document = try HeldDocument(peer: 99)
        try document.importing(fixture.blobs)

        #expect(merged(document, like: fixture.expected) == fixture.expected, "\(fixture.name): \(fixture.description)")
    }

    @Test("merge order does not change the result", arguments: Self.all)
    func orderIndependent(fixture: Fixture) throws {
        // The base has to arrive first — later ops depend on it — but the two
        // concurrent peers may land either way round.
        let base = fixture.blobs[0]
        let concurrent = Array(fixture.blobs.dropFirst())

        for order in [concurrent, concurrent.reversed()] {
            let document = try HeldDocument(peer: 99)
            try document.importing([base] + order)

            #expect(merged(document, like: fixture.expected) == fixture.expected, "\(fixture.name): order-dependent merge")
        }
    }

    @Test("replaying one blob at a time, twice over, converges the same way", arguments: Self.all)
    func incrementalAndIdempotent(fixture: Fixture) throws {
        let document = try HeldDocument(peer: 99)
        for blob in fixture.blobs { try document.importing([blob]) }
        // And again — importing a delta twice must be a no-op.
        for blob in fixture.blobs { try document.importing([blob]) }

        #expect(merged(document, like: fixture.expected) == fixture.expected, "\(fixture.name): incremental replay diverged")
    }
}

extension DocumentCorpusTests.Fixture: CustomTestStringConvertible {
    var testDescription: String { name }
}

extension ReplicaValue {
    /// JSON gives us `NSNumber` for every numeric, so integer-vs-float has to
    /// be recovered from the CFNumber type — otherwise `weight: 1.0` reads as
    /// an integer here and a float on the server, and every fixture fails on a
    /// difference that isn't real.
    init?(documentJSON json: Any) {
        switch json {
        case is NSNull:
            self = .null
        case let value as NSNumber:
            if CFGetTypeID(value) == CFBooleanGetTypeID() {
                self = .bool(value.boolValue)
            } else if CFNumberIsFloatType(value) {
                self = .number(value.doubleValue)
            } else {
                self = .integer(value.int64Value)
            }
        case let value as String:
            self = .string(value)
        case let value as [Any]:
            self = .array(value.compactMap(ReplicaValue.init(documentJSON:)))
        case let value as [String: Any]:
            self = .object(value.compactMapValues(ReplicaValue.init(documentJSON:)))
        default:
            return nil
        }
    }
}
