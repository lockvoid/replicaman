import Foundation
import XCTest
@testable import ReplicaMan

/// Matrix 11 — codegen. The generated fixture models are CHECKED IN under
/// `Tests/…/Generated`; this test regenerates from the committed dummy
/// manifest and asserts byte-identity (the SDL pattern — drift shows up in
/// review, never silently). Readonly streams have no write verbs at all —
/// compile-time by construction (`Job` conforms to `ReplicaRowModel` only,
/// so no `RowStream<Job>` can exist), asserted here at the type level.
final class CodegenTests: XCTestCase {

    private var packageRoot: URL {
        URL(fileURLWithPath: #filePath)          // swift/Tests/ReplicaManTests/CodegenTests.swift
            .deletingLastPathComponent()         // swift/Tests/ReplicaManTests
            .deletingLastPathComponent()         // swift/Tests
            .deletingLastPathComponent()         // swift
    }

    private var repoRoot: URL {
        packageRoot.deletingLastPathComponent()
    }

    private var dummyManifest: URL {
        packageRoot.appendingPathComponent("Tests/ReplicaManTests/Fixtures/manifest.json")
    }

    private var sampleManifest: URL {
        repoRoot.appendingPathComponent("protocol/fixtures/consumer-manifest.json")
    }

    private func scratchDirectory(_ prefix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
    }

    private func runCodegen(_ arguments: [String]) throws {
        let config = repoRoot.appendingPathComponent("codegen/tests/fixtures/consumer-codegen.json")
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["ruby", repoRoot.appendingPathComponent("codegen/bin/replica-codegen").path,
                             "--language", "swift", "--config", config.path] + arguments
        let stderr = Pipe()
        process.standardError = stderr
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus != 0 else { return }
        let noise = String(decoding: stderr.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
        throw NSError(domain: "ReplicaCodegenTests", code: Int(process.terminationStatus),
                      userInfo: [NSLocalizedDescriptionKey: "replica-codegen failed (\(process.terminationStatus)): \(noise)"])
    }

    /// The dummy world pins its container name: a bare default mints stray duplicates.
    private func regenerateFixture() throws -> URL {
        let out = scratchDirectory("replica-codegen")
        try runCodegen(["--manifest", dummyManifest.path, "--out", out.path, "--name", "Replica"])
        return out
    }

    private func regenerateDocumentFixture() throws -> URL {
        let root = scratchDirectory("replica-document-codegen")
        let documents = root.appendingPathComponent("Documents", isDirectory: true)
        try runCodegen(["--manifest", dummyManifest.path, "--out", root.appendingPathComponent("Rows").path,
                        "--document-out", documents.path])
        return documents
    }

    private func regenerateSampleFixture() throws -> URL {
        let out = scratchDirectory("sample-replica-codegen")
        try runCodegen(["--manifest", sampleManifest.path, "--out", out.path, "--document-out", out.path,
                        "--name", "SampleReplica"])
        return out
    }

    private func sampleSources(_ files: String...) throws -> [String] {
        let out = try regenerateSampleFixture()
        return try files.map { try String(contentsOf: out.appendingPathComponent($0), encoding: .utf8) }
    }

    /// The manifest's `indexes:` become ONE `Field` enum per model — its
    /// indexed fields, raw value = wire name — and the container's index
    /// specs; a model without indexes aliases `ReplicaNoField`, so no
    /// predicate over it can be formed.
    func testIndexesGenerateTheFieldEnumAndTheSchemaSpecs() {
        XCTAssertEqual(Item.Field.boardId.rawValue, "boardId")
        XCTAssertEqual(Ticket.Field.note.rawValue, "note")
        XCTAssertEqual(Ticket.Field.userId.rawValue, "userId")
        XCTAssertTrue(Board.Field.self == ReplicaNoField.self, "a document stream without indexes has nothing to scope on")
        XCTAssertTrue(Job.Field.self == ReplicaNoField.self)
        XCTAssertEqual(Replica.schema.indexes, [
            ReplicaIndexSpec(stream: "items", field: "boardId", kind: .btree),
            ReplicaIndexSpec(stream: "tickets", field: "note", kind: .fts5),
            ReplicaIndexSpec(stream: "tickets", field: "userId", kind: .btree),
        ])
    }

    func testGeneratedDiscriminatedShapeMaterializesInBothModelInitializers() throws {
        let job = try String(contentsOf: try regenerateFixture().appendingPathComponent("Job.swift"), encoding: .utf8)

        XCTAssertTrue(job.contains("public var payload: JobPayload? {\n        didSet { payloadJSON = payload?.replicaValue }\n    }"))
        XCTAssertEqual(job.components(separatedBy: "self.payload = JobPayload(replicaValue:").count - 1, 2)
        XCTAssertTrue(job.contains("public var summary: JobSummary?\n"), "a non-union shape is the column's type")
        XCTAssertTrue(job.contains("self.summary = (fields[\"summary\"].flatMap { try? ReplicaValueCoding.decode(JobSummary.self, from: $0) })"))
    }

    func testARequiredDiscriminatedShapeFallsBackToAnEmptyRawObject() throws {
        let export = try sampleSources("Export.swift")[0]

        XCTAssertTrue(export.contains("public var result: ExportResult? {\n        didSet { resultJSON = result?.replicaValue ?? .object([:]) }\n    }"))
        XCTAssertEqual(export.components(separatedBy: "self.result = ExportResult(replicaValue:").count - 1, 2)
    }

    func testARequiredShapeIsNonOptionalAndGuardsTheRow() throws {
        let workflow = try sampleSources("Workflow.swift")[0]

        XCTAssertTrue(workflow.contains("public var graph: WorkflowGraph\n"), "a required shape is non-optional")
        XCTAssertTrue(workflow.contains("guard let graph = (fields[\"graph\"].flatMap { try? ReplicaValueCoding.decode(WorkflowGraph.self, from: $0) }) else { return nil }"),
                      "a row whose required shape does not decode is not a row")
    }
    func testDocumentShapesEmitIntoTheirOwnRootWithConversionsAndDefaults() throws {
        let out = try regenerateDocumentFixture()
        let document = try String(
            contentsOf: out.appendingPathComponent("BoardDocument.swift"),
            encoding: .utf8
        )
        let defaults = try String(
            contentsOf: out.appendingPathComponent("Board+Defaults.swift"),
            encoding: .utf8
        )

        XCTAssertTrue(document.contains("public struct BoardDocument: Sendable, Hashable, Codable"))
        XCTAssertTrue(document.contains("public var meta: Meta"))
        XCTAssertTrue(document.contains("public init?(document: ReplicaValue)"))
        XCTAssertTrue(document.contains("public var documentRoots: [String: ReplicaValue]"))
        XCTAssertTrue(document.contains("public struct Meta: Sendable, Hashable, Codable"))
        XCTAssertTrue(document.contains("public var name: String"))
        XCTAssertTrue(defaults.contains("public enum BoardDefaults"))
        XCTAssertTrue(defaults.contains("public static let meta = BoardDocument.Meta(name: \"Untitled\")"))
    }

    func testSampleDocumentRootsAreTypedRegistriesAndObjects() throws {
        let deck = try sampleSources("DeckDocument.swift")[0]

        XCTAssertTrue(deck.contains("public struct DeckDocument: Sendable, Hashable, Codable"))
        XCTAssertTrue(deck.contains("public var slides: [Slide]"))
        XCTAssertTrue(deck.contains("public var layouts: [Layout]"))
        XCTAssertTrue(deck.contains("public var settings: Settings"))
        XCTAssertTrue(deck.contains("public enum TextDecoration: String, Codable, Sendable, CaseIterable"))
        XCTAssertTrue(deck.contains("public var textDecoration: [TextDecoration]?"))
        XCTAssertTrue(deck.contains("public var fillColor: String?"),
                      "hex_color is a declared string scalar, not raw ReplicaValue")
    }

    func testSampleDocumentDefaultsAreTypedLiterals() throws {
        let defaults = try sampleSources("Deck+Defaults.swift")[0]

        XCTAssertTrue(defaults.contains("public enum DeckDefaults"))
        XCTAssertTrue(defaults.contains("public static let layouts: [DeckDocument.Layout]"))
        XCTAssertTrue(defaults.contains("public static let settings = DeckDocument.Settings("))
    }

    func testSampleMapAndUnionGenerationIsGeneric() throws {
        let sources = try sampleSources("Workflow.swift", "Export.swift")
        let (workflow, export) = (sources[0], sources[1])

        XCTAssertTrue(workflow.contains("public struct WorkflowGraph: Sendable, Hashable, Codable"))
        XCTAssertTrue(workflow.contains("public var steps: [String: Steps]"))
        XCTAssertTrue(workflow.contains("public var links: [String: Links]"))
        XCTAssertTrue(workflow.contains("public var graph: WorkflowGraph\n"))
        XCTAssertTrue(export.contains("public var colorSpace: String?"),
                      "a discriminated variant keeps its established raw-value surface")
        XCTAssertTrue(export.contains("public var colorSpaceValue: ColorSpace?"),
                      "the same generic enum metadata remains available as a typed accessor")
    }

    func testShapeBackedColumnKeepsLosslessRawJSON() throws {
        let empty: ReplicaValue = .object([:])
        let stateOnly = try XCTUnwrap(Job(id: "j1", type: nil, data: [
            "payload": empty,
            "priority": .string("low"),
            "state": .string("running"),
            "tags": .array([]),
            "userId": .string("u1"),
        ]))
        XCTAssertEqual(stateOnly.payloadJSON, empty)
        XCTAssertNil(stateOnly.payload)

        let future: ReplicaValue = .object([
            "kind": .string("future"),
            "opaque": .number(7),
        ])
        let unknown = try XCTUnwrap(Job(id: "j2", type: nil, data: [
            "payload": future,
            "priority": .string("high"),
            "state": .string("done"),
            "tags": .array([.string("urgent")]),
            "userId": .string("u1"),
        ]))
        guard case .unknown(let shared) = unknown.payload else {
            return XCTFail("unknown discriminator must remain a typed unknown projection")
        }
        XCTAssertEqual(shared.kind, "future")
        XCTAssertEqual(unknown.payloadJSON, future)
        XCTAssertTrue(unknown.encode().isEmpty,
                      "a readonly stream's push set is empty — encode() can leak nothing up the wire")
    }

    func testArrayColumnGeneratesTypedList() throws {
        let job = try XCTUnwrap(Job(id: "j3", type: nil, data: [
            "priority": .string("low"),
            "state": .string("queued"),
            "tags": .array([.string("alpha"), .string("beta")]),
            "userId": .string("u1"),
        ]))

        XCTAssertEqual(job.tags, ["alpha", "beta"],
                       "a Postgres array column decodes to a typed list — the subtype alone would declare a scalar")

        // The encode half lives on a WRITABLE stream — a readonly model's
        // encode is empty by construction.
        let item = try XCTUnwrap(Item(id: "i1", type: "TextItem", data: [
            "boardId": .string("b1"),
            "body": .string("hello"),
            "rank": .string("a0"),
            "tags": .array([.string("alpha"), .string("beta")]),
        ]))
        XCTAssertEqual(item.encode()["tags"], .array([.string("alpha"), .string("beta")]))
    }

    func testReadonlyStreamModelsCarryNoWriteCapability() {
        XCTAssertFalse(
            Job.self is any ReplicaWritableRowModel.Type,
            "a readonly stream's model must not be writable — codegen emits no write verbs for it"
        )
        XCTAssertFalse(Ticket.self is any ReplicaWritableRowModel.Type)
        // The positive half is compile-time: this witness only accepts
        // writable models, so a codegen regression fails the BUILD.
        func writable<Model: ReplicaWritableRowModel>(_: Model.Type) {}
        writable(Item.self)
    }

    func testGeneratedSchemaMatchesTheManifest() {
        let schema = Replica.schema
        XCTAssertEqual(schema.spec("boards")?.lane, .document)
        XCTAssertEqual(schema.spec("boards")?.codec, "loro@1")
        XCTAssertEqual(schema.spec("boards")?.reflections, [ReplicaReflection(field: "name", path: ["meta", "name"])])
        XCTAssertEqual(schema.spec("items")?.lane, .row)
        XCTAssertEqual(schema.spec("items")?.readonly, false)
        XCTAssertEqual(schema.spec("jobs")?.readonly, true)
        XCTAssertEqual(schema.shards, ["user"])
    }

    func testGeneratedTypedDecodeRoundTrips() {
        let photo = Item(id: "i1", type: "PhotoItem", data: [
            "boardId": .string("b1"), "rank": .string("a"),
            "caption": .string("wide"), "width": .number(3),
        ])
        XCTAssertEqual(photo, .photoItem(PhotoItem(id: "i1", boardId: "b1", rank: "a", caption: .wide, width: 3)))
        XCTAssertEqual(photo?.encode()["width"], .number(3))

        XCTAssertNil(
            Item(id: "i2", type: "HologramItem", data: ["boardId": .string("b1"), "rank": .string("a")]),
            "an unknown STI subtype decodes to nothing — stored raw, skipped typed"
        )

        let text = Item(id: "i3", type: "TextItem", data: [
            "boardId": .string("b1"), "rank": .string("a"), "body": .string("hello"),
        ])
        XCTAssertEqual(text, .textItem(TextItem(id: "i3", boardId: "b1", rank: "a", body: "hello")))
        XCTAssertEqual(text?.encode()["body"], .string("hello"))
        XCTAssertNil(
            Item(id: "i4", type: "TextItem", data: ["boardId": .string("b1"), "rank": .string("a")]),
            "a variant field its kind requires is guarded like a base one"
        )
    }

    func testCollidingVariantsTakeTheirConfiguredNamesAndShareOneVocabulary() throws {
        let template = try sampleSources("ItemTemplate.swift")[0]

        XCTAssertTrue(template.contains("public struct PhotoItemTemplate: Identifiable, ReplicaVariant"))
        XCTAssertTrue(template.contains(#"public static let wireType = "PhotoItem""#),
                      "only the emitted name moves — the wire type stays the manifest's")
        XCTAssertEqual(template.components(separatedBy: "public enum ItemTemplateTone:").count - 1, 1,
                       "a vocabulary two variants share is emitted once, owned by the stream model")
    }

    func testGeneratedDeckCreateAcceptsOnlyAuthoredFields() throws {
        let deck = try sampleSources("Deck.swift")[0]

        XCTAssertTrue(
            deck.contains("public func create(id: String, changeSeq: Int, snapshot: Data, documentPeer: UInt64) async throws -> Bool"),
            "the generated deck verb must accept the authored value, not timestamps, user provenance or a column the server computes"
        )
        XCTAssertFalse(deck.contains("create(id: String, createdAt:"))
        XCTAssertFalse(deck.contains(#""slideCount": ."#), "a pull-only column never rides a birth's data")
    }

    func testTheContainerSpecCarriesStampsAndReflections() throws {
        let container = try sampleSources("SampleReplica.swift")[0]

        XCTAssertTrue(
            container.contains(#"ReplicaStreamSpec(name: "decks", lane: .document, readonly: false, shard: "user", codec: "loro@1", stamp: .standard)"#),
            "the engine fills the deck's provenance from its spec — at birth and whenever its document moves"
        )
        XCTAssertTrue(
            container.contains(#"ReplicaStreamSpec(name: "boards", lane: .document, readonly: false, shard: "user", codec: "loro@1", reflections: [ReplicaReflection(field: "name", path: ["meta", "name"])])"#),
            "a reflected document scalar rides the spec, and a stream without the standard columns carries no stamp"
        )
    }
}
