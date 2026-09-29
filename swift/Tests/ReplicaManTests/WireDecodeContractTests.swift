import Foundation
import XCTest
@testable import ReplicaMan

final class WireDecodeContractTests: XCTestCase {
    private struct JournalCase: Decodable { let name: String; let valid: Bool; let op: String }
    private struct PullCase: Decodable { let name: String; let valid: Bool; let pull: String }
    private struct Page: Equatable { let frame: ReplicaFrame; var reset = false; var more = false }

    private static let pages: [String: Page] = [
        "row": Page(frame: .rowSet(stream: "notes", id: "n1", type: nil, data: [:], revision: 1)),
        "snapshot": Page(frame: .docSnapshot(
            stream: "boards", id: "b1", codec: "loro@1", snapshot: Data([1, 2, 3]), data: [:], revision: 2)),
        "delta": Page(frame: .docDelta(stream: "boards", id: "b1", seq: 1, codec: "loro@1", payload: Data([1, 2, 3]))),
        "delete": Page(frame: .rowDelete(stream: "notes", id: "n1", revision: 4)),
        "reset page with more to come": Page(
            frame: .rowSet(stream: "notes", id: "n1", type: nil, data: [:], revision: 1), reset: true, more: true),
        "unknown data field": Page(frame: .rowSet(stream: "notes", id: "n1", type: nil, data: [
            "future": .object(["nested": .array([.bool(true), .null, .integer(9_007_199_254_740_993)])]),
        ], revision: 1)),
    ]

    func testSharedJournalContract() throws {
        let cases = try fixture([JournalCase].self, "journal-decode.json")
        XCTAssertEqual(cases.count, 16)
        for scenario in cases where !scenario.valid {
            XCTAssertThrowsError(try decodeOp(scenario.op), scenario.name)
        }
        let valid = try Dictionary(uniqueKeysWithValues: cases.filter(\.valid).map { ($0.name, try decodeOp($0.op)) })
        XCTAssertEqual(valid.keys.sorted(), ["bound parent", "explicit rebirth", "ordinary birth", "reference limit"])
        XCTAssertEqual(valid.values.map(\.incarnation), Array(repeating: "born", count: 4))
        XCTAssertEqual(valid["ordinary birth"]?.replaces, nil)
        XCTAssertEqual(valid["explicit rebirth"]?.replaces, "previous")
        XCTAssertEqual(valid["bound parent"]?.references,
                       [ReplicaReference(name: "parent", stream: "boards", id: "b1", incarnation: "parent-birth")])
        XCTAssertEqual(valid["reference limit"]?.references.map(\.name), (0..<64).map { "parent\($0)" })
    }

    func testSharedPullContract() throws {
        let cases = try fixture([PullCase].self, "pull-decode.json")
        XCTAssertEqual(cases.count, 43)
        XCTAssertEqual(Set(cases.filter(\.valid).map(\.name)), Set(Self.pages.keys))
        for scenario in cases {
            if let expected = Self.pages[scenario.name] {
                XCTAssertEqual(try decodePage(scenario.pull), expected, scenario.name)
            } else {
                XCTAssertThrowsError(try ReplicaPullPage.decode(Data(scenario.pull.utf8)), scenario.name)
            }
        }
    }

    private func fixture<T: Decodable>(_ type: T.Type, _ name: String) throws -> T {
        let root = (0..<4).reduce(URL(fileURLWithPath: #filePath)) { url, _ in url.deletingLastPathComponent() }
        return try JSONDecoder().decode(type, from: Data(contentsOf: root.appendingPathComponent("protocol/fixtures/\(name)")))
    }

    private func decodeOp(_ json: String) throws -> ReplicaOp {
        try JSONDecoder().decode(ReplicaOp.self, from: Data(json.utf8))
    }

    private func decodePage(_ json: String) throws -> Page? {
        let page = try ReplicaPullPage.decode(Data(json.utf8))
        XCTAssertEqual(page.shard, "user")
        XCTAssertEqual(page.cursor, "W1sidXNlcjoxIiwiOTkiXV0")
        XCTAssertEqual(page.frames.map(\.incarnation), ["life-1"])
        return page.frames.first.map { Page(frame: $0.frame, reset: page.reset, more: page.more) }
    }
}
