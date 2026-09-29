import Foundation
import XCTest
@testable import ReplicaMan

final class DataDecodeCorpusTests: XCTestCase {
    func testStoredValueContract() throws {
        struct Scenario: Decodable { let name: String; let json: String; let valid: Bool; let integers: [String: String]? }
        let root = (0..<4).reduce(URL(fileURLWithPath: #filePath)) { url, _ in url.deletingLastPathComponent() }
        let scenarios = try JSONDecoder().decode([Scenario].self, from: Data(contentsOf: root.appendingPathComponent("protocol/fixtures/stored-values.json")))
        for scenario in scenarios {
            if !scenario.valid {
                XCTAssertThrowsError(try ReplicaStateStore.decodeData(scenario.json), scenario.name)
                continue
            }
            let fields = try ReplicaStateStore.decodeData(scenario.json)
            let wire = try ReplicaJSON.decoder().decode([String: ReplicaValue].self, from: Data(scenario.json.utf8))
            XCTAssertEqual(fields, wire, scenario.name)
            let encoded = try ReplicaStateStore.encodeData(fields)
            XCTAssertEqual(try ReplicaStateStore.decodeData(encoded), fields, scenario.name)
            for (key, literal) in scenario.integers ?? [:] {
                XCTAssertEqual(fields[key]?.int, Int(literal), key)
                XCTAssertTrue(encoded.contains("\"\(key)\":\(literal)"), "integer changed: \(encoded)")
            }
        }
    }

    func testIntegerAccessNeverRoundsOrTraps() {
        for value in [0.5, -0.5, Double.infinity, Double.nan, 9_223_372_036_854_775_808] {
            XCTAssertNil(ReplicaValue.number(value).int)
        }
    }
}
