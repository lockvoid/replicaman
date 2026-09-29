import Foundation
import XCTest
@testable import ReplicaMan

/// The direct coder must be a DROP-IN for the JSON round-trip it replaces
/// (`ReplicaValue` → JSONEncoder bytes → JSONDecoder w/ convertFromSnakeCase
/// → typed struct, and back with default keys). Every case here decodes the
/// same input through BOTH paths and demands identical results — the old
/// bridge is the oracle, not a hand-written expectation.
final class ReplicaValueCodingTests: XCTestCase {

    private struct Shadow: Codable, Equatable {
        var x: Double
        var y: Double
    }

    private struct Style: Codable, Equatable {
        var fontFamily: String?
        var fontSize: Double?
        var shadowOffset: Shadow?
        var strokeWidth: Int?
        var visible: Bool?
        var tags: [String]?
        var weights: [Double]?
    }

    // MARK: - Oracles (the exact generated-bridge implementations)
    //
    // The `try?` below is NOT a swallowed test error: it is part of the oracle,
    // which models the generated bridge's own "nil on failure" contract. The
    // rejection tests compare against that nil deliberately.

    private func bridgeDecode<T: Decodable>(_ type: T.Type, _ value: ReplicaValue) -> T? {
        guard let bytes = try? JSONEncoder().encode(value) else { return nil }
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        return try? decoder.decode(T.self, from: bytes)
    }

    private func bridgeEncode<T: Encodable>(_ value: T) -> ReplicaValue {
        guard let bytes = try? JSONEncoder().encode(value),
              let decoded = try? JSONDecoder().decode(ReplicaValue.self, from: bytes)
        else { return .null }
        return decoded
    }

    // MARK: - Decode parity

    func testDecodesCamelCasePayloadIdenticallyToBridge() throws {
        let value = ReplicaValue.object([
            "fontFamily": .string("Diatype"),
            "fontSize": .number(24),
            "shadowOffset": .object(["x": .number(1.5), "y": .number(-2)]),
            "strokeWidth": .number(3),
            "visible": .bool(true),
            "tags": .array([.string("a"), .string("b")]),
            "weights": .array([.number(0.25), .number(1)]),
        ])

        let direct = try ReplicaValueCoding.decode(Style.self, from: value)
        XCTAssertEqual(direct, bridgeDecode(Style.self, value))
    }

    func testDecodesSnakeCasePayloadIdenticallyToBridge() throws {
        let value = ReplicaValue.object([
            "font_family": .string("Inter"),
            "font_size": .number(12),
            "shadow_offset": .object(["x": .number(0), "y": .number(4)]),
            "stroke_width": .number(0),
            "visible": .bool(false),
        ])

        let direct = try ReplicaValueCoding.decode(Style.self, from: value)
        XCTAssertEqual(direct, bridgeDecode(Style.self, value))
        XCTAssertEqual(direct.fontFamily, "Inter", "snake_case keys answer camelCase properties")
    }

    func testMissingAndNullFieldsMatchBridge() throws {
        let value = ReplicaValue.object([
            "fontFamily": .null,
            "visible": .bool(true),
        ])

        let direct = try ReplicaValueCoding.decode(Style.self, from: value)
        XCTAssertEqual(direct, bridgeDecode(Style.self, value))
        XCTAssertNil(direct.fontFamily)
        XCTAssertNil(direct.fontSize)
    }

    func testNonIntegralIntRejectsLikeBridge() {
        let value = ReplicaValue.object(["strokeWidth": .number(3.5)])
        XCTAssertNil(bridgeDecode(Style.self, value), "oracle: JSONDecoder refuses 3.5 for Int")
        XCTAssertThrowsError(try ReplicaValueCoding.decode(Style.self, from: value))
    }

    /// A document's integer (`.integer`, Loro `i64`) decodes wherever a number
    /// does: into an Int exactly, into a Double as its value — the same answers
    /// the JSON bridge gives the same number.
    func testADocumentIntegerDecodesLikeTheSameNumber() throws {
        let value = ReplicaValue.object(["strokeWidth": .integer(3), "fontSize": .integer(17), "weights": .array([.integer(1), .number(0.5)])])
        let direct = try ReplicaValueCoding.decode(Style.self, from: value)

        XCTAssertEqual(direct, bridgeDecode(Style.self, value))
        XCTAssertEqual(direct.strokeWidth, 3)
        XCTAssertEqual(direct.fontSize, 17)
        XCTAssertEqual(direct.weights, [1, 0.5])
    }

    /// The wire has one number: an integer encodes as the whole number it is.
    func testAnIntegerEncodesAsTheWholeNumber() throws {
        XCTAssertEqual(String(decoding: try JSONEncoder().encode(ReplicaValue.integer(30)), as: UTF8.self), "30")
        XCTAssertEqual(ReplicaValue.integer(30).number, 30)
        XCTAssertEqual(ReplicaValue.integer(30).int, 30)
        XCTAssertNotEqual(ReplicaValue.integer(30), .number(30), "a document keeps an integer and a float apart")
    }

    func testFloatOverflowRejectsLikeBridge() {
        struct Row: Codable, Equatable { var scale: Float }
        let value = ReplicaValue.object(["scale": .number(1e40)])
        XCTAssertNil(bridgeDecode(Row.self, value), "oracle: JSONDecoder refuses a Double that overflows Float")
        XCTAssertThrowsError(
            try ReplicaValueCoding.decode(Row.self, from: value),
            "1e40 must be a decode failure, never a silent .inf"
        )
    }

    func testATypeMismatchRejectsLikeBridge() {
        let wrongScalar = ReplicaValue.object(["fontFamily": .integer(7)])
        XCTAssertNil(bridgeDecode(Style.self, wrongScalar), "oracle: JSONDecoder refuses a number for a String")
        XCTAssertThrowsError(try ReplicaValueCoding.decode(Style.self, from: wrongScalar))

        let wrongList = ReplicaValue.object(["tags": .string("caption")])
        XCTAssertNil(bridgeDecode(Style.self, wrongList), "oracle: JSONDecoder refuses a string for a list")
        XCTAssertThrowsError(try ReplicaValueCoding.decode(Style.self, from: wrongList))
    }

    func testAMissingRequiredKeyRejectsLikeBridge() {
        struct Row: Codable, Equatable { var kind: String }
        let value = ReplicaValue.object([:])
        XCTAssertNil(bridgeDecode(Row.self, value), "oracle: JSONDecoder refuses a missing required key")
        XCTAssertThrowsError(try ReplicaValueCoding.decode(Row.self, from: value))
    }

    func testStringBackedEnumDecodes() throws {
        enum Kind: String, Codable { case video, audio }
        struct Row: Codable, Equatable { var kind: Kind }
        let value = ReplicaValue.object(["kind": .string("audio")])
        XCTAssertEqual(try ReplicaValueCoding.decode(Row.self, from: value), bridgeDecode(Row.self, value))
    }

    func testTopLevelArrayDecodes() throws {
        let value = ReplicaValue.array([
            .object(["x": .number(1), "y": .number(2)]),
            .object(["x": .number(3), "y": .number(4)]),
        ])
        XCTAssertEqual(
            try ReplicaValueCoding.decode([Shadow].self, from: value),
            bridgeDecode([Shadow].self, value)
        )
    }

    // MARK: - Encode parity

    func testEncodesIdenticallyToBridge() throws {
        let style = Style(
            fontFamily: "Diatype", fontSize: 24,
            shadowOffset: Shadow(x: 1.5, y: -2), strokeWidth: 3,
            visible: true, tags: ["a", "b"], weights: [0.25, 1]
        )
        XCTAssertEqual(try ReplicaValueCoding.encode(style), bridgeEncode(style))
    }

    func testEncodeOmitsNilExactlyLikeBridge() throws {
        let style = Style(fontFamily: nil, fontSize: 12)
        let direct = try ReplicaValueCoding.encode(style)
        XCTAssertEqual(direct, bridgeEncode(style))
        guard case .object(let object) = direct else { return XCTFail("expected object") }
        XCTAssertNil(object["fontFamily"], "Codable synthesis skips nils; the tree must too")
    }
    // MARK: - Key conversion

    /// Repeated because the conversion is memoized: the cached answer must be
    /// the computed answer, or snake-key fallback silently breaks everywhere.
    func testSnakeCasing() {
        for _ in 0..<3 {
            XCTAssertEqual(ReplicaValueCoding.snakeCased("animationInId"), "animation_in_id")
            XCTAssertEqual(ReplicaValueCoding.snakeCased("x"), "x")
            XCTAssertEqual(ReplicaValueCoding.snakeCased("fontFamily"), "font_family")
            XCTAssertEqual(ReplicaValueCoding.snakeCased("visible"), "visible")
        }
    }
}
