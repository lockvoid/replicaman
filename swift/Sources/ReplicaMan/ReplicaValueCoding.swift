import Foundation

// Direct Codable bridge over the `ReplicaValue` tree. The generated models
// used to round-trip through JSON (`ReplicaValue` → JSONEncoder bytes →
// JSONDecoder → typed struct) on EVERY typed read — hang-sampler stacks
// caught that double serialization grinding on main inside project open.
// These coders walk the tree in place: zero bytes, zero parsing.
//
// Contract parity with the JSON bridge it replaces:
// - Decoding accepts BOTH the payload's own key and its snake_case variant
//   (the old bridge ran `convertFromSnakeCase`; row data is camelized at
//   capture, doc/catalog payloads may carry snake_case).
// - Encoding emits property names AS IS (the old bridge used the default
//   JSONEncoder strategy — no snake conversion on the way out).
// - Integers decode from `.number` only when exactly representable
//   (JSONDecoder rejects 3.5 for Int; so do we), and Float rejects
//   non-finite results (1e40 was a decode failure through JSON; it must
//   not silently become `.inf` here).
//
// KNOWN divergences (deliberate, review-weighed; no domain payload hits
// either):
// - Encoding a NaN Double lands `.number(nan)` in the tree, which then
//   fails at wire/journal JSON-encode time — the old bridge silently
//   collapsed the WHOLE payload to `.null` instead. A loud downstream
//   error beats a silent data wipe.
// - Two sequential `container(keyedBy:)` calls on one encoder overwrite
//   the first group (JSONEncoder merges). Synthesized Codable never does
//   this; a hand-written `encode(to:)` must use a single container.

public enum ReplicaValueCoding {
    /// Decode a `Decodable` straight off a `ReplicaValue` tree.
    public static func decode<T: Decodable>(_ type: T.Type, from value: ReplicaValue) throws -> T {
        try T(from: ValueDecoder(value: value, codingPath: []))
    }

    /// Encode an `Encodable` into a `ReplicaValue` tree.
    public static func encode<T: Encodable>(_ value: T) throws -> ReplicaValue {
        let encoder = ValueEncoder(codingPath: [])
        try value.encode(to: encoder)
        return encoder.box.value ?? .null
    }

    /// camelCase → snake_case, mirroring Foundation's `convertToSnakeCase`
    /// far enough for identifier-shaped keys ("layoutKeyId" →
    /// "layout_key_id").
    ///
    /// Memoized: keys are Codable field names — a small closed set — and
    /// this runs on every keyed-lookup MISS (absent optional fields hit it
    /// once for `contains` and again for the decode), where the per-scalar
    /// ICU `properties.isUppercase` walk was a hang-sampler stall
    /// (225ms of one nested-shape decode inside one open).
    static func snakeCased(_ key: String) -> String {
        memoLock.lock()
        if let hit = memo[key] {
            memoLock.unlock()
            return hit
        }
        memoLock.unlock()
        var out = ""
        out.reserveCapacity(key.count + 4)
        for scalar in key.unicodeScalars {
            if scalar.properties.isUppercase {
                out.append("_")
                out.append(String(scalar).lowercased())
            } else {
                out.unicodeScalars.append(scalar)
            }
        }
        memoLock.lock()
        memo[key] = out
        memoLock.unlock()
        return out
    }

    private static let memoLock = NSLock()
    nonisolated(unsafe) private static var memo: [String: String] = [:]
}

// MARK: - Decoder

private struct ValueDecoder: Decoder {
    let value: ReplicaValue
    let codingPath: [CodingKey]
    var userInfo: [CodingUserInfoKey: Any] { [:] }

    func container<Key: CodingKey>(keyedBy _: Key.Type) throws -> KeyedDecodingContainer<Key> {
        guard case .object(let object) = value else {
            throw DecodingError.typeMismatch([String: ReplicaValue].self, .init(
                codingPath: codingPath, debugDescription: "expected object, got \(value)"
            ))
        }
        return KeyedDecodingContainer(KeyedContainer(object: object, codingPath: codingPath))
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        guard case .array(let array) = value else {
            throw DecodingError.typeMismatch([ReplicaValue].self, .init(
                codingPath: codingPath, debugDescription: "expected array, got \(value)"
            ))
        }
        return UnkeyedContainer(array: array, codingPath: codingPath)
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        SingleContainer(value: value, codingPath: codingPath)
    }
}

private func unwrap<T>(_ value: T?, _ type: Any.Type, _ raw: ReplicaValue, _ path: [CodingKey]) throws -> T {
    guard let value else {
        throw DecodingError.typeMismatch(type, .init(
            codingPath: path, debugDescription: "expected \(type), got \(raw)"
        ))
    }
    return value
}

private func exactInt<T: FixedWidthInteger>(_ raw: ReplicaValue, _ path: [CodingKey]) throws -> T {
    if case .integer(let integer) = raw, let exact = T(exactly: integer) { return exact }
    guard case .number(let number) = raw, let exact = T(exactly: number) else {
        throw DecodingError.typeMismatch(T.self, .init(
            codingPath: path, debugDescription: "expected exact \(T.self), got \(raw)"
        ))
    }
    return exact
}

/// A Double that overflows Float is a decode FAILURE, not `.inf` — the JSON
/// bridge this replaces rejected it (the generated `init?` returned nil).
private func finiteFloat(_ raw: ReplicaValue, _ path: [CodingKey]) throws -> Float {
    guard let number = raw.number, case let float = Float(number), float.isFinite else {
        throw DecodingError.typeMismatch(Float.self, .init(
            codingPath: path, debugDescription: "expected finite Float, got \(raw)"
        ))
    }
    return float
}

private struct SingleContainer: SingleValueDecodingContainer {
    let value: ReplicaValue
    let codingPath: [CodingKey]

    func decodeNil() -> Bool { value == .null }
    func decode(_: Bool.Type) throws -> Bool { try unwrap(value.bool, Bool.self, value, codingPath) }
    func decode(_: String.Type) throws -> String { try unwrap(value.string, String.self, value, codingPath) }
    func decode(_: Double.Type) throws -> Double { try unwrap(value.number, Double.self, value, codingPath) }
    func decode(_: Float.Type) throws -> Float { try finiteFloat(value, codingPath) }
    func decode(_: Int.Type) throws -> Int { try exactInt(value, codingPath) }
    func decode(_: Int8.Type) throws -> Int8 { try exactInt(value, codingPath) }
    func decode(_: Int16.Type) throws -> Int16 { try exactInt(value, codingPath) }
    func decode(_: Int32.Type) throws -> Int32 { try exactInt(value, codingPath) }
    func decode(_: Int64.Type) throws -> Int64 { try exactInt(value, codingPath) }
    func decode(_: UInt.Type) throws -> UInt { try exactInt(value, codingPath) }
    func decode(_: UInt8.Type) throws -> UInt8 { try exactInt(value, codingPath) }
    func decode(_: UInt16.Type) throws -> UInt16 { try exactInt(value, codingPath) }
    func decode(_: UInt32.Type) throws -> UInt32 { try exactInt(value, codingPath) }
    func decode(_: UInt64.Type) throws -> UInt64 { try exactInt(value, codingPath) }
    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        try T(from: ValueDecoder(value: value, codingPath: codingPath))
    }
}

private struct KeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    let object: [String: ReplicaValue]
    let codingPath: [CodingKey]

    var allKeys: [Key] { object.keys.compactMap { Key(stringValue: $0) } }

    /// The old bridge's `convertFromSnakeCase` equivalence: the payload's
    /// own key wins, its snake_case spelling answers second.
    private func lookup(_ key: Key) -> ReplicaValue? {
        object[key.stringValue] ?? object[ReplicaValueCoding.snakeCased(key.stringValue)]
    }

    func contains(_ key: Key) -> Bool { lookup(key) != nil }

    private func require(_ key: Key) throws -> ReplicaValue {
        guard let value = lookup(key) else {
            throw DecodingError.keyNotFound(key, .init(
                codingPath: codingPath, debugDescription: "no value for \(key.stringValue)"
            ))
        }
        return value
    }

    func decodeNil(forKey key: Key) throws -> Bool { try require(key) == .null }

    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        try T(from: ValueDecoder(value: require(key), codingPath: codingPath + [key]))
    }

    func decode(_: Bool.Type, forKey key: Key) throws -> Bool {
        let raw = try require(key)
        return try unwrap(raw.bool, Bool.self, raw, codingPath + [key])
    }

    func decode(_: String.Type, forKey key: Key) throws -> String {
        let raw = try require(key)
        return try unwrap(raw.string, String.self, raw, codingPath + [key])
    }

    func decode(_: Double.Type, forKey key: Key) throws -> Double {
        let raw = try require(key)
        return try unwrap(raw.number, Double.self, raw, codingPath + [key])
    }

    func decode(_: Float.Type, forKey key: Key) throws -> Float {
        try finiteFloat(try require(key), codingPath + [key])
    }

    func decode(_: Int.Type, forKey key: Key) throws -> Int { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: Int8.Type, forKey key: Key) throws -> Int8 { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: Int16.Type, forKey key: Key) throws -> Int16 { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: Int32.Type, forKey key: Key) throws -> Int32 { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: Int64.Type, forKey key: Key) throws -> Int64 { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: UInt.Type, forKey key: Key) throws -> UInt { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: UInt8.Type, forKey key: Key) throws -> UInt8 { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: UInt16.Type, forKey key: Key) throws -> UInt16 { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: UInt32.Type, forKey key: Key) throws -> UInt32 { try exactInt(try require(key), codingPath + [key]) }
    func decode(_: UInt64.Type, forKey key: Key) throws -> UInt64 { try exactInt(try require(key), codingPath + [key]) }

    func nestedContainer<NestedKey: CodingKey>(
        keyedBy type: NestedKey.Type, forKey key: Key
    ) throws -> KeyedDecodingContainer<NestedKey> {
        try ValueDecoder(value: require(key), codingPath: codingPath + [key]).container(keyedBy: type)
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        try ValueDecoder(value: require(key), codingPath: codingPath + [key]).unkeyedContainer()
    }

    func superDecoder() throws -> Decoder {
        ValueDecoder(value: .object(object), codingPath: codingPath)
    }

    func superDecoder(forKey key: Key) throws -> Decoder {
        ValueDecoder(value: try require(key), codingPath: codingPath + [key])
    }
}

private struct UnkeyedContainer: UnkeyedDecodingContainer {
    let array: [ReplicaValue]
    let codingPath: [CodingKey]
    var currentIndex = 0

    var count: Int? { array.count }
    var isAtEnd: Bool { currentIndex >= array.count }

    private struct IndexKey: CodingKey {
        let intValue: Int?
        var stringValue: String { "Index \(intValue ?? -1)" }
        init(_ index: Int) { intValue = index }
        init?(stringValue _: String) { nil }
        init?(intValue: Int) { self.intValue = intValue }
    }

    private mutating func next() throws -> ReplicaValue {
        guard !isAtEnd else {
            throw DecodingError.valueNotFound(ReplicaValue.self, .init(
                codingPath: codingPath, debugDescription: "unkeyed container exhausted"
            ))
        }
        defer { currentIndex += 1 }
        return array[currentIndex]
    }

    mutating func decodeNil() throws -> Bool {
        guard !isAtEnd else { return false }
        if array[currentIndex] == .null {
            currentIndex += 1
            return true
        }
        return false
    }

    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
        let index = currentIndex
        return try T(from: ValueDecoder(value: try next(), codingPath: codingPath + [IndexKey(index)]))
    }

    mutating func decode(_: Bool.Type) throws -> Bool {
        let raw = try next()
        return try unwrap(raw.bool, Bool.self, raw, codingPath)
    }

    mutating func decode(_: String.Type) throws -> String {
        let raw = try next()
        return try unwrap(raw.string, String.self, raw, codingPath)
    }

    mutating func decode(_: Double.Type) throws -> Double {
        let raw = try next()
        return try unwrap(raw.number, Double.self, raw, codingPath)
    }

    mutating func decode(_: Float.Type) throws -> Float { try finiteFloat(try next(), codingPath) }
    mutating func decode(_: Int.Type) throws -> Int { try exactInt(try next(), codingPath) }
    mutating func decode(_: Int8.Type) throws -> Int8 { try exactInt(try next(), codingPath) }
    mutating func decode(_: Int16.Type) throws -> Int16 { try exactInt(try next(), codingPath) }
    mutating func decode(_: Int32.Type) throws -> Int32 { try exactInt(try next(), codingPath) }
    mutating func decode(_: Int64.Type) throws -> Int64 { try exactInt(try next(), codingPath) }
    mutating func decode(_: UInt.Type) throws -> UInt { try exactInt(try next(), codingPath) }
    mutating func decode(_: UInt8.Type) throws -> UInt8 { try exactInt(try next(), codingPath) }
    mutating func decode(_: UInt16.Type) throws -> UInt16 { try exactInt(try next(), codingPath) }
    mutating func decode(_: UInt32.Type) throws -> UInt32 { try exactInt(try next(), codingPath) }
    mutating func decode(_: UInt64.Type) throws -> UInt64 { try exactInt(try next(), codingPath) }

    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy type: NestedKey.Type
    ) throws -> KeyedDecodingContainer<NestedKey> {
        try ValueDecoder(value: try next(), codingPath: codingPath).container(keyedBy: type)
    }

    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        try ValueDecoder(value: try next(), codingPath: codingPath).unkeyedContainer()
    }

    mutating func superDecoder() throws -> Decoder {
        ValueDecoder(value: try next(), codingPath: codingPath)
    }
}

// MARK: - Encoder

/// Shared mutable landing slot: containers write their finished subtree up
/// into their parent through these boxes.
private final class ValueBox {
    var onChange: ((ReplicaValue?) -> Void)?
    var value: ReplicaValue? { didSet { onChange?(value) } }
}

private struct ValueEncoder: Encoder {
    let codingPath: [CodingKey]
    let box = ValueBox()
    var userInfo: [CodingUserInfoKey: Any] { [:] }

    func container<Key: CodingKey>(keyedBy _: Key.Type) -> KeyedEncodingContainer<Key> {
        let storage = ObjectStorage()
        box.value = nil
        storage.onChange = { [box] in box.value = .object($0) }
        return KeyedEncodingContainer(EncodingKeyedContainer(storage: storage, codingPath: codingPath))
    }

    func unkeyedContainer() -> UnkeyedEncodingContainer {
        let storage = ArrayStorage()
        storage.onChange = { [box] in box.value = .array($0) }
        return EncodingUnkeyedContainer(storage: storage, codingPath: codingPath)
    }

    func singleValueContainer() -> SingleValueEncodingContainer {
        SingleEncodingContainer(box: box, codingPath: codingPath)
    }
}

private final class ObjectStorage {
    var object: [String: ReplicaValue] = [:] { didSet { onChange?(object) } }
    var onChange: (([String: ReplicaValue]) -> Void)? {
        didSet { onChange?(object) }
    }
}

private final class ArrayStorage {
    var array: [ReplicaValue] = [] { didSet { onChange?(array) } }
    var onChange: (([ReplicaValue]) -> Void)? {
        didSet { onChange?(array) }
    }
}

private func encoded<T: Encodable>(_ value: T, path: [CodingKey]) throws -> ReplicaValue {
    let encoder = ValueEncoder(codingPath: path)
    try value.encode(to: encoder)
    return encoder.box.value ?? .null
}

private struct SingleEncodingContainer: SingleValueEncodingContainer {
    let box: ValueBox
    let codingPath: [CodingKey]

    mutating func encodeNil() { box.value = .null }
    mutating func encode(_ value: Bool) { box.value = .bool(value) }
    mutating func encode(_ value: String) { box.value = .string(value) }
    mutating func encode(_ value: Double) { box.value = .number(value) }
    mutating func encode(_ value: Float) { box.value = .number(Double(value)) }
    mutating func encode(_ value: Int) { box.value = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int8) { box.value = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int16) { box.value = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int32) { box.value = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int64) { box.value = .signedInteger(Int64(value)) }
    mutating func encode(_ value: UInt) throws { box.value = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt8) throws { box.value = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt16) throws { box.value = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt32) throws { box.value = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt64) throws { box.value = try replicaUnsigned(value, path: codingPath) }
    mutating func encode<T: Encodable>(_ value: T) throws {
        box.value = try encoded(value, path: codingPath)
    }
}

private struct EncodingKeyedContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
    let storage: ObjectStorage
    let codingPath: [CodingKey]

    mutating func encodeNil(forKey key: Key) { storage.object[key.stringValue] = .null }
    mutating func encode(_ value: Bool, forKey key: Key) { storage.object[key.stringValue] = .bool(value) }
    mutating func encode(_ value: String, forKey key: Key) { storage.object[key.stringValue] = .string(value) }
    mutating func encode(_ value: Double, forKey key: Key) { storage.object[key.stringValue] = .number(value) }
    mutating func encode(_ value: Float, forKey key: Key) { storage.object[key.stringValue] = .number(Double(value)) }
    mutating func encode(_ value: Int, forKey key: Key) { storage.object[key.stringValue] = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int8, forKey key: Key) { storage.object[key.stringValue] = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int16, forKey key: Key) { storage.object[key.stringValue] = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int32, forKey key: Key) { storage.object[key.stringValue] = .signedInteger(Int64(value)) }
    mutating func encode(_ value: Int64, forKey key: Key) { storage.object[key.stringValue] = .signedInteger(Int64(value)) }
    mutating func encode(_ value: UInt, forKey key: Key) throws { storage.object[key.stringValue] = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt8, forKey key: Key) throws { storage.object[key.stringValue] = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt16, forKey key: Key) throws { storage.object[key.stringValue] = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt32, forKey key: Key) throws { storage.object[key.stringValue] = try replicaUnsigned(value, path: codingPath) }
    mutating func encode(_ value: UInt64, forKey key: Key) throws { storage.object[key.stringValue] = try replicaUnsigned(value, path: codingPath) }

    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
        storage.object[key.stringValue] = try encoded(value, path: codingPath + [key])
    }

    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy _: NestedKey.Type, forKey key: Key
    ) -> KeyedEncodingContainer<NestedKey> {
        let nested = ObjectStorage()
        nested.onChange = { [storage] in storage.object[key.stringValue] = .object($0) }
        return KeyedEncodingContainer(EncodingKeyedContainer<NestedKey>(storage: nested, codingPath: codingPath + [key]))
    }

    mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        let nested = ArrayStorage()
        nested.onChange = { [storage] in storage.object[key.stringValue] = .array($0) }
        return EncodingUnkeyedContainer(storage: nested, codingPath: codingPath + [key])
    }

    mutating func superEncoder() -> Encoder { superEncoder(forKey: Key(stringValue: "super")!) }

    mutating func superEncoder(forKey key: Key) -> Encoder {
        let encoder = ValueEncoder(codingPath: codingPath + [key])
        encoder.box.onChange = { [storage] in storage.object[key.stringValue] = $0 ?? .null }
        return encoder
    }
}

private struct EncodingUnkeyedContainer: UnkeyedEncodingContainer {
    let storage: ArrayStorage
    let codingPath: [CodingKey]

    var count: Int { storage.array.count }

    mutating func encodeNil() { storage.array.append(.null) }
    mutating func encode(_ value: Bool) { storage.array.append(.bool(value)) }
    mutating func encode(_ value: String) { storage.array.append(.string(value)) }
    mutating func encode(_ value: Double) { storage.array.append(.number(value)) }
    mutating func encode(_ value: Float) { storage.array.append(.number(Double(value))) }
    mutating func encode(_ value: Int) { storage.array.append(.signedInteger(Int64(value))) }
    mutating func encode(_ value: Int8) { storage.array.append(.signedInteger(Int64(value))) }
    mutating func encode(_ value: Int16) { storage.array.append(.signedInteger(Int64(value))) }
    mutating func encode(_ value: Int32) { storage.array.append(.signedInteger(Int64(value))) }
    mutating func encode(_ value: Int64) { storage.array.append(.signedInteger(Int64(value))) }
    mutating func encode(_ value: UInt) throws { storage.array.append(try replicaUnsigned(value, path: codingPath)) }
    mutating func encode(_ value: UInt8) throws { storage.array.append(try replicaUnsigned(value, path: codingPath)) }
    mutating func encode(_ value: UInt16) throws { storage.array.append(try replicaUnsigned(value, path: codingPath)) }
    mutating func encode(_ value: UInt32) throws { storage.array.append(try replicaUnsigned(value, path: codingPath)) }
    mutating func encode(_ value: UInt64) throws { storage.array.append(try replicaUnsigned(value, path: codingPath)) }

    mutating func encode<T: Encodable>(_ value: T) throws {
        storage.array.append(try encoded(value, path: codingPath))
    }

    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy _: NestedKey.Type
    ) -> KeyedEncodingContainer<NestedKey> {
        let nested = ObjectStorage()
        let index = storage.array.count
        storage.array.append(.object([:]))
        nested.onChange = { [storage] in storage.array[index] = .object($0) }
        return KeyedEncodingContainer(EncodingKeyedContainer<NestedKey>(storage: nested, codingPath: codingPath))
    }

    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        let nested = ArrayStorage()
        let index = storage.array.count
        storage.array.append(.array([]))
        nested.onChange = { [storage] in storage.array[index] = .array($0) }
        return EncodingUnkeyedContainer(storage: nested, codingPath: codingPath)
    }

    mutating func superEncoder() -> Encoder {
        let encoder = ValueEncoder(codingPath: codingPath)
        let index = storage.array.count
        storage.array.append(.null)
        encoder.box.onChange = { [storage] in storage.array[index] = $0 ?? .null }
        return encoder
    }
}

private func replicaUnsigned<T: BinaryInteger>(_ value: T, path: [CodingKey]) throws -> ReplicaValue {
    guard let signed = Int64(exactly: value) else {
        throw EncodingError.invalidValue(value, .init(codingPath: path, debugDescription: "ReplicaMan integers must fit signed 64 bits"))
    }
    return .signedInteger(signed)
}
