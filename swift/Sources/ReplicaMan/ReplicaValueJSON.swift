import yyjson

/// Strict stored-row decoding. A damaged row must fail the read before a
/// caller can author a patch from an invented empty baseline.
enum ReplicaValueJSON {
    static func decodeObject(_ json: String) throws -> [String: ReplicaValue] {
        var json = json
        return try json.withUTF8 { utf8 -> [String: ReplicaValue] in
            guard let base = utf8.baseAddress, !utf8.isEmpty else { throw ReplicaError.storage("Invalid snapshot JSON object") }
            let text = UnsafeRawPointer(base).assumingMemoryBound(to: CChar.self)
            guard let doc = yyjson_read(text, utf8.count, YYJSON_READ_NOFLAG) else { throw ReplicaError.storage("Invalid snapshot JSON object") }
            defer { yyjson_doc_free(doc) }
            guard let root = yyjson_doc_get_root(doc), yyjson_is_obj(root) else { throw ReplicaError.storage("Invalid snapshot JSON object") }
            return try object(root, depth: 0)
        }
    }

    private static func object(_ obj: UnsafeMutablePointer<yyjson_val>, depth: Int) throws -> [String: ReplicaValue] {
        guard depth < 128 else { throw ReplicaError.storage("Snapshot JSON nesting exceeds 128") }
        var out: [String: ReplicaValue] = [:]
        out.reserveCapacity(yyjson_obj_size(obj))
        var iterator = yyjson_obj_iter()
        yyjson_obj_iter_init(obj, &iterator)
        while let key = yyjson_obj_iter_next(&iterator) {
            let name = string(key)
            let decoded = try value(yyjson_obj_iter_get_val(key), depth: depth + 1)
            if let first = out.updateValue(decoded, forKey: name) {
                out[name] = first
            }
        }
        return out
    }

    private static func array(_ arr: UnsafeMutablePointer<yyjson_val>, depth: Int) throws -> [ReplicaValue] {
        guard depth < 128 else { throw ReplicaError.storage("Snapshot JSON nesting exceeds 128") }
        var out: [ReplicaValue] = []
        out.reserveCapacity(yyjson_arr_size(arr))
        var iterator = yyjson_arr_iter()
        yyjson_arr_iter_init(arr, &iterator)
        while let item = yyjson_arr_iter_next(&iterator) {
            out.append(try value(item, depth: depth + 1))
        }
        return out
    }

    private static func value(_ val: UnsafeMutablePointer<yyjson_val>?, depth: Int) throws -> ReplicaValue {
        guard let val else { return .null }
        if yyjson_is_str(val) { return .string(string(val)) }
        if yyjson_is_sint(val) { return .signedInteger(yyjson_get_sint(val)) }
        if yyjson_is_uint(val), let integer = Int64(exactly: yyjson_get_uint(val)) { return .signedInteger(integer) }
        if yyjson_is_num(val) { return .number(yyjson_get_num(val)) }
        if yyjson_is_bool(val) { return .bool(yyjson_get_bool(val)) }
        if yyjson_is_obj(val) { return .object(try object(val, depth: depth)) }
        if yyjson_is_arr(val) { return .array(try array(val, depth: depth)) }
        return .null
    }

    private static func string(_ val: UnsafeMutablePointer<yyjson_val>) -> String {
        guard let bytes = yyjson_get_str(val) else { return "" }
        return String(decoding: UnsafeRawBufferPointer(start: bytes, count: yyjson_get_len(val)), as: UTF8.self)
    }
}
