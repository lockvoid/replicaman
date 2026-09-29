import Foundation
import zlib

/// Gzip framing with explicit errors and a bounded decoded body.
public enum Gzip {
    public static func compress(_ data: Data) throws -> Data {
        guard data.count <= Int(uInt.max) else {
            throw ReplicaError.codec("gzip input exceeds its size limit")
        }

        var stream = z_stream()
        let initialized = deflateInit2_(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8,
                                        Z_DEFAULT_STRATEGY, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size))
        guard initialized == Z_OK else {
            throw ReplicaError.codec("gzip compression initialization failed: \(initialized)")
        }
        defer { deflateEnd(&stream) }

        let capacity = deflateBound(&stream, uLong(data.count))
        guard capacity <= uInt.max else {
            throw ReplicaError.codec("gzip output buffer exceeds its size limit")
        }
        var output = Data(count: Int(capacity))
        let status = data.withUnsafeBytes { input in
            output.withUnsafeMutableBytes { buffer in
                stream.next_in = UnsafeMutablePointer(mutating: input.bindMemory(to: Bytef.self).baseAddress)
                stream.avail_in = uInt(data.count)
                stream.next_out = buffer.bindMemory(to: Bytef.self).baseAddress
                stream.avail_out = uInt(buffer.count)
                return deflate(&stream, Z_FINISH)
            }
        }
        guard status == Z_STREAM_END else {
            throw ReplicaError.codec("gzip compression failed: \(status)")
        }
        output.count = Int(stream.total_out)
        return output
    }

    public static func decompress(_ data: Data, limit: Int = 32 * 1024 * 1024) throws -> Data {
        guard limit >= 0, data.count <= Int(uInt.max) else {
            throw ReplicaError.codec("invalid gzip size limit")
        }

        var stream = z_stream()
        let initialized = inflateInit2_(&stream, MAX_WBITS + 16, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size))
        guard initialized == Z_OK else {
            throw ReplicaError.codec("gzip decompression initialization failed: \(initialized)")
        }
        defer { inflateEnd(&stream) }

        var output = Data()
        var chunk = [Bytef](repeating: 0, count: 8192)
        let status: Int32 = try data.withUnsafeBytes { input in
            stream.next_in = UnsafeMutablePointer(mutating: input.bindMemory(to: Bytef.self).baseAddress)
            stream.avail_in = uInt(data.count)
            var result = Z_OK
            repeat {
                let written = chunk.withUnsafeMutableBufferPointer { buffer in
                    stream.next_out = buffer.baseAddress
                    stream.avail_out = uInt(buffer.count)
                    result = inflate(&stream, Z_NO_FLUSH)
                    return buffer.count - Int(stream.avail_out)
                }
                guard written <= limit - output.count else {
                    throw ReplicaError.codec("gzip output exceeds its size limit")
                }
                output.append(contentsOf: chunk[..<written])
            } while result == Z_OK
            return result
        }
        guard status == Z_STREAM_END else {
            throw ReplicaError.codec("gzip decompression failed: \(status)")
        }
        return output
    }
}
