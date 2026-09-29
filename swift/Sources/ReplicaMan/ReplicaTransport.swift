import Foundation

/// Authentication and HTTP belong here; durable synchronization belongs to the
/// engine. Custom transports preserve request bytes and propagate cancellation.
public protocol ReplicaTransport: Sendable {
    func exchange(_ endpoint: ReplicaEndpoint, body: Data) async throws -> Data
}

public struct HTTPReplicaTransport: ReplicaTransport {
    private let retryDelay = ReplicaRetryDelay()
    private let baseURL: URL
    private let session: URLSession
    private let token: @Sendable () -> String?
    private let headers: @Sendable () -> [String: String]

    public init(
        baseURL: URL,
        session: URLSession = .shared,
        token: @escaping @Sendable () -> String?,
        headers: @escaping @Sendable () -> [String: String] = { [:] }
    ) {
        self.baseURL = baseURL
        self.session = session
        self.token = token
        self.headers = headers
    }

    public func exchange(_ endpoint: ReplicaEndpoint, body: Data) async throws -> Data {
        try await retryDelay.wait()
        var request = URLRequest(url: baseURL.appendingPathComponent(endpoint.rawValue))
        request.httpMethod = "POST"
        request.timeoutInterval = 60
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("gzip", forHTTPHeaderField: "Content-Encoding")
        request.httpBody = try Gzip.compress(body)

        if let token = token() {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        for (field, value) in headers() {
            request.setValue(value, forHTTPHeaderField: field)
        }

        let (bytes, response) = try await session.bytes(for: request)
        defer { bytes.task.cancel() }

        guard let http = response as? HTTPURLResponse else {
            throw ReplicaError.transport("Replica response was not HTTP")
        }

        if http.statusCode == 429 || http.statusCode >= 500 {
            await retryDelay.record(http.value(forHTTPHeaderField: "Retry-After"))
        }

        var received = Data()
        let limit = http.statusCode == 200 ? ReplicaProtocol.responseBytes : 4096

        for try await byte in bytes {
            guard received.count < limit else {
                throw ReplicaError.transport("Replica response exceeded the size limit")
            }
            received.append(byte)
        }

        guard http.statusCode == 200 else {
            try throwFailure(status: http.statusCode, data: received)
        }

        return received
    }

    private func throwFailure(status: Int, data: Data) throws -> Never {
        let detail = String(decoding: data.prefix(512), as: UTF8.self)
        if status == 429 || status >= 500 {
            throw ReplicaError.transport("HTTP \(status): \(detail)")
        }

        struct Failure: Decodable {
            let error: String
            let message: String?
        }

        // A refusal the protocol did not write — the host's gate in front of
        // the endpoint, a proxy — keeps its status and body for the host.
        let failure: Failure
        do {
            failure = try ReplicaJSON.decoder().decode(Failure.self, from: data)
        } catch {
            throw ReplicaError.transport("HTTP \(status): \(detail)")
        }

        throw ReplicaError.protocolFailure(
            code: failure.error,
            message: "HTTP \(status): \(failure.message ?? failure.error)"
        )
    }
}
