import Foundation
import XCTest
@testable import ReplicaMan

/// The HTTP transport's reading of a refusal: the protocol's own
/// `{error, message}` is a protocol failure; any other refusal — the host's
/// gate in front of the endpoint, a proxy — keeps its status and body.
final class HTTPTransportTests: XCTestCase {
    private func transport(answering status: Int, body: String) -> HTTPReplicaTransport {
        CannedResponse.answer = (status, Data(body.utf8))
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [CannedResponse.self]
        return HTTPReplicaTransport(baseURL: URL(string: "https://replica.invalid/replica")!,
                                    session: URLSession(configuration: configuration), token: { "token" })
    }

    func testAProtocolRefusalNamesItsCode() async throws {
        let refusing = transport(answering: 409, body: #"{"error":"DatasetChanged","message":"restored"}"#)
        do {
            _ = try await refusing.exchange(.pull, body: Data("{}".utf8))
            XCTFail("a refusal must throw")
        } catch let error as ReplicaError {
            XCTAssertEqual(error, .protocolFailure(code: "DatasetChanged", message: "HTTP 409: restored"))
        }
    }

    func testAHostRefusalKeepsItsStatusAndBody() async throws {
        let body = #"{"code":"client_update_required","message":"Update to keep working."}"#
        let refusing = transport(answering: 426, body: body)
        do {
            _ = try await refusing.exchange(.push, body: Data("{}".utf8))
            XCTFail("a refusal must throw")
        } catch let error as ReplicaError {
            XCTAssertEqual(error, .transport("HTTP 426: \(body)"))
        }
    }
}

private final class CannedResponse: URLProtocol, @unchecked Sendable {
    nonisolated(unsafe) static var answer: (status: Int, body: Data) = (200, Data())

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let (status, body) = Self.answer
        let response = HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: "HTTP/1.1",
                                       headerFields: ["Content-Type": "application/json"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}
