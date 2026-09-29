import Foundation

/// One authenticated exchange. SQLite holds the dataset and every frozen
/// byte, so a connection keeps no protocol state of its own.
struct ReplicaConnection: Sendable {
    let transport: any ReplicaTransport
    let schema: ReplicaSchema

    /// `dataset` is nil only before the store's first pull, which is the one
    /// request the server admits without it.
    private func send(
        _ endpoint: ReplicaEndpoint, dataset: String?, fields: [String: ReplicaValue]
    ) async throws -> Data {
        var request = fields
        request["protocol"] = .integer(Int64(ReplicaProtocol.version))
        request["namespace"] = .string(schema.namespace)
        request["schema"] = .integer(Int64(schema.version))
        request["dataset"] = dataset.map(ReplicaValue.string) ?? .null
        let bytes = try await transport.exchange(endpoint, body: ReplicaJSON.encoder().encode(ReplicaValue.object(request)))
        try ReplicaJSON.decoder().decode(ReplicaProtocolHeader.self, from: bytes)
            .validate(schema: schema, dataset: dataset)
        return bytes
    }

    /// The page and the exact bytes the server sent, which a round stages.
    func pull(shard: String, cursor: String?, limit: Int, dataset: String?) async throws -> (ReplicaPullPage, Data) {
        let bytes = try await send(.pull, dataset: dataset, fields: [
            "shard": .string(shard), "cursor": cursor.map(ReplicaValue.string) ?? .null, "limit": .integer(Int64(limit)),
        ])
        let page = try ReplicaPullPage.decode(bytes)
        guard page.shard == shard, page.reset == (cursor == nil) else {
            throw ReplicaProtocol.invalidResponse("Pull answered another shard or round")
        }
        for item in page.frames {
            guard let spec = schema.spec(item.frame.stream) else {
                throw ReplicaError.protocolFailure(code: "UpgradeRequired", message: "Pulled frame names an undeclared stream")
            }
            guard spec.shard == shard else { throw ReplicaProtocol.invalidResponse("Pulled frame belongs to another shard") }
        }
        return (page, bytes)
    }

    /// Verdicts are validated against the operations sent before anything
    /// local changes: a missing, duplicate or foreign id acknowledges nothing.
    func push(_ submissions: [ReplicaSubmission], dataset: String) async throws -> [ReplicaVerdict] {
        let operations = try submissions.flatMap { try ReplicaJSON.decoder().decode([ReplicaValue].self, from: $0.content) }
        let bytes = try await send(.push, dataset: dataset, fields: ["ops": .array(operations)])
        let verdicts = try ReplicaJSON.decoder().decode(ReplicaPushAnswer.self, from: bytes).verdicts
        guard verdicts.map(\.id) == submissions.flatMap(\.ids) else {
            throw ReplicaProtocol.invalidResponse("Push verdicts do not answer the operations sent")
        }
        var offset = 0
        for submission in submissions {
            let answered = verdicts[offset..<offset + submission.ids.count]
            offset += submission.ids.count
            guard Set(answered.map(\.outcome)).count == 1 else {
                throw ReplicaProtocol.invalidResponse("One submission received different outcomes")
            }
        }
        return verdicts
    }

    func verify(shard: String, cursor: String, dataset: String) async throws -> ReplicaVerifyAnswer {
        let bytes = try await send(.verify, dataset: dataset, fields: ["shard": .string(shard), "cursor": .string(cursor)])
        let answer = try ReplicaJSON.decoder().decode(ReplicaVerifyAnswer.self, from: bytes)
        guard answer.shard == shard, answer.cursor == cursor, ReplicaProtocol.isDigest(answer.digest) else {
            throw ReplicaProtocol.invalidResponse("Integrity answer names another shard or cursor")
        }
        return answer
    }
}
