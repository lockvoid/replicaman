import Foundation
import GRDB
import Loro
import ReplicaMan
import ReplicaManLoro

struct Item: ReplicaWritableRowModel {
    static let streamName = "items"
    var id: String
    var typeName: String?
    var data: [String: ReplicaValue]
    init(id: String, type: String?, data: [String: ReplicaValue]) {
        self.id = id; self.typeName = type; self.data = data
    }
    func encode() -> [String: ReplicaValue] { data }
}

struct HoldingRank: SyncGate {
    let id = "conformance-hold"
    let stream: String? = "items"
    func judge(_ change: SyncChange) -> SyncVerdict {
        change.local["rank"]?.string?.hasPrefix("hold:") == true ? .gate("test hold") : .push
    }
}

// Line-oriented process boundary: tests drive the shipped APIs and inspect a
// real reopened database. No test transport or substitute CRDT lives here.
@main struct Worker {
    static func main() async throws {
        let args = CommandLine.arguments
        let owner = Int(args[3])!
        let transport = HTTPReplicaTransport(baseURL: URL(string: args[1])!, token: { nil }, headers: { ["X-User-Id": String(owner)] })
        let schema = ReplicaSchema(streams: [
            ReplicaStreamSpec(name: "items", lane: .row),
            ReplicaStreamSpec(name: "boards", lane: .document, codec: LoroReplicaCodec.codecName,
                reflections: [ReplicaReflection(field: "name", path: ["meta", "name"])]),
        ], namespace: "replicaman-test")
        let engine = ReplicaEngine(home: URL(fileURLWithPath: args[2]), transport: transport, schema: schema,
                                   codecs: [LoroReplicaCodec()], batchLimit: Int(ProcessInfo.processInfo.environment["REPLICAMAN_PAGE_LIMIT"] ?? "500")!, automaticallyPushWrites: false, syncGates: [HoldingRank()])
        try await engine.open(owner: owner)
        emit(["ready": .bool(true), "language": .string("swift")])
        while let line = readLine() {
            do {
                let request = try ReplicaJSON.decoder().decode([String: ReplicaValue].self, from: Data(line.utf8))
                let command = request["command"]?.string ?? ""
                let stream = request["stream"]?.string ?? "items"
                let id = request["id"]?.string ?? "b1"
                var answer: [String: ReplicaValue] = ["ok": .bool(true)]
                switch command {
                case "save":
                    let fields = request["data"]?.object ?? [:]
                    try await engine.write { transaction in
                        if try transaction.find(Item.self, id) == nil {
                            try transaction.create(Item(id: id, type: request["type"]?.string, data: fields))
                        } else {
                            _ = try transaction.update(Item.self, id) { $0.data.merge(fields) { _, value in value } }
                        }
                    }
                case "load":
                    let start = Int(request["start"]!.number!)
                    let count = Int(request["count"]!.number!)
                    let body = String(repeating: "x", count: 256)
                    try await engine.write { tx in
                        for index in start..<(start + count) {
                            try tx.create(Item(id: "load-\(index)", type: "TextItem", data: [
                                "boardId": .string("b1"), "rank": .string(String(index)),
                                "body": .string(body),
                            ]))
                        }
                    }
                case "statistics":
                    let store = await engine.store!
                    let status = try store.syncStatus()
                    answer["queued"] = .number(Double(status.queuedOperations))
                    answer["journalBytes"] = .number(Double(status.journalBytes))
                case "atomic":
                    guard let members = request["members"]?.items else {
                        throw ReplicaError.storage("Atomic worker command needs members")
                    }
                    try engine.writeAtomically { tx in
                        for member in members {
                            guard let id = member["id"]?.string, let fields = member["data"]?.object else {
                                throw ReplicaError.storage("Invalid atomic member")
                            }
                            try tx.create(Item(id: id, type: member["type"]?.string, data: fields))
                        }
                    }
                case "delete": try await engine.deleteRow(stream: stream, id: id)
                case "drain": _ = try await engine.drain()
                case "pull": _ = try await engine.pullUntilCaughtUp()
                case "pull_page": answer["applied"] = .integer(Int64(try await engine.pullOnce()))
                case "reset": try await engine.resetCursors()
                case "verify": try await engine.verifyIntegrity()
                case "edit", "rich":
                    guard let fold = try engine.docFold(stream: "boards", id: id), let peer = try engine.docPeer(stream: "boards", id: id) else {
                        throw ReplicaError.unknownDocument(stream: "boards", id: id)
                    }
                    let doc = LoroDoc()
                    doc.setRecordTimestamp(record: false)
                    try doc.setPeerId(peer: peer)
                    _ = try doc.import(bytes: fold)
                    let version = doc.oplogVv()
                    if command == "rich" {
                        try doc.getText(id: "body").insert(pos: 0, s: request["value"]!.string!)
                        try doc.getList(id: "labels").insert(pos: 0, v: request["value"]!.string!)
                    } else {
                        try doc.getMap(id: "meta").insert(key: request["key"]!.string!, v: request["value"]!.string!)
                    }
                    doc.commit()
                    try await engine.recordDocDelta(stream: "boards", id: id, payload: doc.export(mode: .updates(from: version)))
                case "rebuild":
                    let doc = LoroDoc()
                    let peer = try engine.docPeer(stream: "boards", id: id)!
                    try doc.setPeerId(peer: peer &+ 100)
                    try doc.getMap(id: "meta").insert(key: request["key"]!.string!, v: "recovered")
                    doc.commit()
                    try await engine.rebuildDocument(stream: "boards", id: id, fold: doc.export(mode: .snapshot), peer: peer &+ 100)
                case "inspect":
                    answer["cursor"] = try await engine.currentCursor().map(ReplicaValue.string) ?? .null
                    let rows = try await engine.store!.reader.read { db in
                        try Row.fetchAll(db, sql: "SELECT stream, row_id, data FROM snapshots ORDER BY stream, row_id").map { row -> ReplicaValue in
                            let data: String = row["data"]
                            return .object(["stream": .string(row["stream"]), "id": .string(row["row_id"]),
                                "data": .object(try ReplicaJSON.decoder().decode([String: ReplicaValue].self, from: Data(data.utf8)))])
                        }
                    }
                    answer["rows"] = .array(rows)
                    if let fold = try engine.docFold(stream: "boards", id: id) {
                        let doc = LoroDoc()
                        _ = try doc.import(bytes: fold)
                        if case .map(let root) = doc.getDeepValue() {
                            if case .string(let value)? = root["body"] { answer["body"] = .string(value) }
                            if case .list(let values)? = root["labels"] { answer["labels"] = .array(values.compactMap { if case .string(let value) = $0 { .string(value) } else { nil } }) }
                        }
                        if case .map(let root) = doc.getDeepValue(), case .map(let meta)? = root["meta"] {
                            answer["document"] = .object(meta.compactMapValues { if case .string(let value) = $0 { .string(value) } else { nil } })
                        }
                    }
                case "close": try await engine.close()
                default: throw ReplicaError.transport("unknown worker command")
                }
                if command != "load" && command != "statistics" {
                    answer["pending"] = .number(Double(try await engine.pendingOps().count))
                }
                emit(answer)
                if command == "close" { break }
            } catch {
                emit(["ok": .bool(false), "error": .string(String(describing: error))])
            }
        }
    }

    static func emit(_ value: [String: ReplicaValue]) {
        let bytes = try! ReplicaJSON.encoder().encode(value)
        FileHandle.standardOutput.write(bytes + Data([10]))
    }
}
