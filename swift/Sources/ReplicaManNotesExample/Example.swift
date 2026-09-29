import Foundation
import ReplicaMan

struct Offline: ReplicaTransport {
    func exchange(_ endpoint: ReplicaEndpoint, body: Data) async throws -> Data {
        throw ReplicaError.transport("offline example")
    }
}

@main struct Example {
    static func main() async throws {
        let home = FileManager.default.temporaryDirectory.appendingPathComponent("replicaman-example-" + UUID().uuidString)
        let engine = ReplicaEngine(home: home, transport: Offline(), schema: NotesReplica.schema, automaticallyPushWrites: false)
        try await engine.open(owner: 42)
        let replica = NotesReplica(engine: engine)
        try await replica.write { tx in
            try tx.notes.create(Note(id: "first-note", title: "Draft", userId: 42))
            try tx.notes.update("first-note") { $0.title = "Saved offline" }
        }
        // This pair must be accepted or refused together by the server.
        try engine.writeAtomically { tx in
            try tx.notes.create(Note(id: "group-a", title: "First member", userId: 42))
            try tx.notes.create(Note(id: "group-b", title: "Second member", userId: 42))
        }
        try await engine.close()
        try await engine.open(owner: 42)
        guard try replica.notes.find("first-note")?.title == "Saved offline" else { fatalError("saved row did not reopen") }
        guard try await engine.pendingOps().count == 4 else { fatalError("outbound work did not reopen") }
        try await engine.close()
        try FileManager.default.removeItem(at: home)
        print("PASS typed Swift save, atomic action and durable reopen")
    }
}
