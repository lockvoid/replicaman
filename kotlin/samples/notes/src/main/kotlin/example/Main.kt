package example

import example.generated.*
import io.replicaman.*
import kotlinx.coroutines.runBlocking
import java.nio.file.Files

private object Offline : ReplicaTransport {
    override suspend fun exchange(endpoint: ReplicaEndpoint, body: ByteArray): ByteArray =
        throw ReplicaError.Transport("offline example")
}

fun main() = runBlocking {
    val home = Files.createTempDirectory("replicaman-example-").toFile()
    val engine = ReplicaEngine(home = home, transport = Offline, schema = NotesReplica.schema, automaticallyPushWrites = false)
    try {
        engine.open(42)
        val replica = NotesReplica(engine)
        replica.write { tx ->
            tx.notes.create(Note("first-note", "Draft", 42))
            tx.notes.update("first-note") { it.copy(title = "Saved offline") }
        }
        // This pair receives one all-or-nothing server result.
        engine.writeAtomically { tx ->
            tx.notes.create(Note("group-a", "First member", 42))
            tx.notes.create(Note("group-b", "Second member", 42))
        }
        engine.close()
        engine.open(42)
        check(replica.notes.find("first-note")?.title == "Saved offline")
        check(engine.pendingOps().size == 4)
        println("PASS typed Kotlin save, atomic action and durable reopen")
    } finally {
        engine.close()
        check(home.deleteRecursively()) { "Cannot remove the example temporary directory" }
    }
}
