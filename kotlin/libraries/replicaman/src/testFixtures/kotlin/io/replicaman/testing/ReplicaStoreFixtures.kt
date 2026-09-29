package io.replicaman.testing

import androidx.sqlite.SQLiteConnection
import io.replicaman.*
import java.util.UUID

/**
 * Test-only access for fixture seeding and SQLite fault injection. It runs under
 * the real store transaction/notification boundary. Production artifacts do not
 * contain this entry point; test consumers explicitly depend on testFixtures.
 */
public fun <T> ReplicaStateStore.fixtureWrite(body: (SQLiteConnection) -> T): T = write(body)

/** A store that has already synchronized with `dataset`: its next push needs no first pull. */
public fun ReplicaStateStore.fixtureSynchronized(dataset: String = ProtocolFixture.DATASET): Unit =
    write { db -> adoptDataset(db, dataset) }

/** An established server row, without local intent or an invented command reply. */
public fun ReplicaStateStore.fixtureSeedRow(
    schema: ReplicaSchema, stream: String, id: String,
    data: Map<String, ReplicaValue>, type: String? = null,
): Unit = write { db ->
    check(incarnation(db, stream, id) == null) { "Fixture address already exists: $stream/$id" }
    val shard = schema.spec(stream)?.shard ?: "user"
    val op = identify(
        db, ReplicaOp(id = UUID.randomUUID().toString(), verb = ReplicaOp.Verb.ROW_CREATE,
            stream = stream, rowId = id, type = type, data = data),
        schema, birth = true,
    )
    upsertSnapshot(db, stream, id, shard, type, data)
    db.exec("""
        INSERT INTO base (stream, row_id, shard, incarnation, revision, type, data)
        VALUES (?, ?, ?, ?, 1, ?, ?)
        """.trimIndent(),
        listOf(stream, id, shard, requireNotNull(op.incarnation), type,
            ReplicaJSON.encodeToString(ReplicaValue.Obj(data))))
}

/**
 * The server's next revision of an established row, published the way a pull
 * round publishes it: the base moves and the row materializes again over it,
 * local intent rebased. For suites whose transport was fixed before they
 * started, such as an Android device process.
 */
public fun ReplicaEngine.fixtureReviseRow(
    stream: String, id: String, data: Map<String, ReplicaValue>, type: String? = null,
) {
    val store = checkNotNull(store) { "No owner is open" }
    val shard = schema.spec(stream)?.shard ?: "user"
    val publication = ReplicaPublication()
    liveDocuments.publishing {
        store.write { db ->
            val base = checkNotNull(store.baseRow(db, stream, id)) { "Fixture address is not established: $stream/$id" }
            store.saveBase(db, stream, id, shard, base.copy(revision = base.revision + 1, type = type, data = data))
            materializeBase(db, stream, id, shard, store, publication)
        }
        publication.deliver(liveDocuments)
    }
}

/** Capture a real recovery branch without manufacturing a remote checkpoint. */
public fun ReplicaStateStore.fixtureArchiveEntity(stream: String, id: String, reason: String): Unit =
    write { db -> archiveEntity(db, stream, id, reason) }

/**
 * Seed the accepted-but-not-checkpointed stage from a real frozen row submission.
 * This tests application readers, not wire acknowledgement; native HTTP tests own that contract.
 */
public fun ReplicaStateStore.fixtureAcceptOnlyRow(stream: String, id: String): Unit = write { db ->
    val submission = frozenSubmissions(db).single()
    val entry = submission.entries.single()
    val op = entry.op()
    check(op.stream == stream && op.rowId == id && op.verb != ReplicaOp.Verb.DOC_DELTA)
    accept(db, entry.id)
    finishSubmission(db, submission.sequence)
}
