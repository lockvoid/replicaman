package io.replicaman

import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubCodec
import io.replicaman.support.StubTransport
import io.replicaman.support.TestNote
import io.replicaman.support.allSnapshots
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import io.replicaman.testing.ProtocolFixture
import io.replicaman.testing.ReplicaPullResponse
import io.replicaman.testing.fixtureWrite
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.io.File
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Protocol 2 pushes: frozen operations carry client UUIDs, only an atomic action
 * shares a group, an answer is validated whole, and a failed request is retried
 * with exactly the same bytes.
 */
class PushProtocolTests : ReplicaTestCase() {
    private fun title(value: String) = mapOf("title" to ReplicaValue.Str(value))

    private suspend fun pushes(transport: StubTransport): List<List<ReplicaValue>> =
        transport.protocolFixture.requests(ReplicaEndpoint.PUSH).map { requireNotNull(it["ops"]?.items) }

    /** Rewrites the server's push answer the way a broken server or proxy would. */
    private class VerdictFault(private val wire: StubTransport) : ReplicaTransport {
        @Volatile var fault: String? = null

        override suspend fun exchange(endpoint: ReplicaEndpoint, body: ByteArray): ByteArray {
            val answer = wire.exchange(endpoint, body)
            val kind = fault ?: return answer
            if (endpoint != ReplicaEndpoint.PUSH) return answer
            val fields = (ReplicaProtocol.decode(answer) as ReplicaValue.Obj).fields
            val verdicts = requireNotNull(fields.getValue("verdicts").items)
            val corrupted = when (kind) {
                "missing" -> verdicts.dropLast(1)
                "duplicate" -> listOf(verdicts.first()) + verdicts
                "foreign" -> verdicts.dropLast(1) + ReplicaValue.Obj((verdicts.last() as ReplicaValue.Obj).fields +
                    ("id" to ReplicaValue.Str(UUID.randomUUID().toString())))
                "mixed" -> verdicts.dropLast(1) + ReplicaValue.Obj((verdicts.last() as ReplicaValue.Obj).fields +
                    ("outcome" to ReplicaValue.Str("rejected")) + ("reason" to ReplicaValue.Str("injected")))
                else -> error("unknown verdict fault $kind")
            }
            return ReplicaJSON.encodeToBytes(ReplicaValue.Obj(fields + ("verdicts" to ReplicaValue.Arr(corrupted))))
        }
    }

    /** KILL: send the journal entry ids, or give a single operation a group. */
    @Test fun everyOperationCarriesAFreshUuidAndOnlyAnAtomicActionAGroup() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        engine.saveRow("notes", "a", null, title("a"))
        engine.writeAtomically { tx ->
            tx.create(TestNote, TestNote("b", "b"))
            tx.create(TestNote, TestNote("c", "c"))
        }
        engine.writeAtomically { tx -> tx.create(TestNote, TestNote("d", "d")) }
        val journal = store.peekPending().map { it.id }

        engine.drain()

        val requests = transport.protocolFixture.requests(ReplicaEndpoint.PUSH)
        for (request in requests) {
            assertEquals(setOf("protocol", "namespace", "schema", "dataset", "ops"), (request as ReplicaValue.Obj).fields.keys)
            assertEquals(ReplicaValue.Str(ProtocolFixture.DATASET), request["dataset"])
        }
        val sent = pushes(transport).flatten().map(ReplicaOp::fromValue)
        assertEquals(listOf("b", "c", "d", "a"), sent.map { it.rowId }, "frozen actions leave first, oldest first")
        assertTrue(sent.all { UUID.fromString(it.id).version() == 7 }, "operation ids are UUIDv7: ${sent.map { it.id }}")
        assertEquals(4, sent.map { it.id }.toSet().size)
        assertTrue(sent.none { it.id in journal }, "journal entry ids never reach the wire")
        val group = assertNotNull(sent[0].group)
        assertEquals(7, UUID.fromString(group).version())
        assertEquals(group, sent[1].group)
        assertNull(sent[2].group, "a one-operation action is an ordinary submission")
        assertNull(sent[3].group)
        assertTrue(store.peekPending().isEmpty())
    }

    /** KILL: mint fresh ids on retry — the server executes the committed operation twice. */
    @Test fun aLostReplyIsRetriedWithTheSameBytesAndAnsweredFromTheStoredVerdict() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        engine.saveRow("notes", "n1", null, title("first"))
        transport.protocolFixture.loseReplies()

        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        assertEquals(1, store.peekPending().size, "a lost answer acknowledges nothing")
        engine.saveRow("notes", "n1", null, title("second"))
        engine.drain()

        val requests = pushes(transport)
        assertEquals(3, requests.size)
        assertEquals(requests[0], requests[1], "the retry sends the frozen operation, not the later edit")
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_PATCH),
            transport.pushedBatches().flatten().map { it.verb }, "the stored verdict answered the retry; nothing ran twice")
        assertTrue(store.peekPending().isEmpty())
        assertEquals(ReplicaValue.Str("second"), store.allSnapshots().single().data["title"])
    }

    /** KILL: supersede the frozen delta — its retry carries bytes the server committed under another digest. */
    @Test fun aFrozenDeltaIsNeverSupersededTheNextEditOwesItsOwn() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
        engine.drain()
        engine.recordDocDelta("boards", "b1", "+a".toByteArray())
        transport.protocolFixture.loseReplies()
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        val frozen = store.peekPending().single()

        engine.recordDocDelta("boards", "b1", "+b".toByteArray())

        val pending = store.peekPending()
        assertEquals(frozen, pending.first(), "the frozen delta keeps its bytes")
        assertEquals(listOf(true, false), pending.map { it.sent })
        assertEquals("+a+b", pending.last().op().payload?.decodeToString())
        engine.drain()
        val requests = pushes(transport)
        assertEquals(4, requests.size)
        assertEquals(requests[1], requests[2], "the retry sends the frozen delta")
        assertEquals("+a+b", ReplicaOp.fromValue(requests[3].single()).payload?.decodeToString())
        assertTrue(store.peekPending().isEmpty())
    }

    /** KILL: apply the verdicts that do match — a partial answer consumes part of the queue or of an action. */
    @Test fun anAnswerThatDoesNotMatchTheOperationsSentAcknowledgesNothing() = runBlocking<Unit> {
        for (fault in listOf("missing", "duplicate", "foreign", "mixed")) {
            val store = Fixture.store()
            val wire = StubTransport()
            val transport = VerdictFault(wire)
            val engine = ReplicaEngine(store = store, owner = Fixture.OWNER, transport = transport, schema = Fixture.schema(),
                codecs = listOf(StubCodec()), automaticallyPushWrites = false)
            engine.writeAtomically { tx ->
                tx.create(TestNote, TestNote("a", "a"))
                tx.create(TestNote, TestNote("b", "b"))
            }
            transport.fault = fault

            val error = assertFailsWith<ReplicaError.Protocol>(fault) { engine.drain() }
            assertEquals("InvalidResponse", error.code, fault)
            assertEquals(2, store.peekPending().size, fault)
            assertEquals(1, store.syncStatus().submittedGroups, fault)
            assertEquals(2, store.allSnapshots().size, fault)

            transport.fault = null
            engine.drain()
            assertTrue(store.peekPending().isEmpty(), fault)
            assertEquals(1, wire.pushedBatches().size, "$fault: the retry was answered from the stored verdicts")
        }
    }

    /** KILL: catch `MutationChanged` in the drain — a client bug hides behind a silent retry loop. */
    @Test fun theSameOperationIdWithOtherBytesFailsWithMutationChanged() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        engine.saveRow("notes", "n1", null, title("first"))
        transport.protocolFixture.loseReplies()
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        store.fixtureWrite { db ->
            db.exec("UPDATE submissions SET content = CAST(replace(CAST(content AS TEXT), '\"first\"', '\"forged\"') AS BLOB)")
            db.exec("UPDATE intents SET payload = replace(payload, '\"first\"', '\"forged\"')")
        }

        val error = assertFailsWith<ReplicaError.Protocol> { engine.drain() }

        assertEquals("MutationChanged", error.code)
        assertEquals(ReplicaFailureKind.RECOVERY_REQUIRED, ReplicaFailure("drain", error.toString(), Instant.now(), error).kind)
        assertEquals(1, store.peekPending().size)
        assertEquals(1, store.syncStatus().submittedGroups)
    }

    /** KILL: fill a request past 100 operations, or split an action across two requests. */
    @Test fun aRequestCarriesAtMostOneHundredOperationsAndNeverSplitsAnAction() = runBlocking<Unit> {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)
        repeat(2) { action ->
            engine.writeAtomically { tx -> repeat(60) { tx.create(TestNote, TestNote("n$action-$it", "x")) } }
        }

        engine.drain()

        assertEquals(listOf(60, 60), pushes(transport).map { it.size })
        assertTrue(store.peekPending().isEmpty())
    }

    /** KILL: push with `dataset: null` — the server refuses every write of a store that has not pulled yet. */
    @Test fun aStoreThatNeverSynchronizedLearnsItsDatasetFromOnePullBeforeItPushes() = runBlocking<Unit> {
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(Fixture.directory(), transport)
        engine.open(Fixture.OWNER)
        engine.saveRow("notes", "n1", null, title("offline"))

        engine.drain()

        val events = transport.events()
        assertEquals(StubTransport.Event.Pull("user", null), events.first())
        assertTrue(events.last() is StubTransport.Event.Push, "the push follows the first pull: $events")
        assertEquals(ReplicaValue.Null, transport.protocolFixture.requests(ReplicaEndpoint.PULL).single()["dataset"])
        assertEquals(ReplicaValue.Str(ProtocolFixture.DATASET), transport.protocolFixture.requests(ReplicaEndpoint.PUSH).single()["dataset"])
        assertTrue(engine.pendingOps().isEmpty())
        assertEquals("0:", engine.currentCursor())
    }

    /** KILL: learn the dataset by walking the whole round — the first push waits for every page of the shard. */
    @Test fun learningTheDatasetTakesOnePageAndStagesItWhenMoreIsWaiting() = runBlocking<Unit> {
        val transport = StubTransport()
        val engine = Fixture.unopenedEngine(Fixture.directory(), transport)
        engine.open(Fixture.OWNER)
        val store = requireNotNull(engine.store)
        transport.queuePull("user", ReplicaPullResponse(listOf(Fixture.note("n1", "server")), "c1", more = true))
        engine.saveRow("notes", "n0", null, title("offline"))

        engine.drain()

        val events = transport.events()
        assertEquals(listOf(StubTransport.Event.Pull("user", null)), events.filterIsInstance<StubTransport.Event.Pull>())
        assertTrue(events.last() is StubTransport.Event.Push, "the push follows the one page: $events")
        assertNull(store.peekSnapshot("notes", "n1"), "the page is staged like any other answer with more to come")
        assertEquals("c1", store.read { it.queryString("SELECT cursor FROM downloads WHERE shard = 'user'") })
        assertNull(engine.currentCursor())
        assertTrue(engine.pendingOps().isEmpty())
    }

    /** KILL: bind operation identity to the store's path — a copied store mints new ids and runs its action twice. */
    @Test fun aCopiedStoreReplaysItsFrozenOperationsWithTheSameBytes() = runBlocking<Unit> {
        val path = Fixture.path("original")
        val transport = StubTransport()
        val engine = Fixture.engine(store = Fixture.storeAt(path), transport = transport)
        engine.saveRow("notes", "n1", null, title("sent from two copies"))
        transport.failPushes(true)
        assertFailsWith<ReplicaError.Transport> { engine.drain() }
        engine.close()
        val copy = Fixture.path("copy")
        for (suffix in listOf("", "-wal", "-shm")) {
            val source = File(path + suffix)
            if (source.exists()) source.copyTo(File(copy + suffix))
        }
        transport.failPushes(false)

        val copied = Fixture.storeAt(copy)
        Fixture.engine(store = copied, transport = transport).drain()
        val original = Fixture.storeAt(path)
        Fixture.engine(store = original, transport = transport).drain()

        val requests = pushes(transport)
        assertEquals(3, requests.size)
        assertEquals(requests[0], requests[1])
        assertEquals(requests[1], requests[2], "the copies sent different operations")
        assertEquals(1, transport.pushedBatches().size, "a copied operation ran twice")
        assertTrue(copied.peekPending().isEmpty())
        assertTrue(original.peekPending().isEmpty())
    }
}
