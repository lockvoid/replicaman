package io.replicaman

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The journal's address (stream, row_id) is a COLUMN, and the "was this row
 * ever born on the server?" question reads it with the verb.
 *
 * Both pins here were holes: blanking the verb filter in `entriesAddressing`
 * passed the whole suite, and every caller of it takes a destructive branch
 * on the answer — a pending PATCH counted as a birth silently swallows a
 * delete the server needed to hear, and silently refuses a create.
 */
class JournalAddressTests : ReplicaTestCase() {

    /** KILL: decode every journal payload when deleting one unsent row — unrelated malformed bytes block deletion. */
    @Test fun deletingAnUnsentRowSurvivesUnrelatedMalformedJournalBytes() = runTest {
        val store = Fixture.store(); val engine = Fixture.engine(store = store, transport = StubTransport())
        engine.saveRow("notes", "unsent", null, mapOf("title" to ReplicaValue.Str("draft")))
        store.write { it.exec("INSERT INTO intents (id,stream,row_id,state,op,payload,lane,created_at) VALUES ('corrupt','notes','other','owed','row.patch','{not json','bulk',0)") }
        engine.deleteRow("notes", "unsent")
        assertEquals(listOf("corrupt"), store.read { it.queryStrings("SELECT id FROM intents") })
    }

    private suspend fun acceptAll(transport: StubTransport) {
        transport.scriptPush { ops ->
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.ACCEPTED) }
        }
    }

    /**
     * A row the server already knows, with a pending patch owed for it.
     * The patch is not a birth: deleting must still journal `row.delete`.
     *
     * KILL: drop the `AND op = ?` filter from `entriesAddressing(verb)` —
     * the pending patch reads as a birth and the delete is discarded.
     */
    @Test
    fun aPendingPatchIsNotABirthSoTheDeleteStillShips() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        acceptAll(transport)
        val engine = Fixture.engine(store = store, transport = transport)

        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("born")))
        engine.drain(ReplicaLane.BULK)
        assertTrue(store.pendingOps().isEmpty(), "the create settled — the server has heard of n1")

        // A patch, still owed. The row exists server-side.
        engine.saveRow("notes", "n1", null, mapOf("title" to ReplicaValue.Str("edited")))
        val deleted = engine.deleteRow("notes", "n1")

        assertTrue(deleted, "a known row's delete is real work, not local silence")
        assertEquals(
            listOf(ReplicaOp.Verb.ROW_PATCH, ReplicaOp.Verb.ROW_DELETE),
            store.pendingOps().map { it.op().verb },
            "the row is born server-side: its owed patch keeps its place, and the " +
                "delete is appended behind it — nothing is discarded as never-born"
        )
    }

    /**
     * Same confusion at the other caller: an owed delete on the very address
     * must not make the document create think the document is already born.
     *
     * KILL: the same filter removal — the owed delete reads as a birth and
     * the re-create answers false.
     */
    @Test
    fun anOwedDeleteDoesNotBlockRecreatingTheDocument() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        acceptAll(transport)
        val engine = Fixture.engine(store = store, transport = transport)
        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
        engine.drain(ReplicaLane.BULK)
        assertTrue(engine.deleteRow("boards", "b1"))
        assertEquals(listOf(ReplicaOp.Verb.ROW_DELETE), store.pendingOps().map { it.op().verb })

        assertTrue(engine.createDoc("boards", "b1", "AGAIN".toByteArray(), 8uL), "only a create is a birth")
    }
}
