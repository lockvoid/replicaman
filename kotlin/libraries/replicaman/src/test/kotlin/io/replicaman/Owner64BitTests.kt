package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.TestNote
import io.replicaman.support.StubTransport
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Swift Int is 64-bit. Android must not truncate the auth/manifest Long owner at the store boundary. */
class Owner64BitTests : ReplicaTestCase() {
    /** KILL: name/bind/stamp the world with owner.toInt() — its raw file, owner field or preserved journal changes. */
    @Test fun anOwnerAboveIntMaxValueKeepsItsExactFileStampAndJournalAcrossColdBoot() = runTest {
        val directory = Fixture.directory()
        val owner = 2_147_483_648L
        val engine = Fixture.unopenedEngine(directory, StubTransport(), schema = Fixture.schema(ReplicaStamp.standard))
        engine.open(owner)
        assertEquals(owner, engine.owner)
        engine.saveRow("notes", "owned-note", null, mapOf("title" to ReplicaValue.Str("private high owner")))
        engine.createDoc("boards", "owned-board", "seed".toByteArray(), 7uL,
            mapOf("title" to ReplicaValue.Str("owner stamp")))
        val store = assertNotNull(engine.store)
        assertEquals(owner, store.read { db ->
            db.queryLong("SELECT json_extract(data,'$.userId') FROM snapshots WHERE stream='boards' AND row_id='owned-board'")
        })
        val owed = store.read { it.queryStrings("SELECT id FROM intents ORDER BY created_at,id") }
        assertEquals(2, owed.size)
        assertTrue(File(directory, "replica-2147483648.sqlite").isFile)

        engine.close()
        engine.open(2_147_483_649L)
        assertNull(RowStream(engine, TestNote).find("owned-note"))
        assertTrue(engine.pendingOps().isEmpty())
        engine.close()

        val cold = Fixture.unopenedEngine(directory, StubTransport(), schema = Fixture.schema(ReplicaStamp.standard))
        cold.openForColdBoot(owner)
        assertEquals(owner, cold.owner)
        assertEquals("replica-2147483648.sqlite", cold.storePath?.name)
        assertEquals("private high owner", RowStream(cold, TestNote).find("owned-note")?.title)
        assertEquals(owed, assertNotNull(cold.store).read { it.queryStrings("SELECT id FROM intents ORDER BY created_at,id") })
    }
}
