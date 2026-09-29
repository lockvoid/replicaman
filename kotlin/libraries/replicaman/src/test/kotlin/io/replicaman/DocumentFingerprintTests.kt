package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.*

class DocumentFingerprintTests : ReplicaTestCase() {
    @Test fun sameLengthReplacementHasANewFingerprint(): Unit = runTest {
        val store = Fixture.store(); val engine = Fixture.engine(store, transport = StubTransport())
        engine.createDoc("boards", "b1", "AAA".toByteArray(), 7uL)
        val before = engine.documentFingerprints("boards", listOf("b1"))["b1"]!!
        engine.rebuildDocument("boards", "b1", "BBB".toByteArray(), 8uL)
        val after = engine.documentFingerprints("boards", listOf("b1"))["b1"]!!
        assertFalse(before.contentEquals(after))
        assertContentEquals("BBB".toByteArray(), engine.docFold("boards", "b1"))
    }

    @Test fun deleteAndRebirthNeverReuseTheFingerprint(): Unit = runTest {
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport())
        engine.createDoc("boards", "b1", "AAA".toByteArray(), 7uL)
        val before = engine.documentFingerprints("boards", listOf("b1"))["b1"]!!
        engine.deleteRow("boards", "b1")
        assertTrue(engine.documentFingerprints("boards", listOf("b1")).isEmpty())
        engine.createDoc("boards", "b1", "AAA".toByteArray(), 7uL)
        assertFalse(before.contentEquals(engine.documentFingerprints("boards", listOf("b1"))["b1"]!!))
    }

    @Test fun failedFoldTransactionKeepsTheFingerprintAndBytes(): Unit = runTest {
        val store = Fixture.store(); val engine = Fixture.engine(store, transport = StubTransport())
        engine.createDoc("boards", "b1", "AAA".toByteArray(), 7uL)
        val before = engine.documentFingerprints("boards", listOf("b1"))["b1"]!!
        assertFails { store.write { db -> store.updateDoc(db, "boards", "b1", fold = "BBB".toByteArray()); error("rollback") } }
        assertContentEquals(before, engine.documentFingerprints("boards", listOf("b1"))["b1"])
        assertContentEquals("AAA".toByteArray(), engine.docFold("boards", "b1"))
    }
}
