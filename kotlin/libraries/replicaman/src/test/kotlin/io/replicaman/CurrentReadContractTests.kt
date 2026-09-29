package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.*

class CurrentReadContractTests : ReplicaTestCase() {
    private fun world(): Pair<ReplicaEngine, RowStream<ScopedNote, ScopedNote.Field>> {
        val indexes = listOf(ReplicaIndexSpec("notes", "kind"), ReplicaIndexSpec("notes", "score"), ReplicaIndexSpec("notes", "title"))
        val engine = Fixture.engine(Fixture.store(indexes = indexes), transport = StubTransport(), schema = ReplicaSchema(Fixture.schema().specs, indexes))
        return engine to RowStream(engine, ScopedNote)
    }
    private suspend fun seed(engine: ReplicaEngine) {
        engine.saveRow("notes", "a", "Photo", mapOf("kind" to ReplicaValue.Str("clip"), "score" to ReplicaValue.Num(1.0)))
        engine.saveRow("notes", "b", "Photo", mapOf("kind" to ReplicaValue.Str("still"), "score" to ReplicaValue.Num(1.0)))
        engine.saveRow("notes", "c", "Video", mapOf("kind" to ReplicaValue.Str("clip"), "score" to ReplicaValue.Num(2.0)))
        engine.saveRow("notes", "d", "Video", mapOf("score" to ReplicaValue.Null))
    }
    @Test fun oneOfEmptyAndNullUseSqlSemantics(): Unit = runTest {
        val (engine, notes) = world(); seed(engine)
        assertEquals(listOf("a", "b", "c"), notes.list(ReplicaPredicate.oneOf(ScopedNote.Field.KIND, listOf("still", "clip"))).map { it.id })
        assertTrue(notes.list(ReplicaPredicate.oneOf(ScopedNote.Field.KIND, emptyList())).isEmpty())
        assertEquals(listOf("d"), notes.list(ReplicaPredicate.isNull(ScopedNote.Field.KIND)).map { it.id })
        assertEquals(listOf("d"), notes.list(ReplicaPredicate.isNull(ScopedNote.Field.SCORE)).map { it.id })
    }
    @Test fun andNotAndKindStayBoundAndExcludeUnknownNull(): Unit = runTest {
        val (engine, notes) = world(); seed(engine)
        assertEquals(listOf("b"), notes.list(ReplicaPredicate.not(ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip"))).map { it.id })
        assertEquals(listOf("a"), notes.list(ReplicaPredicate.and(listOf(ReplicaPredicate.Kind("Photo"), ReplicaPredicate.eq(ScopedNote.Field.KIND, "clip")))).map { it.id })
        assertEquals(4, notes.list(ReplicaPredicate.and(emptyList())).size)
        assertTrue(notes.list(ReplicaPredicate.Kind("Photo' OR 1=1 --")).isEmpty())
    }
    @Test fun orderAndLimitUseIndexedValuesAndDirectionForTies(): Unit = runTest {
        val (engine, notes) = world(); seed(engine)
        assertEquals(listOf("c", "b", "a"), notes.list(order = listOf(ReplicaOrder.descending(ScopedNote.Field.SCORE)), limit = 3).map { it.id })
        assertEquals(listOf("d", "a", "b"), notes.list(order = listOf(ReplicaOrder.ascending(ScopedNote.Field.SCORE)), limit = 3).map { it.id })
        assertTrue(notes.list(limit = 0).isEmpty())
    }
    @Test fun transactionDeletesRowsAndPendingBirthsAtomically(): Unit = runTest {
        val (engine, notes) = world(); seed(engine)
        engine.write { it.rows(ScopedNote).delete(listOf("a", "b")) }
        assertEquals(listOf("c", "d"), notes.list().map { it.id })
        assertEquals(setOf("c", "d"), engine.pendingOps().map { it.op().rowId }.toSet())
    }
}
