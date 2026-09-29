package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.generated.app.Theme
import io.replicaman.generated.app.ThemeColor
import io.replicaman.generated.app.SampleReplica
import io.replicaman.generated.app.themes
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekParked
import io.replicaman.support.peekSnapshot
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A generated row must be usable before a server echo, without sending pull-only columns. */
class LocalRowSnapshotTests : ReplicaTestCase() {
    private class World {
        val store = Fixture.store(indexes = SampleReplica.schema.indexes)
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport, schema = SampleReplica.schema)
        val themes = SampleReplica(engine).themes
    }

    private fun theme(id: String = "local-theme") = Theme(
        id = id,
        colors = listOf(ThemeColor(hex = "#1D8A70", id = "accent-green")),
        createdAt = "2026-09-25T06:57:14Z",
        logoRef = "blob://local-logo",
        logoUrl = "https://media.example.test/local-logo.png",
        name = "Local theme",
        updatedAt = "2026-09-25T06:57:14Z",
        userId = Fixture.OWNER,
    )

    private fun Theme.withForgedReadOnlyFields(name: String) = copy(
        name = name,
        createdAt = "1900-01-01T00:00:00Z",
        updatedAt = "1900-01-01T00:00:00Z",
        userId = 999L,
        logoUrl = "https://media.example.test/forged.png",
    )

    private fun assertWritableBirth(op: ReplicaOp) {
        assertEquals(ReplicaOp.Verb.ROW_CREATE, op.verb)
        assertEquals("themes", op.stream)
        assertEquals(
            setOf("colors", "description", "heading", "logoRef", "name", "pinned"),
            assertNotNull(op.data).keys,
            "the server rejects pull-only columns, even when the local model needs them to decode",
        )
    }

    @Test
    fun generatedBirthIsReadableBeforeAnyServerEchoAndPushesOnlyWritableColumns() = runTest {
        val world = World()
        val born = theme()

        world.engine.write { it.themes.create(born) }

        assertEquals(0, world.transport.pushCount())
        assertEquals(born, world.themes.find(born.id))
        assertEquals(listOf(born), world.themes.list(), "an offline theme must appear immediately")
        assertEquals(born.userId, world.store.peekSnapshot("themes", born.id)?.data?.get("userId")?.long)

        world.engine.drain()

        assertWritableBirth(world.transport.pushedBatches().flatten().single())
        assertEquals(born, world.themes.find(born.id), "an acknowledgement without an echo retains the local value")
    }

    @Test
    fun updatingAWritableFieldDoesNotReplaceLocalReadOnlyMetadata() = runTest {
        val world = World()
        val born = theme()
        world.engine.write { it.themes.create(born) }
        world.engine.drain()

        world.engine.write { it.themes.update(born.id) { row -> row.withForgedReadOnlyFields("Renamed") } }

        assertEquals(born.copy(name = "Renamed"), world.themes.find(born.id))
        assertEquals(listOf(born.copy(name = "Renamed")), world.themes.list())
        world.engine.drain()
        val ops = world.transport.pushedBatches().flatten()
        assertEquals(2, ops.size)
        assertWritableBirth(ops.first())
        assertEquals(ReplicaOp.Verb.ROW_PATCH, ops.last().verb)
        assertEquals(mapOf("name" to ReplicaValue.Str("Renamed")), ops.last().data)
    }

    /** KILL: serve transaction reads from the committed read pool, losing read-your-writes. */
    @Test
    fun aBirthAndUpdateKeepFullMetadataInsideTheTransactionAndPublishAtCommit() = runTest {
        val world = World(); val born = theme()
        world.engine.write { tx ->
            tx.themes.create(born)
            assertEquals(born, tx.themes.find(born.id))
            tx.themes.update(born.id) { it.withForgedReadOnlyFields("Edited") }
            assertEquals(born.copy(name = "Edited"), tx.themes.find(born.id))
            assertNull(world.themes.find(born.id), "ordinary point reads remain committed pictures")
            assertTrue(world.themes.list().isEmpty(), "ordinary lists remain committed pictures")
        }
        assertEquals(listOf(born.copy(name = "Edited")), world.themes.list())
        world.engine.drain()
        val ops = world.transport.pushedBatches().flatten()
        assertWritableBirth(ops.first())
        assertEquals(mapOf("name" to ReplicaValue.Str("Edited")), ops.last().data)
    }

    /** KILL: complete the async door before its full snapshot and patch commit. */
    @Test
    fun anAsyncBirthAndUpdateReturnOnlyAfterFullMetadataCommits() = runTest {
        val world = World(); val born = theme()
        world.engine.writeAsync { tx ->
            tx.themes.create(born)
            tx.themes.update(born.id) { it.withForgedReadOnlyFields("Async") }
            assertEquals(born.copy(name = "Async"), tx.themes.find(born.id))
        }
        assertEquals(born.copy(name = "Async"), world.themes.find(born.id))
        assertEquals(born.userId, world.store.peekSnapshot("themes", born.id)?.data?.get("userId")?.long)
        assertEquals(2, world.engine.pendingOps().size)
        world.engine.drain()
        val ops = world.transport.pushedBatches().flatten()
        assertWritableBirth(ops.first())
        assertEquals(mapOf("name" to ReplicaValue.Str("Async")), ops.last().data)
    }

    /** KILL: let an edit replace server-owned fields or mutate the cached model before commit. */
    @Test
    fun anEditOfAnExistingRowUsesCommittedReadOnlyFields() = runTest {
        val world = World(); val born = theme()
        world.engine.write { it.themes.create(born) }
        val cached = assertNotNull(world.themes.find(born.id))
        world.engine.write { tx ->
            tx.themes.update(born.id) { it.withForgedReadOnlyFields("Edited") }
            assertEquals(born.copy(name = "Edited"), tx.themes.find(born.id))
            assertEquals(born, cached)
        }
        world.engine.writeAsync { tx ->
            tx.themes.update(born.id) { it.withForgedReadOnlyFields("Async") }
            assertEquals(born.copy(name = "Async"), tx.themes.find(born.id))
        }
        assertEquals(born.copy(name = "Async"), world.themes.find(born.id))
    }

    @Test
    fun serverEchoReplacesBirthMetadataWithTheAuthoritativeSnapshot() = runTest {
        val world = World()
        val born = theme()
        world.engine.write { it.themes.create(born) }
        world.engine.drain()
        val echoed = born.copy(
            createdAt = "2026-09-25T06:57:15Z",
            updatedAt = "2026-09-25T06:57:15Z",
            logoUrl = "https://media.example.test/resolved-logo.png",
        )
        world.transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(ReplicaFrame.RowSet("themes", born.id, null, echoed.encodeSnapshot())),
                cursor = "1:",
                more = false,
            ),
        )

        world.engine.pullOnce("user")

        assertEquals(echoed, world.themes.find(born.id))
        assertEquals(listOf(echoed), world.themes.list())
        assertTrue(world.engine.pendingOps().isEmpty(), "accepting an echo does not manufacture a client write")
    }

    @Test
    fun rejectedBirthRemovesTheEntireLocalSnapshotAndDependentPatch() = runTest {
        val world = World()
        val born = theme()
        world.engine.write { it.themes.create(born) }
        world.engine.write { it.themes.update(born.id) { row -> row.copy(name = "Changed before rejection") } }
        assertNotNull(world.themes.find(born.id))
        world.transport.scriptPush { ops ->
            ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "theme already exists") }
        }

        world.engine.drain()

        assertNull(world.themes.find(born.id))
        assertNull(world.store.peekSnapshot("themes", born.id))
        assertTrue(world.themes.list().isEmpty())
        assertTrue(world.engine.pendingOps().isEmpty())
        assertWritableBirth(world.store.peekParked().single().op())
    }
}
