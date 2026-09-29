package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekDoc
import io.replicaman.support.peekPending
import io.replicaman.support.peekSnapshot
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The loro-free core. The MODULE GRAPH is the enforcement:
 * `:replicaman` depends on androidx.sqlite, kotlinx and OkHttp alone — no
 * Loro artifact is on this side of the boundary, and the classpath scan
 * below proves it. This suite adds the functional half: a rows-only
 * consumer (no codec registered at all) still replicates rows and even
 * keeps document PROJECTIONS readable.
 */
class LoroFreeCoreTests : ReplicaTestCase() {

    /**
     * KILL: add `implementation(project(":replicaman-loro"))` (or any loro
     * artifact) to `packages/replicaman/build.gradle.kts` — the scan finds it.
     */
    @Test
    fun theCoreCompileClasspathCarriesNoLoro() {
        val classpath = System.getProperty("java.class.path").split(File.pathSeparator)
        val offenders = classpath.filter { entry ->
            val name = File(entry).name.lowercase()
            name.contains("loro") && !name.contains("replicaman")
        }
        assertTrue(
            offenders.isEmpty(),
            "the rows-only core must not link loro; found: $offenders"
        )
    }

    /** KILL: throw on a doc frame whose codec is unregistered — the batch dies. */
    @Test
    fun rowsOnlyEngineWithNoCodecReplicates() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        // No codecs at all — the rows-only consumer.
        val engine = Fixture.engine(store = store, transport = transport, codecs = emptyList(), documentMode = ReplicaDocumentMode.PROJECTIONS_ONLY)

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    Fixture.note("n1", "rows work"),
                    // Document frames arrive anyway; without a codec the fold is
                    // beyond reach, but the projection data still lands in raw
                    // truth (stored-and-skipped, never a throw).
                    ReplicaFrame.DocSnapshot(
                        "boards", "b1", "loro@1", "OPAQUE".toByteArray(),
                        mapOf("name" to ReplicaValue.Str("Plans"))
                    ),
                    ReplicaFrame.DocDelta("boards", "b1", 1, "loro@1", "OPS".toByteArray()),
                ),
                cursor = "6:", more = false
            )
        )
        engine.pullOnce("user")

        assertEquals(
            ReplicaValue.Str("rows work"),
            store.peekSnapshot("notes", "n1")?.data?.get("title")
        )
        assertEquals(
            ReplicaValue.Str("Plans"),
            store.peekSnapshot("boards", "b1")?.data?.get("name"),
            "the document's projection row is still useful without the codec"
        )
        assertNull(store.peekDoc("boards", "b1"), "no codec, no fold — skipped, not thrown")

        // And the row write door works end to end.
        engine.saveRow("notes", "n2", null, mapOf("title" to ReplicaValue.Str("mine")))
        engine.drain()
        assertEquals(0, store.peekPending().size)
    }
}
