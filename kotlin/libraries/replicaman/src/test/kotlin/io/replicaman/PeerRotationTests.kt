package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import io.replicaman.support.peekDoc
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull

/**
 * Peer rotation: a wiped fold's document is reborn with a NEW
 * loro peer. Loro dedups by (peer, counter); a reborn doc reusing its peer
 * would have its edits silently discarded — the v1 lesson, kept.
 */
class PeerRotationTests : ReplicaTestCase() {

    /** KILL: reuse `doc.peer` in the fresh-fold branch of `apply(DocSnapshot)`. */
    @Test
    fun resetWipedDocIsRecreatedWithAFreshPeer() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(
            store = store, transport = transport, minter = Fixture.sequentialMinter(100uL)
        )

        val snapshot = "SNAP-1".toByteArray()
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot(
                        "boards", "b1", "stub@1", snapshot, mapOf("name" to ReplicaValue.Str("Plans"))
                    )
                ),
                cursor = "5:", more = false
            )
        )
        engine.pullOnce("user")

        val born = store.peekDoc("boards", "b1")
        assertNotNull(born)
        assertEquals(100uL, born.peer, "first birth mints the first peer")

        engine.resyncDocument("boards", "b1")

        // Forced resnapshot: the same document arrives again after its fold
        // was wiped with the shard.
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot(
                        "boards", "b1", "stub@1", snapshot, mapOf("name" to ReplicaValue.Str("Plans"))
                    )
                ),
                cursor = "9:", more = false
            )
        )
        engine.pullOnce("user")

        val reborn = store.peekDoc("boards", "b1")
        assertNotNull(reborn)
        assertEquals(101uL, reborn.peer, "a reborn doc NEVER reuses its old peer")
        assertNotEquals(born.peer, reborn.peer)
    }

    /** KILL: mint a peer on every `DocSnapshot` — a live fold rotates under its own edits. */
    @Test
    fun survivingDocKeepsItsPeerAcrossOrdinaryPulls() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(
            store = store, transport = transport, minter = Fixture.sequentialMinter(100uL)
        )

        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocSnapshot("boards", "b1", "stub@1", "SNAP".toByteArray(), emptyMap())
                ),
                cursor = "5:", more = false
            )
        )
        engine.pullOnce("user")

        // An ordinary tail (no reset) touching the same doc must not rotate.
        transport.queuePull(
            "user",
            ReplicaPullResponse(
                frames = listOf(
                    ReplicaFrame.DocDelta("boards", "b1", 1, "stub@1", "+d1".toByteArray())
                ),
                cursor = "6:", more = false
            )
        )
        engine.pullOnce("user")

        val doc = store.peekDoc("boards", "b1")
        assertNotNull(doc)
        assertEquals(100uL, doc.peer, "peers are stable while the fold lives")
    }
}
