package io.replicaman

import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.Fixture
import io.replicaman.support.ReplicaTestCase
import io.replicaman.support.StubTransport
import kotlin.test.assertEquals

/**
 * The doorbell rings for the USER shard only (the server's hook answers no
 * other), so its pull must ask for that shard alone — every ring used to
 * walk every shard, a wasted catalog round-trip per doorbell. Warm-up,
 * reconnect and refresh keep pulling every shard; a catalog edit lands on
 * the next such pull.
 */
class DoorbellShardTests : ReplicaTestCase() {

    /** KILL: ignore the `shards` argument in `pullUntilCaughtUp` — global is pulled too. */
    @Test
    fun pullingNamedShardsTouchesOnlyThose() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.pullUntilCaughtUp(listOf("user"))

        val pulled = transport.events().filterIsInstance<StubTransport.Event.Pull>().map { it.shard }
        assertEquals(listOf("user"), pulled)
    }

    /** KILL: default `shards` to `listOf("user")` — the catalog stops arriving. */
    @Test
    fun pullingEveryShardStillWalksAll() = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store = store, transport = transport)

        engine.pullUntilCaughtUp()

        val pulled = transport.events().filterIsInstance<StubTransport.Event.Pull>().map { it.shard }
        assertEquals(Fixture.schema().shards.toSet(), pulled.toSet())
    }
}
