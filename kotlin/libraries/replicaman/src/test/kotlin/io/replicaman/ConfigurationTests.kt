package io.replicaman

import kotlinx.coroutines.runBlocking
import org.junit.Test
import io.replicaman.support.*
import java.io.File
import kotlin.test.*

class ConfigurationTests : ReplicaTestCase() {
    @Test fun configuredHomeIsCapturedWhenTheEngineIsBorn(): Unit = runBlocking {
        val before = ReplicaMan.Configuration.homePath
        val directory = Fixture.directory("configured")
        try {
            ReplicaMan.Configuration.homePath = directory
            val engine = ReplicaEngine(transport = StubTransport(), schema = Fixture.schema(), automaticallyPushWrites = false)
            ReplicaMan.Configuration.homePath = Fixture.directory("later")
            try {
                engine.open(42)
                assertEquals(directory, engine.home)
                assertEquals(File(directory, "replica-42.sqlite"), engine.storePath)
            } finally { engine.retire() }
        } finally { ReplicaMan.Configuration.homePath = before }
    }
    @Test fun explicitHomeOverridesThePackageDefault(): Unit = runBlocking {
        val directory = Fixture.directory("explicit")
        val engine = ReplicaEngine(home = directory, transport = StubTransport(), schema = Fixture.schema(), automaticallyPushWrites = false)
        try { engine.open(91); assertEquals(File(directory, "replica-91.sqlite"), engine.storePath) }
        finally { engine.retire() }
    }
}
