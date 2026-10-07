package io.replicaman

import io.replicaman.testing.ReplicaPullResponse
import kotlinx.coroutines.test.runTest
import org.junit.Test
import io.replicaman.support.*
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A held row leaves when its gate lets it — on the gate's signal, on the
 * row's next write, or when the store opens — as its STATE, never as the
 * history of its writes, and never ahead of a held row it names.
 */
class GateReleaseTests : ReplicaTestCase() {

    private fun held(engine: ReplicaEngine): List<String> = engine.heldRows().map { it.rowId }

    /** The engine hears a gate's signal on its own coroutine: wait until the holds it asked show the answer. */
    private suspend fun settled(engine: ReplicaEngine, holding: List<String>) {
        eventually(message = "holds never became $holding") { held(engine) == holding }
    }

    /** A gate on one field, released per key by its own ledger. */
    private fun fieldGate(id: String, field: String, ledger: ReleaseLedger) =
        TestGate(id = id, signal = ledger.signal) { change ->
            val key = change.local[field]?.string
            if (key == null || ledger.contains(key)) SyncVerdict.Push else SyncVerdict.Gate("$field $key in flight")
        }

    private fun str(value: String) = ReplicaValue.Str(value)

    // MARK: - the gate's signal

    /** KILL: `releaseHolds` — walk the holds newest first. */
    @Test fun aGateSignalReleasesOnlyItsOwnRowsInTheOrderTheyWereHeld(): Unit = runTest {
        val transport = StubTransport()
        val a = ReleaseLedger()
        val b = ReleaseLedger()
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(fieldGate("a", "a", a), fieldGate("b", "b", b)))
        engine.saveRow("notes", "x1", null, mapOf("a" to str("ka")))
        engine.saveRow("notes", "y1", null, mapOf("b" to str("kb")))
        engine.saveRow("notes", "x2", null, mapOf("a" to str("ka")))

        a.land("ka")
        settled(engine, listOf("y1"))
        engine.drain()

        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf("x1", "x2"), sent.map { it.rowId })
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_CREATE), sent.map { it.verb })
    }

    // MARK: - what leaves

    /** KILL: `admit` — `val knows = true`; the birth then leaves as a patch. */
    @Test fun aRowTheServerNeverSawLeavesAsOneCreateOfItsLastState(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a"), "blob" to str("k1")))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))
        engine.saveRow("notes", "n1", null, mapOf("rank" to str("r")))

        released.land("k1")
        settled(engine, emptyList())
        engine.drain()

        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), sent.map { it.verb })
        assertEquals(mapOf("title" to str("b"), "blob" to str("k1"), "rank" to str("r")), sent.first().data)
    }

    /** KILL: `journalRelease` — release every row as a create; the server already has this one. */
    @Test fun aRowTheServerKnowsLeavesAsOnePatchOfEveryFieldItMoved(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))
        engine.drain()
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))
        assertEquals(listOf("n1"), held(engine))

        released.land("k1")
        settled(engine, emptyList())
        engine.drain()

        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_PATCH), sent.map { it.verb })
        assertEquals(mapOf("title" to str("b"), "blob" to str("k1")), sent.last().data)
    }

    /**
     * The server refuses a column it does not take from devices ("unknown
     * column", `stream.rb#decode`): a released row carries only what the
     * device may send, whatever else its state holds.
     *
     * KILL: `journalRelease` — send `current` unfiltered.
     */
    @Test fun aReleasedRowCarriesOnlyWhatTheDeviceMaySend(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val schema = ReplicaSchema(streams = listOf(
            ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, shard = "user", pushed = setOf("title", "blob")),
        ))
        val engine = Fixture.engine(Fixture.store(), transport = transport, schema = schema, syncGates = listOf(blobGate(released)))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.RowSet("notes", "n1", null, mapOf("title" to str("a"), "url" to str("https://signed"))),
        ), cursor = "5:", more = false))
        engine.pullOnce()
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))

        released.land("k1")
        settled(engine, emptyList())
        engine.drain()

        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_PATCH), sent.map { it.verb })
        assertEquals(mapOf("blob" to str("k1")), sent.first().data)
    }

    /**
     * A pull answering while a row waits must not undo what the device wrote:
     * the held row keeps the fields a device writes, takes the fields only the
     * server writes, and leaves as that state — otherwise the ref
     * vanishes under a pull and the bytes never bind.
     *
     * KILL: `apply(RowSet)` — drop the held-row branch.
     */
    @Test fun aPullLeavesAHeldRowsDeviceFieldsAlone(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val released = ReleaseLedger()
        val schema = ReplicaSchema(streams = listOf(
            ReplicaStreamSpec("notes", ReplicaStreamSpec.Lane.ROW, shard = "user", pushed = setOf("title", "blob")),
        ))
        val engine = Fixture.engine(store, transport = transport, schema = schema, syncGates = listOf(blobGate(released)))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.RowSet("notes", "n1", null, mapOf("title" to str("a"), "url" to str("u1"))),
        ), cursor = "5:", more = false))
        engine.pullOnce()
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))

        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.RowSet("notes", "n1", null, mapOf("title" to str("a"), "url" to str("u2"))),
        ), cursor = "6:", more = false))
        engine.pullOnce()

        assertEquals(mapOf("title" to str("a"), "blob" to str("k1"), "url" to str("u2")), store.peekSnapshot("notes", "n1")?.data)

        released.land("k1")
        settled(engine, emptyList())
        engine.drain()
        assertEquals(mapOf("blob" to str("k1")), transport.pushedBatches().flatten().last().data, "the title the device never moved is not sent")
    }

    /**
     * A field the server moved while the row was held, and the device did not, is the server's:
     * the held row shows it, and the release carries only what the device moved.
     *
     * KILL: `materializeBase` — merge no base field into a held row; or `journalRelease` — send
     * `current` instead of what moved against the base.
     */
    @Test fun aReleasedPatchLeavesAFieldTheServerMovedAlone(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val backup = BackupFlag(on = true)
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(backup.gate))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.RowSet("notes", "n1", null, mapOf("title" to str("a"), "rank" to str("1"))),
        ), cursor = "c1", more = false))
        engine.pullOnce()

        backup.set(false)
        engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))
        transport.queuePull("user", ReplicaPullResponse(frames = listOf(
            ReplicaFrame.RowSet("notes", "n1", null, mapOf("title" to str("a"), "rank" to str("2"))),
        ), cursor = "c2", more = false))
        engine.pullOnce()
        assertEquals(mapOf("title" to str("b"), "rank" to str("2")), store.peekSnapshot("notes", "n1")?.data,
            "the held row keeps what the device moved and shows what the server moved")

        backup.set(true)
        settled(engine, emptyList())
        engine.drain()

        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_PATCH), sent.map { it.verb })
        assertEquals(mapOf("title" to str("b")), sent.first().data, "rank 2 is the server's; the device never touched it")
    }

    @Test fun aRowDeletedWhileHeldLeavesAsADeleteWhenTheServerKnowsIt(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val backup = BackupFlag(on = true)
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(backup.gate))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))
        engine.drain()

        backup.set(false)
        engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))
        engine.deleteRow("notes", "n1")
        assertEquals(listOf("n1"), held(engine), "a delete of a held row stays with its hold")
        assertEquals(0, store.peekPending().size)

        backup.set(true)
        settled(engine, emptyList())
        engine.drain()

        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_DELETE), transport.pushedBatches().flatten().map { it.verb })
    }

    /** KILL: `askAgain` — drop the gone-before-heard branch; the hold then outlives its row. */
    @Test fun aRowDeletedBeforeTheServerHeardOfItLeavesNothing(): Unit = runTest {
        val transport = StubTransport()
        val backup = BackupFlag(on = false)
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(backup.gate))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))

        engine.deleteRow("notes", "n1")
        assertEquals(emptyList(), held(engine), "a hold outlived a row the server never heard of")

        backup.set(true)
        engine.drain()
        assertEquals(0, transport.pushCount())
    }

    /** KILL: `journalRelease` — `codec.diff(doc.fold, null)`; the delta then re-sends the acked birth. */
    @Test fun aDocumentTheServerKnowsLeavesAsOneDeltaPastWhatItAcked(): Unit = runTest {
        val transport = StubTransport()
        val backup = BackupFlag(on = true)
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(backup.gate))
        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
        engine.drain()

        backup.set(false)
        engine.recordDocDelta("boards", "b1", "+a".toByteArray())
        engine.recordDocDelta("boards", "b1", "+b".toByteArray())
        assertEquals(listOf("b1"), held(engine))

        backup.set(true)
        settled(engine, emptyList())
        val release = engine.pendingOps().single()
        assertEquals(ReplicaOp.Verb.DOC_DELTA, release.op().verb)
        val verdicts = engine.drain()

        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.DOC_DELTA), sent.map { it.verb })
        assertEquals(listOf(release.id), verdicts.map { it.id }, "the release is the document's one superseding entry")
        assertEquals(7, java.util.UUID.fromString(sent.last().id).version(), "on the wire it is a fresh UUIDv7")
        assertEquals("+a+b", sent.last().payload?.toString(Charsets.UTF_8))
    }

    @Test fun aDocumentBornHeldLeavesAsOneCreateOfItsFold(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val backup = BackupFlag(on = false)
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(backup.gate))
        engine.createDoc("boards", "b1", "SEED".toByteArray(), 7uL)
        engine.recordDocDelta("boards", "b1", "+x".toByteArray())

        backup.set(true)
        settled(engine, emptyList())
        engine.drain()

        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), sent.map { it.verb })
        assertEquals("SEED+x", sent.first().seed?.toString(Charsets.UTF_8))
        assertEquals("stub@1", sent.first().codec)
        assertEquals("6", store.peekDoc("boards", "b1")?.acked?.toString(Charsets.UTF_8), "the server acked the whole fold")
    }

    /** KILL: `journalRelease` — journal the release with a null preimage. */
    @Test fun aReleasedBirthTheServerRefusesLeavesTheDevice(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))
        released.land("k1")
        settled(engine, emptyList())

        transport.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "no") } }
        engine.drain()

        assertNull(store.peekSnapshot("notes", "n1"))
        assertEquals(1, store.peekParked().size)
    }

    @Test fun rejectedHeldPatchRestoresBaselineAndRemovesIntroducedFields(): Unit = runTest {
        for (pullWhileHeld in listOf(false, true)) {
            val store = Fixture.store()
            val wire = StubTransport()
            val released = ReleaseLedger()
            val engine = Fixture.engine(store, transport = wire, syncGates = listOf(blobGate(released)))
            engine.saveRow("notes", "n", null, mapOf("title" to str("original")))
            engine.drain()
            engine.saveRow("notes", "n", null, mapOf("title" to str("held"), "blob" to str("key")))
            engine.saveRow("notes", "n", null, mapOf("title" to str("latest")))
            if (pullWhileHeld) {
                wire.queuePull("user", ReplicaPullResponse(frames = listOf(
                    ReplicaFrame.RowSet("notes", "n", null, mapOf("title" to str("remote")))
                ), cursor = "9:", more = false))
                engine.pullOnce()
            }
            wire.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "denied") } }
            released.land("key")
            settled(engine, emptyList())
            engine.drain()
            assertEquals(mapOf("title" to str(if (pullWhileHeld) "remote" else "original")), store.peekSnapshot("notes", "n")?.data)
            assertEquals(1, store.peekParked().size)
        }
    }

    @Test fun rejectedFlippedHoldRestoresBeforeTheEntireQueuedSuffix(): Unit = runTest {
        val store = Fixture.store()
        val wire = StubTransport()
        val backup = BackupFlag(on = true)
        val engine = Fixture.engine(store, transport = wire, syncGates = listOf(backup.gate))
        engine.saveRow("notes", "n", null, mapOf("title" to str("original")))
        engine.drain()
        engine.saveRow("notes", "n", null, mapOf("title" to str("one"), "extra" to str("new")))
        engine.saveRow("notes", "n", null, mapOf("title" to str("two")))
        backup.set(false)
        settled(engine, listOf("n"))
        wire.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "denied") } }
        backup.set(true)
        settled(engine, emptyList())
        engine.drain()
        assertEquals(mapOf("title" to str("original")), store.peekSnapshot("notes", "n")?.data)
    }

    // MARK: - a write asks again

    /** KILL: `admit` — return false for a held row without `rejudge`. */
    @Test fun aWriteToAHeldRowAsksItAgain(): Unit = runTest {
        val store = Fixture.store()
        val released = ReleaseLedger()
        val engine = Fixture.engine(store, transport = StubTransport(), syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))
        released.landQuietly("k1")

        engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))

        assertEquals(emptyList(), held(engine))
        val owed = store.peekPending().map { it.op() }
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), owed.map { it.verb })
        assertEquals(mapOf("blob" to str("k1"), "title" to str("b")), owed.first().data)
    }

    // MARK: - parent before child

    /**
     * A brand kit waits for its logo; a voice added meanwhile names it, and
     * the server refuses a voice whose kit it does not know.
     *
     * KILL: `admit` — drop the `heldParent` branch; the voice then leaves first.
     */
    @Test fun aWriteNamingAHeldRowWaitsBehindItAndLeavesAfterIt(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "kit", null, mapOf("blob" to str("logo")))
        engine.saveRow("notes", "voice", null, mapOf("kit" to str("kit")))

        val holds = engine.heldRows()
        assertEquals(listOf("kit", "voice"), holds.map { it.rowId })
        assertEquals("row:notes/kit", holds.last().gateId)

        released.land("logo")
        settled(engine, emptyList())
        engine.drain()

        assertEquals(listOf("kit", "voice"), transport.pushedBatches().flatten().map { it.rowId })
    }

    /** KILL: `rejudge` — ask no row waiting behind the one that left. */
    @Test fun aRowWaitingBehindAParentIsJudgedByItsOwnGatesWhenTheParentLeaves(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "kit", null, mapOf("blob" to str("logo")))
        engine.saveRow("notes", "voice", null, mapOf("kit" to str("kit"), "blob" to str("sample")))

        released.land("logo")
        settled(engine, listOf("voice"))
        assertEquals("blob", engine.heldRows().first().gateId)

        released.land("sample")
        settled(engine, emptyList())
        engine.drain()
        assertEquals(listOf("kit", "voice"), transport.pushedBatches().flatten().map { it.rowId })
    }

    /**
     * The parent's create was still owed when backup turned off, while the
     * child already waited for its bytes: the rows the flip holds go ahead of
     * every earlier hold, so the child still leaves second.
     *
     * KILL: `holdJournal` — insert the moved rows with the next `seq`
     * (`seq = null`); the child then leaves ahead of the kit it names.
     */
    @Test fun aRowHeldBeforeTheFlipWaitsBehindAParentTheFlipHolds(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val backup = BackupFlag(on = true)
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(backup.gate, blobGate(released)))
        engine.saveRow("notes", "kit", null, mapOf("title" to str("kit")))
        engine.saveRow("notes", "voice", null, mapOf("kit" to str("kit"), "blob" to str("sample")))
        assertEquals(listOf("voice"), held(engine))

        backup.set(false)
        settled(engine, listOf("kit", "voice"))
        released.land("sample")
        eventually(message = "the child's landing never reached its hold") { engine.heldRows().lastOrNull()?.gateId == "backup" }

        backup.set(true)
        settled(engine, emptyList())
        engine.drain()
        assertEquals(listOf("kit", "voice"), transport.pushedBatches().flatten().map { it.rowId })
    }

    /**
     * Backup off holds the project, and lets the digest that names it
     * leave — the server works from it, under the user alone. A gate over
     * every stream is a policy: what it lets go does not wait for what it holds.
     *
     * KILL: `heldParent` — drop the `syncGates.orders(parent.gateId)` filter.
     */
    @Test fun aRowAPolicyGateLetsGoDoesNotWaitBehindWhatItHolds(): Unit = runTest {
        val store = Fixture.store()
        val backup = BackupFlag(on = false)
        val policy = TestGate(null, id = "backup", signal = backup.signal) { change ->
            if (backup.isOn || change.stream == "assets") SyncVerdict.Push else SyncVerdict.Gate("cloud backup off")
        }
        val engine = Fixture.engine(store, transport = StubTransport(), syncGates = listOf(policy))
        engine.saveRow("notes", "project", null, mapOf("title" to str("p")))

        engine.saveRow("assets", "digest", null, mapOf("projectId" to str("project")))

        assertEquals(listOf("project"), held(engine))
        assertEquals(listOf("digest"), store.peekPending().map { it.op().rowId })
    }

    /** KILL: `askAgain` — drop the birth's discard → push mapping. */
    @Test fun aBirthItsGatesDiscardWhenAskedAgainStillLeaves(): Unit = runTest {
        val transport = StubTransport()
        val backup = BackupFlag(on = false)
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(
            TestGate(null, id = "backup", signal = backup.signal) { if (backup.isOn) SyncVerdict.Discard else SyncVerdict.Gate("cloud backup off") },
        ))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))

        backup.set(true)
        settled(engine, emptyList())
        engine.drain()

        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE), transport.pushedBatches().flatten().map { it.verb })
    }

    // MARK: - backup off

    /**
     * Turning backup off takes what was still owed off the
     * journal; turning it on sends each row once, in the order it was owed.
     */
    @Test fun turningBackupOffMovesTheOwedJournalIntoHolds(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val backup = BackupFlag(on = true)
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(backup.gate))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))
        engine.saveRow("notes", "n2", null, mapOf("title" to str("c")))

        backup.set(false)
        settled(engine, listOf("n1", "n2"))
        assertEquals(0, store.peekPending().size)

        backup.set(true)
        settled(engine, emptyList())
        engine.drain()
        val sent = transport.pushedBatches().flatten()
        assertEquals(listOf("n1", "n2"), sent.map { it.rowId })
        assertEquals(listOf(ReplicaOp.Verb.ROW_CREATE, ReplicaOp.Verb.ROW_CREATE), sent.map { it.verb })
        assertEquals(mapOf("title" to str("b")), sent.first().data)
    }

    /**
     * An entry already on the wire may be committed server-side: it stays.
     *
     * KILL: `holdJournal` — take the frozen intents with the owed ones (`store.pending`).
     */
    @Test fun anEntryOnTheWireStaysWhenBackupTurnsOff(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val backup = BackupFlag(on = true)
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(backup.gate))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("on the wire")))
        val failure = HookOutcome()
        transport.onPush {
            try {
                engine.saveRow("notes", "n2", null, mapOf("title" to str("owed")))
                backup.set(false)
                until("the flip never took the owed entry") { held(engine) == listOf("n2") }
            } catch (error: Throwable) {
                failure.record(error)
            }
        }

        engine.drain()

        assertNull(failure.failure)
        assertEquals(listOf("n1"), transport.pushedBatches().flatten().map { it.rowId })
        assertEquals(0, store.peekPending().size, "the entry on the wire was acked, the owed one moved")
        assertEquals(listOf("n2"), held(engine))
    }

    // MARK: - open

    /** KILL: `gateChanged` — return early when `id` is null. */
    @Test fun openAsksEveryHoldAgain(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.unopenedEngine(Fixture.directory(), transport, syncGates = listOf(blobGate(released)))
        engine.open(1)
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))
        engine.close()
        released.landQuietly("k1")

        engine.open(1)

        settled(engine, emptyList())
        engine.drain()
        assertEquals(listOf("n1"), transport.pushedBatches().flatten().map { it.rowId })
    }

    /**
     * A gate that fires while the engine is sealed — an identity transition —
     * is not lost: unsealing asks the holds again.
     *
     * KILL: `unsealLocked` — drop `askHoldsAgain()`.
     */
    @Test fun aSignalWhileSealedIsHeardOnUnseal(): Unit = runTest {
        val released = ReleaseLedger()
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport(), syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))
        engine.seal()
        released.land("k1")
        realDelay(100)
        assertEquals(listOf("n1"), held(engine), "a sealed engine let a row go")

        engine.unseal()

        settled(engine, emptyList())
    }

    // MARK: - drafts

    /** A draft's writes meet the gates when it commits. */
    @Test fun aDraftsHeldRowsMoveIntoHoldsWhenItCommits(): Unit = runTest {
        val store = Fixture.store()
        val backup = BackupFlag(on = false)
        val engine = Fixture.engine(store, transport = StubTransport(), syncGates = listOf(backup.gate))
        val draft = engine.beginDraft {
            engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))
            engine.saveRow("notes", "n1", null, mapOf("title" to str("b")))
        }.draft
        assertEquals(2, store.peekDrafted().size)
        assertEquals(emptyList(), held(engine), "a draft was judged before it committed")

        engine.commitDraft(draft)

        assertEquals(listOf("n1"), held(engine))
        assertEquals(0, store.peekDrafted().size)
        assertEquals(0, store.peekPending().size)
    }

    @Test fun aDiscardedDraftLeavesNoHold(): Unit = runTest {
        val store = Fixture.store()
        val backup = BackupFlag(on = false)
        val engine = Fixture.engine(store, transport = StubTransport(), syncGates = listOf(backup.gate))
        val draft = engine.beginDraft { engine.saveRow("notes", "n1", null, mapOf("title" to str("a"))) }.draft

        engine.discardDraft(draft)

        assertEquals(emptyList(), held(engine))
        assertNull(store.peekSnapshot("notes", "n1"))
    }

    // MARK: - who else reads "owed"

    /**
     * Backup off: the server's world never had these rows, and a reset must
     * not erase the only copy.
     *
     * KILL: on a reset, delete the shard's snapshots the round did not deliver — the only copy goes.
     */
    @Test fun aResetKeepsHeldRows(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val backup = BackupFlag(on = false)
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(backup.gate))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("only copy")))

        transport.queuePull("user", ReplicaPullResponse(frames = emptyList(), cursor = "10:", more = false))
        engine.pullOnce()

        assertEquals(str("only copy"), store.peekSnapshot("notes", "n1")?.data?.get("title"))
        assertEquals(listOf("n1"), held(engine))
    }

    /** The staged-bytes keeping set: a held row is still the device's to send. */
    @Test fun pendingRowIdsIncludeHeldRows(): Unit = runTest {
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport(), syncGates = listOf(blobGate(ReleaseLedger())))
        engine.saveRow("notes", "held", null, mapOf("blob" to str("k1")))
        engine.saveRow("notes", "owed", null, mapOf("title" to str("a")))

        assertEquals(setOf("held", "owed"), engine.pendingRowIds("notes").toSet())
    }

    /** A project thrown away before the server heard of it must not come back when its gate opens. */
    @Test fun discardedHoldsNeverLeave(): Unit = runTest {
        val transport = StubTransport()
        val released = ReleaseLedger()
        val engine = Fixture.engine(Fixture.store(), transport = transport, syncGates = listOf(blobGate(released)))
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))

        engine.discardHolds("notes", listOf("n1"))
        released.land("k1")
        engine.settle()
        engine.drain()

        assertEquals(emptyList(), held(engine))
        assertEquals(0, transport.pushCount())
    }

    /**
     * A row waiting behind a discarded row could only be refused: it goes with it.
     *
     * KILL: `discardHolds` — drop only the named rows.
     */
    @Test fun discardingAHeldRowDiscardsTheRowsWaitingBehindIt(): Unit = runTest {
        val engine = Fixture.engine(Fixture.store(), transport = StubTransport(), syncGates = listOf(blobGate(ReleaseLedger())))
        engine.saveRow("notes", "kit", null, mapOf("blob" to str("logo")))
        engine.saveRow("notes", "voice", null, mapOf("kit" to str("kit")))
        engine.saveRow("notes", "take", null, mapOf("voice" to str("voice")))
        assertEquals(listOf("kit", "voice", "take"), held(engine))

        engine.discardHolds("notes", listOf("kit"))

        assertEquals(emptyList(), held(engine))
    }

    /**
     * A refused birth takes its row off the device — and the hold its later
     * writes waited in.
     *
     * KILL: `revert` — drop the `dropHold` after `discardEntries`.
     */
    @Test fun aRefusedBirthTakesItsHoldWithIt(): Unit = runTest {
        val store = Fixture.store()
        val transport = StubTransport()
        val engine = Fixture.engine(store, transport = transport, syncGates = listOf(blobGate(ReleaseLedger())))
        engine.saveRow("notes", "n1", null, mapOf("title" to str("a")))
        engine.saveRow("notes", "n1", null, mapOf("blob" to str("k1")))
        assertEquals(listOf("n1"), held(engine))

        transport.scriptPush { ops -> ops.map { ReplicaVerdict(it.id, ReplicaVerdict.Outcome.REJECTED, "no") } }
        engine.drain()

        assertNull(store.peekSnapshot("notes", "n1"))
        assertEquals(emptyList(), held(engine))
        assertEquals(1, store.peekParked().size)
        assertTrue(store.peekPending().isEmpty())
    }
}
