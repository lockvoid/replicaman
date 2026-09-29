package io.replicaman.loro.binding

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The binding against the REAL core — `native/loro/rust` built by
 * `native/loro/build.sh`, loaded through JNA from `build/host`. Nothing here
 * stubs Loro; a stubbed Loro would be the one thing these tests exist to catch.
 */
class LoroBindingTests {

    private fun doc(peer: ULong): LoroDoc = LoroDoc().apply {
        setRecordTimestamp(false)
        setPeerId(peer)
    }

    private fun deepMap(doc: LoroDoc): Map<String, LoroValue> =
        (doc.getDeepValue() as LoroValue.Map).value

    /**
     * A committed snapshot crosses into a second document and materializes
     * with the same deep value.
     *
     * Kill: a Loro whose `export` stops auto-committing the open transaction, or
     * whose `import` silently no-ops. NOT "drop the `commit()` call":
     * `export` commits for us, so the suite stays green without it. These five
     * grade the vendored core across a version bump, not our own code.
     */
    @Test
    fun `a committed snapshot imports into a second document`() {
        val source = doc(1u)
        source.getMap("meta").insert("name", LoroValue.String("Trip"))
        source.commit()

        val target = doc(2u)
        target.import(source.export(ExportMode.Snapshot))

        val meta = deepMap(target)["meta"] as LoroValue.Map
        assertEquals(LoroValue.String("Trip"), meta.value["name"])
        assertEquals(deepMap(source), deepMap(target))
    }

    /**
     * The version vector encodes, decodes, and answers "what has this peer not
     * seen" — the pair `ProjectDoc.exportDeltas(since:)` rides on.
     *
     * Kill: export against the SOURCE's own version vector instead of the
     * target's — the delta then carries no ops and `Renamed` never reaches the
     * target, which is exactly how an edit vanishes on the wire.
     */
    @Test
    fun `oplog version vector round trips and bounds an update export`() {
        val source = doc(1u)
        source.getMap("meta").insert("name", LoroValue.String("Trip"))
        source.commit()

        val encoded = source.oplogVv().encode()
        val decoded = VersionVector.decode(encoded)
        assertContentEquals(encoded, decoded.encode())
        assertEquals(mapOf(1uL to 1), decoded.toHashmap())

        val target = doc(2u)
        target.import(source.export(ExportMode.Snapshot))
        val stale = VersionVector.decode(target.oplogVv().encode())

        // Caught up: the export carries no ops, so importing it moves nothing.
        // (An updates export is never zero bytes — it always has an envelope.)
        val caughtUp = target.oplogVv().encode()
        target.import(source.export(ExportMode.Updates(VersionVector.decode(caughtUp))))
        assertContentEquals(caughtUp, target.oplogVv().encode())

        // …and the same vv, now stale, does carry the next write.
        source.getMap("meta").insert("name", LoroValue.String("Renamed"))
        source.commit()
        target.import(source.export(ExportMode.Updates(stale)))
        assertFalse(caughtUp.contentEquals(target.oplogVv().encode()))
        assertEquals(
            LoroValue.String("Renamed"),
            (deepMap(target)["meta"] as LoroValue.Map).value["name"],
        )
    }

    /**
     * Two peers writing different keys concurrently converge to one value once
     * each has the other's ops.
     *
     * Kill: skip one import direction — the two deep values stop being equal,
     * because convergence is a property of BOTH sides having both op sets.
     */
    @Test
    fun `two peers converge on concurrent map writes`() {
        val mine = doc(11u)
        val theirs = doc(22u)
        assertEquals(11uL, mine.peerId())
        assertEquals(22uL, theirs.peerId())

        mine.getMap("meta").insert("name", LoroValue.String("Mine"))
        mine.commit()
        theirs.getMap("meta").insert("note", LoroValue.String("Theirs"))
        theirs.commit()

        mine.import(theirs.export(ExportMode.Snapshot))
        theirs.import(mine.export(ExportMode.Snapshot))

        assertEquals(deepMap(mine), deepMap(theirs))
        val meta = deepMap(mine)["meta"] as LoroValue.Map
        assertEquals(LoroValue.String("Mine"), meta.value["name"])
        assertEquals(LoroValue.String("Theirs"), meta.value["note"])
    }

    /**
     * The undo manager inverts this peer's own edits, one recorded step at a
     * time, and offers the inverse back as a redo.
     *
     * Kill: undo twice — the manager recorded ONE step, so the second `undo()`
     * returns false rather than walking off the end of the stack.
     */
    @Test
    fun `undo manager inverts one recorded step and offers it back`() {
        val document = doc(11u)
        document.getMap("meta").insert("name", LoroValue.String("Before"))
        document.commit()

        val undo = UndoManager(document)
        undo.setMergeInterval(0)
        undo.setMaxUndoSteps(100u)

        document.getMap("meta").insert("name", LoroValue.String("After"))
        document.commit()

        assertTrue(undo.canUndo())
        assertTrue(undo.undo())
        val meta = deepMap(document)["meta"] as LoroValue.Map
        assertEquals(LoroValue.String("Before"), meta.value["name"])

        assertTrue(undo.canRedo())
        // The seed was written before recording started, so it is not ours to undo.
        assertFalse(undo.undo())
    }

    /**
     * `ensureMergeableMap` refuses a key already holding a plain value instead
     * of clobbering it — the guard `ProjectDoc.registryEntry` leans on.
     *
     * Kill: make the binding swallow `LoroException` and return the parent map;
     * the write then lands as top-level registry keys.
     */
    @Test
    fun `ensureMergeableMap refuses a key holding a plain value`() {
        val document = doc(11u)
        val clips = document.getMap("clips")
        clips.insert("clip-a", LoroValue.String("not-a-clip"))

        assertFailsWith<LoroException> { clips.ensureMergeableMap("clip-a") }

        // …and a free key still yields a real mergeable child.
        val healthy = clips.ensureMergeableMap("clip-b")
        healthy.insert("start", LoroValue.Double(1.0))
        document.commit()
        val written = (deepMap(document)["clips"] as LoroValue.Map).value["clip-b"] as LoroValue.Map
        assertEquals(LoroValue.Double(1.0), written.value["start"])
    }
}
