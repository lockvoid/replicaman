package io.replicaman.loro

import org.junit.Test
import io.replicaman.loro.binding.ExportMode
import io.replicaman.loro.binding.VersionVector
import io.replicaman.loro.binding.LoroMap
import io.replicaman.loro.binding.LoroValue
import io.replicaman.loro.binding.getMap
import io.replicaman.loro.binding.insert
import io.replicaman.ReplicaError
import io.replicaman.ReplicaReflection
import io.replicaman.ReplicaValue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The codec seam itself: merge refuses missing causal deps (the server
 * codec's refusal, mirrored), version arithmetic reads blob metadata
 * without applying, and empty diffs are recognizably empty.
 */
class LoroCodecTests {
    private val codec = LoroReplicaCodec()

    @Test
    fun mergeReadsTheReflectedPathsOffTheMergedDocument() {
        val author = LoroFixture.doc(peer = 9uL)
        LoroFixture.setMeta(author, "name", "Plans")
        author.getMap("settings").getOrCreateMapContainer("export", LoroMap()).insert("fps", LoroValue.I64(30L))
        author.commit()
        val merged = codec.merge(null, author.export(ExportMode.Snapshot), listOf(
            ReplicaReflection("name", listOf("meta", "name")),
            ReplicaReflection("fps", listOf("settings", "export", "fps")),
            ReplicaReflection("cover", listOf("meta", "cover")),
        ))
        assertEquals(mapOf("name" to ReplicaValue.Str("Plans"), "fps" to ReplicaValue.Num(30.0), "cover" to ReplicaValue.Null), merged.reflected)
        assertEquals("Plans", LoroFixture.meta(merged.fold, "name"))
    }

    @Test
    fun aWriteReadsBackThroughItsPath() {
        val document = codec.open(null, 9uL)
        codec.write(ReplicaValue.Str("Plans"), listOf("meta", "name"), document)
        codec.write(ReplicaValue.Num(30.0), listOf("settings", "export", "fps"), document)
        val merged = codec.merge(null, codec.snapshot(document), listOf(
            ReplicaReflection("name", listOf("meta", "name")),
            ReplicaReflection("fps", listOf("settings", "export", "fps")),
        ))
        assertEquals(mapOf("name" to ReplicaValue.Str("Plans"), "fps" to ReplicaValue.Num(30.0)), merged.reflected)
    }

    /** A real detached document has an undo step but refuses editing; false would hide the failure. */
    @Test
    fun aRefusedUndoThrows() {
        val document = codec.open(null, 9uL)
        codec.write(ReplicaValue.Str("a"), listOf("meta", "name"), document)
        codec.write(ReplicaValue.Str("b"), listOf("meta", "name"), document)
        document.doc.detach()
        assertTrue(document.doc.isDetached())
        assertTrue(codec.canUndo(document))

        assertFailsWith<ReplicaError.Codec> { codec.undo(document) }
    }

    @Test
    fun aRefusedRedoThrows() {
        val document = codec.open(null, 9uL)
        codec.write(ReplicaValue.Str("a"), listOf("meta", "name"), document)
        codec.write(ReplicaValue.Str("b"), listOf("meta", "name"), document)
        assertTrue(codec.undo(document))
        document.doc.detach()
        assertTrue(document.doc.isDetached())
        assertTrue(codec.canRedo(document))

        assertFailsWith<ReplicaError.Codec> { codec.redo(document) }
    }

    /** Kill: ignore `ImportStatus.pending` in `merge` — the second delta is parked silently and reports success. */
    @Test
    fun mergeRefusesAPayloadWithUnseenDeps() {
        val author = LoroFixture.doc(peer = 9uL)
        val first = LoroFixture.editPayload(author, "a", "1")
        val second = LoroFixture.editPayload(author, "b", "2")

        assertFailsWith<ReplicaError.MissingCausalDeps>("a delta depending on unseen changes must be refused, not parked silently") {
            codec.merge(fold = null, payload = second)
        }

        val fold = codec.merge(fold = codec.merge(fold = null, payload = first).fold, payload = second).fold
        assertEquals("1", LoroFixture.meta(fold, "a"))
        assertEquals("2", LoroFixture.meta(fold, "b"))
    }

    /** Kill: append the payload's ops without Loro's dedup (a stub-style concat) — the re-apply doubles the edit's history. */
    @Test
    fun mergeIsIdempotentAcrossReplays() {
        val author = LoroFixture.doc(peer = 9uL)
        val payload = LoroFixture.editPayload(author, "a", "1")

        val once = codec.merge(fold = null, payload = payload).fold
        val twice = codec.merge(fold = once, payload = payload).fold
        assertEquals("1", LoroFixture.meta(twice, "a"), "loro re-apply is harmless on retry")
        assertEquals(codec.version(once).toList(), codec.version(twice).toList(), "a replay moves no version")
    }

    /** Kill: read `partialStartVv` instead of `partialEndVv` in `payloadVersion` — the union no longer includes either payload. */
    @Test
    fun payloadVersionAndMergeVersionsUnion() {
        val alice = LoroFixture.doc(peer = 9uL)
        val payloadA = LoroFixture.editPayload(alice, "a", "1")
        val bob = LoroFixture.doc(peer = 3uL)
        val payloadB = LoroFixture.editPayload(bob, "b", "2")

        val union = codec.mergeVersions(codec.payloadVersion(payloadA), codec.payloadVersion(payloadB))
        val decoded = VersionVector.decode(union)
        assertTrue(decoded.includesVv(alice.oplogVv()), "the union includes the independent author's full history")
        assertTrue(decoded.includesVv(bob.oplogVv()), "both authors must be covered, not the codec's own decoded expectations")
    }

    /**
     * The `since` version comes from the AUTHORING doc's own `oplogVv()`, not
     * from `codec.version(fold)` — otherwise the codec supplies both the
     * input and the expectation and a matching pair of bugs is invisible.
     * This is also the shape the drain actually uses: acked advances by a
     * version derived from the bytes the SERVER acknowledged, never by
     * re-reading our own fold.
     *
     * Kill: make `isEmptyDiff` answer on `payload.isEmpty()` alone — a diff
     * since everything is a non-empty blob with zero changes and reads as owed.
     */
    @Test
    fun emptyDiffIsRecognized() {
        val author = LoroFixture.doc(peer = 9uL)
        LoroFixture.setMeta(author, "a", "1")
        val fold = author.export(ExportMode.Snapshot)
        val serverHasEverything = author.oplogVv().encode()

        val nothing = codec.diff(fold, since = serverHasEverything)
        assertTrue(codec.isEmptyDiff(nothing), "a diff since everything must carry no changes")

        val everything = codec.diff(fold, since = null)
        assertFalse(codec.isEmptyDiff(everything))

        // And the discriminator: an edit made AFTER that version is owed.
        val payload = LoroFixture.editPayload(author, "b", "2")
        val owed = codec.diff(codec.merge(fold, payload).fold, since = serverHasEverything)
        assertFalse(codec.isEmptyDiff(owed), "a new edit must be owed against the version that predates it")
    }
}
