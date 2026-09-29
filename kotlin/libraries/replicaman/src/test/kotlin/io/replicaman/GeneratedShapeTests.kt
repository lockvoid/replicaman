package io.replicaman

import org.junit.Test
import io.replicaman.generated.dummy.JobPayload
import kotlin.test.assertIs
import io.replicaman.generated.dummy.Job
import io.replicaman.generated.dummy.JobPriority
import io.replicaman.generated.dummy.JobState
import io.replicaman.generated.dummy.JobSummary
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * The emitted shape surface, exercised rather than string-matched. Every law
 * here shipped broken in the first cut of the emitter and is invisible to
 * `CodegenTests`, which compares emitted TEXT against a committed fixture —
 * a fixture regenerated from the same wrong emitter matches itself.
 */
class GeneratedShapeTests {
    /**
     * A wire `null` on a nullable enum must null that FIELD, not the whole
     * projection. `fromReplicaValue` wraps the decode in `runCatching`, so a
     * throw there is silent, total data loss.
     *
     * KILL: emit `EnumName.fromRaw(decoder.decodeString())` for the nullable
     * branch of `shape_enum_codec_source` — `decodeString()` on a `Null`
     * throws and the whole `JobSummary` reads back as null.
     */
    @Test
    fun anExplicitWireNullOnAnEnumFieldKeepsTheRestOfTheShape() {
        val summary = JobSummary.fromReplicaValue(
            ReplicaValue.Obj(
                mapOf("active" to ReplicaValue.Bool(true), "state" to ReplicaValue.Null)
            )
        )
        assertNotNull(summary, "an explicit null enum destroyed the whole projection")
        assertEquals(true, summary.active)
        assertNull(summary.state)
    }

    /**
     * A shape enum reached through a List or a Map has no per-field codec —
     * kotlinx falls back to the ENTRY name unless the entries carry
     * `@SerialName`. The entry name is SCREAMING_SNAKE; the wire says
     * `system`.
     *
     * KILL: drop `@Serializable` + `@SerialName` from `shape_enum_source` —
     * the wire value stops decoding and the entry name starts encoding, so
     * the client reads nothing and writes `SYSTEM` up the wire.
     */
    @Test
    fun aShapeEnumInsideAListSpeaksTheWireValueNotTheKotlinEntryName() {
        val decoded = JobSummary.fromReplicaValue(
            ReplicaValue.Obj(
                mapOf("labels" to ReplicaValue.Arr(listOf(ReplicaValue.Str("system"))))
            )
        )
        assertEquals(listOf(JobSummary.Labels.SYSTEM), decoded?.labels)

        assertNull(
            JobSummary.fromReplicaValue(
                ReplicaValue.Obj(
                    mapOf("labels" to ReplicaValue.Arr(listOf(ReplicaValue.Str("SYSTEM"))))
                )
            )?.labels,
            "the Kotlin entry name is not a wire value and must not decode"
        )

        assertEquals(
            ReplicaValue.Arr(listOf(ReplicaValue.Str("system"))),
            JobSummary(labels = listOf(JobSummary.Labels.SYSTEM)).replicaValue["labels"],
        )
    }

    /**
     * Swift's memberwise init runs `self.payload = JobPayload(replicaValue:)`
     * and `didSet` does not fire during init. Kotlin's twin is a property
     * INITIALISER — which also never runs the custom setter, so the raw
     * column keeps every key an unknown variant arrived with.
     *
     * KILL: emit `public var payload: JobPayload? = null` and materialize
     * from `from()` instead — every path that is not `from()` (the
     * constructor, `copy()`, `equals`) loses the projection.
     */
    @Test
    fun theMaterializedProjectionSurvivesTheConstructorAndCopy() {
        val job = Job(
            id = "j1",
            payloadJSON = ReplicaValue.Obj(mapOf("kind" to ReplicaValue.Str("empty"))),
            priority = JobPriority.LOW,
            state = JobState.QUEUED,
            tags = emptyList(),
            userId = "u1",
        )
        assertIs<JobPayload.Empty>(job.payload, "the constructor dropped or changed the literal empty variant")
        assertIs<JobPayload.Empty>(job.copy().payload, "copy() dropped or changed the variant")
        job.payload = JobPayload.Shared(kind = "future-after-edit")
        assertEquals(
            ReplicaValue.Obj(mapOf("kind" to ReplicaValue.Str("future-after-edit"))),
            job.payloadJSON,
            "editing the materialized value must update the exact raw column",
        )
        assertEquals("future-after-edit", assertIs<JobPayload.Shared>(job.copy().payload).kind)
    }
}
