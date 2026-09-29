package io.replicaman

import org.junit.Test
import io.replicaman.generated.dummy.documents.from
import io.replicaman.generated.dummy.documents.projection
import io.replicaman.generated.app.SampleReplica
import io.replicaman.generated.dummy.Board
import io.replicaman.generated.dummy.Item
import io.replicaman.generated.dummy.ItemCaption
import io.replicaman.generated.dummy.Job
import io.replicaman.generated.dummy.JobPayload
import io.replicaman.generated.dummy.PhotoItem
import io.replicaman.generated.dummy.Replica
import io.replicaman.generated.dummy.Ticket
import java.io.File
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Codegen. The generated fixture models are CHECKED IN under
 * `src/test/kotlin/…/generated`; this test regenerates from the committed
 * manifests and asserts byte-identity (the SDL pattern — drift shows up in
 * review, never silently). Readonly streams have no write verbs at all —
 * compile-time by construction (`Job` conforms to `ReplicaRowModel` only, so
 * no `RowStream<Job, …>` can exist), asserted here at the type level.
 */
class CodegenTests {

    private val repoRoot: File
        get() = File(System.getProperty("replicaman.root"))

    private val packageRoot: File
        get() = File(repoRoot, "kotlin/libraries/replicaman")

    private fun regenerate(
        manifest: String,
        name: String,
        packageName: String,
        documents: Boolean = false,
        documentPackageName: String = "$packageName.documents",
        manifestSource: File? = null,
    ): Pair<File, File> {
        val root = File(
            System.getProperty("java.io.tmpdir"),
            "replica-codegen-${UUID.randomUUID()}"
        )
        val rows = File(root, "rows")
        val docs = File(root, "documents")
        val arguments = mutableListOf(
            "ruby", File(repoRoot, "codegen/bin/replica-codegen").path,
            "--config", File(repoRoot, "codegen/tests/fixtures/consumer-codegen.json").path,
            "--language", "kotlin",
            "--manifest", (manifestSource ?: File(packageRoot, "src/test/resources/$manifest")).path,
            "--out", rows.path,
            "--package", packageName,
            "--name", name,
        )
        if (documents) {
            arguments += listOf("--document-out", docs.path, "--document-package", documentPackageName)
        }
        val process = ProcessBuilder(arguments)
            .redirectErrorStream(true)
            .start()
        val noise = process.inputStream.bufferedReader().readText()
        val status = process.waitFor()
        if (status != 0) fail("replica-codegen failed ($status): $noise")
        return rows to docs
    }

    private fun assertGeneratedDirectory(fresh: File, committed: File) {
        val freshNames = fresh.list()?.sorted() ?: emptyList()
        val committedNames = committed.list()?.sorted() ?: emptyList()
        assertEquals(committedNames, freshNames, "the emitted file set drifted from ${committed.path}")

        for (name in committedNames) {
            assertEquals(
                File(committed, name).readText(),
                File(fresh, name).readText(),
                "$name drifted — regenerate the fixture and review the diff"
            )
        }
    }

    /**
     * The manifest's `indexes:` become ONE `Field` enum per model — its
     * indexed fields, raw value = wire name — and the container's index
     * specs; a model without indexes takes `ReplicaNoField`, so no predicate
     * over it can be formed.
     *
     * KILL: emit every column as a `Field` case — a predicate over an
     * unindexed field compiles and then fails at the store.
     */
    @Test
    fun indexesGenerateTheFieldEnumAndTheSchemaSpecs() {
        assertEquals("boardId", Item.Field.BOARD_ID.rawValue)
        assertEquals("note", Ticket.Field.NOTE.rawValue)
        assertEquals("userId", Ticket.Field.USER_ID.rawValue)
        // Compile-time: a model with no declared index takes `ReplicaNoField`,
        // so `ReplicaPredicate<Board.Field>` cannot be spelled at all.
        val boardHasNoFields: ReplicaDocModelType<Board, ReplicaNoField> = Board
        val jobHasNoFields: ReplicaRowModelType<Job, ReplicaNoField> = Job
        assertNotNull(boardHasNoFields)
        assertNotNull(jobHasNoFields)
        assertEquals(
            listOf(
                ReplicaIndexSpec("items", "boardId", ReplicaIndexKind.BTREE),
                ReplicaIndexSpec("tickets", "note", ReplicaIndexKind.FTS5),
                ReplicaIndexSpec("tickets", "userId", ReplicaIndexKind.BTREE),
            ),
            Replica.schema.indexes
        )
    }

    /** KILL: change any emitted line — the committed fixture stops matching. */
    @Test
    fun generatedFixtureIsByteIdenticalToARegeneration() {
        val (rows, _) = regenerate("manifest.json", "Replica", "io.replicaman.generated.dummy")
        assertGeneratedDirectory(
            rows,
            File(packageRoot, "src/test/kotlin/io/replicaman/generated/dummy")
        )
    }

    /** KILL: the same, for the consumer manifest — the sample replica's shapes. */
    @Test
    fun theAppManifestFixtureIsByteIdenticalToARegeneration() {
        assertEquals(
            File(repoRoot, "protocol/fixtures/consumer-manifest.json").readText(),
            File(packageRoot, "src/test/resources/app-manifest.json").readText(),
            "the fixture must remain the consumer manifest the dummy server declares",
        )
        val (rows, _) = regenerate("app-manifest.json", "SampleReplica", "io.replicaman.generated.app")
        assertGeneratedDirectory(
            rows,
            File(packageRoot, "src/test/kotlin/io/replicaman/generated/app")
        )
    }

    /**
     * KILL: emit the document half without its `companion object` — the
     * `Companion.from(documentFields)` extension it declares cannot attach.
     * The emitted text is graded against the committed document fixture:
     * `from(...)` overloads, no per-field enum codec, the inlined
     * `<Model>DocumentCoding` with its `Shape` descriptors.
     */
    @Test
    fun documentShapesEmitIntoTheirOwnRootWithConversionsAndDefaults() {
        val (_, documents) = regenerate(
            "manifest.json", "Replica", "io.replicaman.generated.dummy", documents = true
        )
        assertGeneratedDirectory(
            documents,
            File(packageRoot, "src/test/resources/generated/dummy/documents")
        )

        val value = io.replicaman.generated.dummy.documents.BoardDocument.from(
            io.replicaman.generated.dummy.documents.BoardProjection(meta = mapOf("name" to DocumentValue.String("Saved name")))
        )
        assertNotNull(value)
        assertEquals("Saved name", value.meta.name)
        assertEquals(DocumentValue.String("Saved name"), value.projection.meta["name"])
        assertEquals("Untitled", io.replicaman.generated.dummy.documents.BoardDefaults.meta.name)
    }

    /**
     * KILL: keep the typed projection as the only storage — an unknown
     * discriminator loses every key the build does not know.
     */
    @Test
    fun shapeBackedColumnKeepsLosslessRawJson() {
        val empty: ReplicaValue = ReplicaValue.Obj(emptyMap())
        val stateOnly = Job.from(
            "j1", null,
            mapOf(
                "payload" to empty,
                "priority" to ReplicaValue.Str("low"),
                "state" to ReplicaValue.Str("running"),
                "tags" to ReplicaValue.Arr(emptyList()),
                "userId" to ReplicaValue.Str("u1"),
            )
        )
        assertNotNull(stateOnly)
        assertEquals(empty, stateOnly.payloadJSON)
        assertNull(stateOnly.payload)

        val future: ReplicaValue = ReplicaValue.Obj(
            mapOf(
                "kind" to ReplicaValue.Str("future"),
                "opaque" to ReplicaValue.Num(7.0),
                "nested" to ReplicaValue.Obj(mapOf("deep" to ReplicaValue.Num(7.0))),
            )
        )
        val unknown = Job.from(
            "j2", null,
            mapOf(
                "payload" to future,
                "priority" to ReplicaValue.Str("high"),
                "state" to ReplicaValue.Str("done"),
                "tags" to ReplicaValue.Arr(listOf(ReplicaValue.Str("urgent"))),
                "userId" to ReplicaValue.Str("u1"),
            )
        )
        assertNotNull(unknown)
        val shared = unknown.payload as? JobPayload.Shared
            ?: fail("unknown discriminator must remain a typed unknown projection")
        assertEquals("future", shared.kind)
        assertEquals(future, unknown.payloadJSON)
        assertTrue(
            unknown.encode().isEmpty(),
            "a readonly stream's push set is empty — encode() can leak nothing up the wire"
        )
    }

    /** KILL: type an array column from its subtype alone — a list decodes as a scalar. */
    @Test
    fun arrayColumnGeneratesTypedList() {
        val job = Job.from(
            "j3", null,
            mapOf(
                "priority" to ReplicaValue.Str("low"),
                "state" to ReplicaValue.Str("queued"),
                "tags" to ReplicaValue.Arr(listOf(ReplicaValue.Str("alpha"), ReplicaValue.Str("beta"))),
                "userId" to ReplicaValue.Str("u1"),
            )
        )
        assertNotNull(job)
        assertEquals(
            listOf("alpha", "beta"), job.tags,
            "a Postgres array column decodes to a typed list — the subtype alone would declare a scalar"
        )

        // The encode half lives on a WRITABLE stream — a readonly model's
        // encode is empty by construction.
        val item = Item.from(
            "i1", "TextItem",
            mapOf(
                "boardId" to ReplicaValue.Str("b1"),
                "body" to ReplicaValue.Str("Required text"),
                "rank" to ReplicaValue.Str("a0"),
                "tags" to ReplicaValue.Arr(listOf(ReplicaValue.Str("alpha"), ReplicaValue.Str("beta"))),
            )
        )
        assertNotNull(item)
        assertEquals(
            ReplicaValue.Arr(listOf(ReplicaValue.Str("alpha"), ReplicaValue.Str("beta"))),
            item.encode()["tags"]
        )
    }

    /** KILL: emit `ReplicaWritableRowModel` for a readonly stream — write verbs appear. */
    @Test
    fun readonlyStreamModelsCarryNoWriteCapability() {
        assertFalse(
            ReplicaWritableRowModel::class.java.isAssignableFrom(Job::class.java),
            "a readonly stream's model must not be writable — codegen emits no write verbs for it"
        )
        assertFalse(ReplicaWritableRowModel::class.java.isAssignableFrom(Ticket::class.java))
        // The positive half is compile-time: this witness only accepts
        // writable models, so a codegen regression fails the BUILD.
        val writable: ReplicaWritableRowModelType<Item, Item.Field> = Item
        assertNotNull(writable)
    }

    /** Current generated Replica.swift. KILL: leave a row mutation on a read handle or grant a readonly/document tx write door. */
    @Test
    fun onlyTransactionsExposeRowWritesAndOnlyForWritableRowStreams() {
        val (rows, _) = regenerate("manifest.json", "Replica", "io.replicaman.generated.dummy")
        val source = File(rows, "Replica.kt").readText()
        assertTrue(source.contains("public val ReplicaTransaction.items: TransactionRows<Item, Item.Field>"))
        assertTrue(source.contains("get() = rows(Item)"))
        assertTrue(source.contains("public val ReplicaTransaction.jobs: TransactionReadonlyRows<Job, ReplicaNoField>"))
        assertTrue(source.contains("get() = readonlyRows(Job)"))
        assertTrue(source.contains("public fun <T> write(body: (ReplicaTransaction) -> T): T = engine.write(body)"))
        assertTrue(source.contains("public suspend fun <T> writeAsync(body: (ReplicaTransaction) -> T): T = engine.writeAsync(body)"))
        assertFalse(source.contains("ReplicaTransaction.boards"), "document adapter phase C is not in current source")
        for (handle in listOf(RowStream::class.java, ReadonlyRowStream::class.java, TransactionReadonlyRows::class.java)) {
            assertTrue(handle.declaredMethods.none { it.name in setOf("create", "update", "delete") }, handle.name)
        }
        assertEquals(setOf("create", "update", "delete"), TransactionRows::class.java.declaredMethods.map { it.name }.filter { it in setOf("create", "update", "delete") }.toSet())
    }

    /** KILL: emit the lane/readonly/shard from a default instead of the manifest. */
    @Test
    fun generatedSchemaMatchesTheManifest() {
        val schema = Replica.schema
        assertEquals(ReplicaStreamSpec.Lane.DOCUMENT, schema.spec("boards")?.lane)
        assertEquals("loro@1", schema.spec("boards")?.codec)
        assertEquals(ReplicaStreamSpec.Lane.ROW, schema.spec("items")?.lane)
        assertEquals(false, schema.spec("items")?.readonly)
        assertEquals(true, schema.spec("jobs")?.readonly)
        assertEquals(listOf("user"), schema.shards)
    }

    /** KILL: return the base variant for an unknown STI `type` — an unknown subtype masquerades. */
    @Test
    fun generatedTypedDecodeRoundTrips() {
        val photo = Item.from(
            "i1", "PhotoItem",
            mapOf(
                "boardId" to ReplicaValue.Str("b1"),
                "rank" to ReplicaValue.Str("a"),
                "caption" to ReplicaValue.Str("wide"),
                "width" to ReplicaValue.Num(3.0),
            )
        )
        assertEquals(
            PhotoItem(id = "i1", boardId = "b1", rank = "a", caption = ItemCaption.WIDE, width = 3),
            photo
        )
        assertEquals(ReplicaValue.Num(3.0), photo?.encode()?.get("width"))

        assertNull(
            Item.from(
                "i2", "HologramItem",
                mapOf("boardId" to ReplicaValue.Str("b1"), "rank" to ReplicaValue.Str("a"))
            ),
            "an unknown STI subtype decodes to nothing — stored raw, skipped typed"
        )
    }

    private fun regenerateSample(): Pair<File, File> = regenerate(
        "app-manifest.json", "SampleReplica", "io.replicaman.generated.app", documents = true,
        documentPackageName = "io.replicaman.generated.app.documents",
        manifestSource = File(repoRoot, "protocol/fixtures/consumer-manifest.json"),
    )

    /**
     * KILL: let the generated document create take `createdAt`/`userId` —
     * a caller could author provenance the engine owns.
     */
    @Test
    fun generatedDeckCreateAcceptsOnlyAuthoredFields() {
        val deck = File(regenerateSample().first, "Deck.kt").readText()

        assertTrue(
            deck.contains(
                "public suspend fun DocumentStream<Deck, Deck.Field>.create(" +
                    "id: String, changeSeq: Long, snapshot: ByteArray, documentPeer: ULong): Boolean"
            ),
            "the generated deck verb must accept the authored value, not timestamps, user provenance or a computed column"
        )
        assertFalse(deck.contains("stamp ="))
        assertFalse(deck.contains(".create(id: String, createdAt:"), "the engine stamps provenance; the verb must not offer it")
        assertFalse(deck.contains("\"slideCount\" to"), "a pull-only column never rides a birth's data")
    }

    /** KILL: drop the standard stamp from the container spec — no one fills the deck's provenance. */
    @Test
    fun theContainerSpecStampsOnlyTheStandardDocument() {
        val container = File(regenerateSample().first, "SampleReplica.kt").readText()

        assertTrue(container.contains("ReplicaStreamSpec(name = \"decks\", lane = ReplicaStreamSpec.Lane.DOCUMENT, readonly = false, shard = \"user\", codec = \"loro@1\", stamp = ReplicaStamp.standard)"))
        assertTrue(
            container.contains("ReplicaStreamSpec(name = \"boards\", lane = ReplicaStreamSpec.Lane.DOCUMENT, readonly = false, shard = \"user\", codec = \"loro@1\", reflections = listOf(ReplicaReflection(field = \"name\", path = listOf(\"meta\", \"name\")))),"),
            "a reflected document scalar rides the spec, and a document without the standard columns carries no stamp",
        )
    }

    @Test
    fun sampleDocumentRootsAreTypedRegistriesAndObjects() {
        val deck = File(regenerateSample().second, "DeckDocument.kt").readText()

        assertTrue(deck.contains("public data class DeckDocument("))
        assertTrue(deck.contains("public val slides: List<Slide>"))
        assertTrue(deck.contains("public val layouts: List<Layout>"))
        assertTrue(deck.contains("public val settings: Settings"))
        assertTrue(deck.contains("public enum class TextDecoration(public val rawValue: String)"))
        assertTrue(deck.contains("public val textDecoration: List<TextDecoration>?"))
        assertTrue(deck.contains("public val fillColor: String?"), "hex_color is a declared string scalar, not raw ReplicaValue")
    }

    @Test
    fun sampleDocumentDefaultsAreTypedLiterals() {
        val defaults = File(regenerateSample().second, "DeckDefaults.kt").readText()

        assertTrue(defaults.contains("public object DeckDefaults"))
        assertTrue(defaults.contains("public val layouts: List<DeckDocument.Layout>"))
        assertTrue(defaults.contains("public val settings: DeckDocument.Settings = DeckDocument.Settings("))
    }

    /** KILL: emit a jsonb column as raw `ReplicaValue` only — the typed accessor disappears. */
    @Test
    fun aRequiredMapShapeIsATypedNonNullColumn() {
        val workflow = File(regenerateSample().first, "Workflow.kt").readText()

        assertTrue(workflow.contains("public data class WorkflowGraph("))
        assertTrue(workflow.contains("public val steps: Map<String, Steps>"))
        assertTrue(workflow.contains("public val links: Map<String, Links>"))
        assertTrue(workflow.contains("public var graph: WorkflowGraph,"))
        assertTrue(workflow.contains("ReplicaValueCoding.decode(WorkflowGraph.serializer(), raw)"))
        assertTrue(workflow.contains("val decodedGraph ="))
        assertFalse(workflow.contains("graphJSON"))
    }

    @Test
    fun aDiscriminatedColumnKeepsItsRawAndTypedSurfaces() {
        val export = File(regenerateSample().first, "Export.kt").readText()

        assertTrue(export.contains("public val colorSpaceValue: ColorSpace?"), "the generic enum metadata remains available as a typed accessor")
        assertTrue(
            export.contains("public var result: ExportResult? = ExportResult.fromReplicaValue(resultJSON)"),
            "the discriminated projection materializes in the constructor, as Swift's memberwise init does"
        )
        assertFalse(export.contains("model.resultJSON = resultJSON"), "a property initialiser never runs the custom setter, so the raw column needs no restoring")
        assertTrue(export.contains("public val colorSpace: String? = null"), "a discriminated variant keeps its established raw-value surface")
    }

    /** KILL: emit a shared variant vocabulary per variant — two top-level enums with one name. */
    @Test
    fun collidingVariantsTakeTheirConfiguredNamesAndShareOneVocabulary() {
        val template = File(regenerateSample().first, "ItemTemplate.kt").readText()

        assertTrue(template.contains("public data class PhotoItemTemplate("))
        assertTrue(template.contains("override val wireType: String = \"PhotoItem\""), "only the emitted name moves — the wire type stays the manifest's")
        assertEquals(1, template.split("public enum class ItemTemplateTone(").size - 1, "a vocabulary two variants share is emitted once")
    }

    /** KILL: change any emitted line of the sample contract — the committed fixtures stop matching. */
    @Test
    fun theSampleContractIsByteIdenticalToARegeneration() {
        val (rows, documents) = regenerateSample()

        assertGeneratedDirectory(rows, File(packageRoot, "src/test/kotlin/io/replicaman/generated/app"))
        assertGeneratedDirectory(documents, File(packageRoot, "src/test/generated-documents"))
    }

    /** KILL: emit the container's handles as stored properties — the engine binding goes stale. */
    @Test
    fun theContainerHandsOutHandlesOverTheLiveEngine() {
        val app = File(packageRoot, "src/test/kotlin/io/replicaman/generated/app/SampleReplica.kt").readText()

        assertTrue(app.contains("get() = RowStream(engine, Item)"))
        assertTrue(app.contains("get() = ReadonlyRowStream(engine, Workflow)"))
        assertTrue(app.contains("get() = DocumentStream(engine, Deck)"))
        assertEquals(SampleReplica.schema.shards.toSet(), setOf("user", "global"))
    }
}
