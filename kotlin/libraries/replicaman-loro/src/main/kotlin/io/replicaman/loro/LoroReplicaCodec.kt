package io.replicaman.loro

import io.replicaman.loro.binding.ExportMode
import io.replicaman.loro.binding.LoroDoc
import io.replicaman.loro.binding.LoroException
import io.replicaman.loro.binding.UndoManager
import io.replicaman.loro.binding.VersionVector
import io.replicaman.loro.binding.decodeImportBlobMeta
import io.replicaman.DocumentCodec
import io.replicaman.ReplicaError
import io.replicaman.ReplicaMerge
import io.replicaman.ReplicaMan
import io.replicaman.ReplicaReflection
import io.replicaman.ReplicaValue
import io.replicaman.loro.binding.LoroValue
import io.replicaman.loro.binding.LoroMap
import io.replicaman.loro.binding.Frontiers
import io.replicaman.loro.binding.getMap
import io.replicaman.loro.binding.insert

/**
 * The loro codec plugin — the ONLY module beside `:doc` that imports Loro
 * (module-graph boundary). Same crate pin as the
 * server's `vendor/loro-ruby`; the cross-platform golden fixtures are what
 * prove the bindings agree.
 *
 * Folds and payloads stay opaque `ByteArray` at the seam: every operation
 * loads, works, exports. Loro PARKS an update whose causal deps are missing
 * instead of failing — unguarded that reports success and silently never
 * applies the edit, so `merge` refuses it (`MissingCausalDeps`), matching
 * the server codec's refusal.
 */
public class LoroReplicaCodec : DocumentCodec<LoroDocument> {
    override val codecName: String get() = CODEC_NAME

    override val name: String get() = CODEC_NAME

    override fun merge(fold: ByteArray?, payload: ByteArray, reflections: List<ReplicaReflection>): ReplicaMerge {
        val doc = load(fold)
        val status = try {
            doc.import(payload)
        } catch (error: Exception) {
            throw ReplicaError.Codec("import failed: $error")
        }
        if (!status.pending.isNullOrEmpty()) throw ReplicaError.MissingCausalDeps
        return ReplicaMerge(export(doc), reflections.associate { it.field to value(it.path, doc) })
    }

    override fun diff(fold: ByteArray, since: ByteArray?): ByteArray {
        val doc = load(fold)
        val from = since?.let(::versionVector) ?: VersionVector()
        return try {
            doc.export(ExportMode.Updates(from))
        } catch (error: Exception) {
            throw ReplicaError.Codec("export updates failed: $error")
        }
    }

    override fun version(fold: ByteArray): ByteArray = load(fold).oplogVv().encode()

    override fun payloadVersion(payload: ByteArray): ByteArray = try {
        decodeImportBlobMeta(payload, false).partialEndVv.encode()
    } catch (error: Exception) {
        throw ReplicaError.Codec("blob meta unreadable: $error")
    }

    override fun mergeVersions(a: ByteArray?, b: ByteArray): ByteArray {
        val merged = a?.let(::versionVector) ?: VersionVector()
        try {
            merged.extendToIncludeVv(VersionVector.decode(b))
        } catch (error: Exception) {
            throw ReplicaError.Codec("version vector unreadable: $error")
        }
        return merged.encode()
    }

    override fun isEmptyDiff(payload: ByteArray): Boolean {
        return try {
            decodeImportBlobMeta(payload, false).changeNum == 0u
        } catch (error: Exception) {
            ReplicaMan.logger.error("[loro] payload metadata could not be read — judged by its size: $error")
            payload.isEmpty()
        }
    }

    // MARK: - The live half (DocumentCodec)

    override fun open(fold: ByteArray?, peer: ULong): LoroDocument {
        val doc = load(fold)
        doc.setPeerId(peer)
        return LoroDocument(doc)
    }

    override fun snapshot(document: LoroDocument): ByteArray = export(document.doc)

    override fun documentVersion(document: LoroDocument): ByteArray {
        document.doc.commit()
        return document.doc.oplogVv().encode()
    }

    override fun exportDelta(document: LoroDocument, since: ByteArray): ByteArray {
        val from = versionVector(since) ?: VersionVector()
        return try {
            document.doc.export(ExportMode.Updates(from))
        } catch (error: Exception) {
            throw ReplicaError.Codec("export updates failed: $error")
        }
    }

    override fun importDeltas(document: LoroDocument, payloads: List<ByteArray>) {
        val blobs = payloads.filter { it.isNotEmpty() }
        if (blobs.isEmpty()) return
        val status = try {
            document.doc.importBatch(blobs)
        } catch (error: Exception) {
            throw ReplicaError.Codec("import failed: $error")
        }
        if (!status.pending.isNullOrEmpty()) throw ReplicaError.MissingCausalDeps
    }

    override fun peer(document: LoroDocument): ULong = document.doc.peerId()

    override fun canUndo(document: LoroDocument): Boolean = document.undo.canUndo()

    override fun canRedo(document: LoroDocument): Boolean = document.undo.canRedo()

    override fun undo(document: LoroDocument): Boolean {
        document.doc.commit()
        return try {
            document.undo.undo()
        } catch (error: Exception) {
            throw ReplicaError.Codec("undo failed: $error")
        }
    }

    override fun redo(document: LoroDocument): Boolean {
        document.doc.commit()
        return try {
            document.undo.redo()
        } catch (error: Exception) {
            throw ReplicaError.Codec("redo failed: $error")
        }
    }

    override fun write(value: ReplicaValue, path: List<String>, document: LoroDocument) {
        val root = path.firstOrNull() ?: return
        val key = path.lastOrNull() ?: return
        try {
            var map = document.doc.getMap(root)
            for (name in path.drop(1).dropLast(1)) map = map.getOrCreateMapContainer(name, LoroMap())
            map.insert(key, value.loroValue)
            document.doc.commit()
        } catch (error: Exception) { throw ReplicaError.Codec("write failed: $error") }
    }

    private fun value(path: List<String>, doc: LoroDoc): ReplicaValue {
        val root = path.firstOrNull() ?: return ReplicaValue.Null
        val key = path.lastOrNull() ?: return ReplicaValue.Null
        var map = doc.getMap(root)
        for (name in path.drop(1).dropLast(1)) map = map.get(name)?.asLoroMap() ?: return ReplicaValue.Null
        return map.get(key)?.asValue()?.replicaValue ?: ReplicaValue.Null
    }

    // MARK: - Plumbing

    private fun load(fold: ByteArray?): LoroDoc {
        val doc = LoroDoc()
        // Merge order is peer/lamport, never a wall clock; recording
        // timestamps would also make fold bytes non-deterministic.
        doc.setRecordTimestamp(false)
        if (fold == null || fold.isEmpty()) return doc
        val status = try {
            doc.import(fold)
        } catch (error: Exception) {
            throw ReplicaError.Codec("fold unreadable: $error")
        }
        if (!status.pending.isNullOrEmpty()) throw ReplicaError.MissingCausalDeps
        return doc
    }

    private fun export(doc: LoroDoc): ByteArray = try {
        doc.export(ExportMode.Snapshot)
    } catch (error: Exception) {
        throw ReplicaError.Codec("export snapshot failed: $error")
    }

    public companion object {
        public const val CODEC_NAME: String = "loro@1"

        /** An unreadable cursor costs a longer diff, never a lost edit. */
        internal fun versionVector(bytes: ByteArray): VersionVector? = try {
            VersionVector.decode(bytes)
        } catch (error: LoroException.DecodeVersionVectorException) {
            ReplicaMan.logger.error("[loro] version vector could not be decoded — diffing from empty: $error")
            null
        }


    }
}

/**
 * The document the engine holds for `loro@1`: the loro doc plus its
 * collaborative undo — Loro's own manager, which inverts only THIS peer's
 * ops and rebases the inversion over whatever arrived meanwhile.
 */
public class LoroDocument internal constructor(public val doc: LoroDoc) {
    public val frontiers: ByteArray get() { doc.commit(); return doc.oplogFrontiers().encode() }

    public fun revert(frontiers: ByteArray) {
        doc.commit()
        try { doc.revertTo(Frontiers.decode(frontiers)) }
        catch (error: Exception) { throw ReplicaError.Codec("revert failed: $error") }
    }

    public fun firstMatch(addresses: List<ByteArray>): Int? {
        val here = frontiers
        val now = doc.getDeepValue()
        return addresses.indices.firstOrNull { index ->
            addresses[index].contentEquals(here) || fork(addresses[index])?.use { it.getDeepValue() } == now
        }
    }

    public fun fork(frontiers: ByteArray): LoroDoc? {
        doc.commit()
        return Frontiers.decode(frontiers).use { point ->
            if (doc.frontiersToVv(point)?.use { true } != true) return null
            try {
                doc.forkAt(point)
            } catch (_: LoroException.FrontiersNotFound) {
                // A valid address can be outside the retained branch.
                null
            } catch (_: LoroException.SwitchToVersionBeforeShallowRoot) {
                // This valid address predates the retained shallow history.
                null
            }
        }
    }

    public fun differingRoots(frontiers: ByteArray): List<String>? {
        val past = fork(frontiers) ?: return null
        return past.use {
            val now = doc.getDeepValue()
            val was = it.getDeepValue()
            if (now !is LoroValue.Map || was !is LoroValue.Map) {
                if (now == was) emptyList() else listOf("(root)")
            } else {
                (now.value.keys + was.value.keys).filter { key -> now.value[key] != was.value[key] }.sorted()
            }
        }
    }

    internal val undo: UndoManager = UndoManager(doc).apply {
        // One undo step per commit: a commit IS the boundary of one user
        // action, so coalescing by time would merge two distinct actions.
        setMergeInterval(0)
        setMaxUndoSteps(100u)
    }
}

private val LoroValue.replicaValue: ReplicaValue get() = when (this) {
    is LoroValue.Null, is LoroValue.Binary, is LoroValue.Container -> ReplicaValue.Null
    is LoroValue.Bool -> ReplicaValue.Bool(value)
    is LoroValue.I64 -> ReplicaValue.signedInteger(value)
    is LoroValue.Double -> ReplicaValue.Num(value)
    is LoroValue.String -> ReplicaValue.Str(value)
    is LoroValue.List -> ReplicaValue.Arr(value.map { it.replicaValue })
    is LoroValue.Map -> ReplicaValue.Obj(value.mapValues { it.value.replicaValue })
}

private val ReplicaValue.loroValue: LoroValue get() = when (this) {
    ReplicaValue.Null -> LoroValue.Null
    is ReplicaValue.Bool -> LoroValue.Bool(value)
    is ReplicaValue.Integer -> LoroValue.I64(value)
    is ReplicaValue.Num -> if (value.isFinite() && value >= Long.MIN_VALUE.toDouble() && value < 9223372036854775808.0 && value.toLong().toDouble() == value) LoroValue.I64(value.toLong()) else LoroValue.Double(value)
    is ReplicaValue.Str -> LoroValue.String(value)
    is ReplicaValue.Arr -> LoroValue.List(values.map { it.loroValue })
    is ReplicaValue.Obj -> LoroValue.Map(fields.mapValues { it.value.loroValue })
}
