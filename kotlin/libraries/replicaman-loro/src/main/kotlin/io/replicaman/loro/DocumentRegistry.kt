package io.replicaman.loro

import io.replicaman.DocumentEntry
import io.replicaman.DocumentValue
import io.replicaman.isNull
import io.replicaman.loro.binding.LoroDoc
import io.replicaman.loro.binding.LoroMap
import io.replicaman.loro.binding.getMap
import io.replicaman.loro.binding.insert

/**
 * Write a keyed registry relative to the author's read. Unseen additions survive,
 * deleted entries stay deleted, and unchanged fields do not override peer edits.
 * A null base is authoritative: omitted live keys are deleted.
 *
 * These helpers do not commit. Use them inside ReplicaMan's updateDoc closure;
 * any refused write throws so the engine can roll back the entire action. A
 * caller owning a raw document must discard it after a failed action.
 */
public fun LoroDoc.writeRegistry(
    root: String,
    entries: List<DocumentEntry>,
    base: List<DocumentEntry>? = null,
) {
    val registry = getMap(root)
    val live = registry.keys().toSet()
    val keyed = entries.associateBy { it.key }
    val seen = base?.associateBy { it.key }
    for (stale in (seen?.keys ?: live) - keyed.keys) registry.delete(stale)
    for ((key, entry) in keyed) {
        if (seen?.containsKey(key) == true && key !in live) continue
        val child = registry.ensureMergeableMap(key)
        writeFields(child, entry.fields, seen?.get(key)?.fields)
    }
}

/** An entry is a mergeable child; a plain value at its key refuses the edit. */
public fun LoroDoc.writeEntryField(root: String, key: String, field: String, value: DocumentValue) {
    writeField(getMap(root).ensureMergeableMap(key), field, value)
}

/** Fields omitted by this writer are left alone. */
public fun LoroDoc.writeFields(root: String, fields: Map<String, DocumentValue>, base: Map<String, DocumentValue>? = null) {
    writeFields(getMap(root), fields, base)
}

public fun LoroDoc.writeMapField(root: String, field: String, value: DocumentValue) {
    writeField(getMap(root), field, value)
}

/** Birth preserves explicit nulls; edits suppress null writes to absent fields. */
public fun LoroDoc.initializeRegistry(root: String, entries: List<DocumentEntry>) {
    val registry = getMap(root)
    for (entry in entries) initializeFields(registry.ensureMergeableMap(entry.key), entry.fields)
}

public fun LoroDoc.initializeFields(root: String, fields: Map<String, DocumentValue>) {
    initializeFields(getMap(root), fields)
}

private fun initializeFields(map: LoroMap, fields: Map<String, DocumentValue>) {
    for ((field, value) in fields) map.insert(field, value.loroValue)
}

private fun writeFields(map: LoroMap, fields: Map<String, DocumentValue>, base: Map<String, DocumentValue>?) {
    for ((field, value) in fields) {
        if (base?.get(field) == value) continue
        writeField(map, field, value)
    }
}

private fun writeField(map: LoroMap, field: String, value: DocumentValue) {
    val current = map.get(field)
    if (value.isNull && current == null) return
    if (current?.documentValue == value) return
    map.insert(field, value.loroValue)
}
