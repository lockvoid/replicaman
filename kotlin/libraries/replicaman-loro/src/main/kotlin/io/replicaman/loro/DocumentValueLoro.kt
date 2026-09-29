package io.replicaman.loro

import io.replicaman.DocumentValue

import io.replicaman.loro.binding.LoroValue
import io.replicaman.loro.binding.ValueOrContainer

/**
 * The one place `LoroValue` is translated. Kept beside the boundary rather
 * than inside `ProjectDoc` so the document file stays about document
 * SHAPE, and this stays about value REPRESENTATION.
 */
public fun documentValueOf(value: LoroValue): DocumentValue = when (value) {
    is LoroValue.Null -> DocumentValue.Null
    is LoroValue.Bool -> DocumentValue.Bool(value.value)
    is LoroValue.I64 -> DocumentValue.Int(value.value)
    is LoroValue.Double -> DocumentValue.Double(value.value)
    is LoroValue.String -> DocumentValue.String(value.value)
    is LoroValue.List -> DocumentValue.List(value.value.map(::documentValueOf))
    is LoroValue.Map -> DocumentValue.Map(value.value.mapValues { documentValueOf(it.value) })
    // A materialized projection never holds a container reference — the
    // deep value has already resolved them — and nothing in the timeline is
    // raw bytes. Both collapse to null rather than crashing a merge.
    is LoroValue.Binary, is LoroValue.Container -> DocumentValue.Null
}

public val DocumentValue.loroValue: LoroValue
    get() = when (this) {
        is DocumentValue.Null -> LoroValue.Null
        is DocumentValue.Bool -> LoroValue.Bool(value)
        is DocumentValue.Int -> LoroValue.I64(value)
        is DocumentValue.Double -> LoroValue.Double(value)
        is DocumentValue.String -> LoroValue.String(value)
        is DocumentValue.List -> LoroValue.List(value.map { it.loroValue })
        is DocumentValue.Map -> LoroValue.Map(value.mapValues { it.value.loroValue })
    }

/**
 * The plain value behind a map read, or nil when the key holds a
 * container (a registry child), which is never compared as a value.
 */
public val ValueOrContainer.documentValue: DocumentValue?
    get() = asValue()?.let(::documentValueOf)
