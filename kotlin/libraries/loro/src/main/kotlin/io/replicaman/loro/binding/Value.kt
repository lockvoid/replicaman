package io.replicaman.loro.binding

/**
 * The twin of loro-swift's `Sources/Loro/Value.swift`.
 *
 * Swift retrofits `LoroValueLike` onto the generated `LoroValue` with an
 * extension; Kotlin cannot add a supertype to a generated class, so a value is
 * boxed at the call site instead.
 */
class LoroValueBox(private val value: LoroValue) : LoroValueLike {
    override fun `asLoroValue`(): LoroValue = value
}
