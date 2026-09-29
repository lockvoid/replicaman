package io.replicaman.loro.binding

/**
 * The twin of loro-swift's `Sources/Loro/Container.swift`.
 *
 * `extension String: ContainerIdLike` there; here a wrapper, because Kotlin
 * cannot add a supertype to `kotlin.String`. `ContainerIdLike` is a UniFFI
 * foreign trait, so this crosses back into Rust as a callback.
 */
class RootContainerId(private val name: kotlin.String) : ContainerIdLike {
    override fun `asContainerId`(`ty`: ContainerType): ContainerId =
        ContainerId.Root(name, `ty`)
}

/** `doc.getMap(id: "clips")` — the root container named by a string. */
fun LoroDoc.getMap(id: kotlin.String): LoroMap = getMap(RootContainerId(id))

/** `map.insert(key:v:)` taking a plain value, as the Swift extension does. */
fun LoroMap.insert(key: kotlin.String, v: LoroValue) = insert(key, LoroValueBox(v))

/** Root text and list containers use the same string naming as maps. */
fun LoroDoc.getText(id: kotlin.String): LoroText = getText(RootContainerId(id))
fun LoroDoc.getList(id: kotlin.String): LoroList = getList(RootContainerId(id))
fun LoroList.insert(pos: UInt, v: LoroValue) = insert(pos, LoroValueBox(v))
