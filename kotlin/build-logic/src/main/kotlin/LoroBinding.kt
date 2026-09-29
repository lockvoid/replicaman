import java.io.File

/**
 * UniFFI's `LoroValue` and `Diff` declare nested `List`/`Map` types that shadow the
 * stdlib inside their bodies, so every bare `List<`/`Map<` is fully qualified.
 */
fun qualifyCollections(source: String): String {
    val lines = mutableListOf<String>()
    var start = 0
    while (start < source.length) {
        val end = source.indexOf('\n', start).let { if (it < 0) source.length else it + 1 }
        lines += source.substring(start, end)
        start = end
    }
    return lines.joinToString("") { qualify(qualify(it, "List<"), "Map<") }
}

private fun qualify(line: String, name: String): String {
    val out = StringBuilder(line.length)
    var rest = line
    if (rest.startsWith(name)) {
        out.append("kotlin.collections.").append(name)
        rest = rest.substring(name.length)
    }
    while (rest.isNotEmpty()) {
        val size = Character.charCount(rest.codePointAt(0))
        val character = rest.substring(0, size)
        val after = rest.substring(size)
        out.append(character)
        if (!isIdentifier(character) && after.startsWith(name)) {
            out.append("kotlin.collections.").append(name)
            rest = after.substring(name.length)
        } else {
            rest = after
        }
    }
    return out.toString()
}

private fun isIdentifier(character: String): Boolean {
    if (character.length != 1) return false
    val c = character[0]
    return c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9' || c == '_' || c == '.' || c == '`'
}

/** A 16 KB-page Android device refuses a library with a smaller LOAD alignment. */
fun verifyPageAlignment(readelf: File, library: File) {
    val process = ProcessBuilder(readelf.absolutePath, "-lW", library.absolutePath).redirectErrorStream(true).start()
    val listing = process.inputStream.bufferedReader().readText()
    check(process.waitFor() == 0) { "$readelf failed on $library:\n$listing" }
    val loads = listing.lines().filter { it.trimStart().startsWith("LOAD ") }
    check(loads.isNotEmpty()) { "no ELF load segments in $library" }
    loads.forEach { segment ->
        val alignment = segment.trim().split(Regex("\\s+")).last().removePrefix("0x").toLong(16)
        check(alignment >= 16_384) { "$library is not aligned for 16 KB pages" }
    }
    println("Verified 16 KB ELF alignment for ${loads.size} segments")
}
