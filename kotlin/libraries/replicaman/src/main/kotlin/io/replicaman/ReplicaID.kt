package io.replicaman

import java.security.SecureRandom
import java.util.UUID
import kotlin.random.Random

/**
 * Client-generated ids: ids are ALWAYS minted client-side — offline-first
 * non-negotiable (ARCHITECTURE §3.7). Journal entries are local (ULID);
 * the operations and groups a frozen submission carries are UUIDv7; row ids
 * are the entity's identity forever.
 */
public object ReplicaID {
    private val alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray()
    private val secure = SecureRandom()

    /** RFC 9562 version 7: 48 bits of Unix milliseconds, then randomness. */
    public fun uuid7(now: Long = System.currentTimeMillis()): String {
        val bytes = ByteArray(16)
        secure.nextBytes(bytes)
        for (index in 0 until 6) bytes[index] = (now ushr (40 - 8 * index)).toByte()
        bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x70).toByte()
        bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte()
        var most = 0L
        var least = 0L
        for (index in 0 until 8) most = (most shl 8) or (bytes[index].toLong() and 0xff)
        for (index in 8 until 16) least = (least shl 8) or (bytes[index].toLong() and 0xff)
        return UUID(most, least).toString()
    }

    public fun ulid(now: Long = System.currentTimeMillis()): String {
        val characters = StringBuilder(26)

        var milliseconds = now
        val time = CharArray(10)
        for (index in 0 until 10) {
            time[index] = alphabet[(milliseconds and 0x1F).toInt()]
            milliseconds = milliseconds ushr 5
        }
        for (index in 9 downTo 0) {
            characters.append(time[index])
        }

        for (index in 0 until 16) {
            characters.append(alphabet[Random.nextInt(0, 32)])
        }
        return characters.toString()
    }

    /**
     * A fresh loro peer id, clear of the reserved actors (server = 1,
     * agent = 2) and of 0. Minted whenever a doc fold is created or
     * recreated — a reborn doc reusing its peer would have its edits
     * silently discarded (loro dedups by (peer, counter) — the v1 lesson).
     * Inside `Int64`: loro reserves the top of the unsigned range, and a peer
     * minted there reopens the document under a reserved actor.
     */
    public fun peer(): ULong {
        val span = Long.MAX_VALUE.toULong() - 16uL
        return 16uL + (Random.nextULong() % span)
    }

    private fun Random.nextULong(): ULong = nextLong().toULong()
}
