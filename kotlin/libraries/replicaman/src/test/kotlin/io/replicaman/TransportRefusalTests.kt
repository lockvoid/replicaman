package io.replicaman

import kotlinx.coroutines.test.runTest
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import org.junit.Test
import java.net.InetSocketAddress
import java.net.ServerSocket
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A refusal is evidence. `HTTP <status>: <body>` is the shape every other
 * transport in the host leaves one in, so one parser reads them all —
 * without the body a `push answered 400` is unreadable and the journal
 * retries it forever.
 */
class TransportRefusalTests {

    /** One canned response on a loopback port; the socket dies with the test. */
    private suspend fun serve(status: String, body: String, block: suspend (Int) -> Unit) {
        val socket = ServerSocket()
        socket.reuseAddress = true
        socket.bind(InetSocketAddress("127.0.0.1", 0))
        thread(isDaemon = true) {
            while (!socket.isClosed) {
                try {
                    socket.accept().use { client ->
                        val reader = client.getInputStream().bufferedReader()
                        reader.readLine()
                        while (reader.readLine()?.isNotEmpty() == true) Unit
                        client.getOutputStream().write(
                            (
                                "HTTP/1.1 $status\r\nContent-Type: application/json\r\n" +
                                    "Content-Length: ${body.toByteArray().size}\r\n\r\n$body"
                                ).toByteArray()
                        )
                        client.getOutputStream().flush()
                    }
                } catch (_: Exception) {
                    return@thread
                }
            }
        }
        try {
            block(socket.localPort)
        } finally {
            socket.close()
        }
    }

    private fun transport(port: Int) = HTTPReplicaTransport(
        baseURL = "http://127.0.0.1:$port/replica".toHttpUrl(),
        client = OkHttpClient(),
        token = { "t" },
    )

    /** KILL: `throw ReplicaError.Transport("pull answered ${response.code}")`. */
    @Test
    fun aRefusedPullCarriesTheServersOwnWords() = runTest {
        val body = """{"error":"malformed cursor"}"""
        serve("400 Bad Request", body) { port ->
            val message = runCatching { transport(port).exchange(ReplicaEndpoint.PULL, "{}".toByteArray()) }
                .exceptionOrNull()?.message ?: ""
            assertTrue(
                message.contains("HTTP 400") && message.contains("malformed cursor"),
                "a refusal must quote the status and the body: $message"
            )
        }
    }

    /**
     * The server's real answer when a `Content-Encoding: gzip` body is not gzip.
     * KILL: throw `push answered HTTP ${response.code}` without the body.
     */
    @Test
    fun aRefusedPushCarriesTheServersOwnWords() = runTest {
        val body = """{"error":"malformed gzip body"}"""
        serve("400 Bad Request", body) { port ->
            val message = runCatching { transport(port).exchange(ReplicaEndpoint.PUSH, "{}".toByteArray()) }
                .exceptionOrNull()?.message ?: ""
            assertTrue(
                message.contains("HTTP 400") && message.contains("malformed gzip body"),
                "a refusal must quote the status and the body: $message"
            )
        }
    }

    /**
     * A host that refuses before the replica sees the request answers in its
     * own envelope, not the protocol's `{error}`: the status and body are the
     * only evidence the host can read back.
     * KILL: decode every non-200 body as a protocol failure — the host's 426
     * becomes `InvalidResponse: Missing string: error`.
     */
    @Test
    fun aHostRefusalOutsideTheProtocolKeepsItsStatusAndBody() = runTest {
        val body = """{"code":"client_update_required","message":"Update to keep working."}"""
        serve("426 Upgrade Required", body) { port ->
            val failure = assertFailsWith<ReplicaError.Transport> {
                transport(port).exchange(ReplicaEndpoint.PULL, "{}".toByteArray())
            }
            assertEquals("HTTP 426: $body", failure.reason)
        }
    }

    private suspend fun pullFailure(port: Int): ReplicaError.Transport = assertFailsWith<ReplicaError.Transport> {
        transport(port).exchange(ReplicaEndpoint.PULL, "{}".toByteArray())
    }

    /**
     * A proxy's HTML page is diagnostics nobody reads: the quote is capped.
     * KILL: quote the whole bounded `content` instead of `content.take(512)`.
     */
    @Test
    fun aRefusalQuotesOnlyTheHeadOfItsBody() = runTest {
        val body = "x".repeat(2_000)
        serve("502 Bad Gateway", body) { port ->
            assertEquals("HTTP 502: " + body.take(512), pullFailure(port).reason)
        }
    }

    /** KILL: read a refusal's body up to `RESPONSE_BYTES` instead of 4096 — a proxy's page is read whole. */
    @Test
    fun aRefusalBodyPastItsLimitIsNeverRead() = runTest {
        serve("502 Bad Gateway", "<html>" + "x".repeat(20_000) + "</html>") { port ->
            assertEquals("Response exceeded its size limit", pullFailure(port).reason)
        }
    }
}
