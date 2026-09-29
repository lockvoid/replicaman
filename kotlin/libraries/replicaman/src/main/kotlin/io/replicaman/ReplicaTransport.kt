package io.replicaman

import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.HttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.ByteArrayOutputStream
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/** Transports preserve request bytes; the engine owns durable protocol state. */
public interface ReplicaTransport {
    public suspend fun exchange(endpoint: ReplicaEndpoint, body: ByteArray): ByteArray
}

public class HTTPReplicaTransport(
    private val baseURL: HttpUrl,
    private val client: OkHttpClient = OkHttpClient(),
    private val token: () -> String?,
    private val headers: () -> Map<String, String> = { emptyMap() },
) : ReplicaTransport {
    private val retryDelay = ReplicaRetryDelay()

    override suspend fun exchange(endpoint: ReplicaEndpoint, body: ByteArray): ByteArray {
        retryDelay.await()
        val request = Request.Builder()
            .url(baseURL.newBuilder().addPathSegment(endpoint.path).build())
            .post(Gzip.compress(body).toRequestBody("application/json".toMediaType()))
            .header("Content-Encoding", "gzip")
        token()?.let { request.header("Authorization", "Bearer $it") }
        for ((key, value) in headers()) request.header(key, value)
        val call = client.newCall(request.build())
        call.timeout().timeout(60, java.util.concurrent.TimeUnit.SECONDS)

        return suspendCancellableCoroutine { continuation ->
            continuation.invokeOnCancellation { call.cancel() }
            call.enqueue(object : Callback {
                override fun onFailure(call: Call, error: IOException) {
                    // Cancellation already delivered its exception to the caller.
                    // OkHttp's subsequent cancellation callback has no second result.
                    if (continuation.isActive) continuation.resumeWithException(transportFailure(error))
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        val bytes = response.use {
                            if (it.code == 429 || it.code >= 500) retryDelay.record(it.header("Retry-After"))
                            val limit = if (it.code == 200) ReplicaProtocol.RESPONSE_BYTES else 4096
                            val content = boundedBody(it, limit)
                            if (it.code == 429 || it.code >= 500) {
                                throw ReplicaError.Transport("HTTP ${it.code}: ${content.take(512).toByteArray().decodeToString()}")
                            }
                            if (it.code != 200) throw refusal(it.code, content)
                            content
                        }
                        if (continuation.isActive) continuation.resume(bytes)
                    } catch (error: Exception) {
                        // response.use closes the stream on every path. A canceled
                        // continuation already owns the failure; otherwise propagate.
                        if (continuation.isActive) continuation.resumeWithException(transportFailure(error))
                    }
                }
            })
        }
    }

    /**
     * A protocol error answers `{error, message}`. Anything else refused the
     * request outside the protocol — a host gate, a proxy — and its status
     * and body are the only evidence left.
     */
    private fun refusal(status: Int, content: ByteArray): ReplicaError {
        val answer = try {
            ReplicaProtocol.decode(content)
        } catch (_: Exception) {
            null
        }
        val code = answer?.get("error")?.string
        if (answer == null || code == null) {
            return ReplicaError.Transport("HTTP $status: ${content.take(512).toByteArray().decodeToString()}")
        }
        return ReplicaError.Protocol(code, "HTTP $status: ${answer.optionalText("message") ?: code}")
    }

    private fun transportFailure(error: Exception): Exception =
        if (error is IOException) ReplicaError.Transport(error.message ?: "Network request failed").also { it.initCause(error) }
        else error

    private fun boundedBody(response: Response, limit: Int): ByteArray {
        val output = ByteArrayOutputStream()
        val buffer = ByteArray(8192)
        response.body.byteStream().use { input ->
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                if (output.size() + count > limit) throw ReplicaError.Transport("Response exceeded its size limit")
                output.write(buffer, 0, count)
            }
        }
        return output.toByteArray()
    }
}
