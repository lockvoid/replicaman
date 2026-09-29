package io.replicaman.testing

import io.replicaman.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.UUID

/**
 * One scripted `/pull` answer: the frames past the request's cursor, the opaque
 * cursor they reach, and whether the shard's heads are further ahead. Whether it
 * answers a baseline is the request's to say, as on the server.
 */
public data class ReplicaPullResponse(
    val frames: List<ReplicaFrame>,
    val cursor: String,
    val more: Boolean,
)

/**
 * A scripted server behind the engine's real encoder and admission path. The
 * transport scripts what the server knows (pages, verdicts); the fixture speaks
 * protocol 2 around it like `ruby/lib/replica_man`. Real HTTP/PostgreSQL
 * histories cover server execution and concurrency.
 */
interface FixtureTransport : ReplicaTransport {
    val protocolFixture: ProtocolFixture

    /** A scripted page. Throw `ReplicaError.Protocol("CursorInvalid", …)` to refuse the cursor. */
    suspend fun pull(shard: String, cursor: String?, limit: Int): ReplicaPullResponse

    /** Verdicts for operations the server has not claimed yet, by operation id. */
    suspend fun push(ops: List<ReplicaOp>): List<ReplicaVerdict>

    override suspend fun exchange(endpoint: ReplicaEndpoint, body: ByteArray): ByteArray =
        protocolFixture.exchange(endpoint, body, this)
}

/**
 * Pushes claim every operation id once with the digest of its bytes: a retry
 * replays the stored verdict without asking the transport again, and the same
 * id with other bytes fails the request with `MutationChanged`. A refused
 * member refuses its whole group. Pulls answer `reset` for a request without a
 * cursor and pass the transport's opaque cursor and `more` through; the served
 * frames are the live membership `/verify` digests at the latest cursor.
 */
class ProtocolFixture(val dataset: String = DATASET) {
    private class Claim(val digest: String, val outcome: ReplicaVerdict.Outcome, val reason: String?)
    private data class Address(val stream: String, val id: String)
    private data class Member(val incarnation: String, val revision: Long)

    private val claims = mutableMapOf<String, Claim>()
    private val incarnations = mutableMapOf<Address, String>()
    private val members = mutableMapOf<String, MutableMap<Address, Member>>()
    private val heads = mutableMapOf<String, String>()
    private val received = mutableListOf<Pair<ReplicaEndpoint, ReplicaValue>>()
    private var lostReplies = 0
    private var responses = 0L
    private val mutex = Mutex()

    /** Every request the server received for `endpoint`, failed ones included. */
    suspend fun requests(endpoint: ReplicaEndpoint): List<ReplicaValue> =
        mutex.withLock { received.filter { it.first == endpoint }.map { it.second } }

    /** The next `count` pushes commit their verdicts, then lose the answer. */
    suspend fun loseReplies(count: Int = 1) {
        mutex.withLock { lostReplies = count }
    }

    suspend fun exchange(endpoint: ReplicaEndpoint, body: ByteArray, transport: FixtureTransport): ByteArray {
        val request = ReplicaProtocol.decode(body)
        mutex.withLock { received += endpoint to request }
        if (request["protocol"]?.long != ReplicaProtocol.VERSION.toLong()) refuse("UpgradeRequired")
        val initial = endpoint == ReplicaEndpoint.PULL && request["dataset"] == ReplicaValue.Null
        if (!initial && request["dataset"]?.string != dataset) refuse("DatasetChanged")
        val answer = when (endpoint) {
            ReplicaEndpoint.PUSH -> push(request, transport)
            ReplicaEndpoint.PULL -> pull(request, transport)
            ReplicaEndpoint.VERIFY -> verify(request)
        }
        val header = mapOf(
            "protocol" to ReplicaValue.Integer(ReplicaProtocol.VERSION.toLong()),
            "namespace" to (request["namespace"] ?: invalidRequest("missing request field: namespace")),
            "schema" to (request["schema"] ?: invalidRequest("missing request field: schema")),
            "dataset" to ReplicaValue.Str(dataset),
        )
        return ReplicaJSON.encodeToBytes(ReplicaValue.Obj(header + answer))
    }

    private suspend fun push(request: ReplicaValue, transport: FixtureTransport): Map<String, ReplicaValue> {
        val raw = request["ops"]?.items ?: invalidRequest("operations must be an array")
        if (raw.size > ReplicaProtocol.MAX_OPERATIONS) invalidRequest("at most 100 operations are allowed")
        val ops = raw.map(ReplicaOp::fromValue)
        if (ops.any { it.incarnation == null }) invalidRequest("missing request field: incarnation")
        if (ops.any { !UUID_PATTERN.matches(it.id) || (it.group != null && !UUID_PATTERN.matches(it.group)) }) {
            invalidRequest("operation id and group must be UUIDs")
        }
        if (ops.map { it.id }.toSet().size != ops.size) invalidRequest("operation IDs must be unique within a submission")
        val digests = raw.map { ReplicaProtocol.digest(ReplicaJSON.encodeToBytes(it)) }
        val units = units(ops)

        val fresh = mutex.withLock {
            units.filter { unit ->
                val claimed = unit.map { claims[ops[it].id] }
                if (claimed.all { it == null }) return@filter true
                if (claimed.any { it == null }) invalidRequest("an operation group changed after it was applied")
                if (unit.any { claims.getValue(ops[it].id).digest != digests[it] }) refuse("MutationChanged")
                false
            }
        }
        val executed = if (fresh.isEmpty()) emptyMap() else transport.push(fresh.flatten().map { ops[it] }).associateBy { it.id }
        mutex.withLock {
            for (unit in fresh) {
                val refusal = unit.map { executed[ops[it].id] ?: error("The transport answered no verdict for ${ops[it].id}") }
                    .firstOrNull { it.outcome == ReplicaVerdict.Outcome.REJECTED }
                for (index in unit) {
                    val op = ops[index]
                    claims[op.id] = if (refusal == null) Claim(digests[index], ReplicaVerdict.Outcome.ACCEPTED, null)
                    else Claim(digests[index], ReplicaVerdict.Outcome.REJECTED, refusal.reason)
                    if (refusal == null) incarnations[Address(op.stream, op.rowId)] = requireNotNull(op.incarnation)
                }
            }
            if (lostReplies > 0) {
                lostReplies -= 1
                throw ReplicaError.Transport("the server committed; the answer was lost")
            }
            return mapOf("verdicts" to ReplicaValue.Arr(ops.map { op ->
                val claim = claims.getValue(op.id)
                val verdict = mutableMapOf<String, ReplicaValue>("id" to ReplicaValue.Str(op.id),
                    "outcome" to ReplicaValue.Str(claim.outcome.rawValue))
                claim.reason?.let { verdict["reason"] = ReplicaValue.Str(it) }
                ReplicaValue.Obj(verdict)
            }))
        }
    }

    /** Consecutive members of one group apply together; every other operation alone. */
    private fun units(ops: List<ReplicaOp>): List<List<Int>> {
        val units = mutableListOf<MutableList<Int>>()
        for (index in ops.indices) {
            val group = ops[index].group
            val previous = units.lastOrNull()
            if (group != null && previous != null && ops[previous.last()].group == group) previous += index
            else units += mutableListOf(index)
        }
        val groups = units.mapNotNull { ops[it.first()].group }
        if (groups.toSet().size != groups.size) invalidRequest("an operation group must be contiguous")
        return units
    }

    private suspend fun pull(request: ReplicaValue, transport: FixtureTransport): Map<String, ReplicaValue> {
        val shard = request["shard"]?.string ?: invalidRequest("missing request field: shard")
        val cursor = when (val value = request["cursor"]) {
            null, ReplicaValue.Null -> null
            else -> value.string ?: invalidRequest("cursor must be a string")
        }
        val limit = request["limit"]?.long ?: 500
        if (limit !in 1..1000) invalidRequest("limit must be an integer from 1 to 1000")
        val answer = transport.pull(shard, cursor, limit.toInt())
        return mutex.withLock {
            val revision = ++responses
            val live = members.getOrPut(shard) { mutableMapOf() }
            if (cursor == null) live.clear()
            val frames = answer.frames.map { frame ->
                val address = Address(frame.stream, frame.id)
                val incarnation = incarnations.getOrPut(address) { UUID.randomUUID().toString() }
                val value = linkedMapOf<String, ReplicaValue>("stream" to ReplicaValue.Str(frame.stream),
                    "id" to ReplicaValue.Str(frame.id), "incarnation" to ReplicaValue.Str(incarnation))
                when (frame) {
                    is ReplicaFrame.RowSet -> {
                        value["frame"] = ReplicaValue.Str("row.set")
                        value["revision"] = ReplicaValue.Str((frame.revision ?: revision).toString())
                        value["type"] = frame.type?.let(ReplicaValue::Str) ?: ReplicaValue.Null
                        value["data"] = ReplicaValue.Obj(frame.data)
                        live[address] = Member(incarnation, frame.revision ?: revision)
                    }
                    is ReplicaFrame.RowDelete -> {
                        value["frame"] = ReplicaValue.Str("row.delete")
                        value["revision"] = ReplicaValue.Str((frame.revision ?: revision).toString())
                        live.remove(address)
                    }
                    is ReplicaFrame.DocSnapshot -> {
                        value["frame"] = ReplicaValue.Str("doc.snapshot")
                        value["revision"] = ReplicaValue.Str((frame.revision ?: revision).toString())
                        value["codec"] = ReplicaValue.Str(frame.codec)
                        value["snapshot"] = ReplicaValue.Str(ReplicaBase64.encode(frame.snapshot))
                        value["data"] = ReplicaValue.Obj(frame.data)
                        live[address] = Member(incarnation, frame.revision ?: revision)
                    }
                    is ReplicaFrame.DocDelta -> {
                        value["frame"] = ReplicaValue.Str("doc.delta")
                        value["seq"] = ReplicaValue.Integer(frame.seq)
                        value["codec"] = ReplicaValue.Str(frame.codec)
                        value["payload"] = ReplicaValue.Str(ReplicaBase64.encode(frame.payload))
                    }
                }
                ReplicaValue.Obj(value)
            }
            heads[shard] = answer.cursor
            mapOf("shard" to ReplicaValue.Str(shard), "reset" to ReplicaValue.Bool(cursor == null),
                "frames" to ReplicaValue.Arr(frames), "cursor" to ReplicaValue.Str(answer.cursor),
                "more" to ReplicaValue.Bool(answer.more))
        }
    }

    private suspend fun verify(request: ReplicaValue): Map<String, ReplicaValue> {
        val shard = request["shard"]?.string ?: invalidRequest("missing request field: shard")
        val cursor = request["cursor"]?.string ?: invalidRequest("missing request field: cursor")
        return mutex.withLock {
            if (heads[shard] != cursor) refuse("CursorBehind")
            val live = members[shard].orEmpty().entries.sortedWith { left, right ->
                compareBytes(left.key.stream, right.key.stream).takeIf { it != 0 } ?: compareBytes(left.key.id, right.key.id)
            }
            val hash = ReplicaIntegrityHash("replicaman-view")
            for ((address, member) in live) {
                for (field in listOf(address.stream, address.id, member.incarnation, member.revision.toString())) hash.append(field)
            }
            mapOf("shard" to ReplicaValue.Str(shard), "cursor" to ReplicaValue.Str(cursor),
                "count" to ReplicaValue.Str(live.size.toString()), "digest" to ReplicaValue.Str(hash.finish()))
        }
    }

    private fun compareBytes(left: String, right: String): Int {
        val a = left.toByteArray(Charsets.UTF_8)
        val b = right.toByteArray(Charsets.UTF_8)
        for (index in 0 until minOf(a.size, b.size)) {
            val difference = (a[index].toInt() and 0xff) - (b[index].toInt() and 0xff)
            if (difference != 0) return difference
        }
        return a.size - b.size
    }

    private fun refuse(code: String): Nothing = throw ReplicaError.Protocol(code, "HTTP 409: $code")

    private fun invalidRequest(message: String): Nothing = throw ReplicaError.Protocol(message, "HTTP 400: $message")

    companion object {
        /** The dataset every fixture server serves and every fixture store has already synchronized with. */
        const val DATASET: String = "fixture-dataset"

        private val UUID_PATTERN = Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
    }
}
