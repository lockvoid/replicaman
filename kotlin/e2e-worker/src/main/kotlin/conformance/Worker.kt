package conformance

import java.io.File
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import okhttp3.HttpUrl.Companion.toHttpUrl
import io.replicaman.*
import io.replicaman.loro.LoroReplicaCodec
import io.replicaman.loro.binding.*

data class Item(override val id: String, override val typeName: String?, val data: Map<String, ReplicaValue>) : ReplicaWritableRowModel {
    override fun encode() = data
    companion object : ReplicaWritableRowModelType<Item, ReplicaNoField> {
        override val streamName = "items"
        override val modelKey = Item::class
        override fun from(id: String, type: String?, data: Map<String, ReplicaValue>) = Item(id, type, data)
    }
}

private class HoldingRank : SyncGate {
    override val id = "conformance-hold"
    override val stream = "items"
    override fun judge(change: SyncChange): SyncVerdict =
        if (change.local["rank"]?.string?.startsWith("hold:") == true) SyncVerdict.Gate("test hold")
        else SyncVerdict.Push
}

fun main(args: Array<String>) = runBlocking {
    val owner = args[2].toLong()
    val transport = HTTPReplicaTransport(args[0].toHttpUrl(), token = { null }, headers = { mapOf("X-User-Id" to owner.toString()) })
    val schema = ReplicaSchema(listOf(
        ReplicaStreamSpec("items", ReplicaStreamSpec.Lane.ROW),
        ReplicaStreamSpec("boards", ReplicaStreamSpec.Lane.DOCUMENT, codec = LoroReplicaCodec.CODEC_NAME,
            reflections = listOf(ReplicaReflection("name", listOf("meta", "name")))),
    ), namespace = "replicaman-test")
    val engine = ReplicaEngine(home = File(args[1]), transport = transport, schema = schema,
        codecs = listOf(LoroReplicaCodec()), syncGates = listOf(HoldingRank()), batchLimit = System.getenv("REPLICAMAN_PAGE_LIMIT")?.toInt() ?: 500, automaticallyPushWrites = false)
    engine.open(owner)
    println("""{"ready":true,"language":"kotlin"}""")
    while (true) {
        val line = readlnOrNull() ?: break
        try {
            val input = Json.parseToJsonElement(line).jsonObject
            fun str(key: String, default: String = "") = input[key]?.jsonPrimitive?.content ?: default
            val command = str("command")
            val stream = str("stream", "items")
            val id = str("id", "b1")
            val answer = mutableMapOf<String, JsonElement>("ok" to JsonPrimitive(true))
            when (command) {
                "save" -> {
                    val fields = (input["data"] as? JsonObject)?.mapValues { ReplicaValueSerializer.fromJson(it.value) }.orEmpty()
                    engine.write { tx ->
                        if (tx.find(Item, id) == null) tx.create(Item, Item(id, input["type"]?.jsonPrimitive?.content, fields))
                        else tx.update(Item, id) { it.copy(data = it.data + fields) }
                    }
                }
                "load" -> {
                    val start = input.getValue("start").jsonPrimitive.int
                    val count = input.getValue("count").jsonPrimitive.int
                    val body = "x".repeat(256)
                    engine.write { tx ->
                        for (index in start until start + count) {
                            tx.create(Item, Item("load-$index", "TextItem", mapOf(
                                "boardId" to ReplicaValue.Str("b1"),
                                "rank" to ReplicaValue.Str(index.toString()),
                                "body" to ReplicaValue.Str(body),
                            )))
                        }
                    }
                }
                "statistics" -> {
                    val status = requireNotNull(engine.store).syncStatus()
                    answer["queued"] = JsonPrimitive(status.queuedOperations)
                    answer["journalBytes"] = JsonPrimitive(status.journalBytes)
                }
                "atomic" -> {
                    val members = input.getValue("members").jsonArray
                    engine.writeAtomically { tx ->
                        for (member in members) {
                            val fields = member.jsonObject
                            val data = fields.getValue("data").jsonObject.mapValues { ReplicaValueSerializer.fromJson(it.value) }
                            tx.create(Item, Item(fields.getValue("id").jsonPrimitive.content,
                                fields["type"]?.jsonPrimitive?.content, data))
                        }
                    }
                }
                "delete" -> engine.deleteRow(stream, id)
                "drain" -> engine.drain()
                "pull" -> engine.pullUntilCaughtUp()
                "pull_page" -> answer["applied"] = JsonPrimitive(engine.pullOnce())
                "reset" -> engine.resetCursors()
                "verify" -> engine.verifyIntegrity()
                "edit", "rich" -> {
                    val doc = LoroDoc()
                    doc.setRecordTimestamp(false)
                    doc.setPeerId(engine.docPeer("boards", id) ?: error("missing peer"))
                    doc.import(engine.docFold("boards", id) ?: error("missing document"))
                    val version = doc.oplogVv()
                    if (command == "rich") {
                        doc.getText("body").insert(0u, str("value"))
                        doc.getList("labels").insert(0u, LoroValue.String(str("value")))
                    } else doc.getMap("meta").insert(str("key"), LoroValue.String(str("value")))
                    doc.commit()
                    engine.recordDocDelta("boards", id, doc.export(ExportMode.Updates(version)))
                }
                "rebuild" -> {
                    val doc = LoroDoc()
                    val peer = (engine.docPeer("boards", id) ?: error("missing peer")) + 100uL
                    doc.setPeerId(peer)
                    doc.getMap("meta").insert(str("key"), LoroValue.String("recovered"))
                    doc.commit()
                    engine.rebuildDocument("boards", id, doc.export(ExportMode.Snapshot), peer)
                }
                "inspect" -> {
                    answer["cursor"] = engine.currentCursor()?.let(::JsonPrimitive) ?: JsonNull
                    answer["rows"] = engine.store!!.read { db ->
                        db.prepare("SELECT stream, row_id, data FROM snapshots ORDER BY stream, row_id").use { stmt ->
                            buildJsonArray {
                                while (stmt.step()) add(buildJsonObject {
                                    put("stream", stmt.getText(0)); put("id", stmt.getText(1))
                                    put("data", Json.parseToJsonElement(stmt.getText(2)))
                                })
                            }
                        }
                    }
                    engine.docFold("boards", id)?.let { fold ->
                        val doc = LoroDoc(); doc.import(fold)
                        val root = (doc.getDeepValue() as? LoroValue.Map)?.value.orEmpty()
                        (root["body"] as? LoroValue.String)?.let { answer["body"] = JsonPrimitive(it.value) }
                        (root["labels"] as? LoroValue.List)?.let { answer["labels"] = JsonArray(it.value.map { JsonPrimitive((it as LoroValue.String).value) }) }
                        val meta = root["meta"] as? LoroValue.Map
                        answer["document"] = JsonObject(meta?.value.orEmpty().mapNotNull { (key, value) ->
                            (value as? LoroValue.String)?.let { key to JsonPrimitive(it.value) }
                        }.toMap())
                    }
                }
                "close" -> engine.close()
                else -> error("unknown worker command")
            }
            if (command != "load" && command != "statistics") {
                answer["pending"] = JsonPrimitive(engine.pendingOps().size)
            }
            println(JsonObject(answer))
            if (command == "close") break
        } catch (error: Throwable) {
            println(buildJsonObject { put("ok", false); put("error", error.toString()) })
        }
    }
}
