use loro::{ExportMode, LoroDoc};
use replicaman::transport::{BoxFuture, HttpClient, HttpReplicaTransport, HttpResponse};
use replicaman::{
    ReplicaEngine, ReplicaEngineOptions, ReplicaError, ReplicaFields, ReplicaResult, ReplicaSchema,
    ReplicaStreamSpec, ReplicaValue,
};
use replicaman_loro::LoroReplicaCodec;
use serde_json::{Value, json};
use std::io::{BufRead, Write};
use std::sync::Arc;

struct Network(reqwest::Client);
impl HttpClient for Network {
    fn request<'a>(
        &'a self,
        method: &'a str,
        url: &'a str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
        response_limit: usize,
    ) -> BoxFuture<'a, ReplicaResult<HttpResponse>> {
        Box::pin(async move {
            let mut request = self.0.request(method.parse().unwrap(), url);
            for (key, value) in headers {
                request = request.header(key, value);
            }
            if let Some(body) = body {
                request = request.body(body);
            }
            let mut response = request
                .send()
                .await
                .map_err(|e| ReplicaError::Transport(e.to_string()))?;
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| ReplicaError::Transport(e.to_string()))?
            {
                if bytes.len() + chunk.len() > response_limit {
                    return Err(ReplicaError::Transport(
                        "Response exceeded size limit".into(),
                    ));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(HttpResponse {
                status,
                body: bytes,
                retry_after,
            })
        })
    }
}

struct HoldingRank;
impl replicaman::SyncGate for HoldingRank {
    fn id(&self) -> &str {
        "conformance-hold"
    }
    fn stream(&self) -> Option<&str> {
        Some("items")
    }
    fn judge(&self, change: &replicaman::SyncChange) -> replicaman::SyncGateDecision {
        if change
            .local
            .get("rank")
            .and_then(ReplicaValue::as_string)
            .is_some_and(|rank| rank.starts_with("hold:"))
        {
            replicaman::SyncGateDecision::Hold("test hold".into())
        } else {
            replicaman::SyncGateDecision::Push
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let owner: i64 = args[3].parse()?;
    let transport = Arc::new(HttpReplicaTransport::new(
        &args[1],
        Network(reqwest::Client::new()),
        Box::new(|| None),
        Box::new(move || vec![("X-User-Id".into(), owner.to_string())]),
    ));
    let mut schema = ReplicaSchema::new(vec![
        ReplicaStreamSpec::row("items"),
        ReplicaStreamSpec::document("boards").codec(LoroReplicaCodec::CODEC_NAME),
    ]);
    schema.namespace = "replicaman-test".into();
    let mut options = ReplicaEngineOptions::new(
        &args[2],
        transport,
        schema,
        Arc::new(|future| {
            tokio::spawn(future);
        }),
    );
    options.codecs = vec![Arc::new(LoroReplicaCodec::new())];
    options.automatically_push_writes = false;
    options.sync_gates = vec![Arc::new(HoldingRank)];
    options.batch_limit =
        std::env::var("REPLICAMAN_PAGE_LIMIT").map_or(Ok(500), |value| value.parse())?;
    let engine = ReplicaEngine::new(options);
    engine.open(owner).await?;
    emit(json!({"ready":true,"language":"rust"}));
    for line in std::io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line?)?;
        let command = request["command"].as_str().unwrap_or("");
        match execute(&engine, &request).await {
            Ok(mut answer) => {
                answer["ok"] = json!(true);
                if command != "load" && command != "statistics" {
                    answer["pending"] = json!(engine.pending_ops().await?.len());
                }
                emit(answer);
            }
            Err(error) => emit(json!({"ok":false,"error":error.to_string()})),
        }
        if command == "close" {
            break;
        }
    }
    Ok(())
}

async fn execute(
    engine: &Arc<ReplicaEngine>,
    request: &Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let stream = request["stream"].as_str().unwrap_or("items");
    let id = request["id"].as_str().unwrap_or("b1");
    let mut answer = json!({});
    match request["command"].as_str().unwrap_or("") {
        "save" => {
            let data: ReplicaFields = serde_json::from_value(request["data"].clone())?;
            engine
                .save_row(stream, id, request["type"].as_str(), &data)
                .await?;
        }
        "load" => {
            let start = request
                .get("start")
                .and_then(Value::as_u64)
                .ok_or("missing start")?;
            let count = request
                .get("count")
                .and_then(Value::as_u64)
                .ok_or("missing count")?;
            let body = "x".repeat(256);
            engine
                .write(|tx| {
                    for index in start..start + count {
                        let fields = ReplicaFields::from([
                            ("boardId".into(), ReplicaValue::string("b1")),
                            ("rank".into(), ReplicaValue::string(index.to_string())),
                            ("body".into(), ReplicaValue::string(&body)),
                        ]);
                        tx.create("items", &format!("load-{index}"), Some("TextItem"), &fields)?;
                    }
                    Ok(())
                })
                .await?;
        }
        "statistics" => {
            let status = engine.store().ok_or("missing store")?.sync_status()?;
            answer["queued"] = json!(status.queued_operations);
            answer["journalBytes"] = json!(status.journal_bytes);
        }
        "atomic" => {
            let members = request
                .get("members")
                .and_then(Value::as_array)
                .ok_or("missing atomic members")?;
            let rows = members
                .iter()
                .map(|member| {
                    Ok((
                        member
                            .get("id")
                            .and_then(Value::as_str)
                            .ok_or("missing atomic id")?
                            .to_owned(),
                        member
                            .get("type")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        serde_json::from_value::<ReplicaFields>(
                            member.get("data").ok_or("missing atomic data")?.clone(),
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
            engine
                .write_atomically(|tx| {
                    for (id, row_type, data) in &rows {
                        tx.create("items", id, row_type.as_deref(), data)?;
                    }
                    Ok(())
                })
                .await?;
        }
        "delete" => {
            engine.delete_row(stream, id).await?;
        }
        "drain" => {
            engine.drain().await?;
        }
        "pull" => {
            engine.pull_until_caught_up(None).await?;
        }
        "pull_page" => {
            answer["applied"] = json!(engine.pull_once("user").await?);
        }
        "verify" => engine.verify_integrity("user").await?,
        "reset" => {
            engine.reset_cursors().await?;
        }
        "edit" | "rich" => {
            let fold = engine.doc_fold("boards", id)?.ok_or("missing document")?;
            let peer = engine.doc_peer("boards", id)?.ok_or("missing peer")?;
            let doc = LoroDoc::new();
            doc.set_record_timestamp(false);
            doc.set_peer_id(peer)?;
            doc.import(&fold)?;
            let before = doc.oplog_vv();
            if request["command"] == "rich" {
                doc.get_text("body")
                    .insert(0, request["value"].as_str().unwrap())?;
                doc.get_list("labels")
                    .insert(0, request["value"].as_str().unwrap())?;
            } else {
                doc.get_map("meta").insert(
                    request["key"].as_str().unwrap(),
                    request["value"].as_str().unwrap(),
                )?;
            }
            doc.commit();
            engine
                .record_doc_delta("boards", id, &doc.export(ExportMode::updates(&before))?)
                .await?;
        }
        "rebuild" => {
            let doc = LoroDoc::new();
            let peer = engine
                .doc_peer("boards", id)?
                .ok_or("missing peer")?
                .wrapping_add(100);
            doc.set_peer_id(peer)?;
            doc.get_map("meta")
                .insert(request["key"].as_str().unwrap(), "recovered")?;
            doc.commit();
            engine
                .rebuild_document("boards", id, &doc.export(ExportMode::Snapshot)?, peer)
                .await?;
        }
        "inspect" => {
            let store = engine.store().ok_or("no store")?;
            answer["cursor"] = json!(engine.current_cursor("user").await?);
            answer["rows"] = store.pool().read(|db| {
                let mut query = db.prepare("SELECT stream, row_id, data FROM snapshots ORDER BY stream, row_id")?;
                let mut rows = query.query([])?;
                let mut found = Vec::new();
                while let Some(row) = rows.next()? {
                    let data: String = row.get(2)?;
                    found.push(json!({"stream":row.get::<_,String>(0)?,"id":row.get::<_,String>(1)?,
                        "data":serde_json::from_str::<Value>(&data).map_err(|e| ReplicaError::Storage(e.to_string()))?}));
                }
                Ok(json!(found))
            })?;
            if let Some(fold) = engine.doc_fold("boards", id)? {
                let doc = LoroDoc::new();
                doc.import(&fold)?;
                let root = serde_json::to_value(doc.get_deep_value())?;
                answer["document"] = root["meta"].clone();
                if !root["body"].is_null() {
                    answer["body"] = root["body"].clone();
                }
                if !root["labels"].is_null() {
                    answer["labels"] = root["labels"].clone();
                }
            }
        }
        "close" => engine.try_close().await?,
        _ => return Err("unknown worker command".into()),
    }
    Ok(answer)
}

fn emit(value: Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
