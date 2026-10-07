use super::support::*;
use crate::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use std::sync::Arc;

fn hint(dataset: &str, shards: &[&str]) -> String {
    STANDARD.encode(
        serde_json::to_vec(&json!({
            "protocol": 2, "namespace": "replicaman", "schema": 1,
            "dataset": dataset, "shards": shards
        }))
        .unwrap(),
    )
}

async fn publish(
    engine: &ReplicaEngine,
    transport: &StubTransport,
    frames: Vec<ScriptedFrame>,
    cursor: &str,
) {
    transport.queue_pull("user", ScriptedPull::new(frames, cursor, false));
    let session = engine.commit_session().await.unwrap();
    engine
        .apply_commit(&hint("fixture-dataset", &["user"]), &session)
        .await
        .unwrap();
}

#[tokio::test]
async fn command_refresh_publishes_rows_and_cursor_together() {
    let store = store("command-refresh");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    publish(
        &engine,
        &transport,
        vec![note("n1", "actual", None)],
        "after",
    )
    .await;
    assert_eq!(
        store.peek_snapshot("notes", "n1").unwrap().unwrap().data["title"],
        text("actual")
    );
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("after")
    );
    assert!(store.peek_pending().unwrap().is_empty());
}

#[tokio::test]
async fn rejected_patches_rebase_against_refreshed_truth_and_later_local_edits() {
    for accept_last in [false, true] {
        let store = store("command-rejection");
        let transport = StubTransport::new();
        let engine = engine(store.clone(), transport.clone());
        publish(&engine, &transport, vec![note("n1", "old", None)], "1:").await;
        engine
            .update_row("notes", "n1", None, &fields(&[("title", text("first"))]))
            .await
            .unwrap();
        engine
            .update_row(
                "notes",
                "n1",
                None,
                &fields(&[("title", text("last")), ("mood", text("bold"))]),
            )
            .await
            .unwrap();
        transport.fail_pushes(true);
        publish(&engine, &transport, vec![note("n1", "remote", None)], "2:").await;
        transport.fail_pushes(false);
        transport.script_push(move |ops| {
            ops.iter()
                .map(|op| {
                    let last = op
                        .data
                        .as_ref()
                        .is_some_and(|data| data.contains_key("mood"));
                    if accept_last && last {
                        ReplicaVerdict::accepted(&op.id)
                    } else {
                        ReplicaVerdict::rejected(&op.id, "refused")
                    }
                })
                .collect()
        });
        engine.drain().await.unwrap();
        let row = store.peek_snapshot("notes", "n1").unwrap().unwrap();
        assert_eq!(
            row.data["title"],
            text(if accept_last { "last" } else { "remote" })
        );
        assert_eq!(
            row.data.get("mood").cloned(),
            if accept_last {
                Some(text("bold"))
            } else {
                None
            }
        );
        publish(&engine, &transport, vec![note("n1", "latest", None)], "3:").await;
        assert_eq!(
            store.peek_snapshot("notes", "n1").unwrap().unwrap().data["title"],
            text("latest")
        );
    }
}

#[tokio::test]
async fn invalid_hints_and_failed_publication_keep_the_old_checkpoint() {
    let store = store("command-atomic");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine.pull_once("user").await.unwrap();
    let cursor = engine.current_cursor("user").await.unwrap();
    let session = engine.commit_session().await.unwrap();
    for invalid in [
        "invalid".to_owned(),
        hint("restored", &["user"]),
        hint("fixture-dataset", &["private"]),
        hint("fixture-dataset", &["user", "user"]),
    ] {
        assert!(engine.apply_commit(&invalid, &session).await.is_err());
    }
    engine
        .set_checkpoint_fault(Some(Arc::new(|| {
            Err(ReplicaError::Storage("injected".into()))
        })))
        .await;
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "next", None)], "1:", false),
    );
    assert!(matches!(
        engine
            .apply_commit(&hint("fixture-dataset", &["user"]), &session)
            .await,
        Err(ReplicaError::Storage(_))
    ));
    assert!(store.peek_snapshot("notes", "n1").unwrap().is_none());
    assert_eq!(engine.current_cursor("user").await.unwrap(), cursor);
    engine.set_checkpoint_fault(None).await;
    publish(&engine, &transport, vec![note("n1", "next", None)], "1:").await;
    assert_eq!(
        store.peek_snapshot("notes", "n1").unwrap().unwrap().data["title"],
        text("next")
    );
}

#[tokio::test]
async fn command_session_cannot_cross_engines_or_owners() {
    let source = engine(store("command-source").clone(), StubTransport::new());
    let session = source.commit_session().await.unwrap();
    let target_store = store("command-target");
    let target = engine(target_store.clone(), StubTransport::new());
    assert!(matches!(
        target
            .apply_commit(&hint("fixture-dataset", &["user"]), &session)
            .await,
        Err(ReplicaError::StaleCommit)
    ));
    assert!(target_store.all_snapshots().unwrap().is_empty());
}

#[tokio::test]
async fn refresh_preserves_offline_authoring_and_records_delivery_failure() {
    let store = store("command-offline");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    publish(
        &engine,
        &transport,
        vec![note("n1", "before", Some("a"))],
        "1:",
    )
    .await;
    engine
        .update_row("notes", "n1", None, &fields(&[("title", text("offline"))]))
        .await
        .unwrap();
    transport.fail_pushes(true);
    publish(
        &engine,
        &transport,
        vec![note("n1", "remote", Some("b"))],
        "2:",
    )
    .await;
    let row = store.peek_snapshot("notes", "n1").unwrap().unwrap();
    assert_eq!(row.data["title"], text("offline"));
    assert_eq!(row.data["rank"], text("b"));
    assert_eq!(store.peek_pending().unwrap().len(), 1);
    let failure = engine.health.last_failure().unwrap();
    assert_eq!(failure.operation, "push before pull");
    assert_eq!(
        failure.error,
        ReplicaError::Transport("push refused (stub)".into())
    );
}
