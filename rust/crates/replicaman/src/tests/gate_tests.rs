use super::support::*;
use crate::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Gate {
    open: AtomicBool,
    calls: AtomicUsize,
    stream: Option<&'static str>,
    signal: Arc<SyncGateSignal>,
}
impl Gate {
    fn new(stream: Option<&'static str>) -> Arc<Self> {
        Arc::new(Self {
            open: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            stream,
            signal: SyncGateSignal::new(),
        })
    }
}
impl SyncGate for Gate {
    fn id(&self) -> &str {
        "upload"
    }
    fn stream(&self) -> Option<&str> {
        self.stream
    }
    fn changes(&self) -> Option<Arc<SyncGateSignal>> {
        Some(self.signal.clone())
    }
    fn judge(&self, change: &SyncChange) -> SyncGateDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.open.load(Ordering::SeqCst) && change.local.get("blob").is_some() {
            SyncGateDecision::Hold("upload pending".into())
        } else {
            SyncGateDecision::Push
        }
    }
}
fn options(
    directory: &std::path::Path,
    wire: Arc<StubTransport>,
    gate: Arc<Gate>,
) -> ReplicaEngineOptions {
    let mut options = ReplicaEngineOptions::new(
        directory,
        wire,
        ReplicaSchema::new(vec![
            ReplicaStreamSpec::row("notes"),
            ReplicaStreamSpec::row("children"),
        ]),
        Arc::new(TokioSpawner),
    );
    options.sync_gates = vec![gate];
    options.automatically_push_writes = false;
    options
}

#[tokio::test]
async fn holds_persist_and_release_the_latest_row_without_judging_on_drain() {
    let directory = temp_directory("durable-gates");
    let wire = StubTransport::new();
    let gate = Gate::new(Some("notes"));
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), gate.clone()));
    engine.open(42).await.unwrap();
    engine
        .save_row(
            "notes",
            "n",
            None,
            &fields(&[("title", text("first")), ("blob", text("local:key"))]),
        )
        .await
        .unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("latest"))]))
        .await
        .unwrap();
    assert!(engine.pending_ops().await.unwrap().is_empty());
    assert_eq!(engine.held_rows().unwrap().len(), 1);
    let calls = gate.calls.load(Ordering::SeqCst);
    engine.drain().await.unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), calls);
    engine.try_close().await.unwrap();
    drop(engine);
    gate.open.store(true, Ordering::SeqCst);
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), gate));
    engine.open(42).await.unwrap();
    assert!(engine.held_rows().unwrap().is_empty());
    engine.drain().await.unwrap();
    let sent = wire.pushed_ops();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].verb, verb::ROW_CREATE);
    assert_eq!(sent[0].data.as_ref().unwrap()["title"], text("latest"));
    assert_eq!(sent[0].data.as_ref().unwrap()["blob"], text("local:key"));
}

#[tokio::test]
async fn a_gate_signal_releases_parents_before_children() {
    let directory = temp_directory("gate-dependencies");
    let wire = StubTransport::new();
    let gate = Gate::new(Some("notes"));
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), gate.clone()));
    engine.open(42).await.unwrap();
    engine
        .save_row(
            "notes",
            "parent",
            None,
            &fields(&[("blob", text("local:key"))]),
        )
        .await
        .unwrap();
    engine
        .save_row(
            "children",
            "child",
            None,
            &fields(&[("parentId", text("parent"))]),
        )
        .await
        .unwrap();
    assert_eq!(engine.held_rows().unwrap().len(), 2);
    gate.open.store(true, Ordering::SeqCst);
    gate.signal.fire();
    until("gate signal never released rows", || {
        let engine = engine.clone();
        async move { engine.held_rows().unwrap().is_empty() }
    })
    .await;
    engine.drain().await.unwrap();
    let sent: Vec<_> = wire.pushed_ops().into_iter().map(|op| op.row_id).collect();
    assert_eq!(sent, ["parent", "child"]);
}

#[tokio::test]
async fn global_policy_holds_do_not_block_other_rows_that_name_them() {
    let directory = temp_directory("gate-policy");
    let wire = StubTransport::new();
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), Gate::new(None)));
    engine.open(42).await.unwrap();
    engine
        .save_row(
            "notes",
            "parent",
            None,
            &fields(&[("blob", text("local:key"))]),
        )
        .await
        .unwrap();
    engine
        .save_row(
            "children",
            "child",
            None,
            &fields(&[("parentId", text("parent"))]),
        )
        .await
        .unwrap();
    engine.drain().await.unwrap();
    assert_eq!(
        wire.pushed_ops()
            .iter()
            .map(|op| op.row_id.as_str())
            .collect::<Vec<_>>(),
        ["child"]
    );
}

#[tokio::test]
async fn reset_and_server_projection_preserve_held_fields_with_default_schema() {
    let directory = temp_directory("gate-pull");
    let wire = StubTransport::new();
    let gate = Gate::new(Some("notes"));
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), gate.clone()));
    engine.open(42).await.unwrap();
    wire.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n", "server", None)], "1:", false),
    );
    engine.pull_once("user").await.unwrap();
    engine
        .save_row(
            "notes",
            "n",
            None,
            &fields(&[("title", text("local")), ("blob", text("local:key"))]),
        )
        .await
        .unwrap();
    engine.reset_cursors().await.unwrap();
    wire.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n", "stale", None)], "2:", false),
    );
    engine.pull_once("user").await.unwrap();
    let row = engine
        .store()
        .unwrap()
        .peek_snapshot("notes", "n")
        .unwrap()
        .unwrap();
    assert_eq!(row.data["title"], text("local"));
    gate.open.store(true, Ordering::SeqCst);
    engine.refresh_sync_gates(None).await.unwrap();
    engine.drain().await.unwrap();
    assert_eq!(
        wire.pushed_ops().last().unwrap().data.as_ref().unwrap()["title"],
        text("local")
    );
}

#[tokio::test]
async fn failed_release_keeps_the_hold_and_current_row_atomic() {
    let directory = temp_directory("gate-failure");
    let gate = Gate::new(Some("notes"));
    let engine = ReplicaEngine::new(options(&directory, StubTransport::new(), gate.clone()));
    engine.open(42).await.unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("blob", text("local:key"))]))
        .await
        .unwrap();
    let store = engine.store().unwrap();
    store.pool().write(|ctx| { ctx.tx.execute_batch("CREATE TRIGGER fail_release BEFORE INSERT ON intents BEGIN SELECT RAISE(ABORT, 'disk fault'); END")?; Ok(()) }).unwrap();
    gate.open.store(true, Ordering::SeqCst);
    assert!(engine.refresh_sync_gates(None).await.is_err());
    assert_eq!(engine.held_rows().unwrap().len(), 1);
    assert!(engine.pending_ops().await.unwrap().is_empty());
    assert_eq!(
        store.peek_snapshot("notes", "n").unwrap().unwrap().data["blob"],
        text("local:key")
    );
    store
        .pool()
        .write(|ctx| {
            ctx.tx.execute_batch("DROP TRIGGER fail_release")?;
            Ok(())
        })
        .unwrap();
    engine.refresh_sync_gates(None).await.unwrap();
    assert!(engine.held_rows().unwrap().is_empty());
    assert_eq!(engine.pending_ops().await.unwrap().len(), 1);
}

#[tokio::test]
async fn rejected_converted_hold_restores_before_the_entire_queued_suffix() {
    let directory = temp_directory("gate-preimage");
    let wire = StubTransport::new();
    let gate = Gate::new(Some("notes"));
    gate.open.store(true, Ordering::SeqCst);
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), gate.clone()));
    engine.open(42).await.unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("original"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();
    engine
        .save_row(
            "notes",
            "n",
            None,
            &fields(&[("title", text("one")), ("blob", text("key"))]),
        )
        .await
        .unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("two"))]))
        .await
        .unwrap();
    gate.open.store(false, Ordering::SeqCst);
    engine.refresh_sync_gates(None).await.unwrap();
    assert_eq!(engine.held_rows().unwrap().len(), 1);
    wire.script_push(|ops| {
        ops.iter()
            .map(|op| ReplicaVerdict::rejected(&op.id, "denied"))
            .collect()
    });
    gate.open.store(true, Ordering::SeqCst);
    engine.refresh_sync_gates(None).await.unwrap();
    engine.drain().await.unwrap();
    assert_eq!(
        engine
            .store()
            .unwrap()
            .peek_snapshot("notes", "n")
            .unwrap()
            .unwrap()
            .data,
        fields(&[("title", text("original"))])
    );
}

#[tokio::test]
async fn rejected_held_patch_restores_latest_server_baseline() {
    for pull in [false, true] {
        let directory = temp_directory("gate-patch-preimage");
        let wire = StubTransport::new();
        let gate = Gate::new(Some("notes"));
        let engine = ReplicaEngine::new(options(&directory, wire.clone(), gate.clone()));
        engine.open(42).await.unwrap();
        engine
            .save_row("notes", "n", None, &fields(&[("title", text("original"))]))
            .await
            .unwrap();
        engine.drain().await.unwrap();
        engine
            .save_row(
                "notes",
                "n",
                None,
                &fields(&[("title", text("one")), ("blob", text("key"))]),
            )
            .await
            .unwrap();
        engine
            .save_row("notes", "n", None, &fields(&[("title", text("two"))]))
            .await
            .unwrap();
        if pull {
            wire.queue_pull(
                "user",
                ScriptedPull::new(vec![note("n", "remote", None)], "9:", false),
            );
            engine.pull_once("user").await.unwrap();
        }
        wire.script_push(|ops| {
            ops.iter()
                .map(|op| ReplicaVerdict::rejected(&op.id, "denied"))
                .collect()
        });
        gate.open.store(true, Ordering::SeqCst);
        engine.refresh_sync_gates(None).await.unwrap();
        engine.drain().await.unwrap();
        assert_eq!(
            engine
                .store()
                .unwrap()
                .peek_snapshot("notes", "n")
                .unwrap()
                .unwrap()
                .data,
            fields(&[("title", text(if pull { "remote" } else { "original" }))])
        );
    }
}

#[tokio::test]
async fn unreadable_rollback_image_refuses_verdict_and_retains_pending_write() {
    let directory = temp_directory("corrupt-preimage");
    let wire = StubTransport::new();
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), Gate::new(Some("notes"))));
    engine.open(42).await.unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("original"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("pending"))]))
        .await
        .unwrap();
    let store = engine.store().unwrap();
    store
        .pool()
        .write(|ctx| {
            ctx.tx.execute(
                "UPDATE intents SET preimage = ? WHERE state <> 'accepted'",
                ["{broken"],
            )?;
            Ok(())
        })
        .unwrap();
    wire.script_push(|ops| {
        ops.iter()
            .map(|op| ReplicaVerdict::rejected(&op.id, "denied"))
            .collect()
    });
    assert!(engine.drain().await.is_err());
    assert_eq!(engine.pending_ops().await.unwrap().len(), 1);
    assert!(store.peek_parked().unwrap().is_empty());
    assert_eq!(
        store.peek_snapshot("notes", "n").unwrap().unwrap().data["title"],
        text("pending")
    );
}

#[tokio::test]
async fn held_ordinary_child_retains_its_parent_lifetime_after_reopen() {
    let directory = temp_directory("gate-parent-lifetime");
    let wire = StubTransport::new();
    let gate = Gate::new(Some("children"));
    let configured = || {
        let mut options = options(&directory, wire.clone(), gate.clone());
        let mut child = ReplicaStreamSpec::row("children");
        child
            .references
            .push(ReplicaReferenceSpec::field("parent", "notes", "parentId"));
        options.schema = ReplicaSchema::new(vec![ReplicaStreamSpec::row("notes"), child]);
        options
    };
    let engine = ReplicaEngine::new(configured());
    engine.open(42).await.unwrap();
    engine
        .create_row("notes", "parent", None, &ReplicaFields::new())
        .await
        .unwrap();
    engine
        .create_row(
            "children",
            "child",
            None,
            &fields(&[
                ("parentId", text("parent")),
                ("blob", text("local:key")),
                ("title", text("keep")),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(engine.held_rows().unwrap().len(), 1);
    let store = engine.store().unwrap();
    store
        .pool()
        .write(|ctx| store.set_incarnation(&ctx.tx, "notes", "parent", "user", "replacement"))
        .unwrap();
    engine.try_close().await.unwrap();
    drop(engine);
    drop(store);

    let engine = ReplicaEngine::new(configured());
    engine.open(42).await.unwrap();
    gate.open.store(true, Ordering::SeqCst);
    assert!(matches!(
        engine.refresh_sync_gates(None).await,
        Err(ReplicaError::Storage(_))
    ));
    assert_eq!(engine.held_rows().unwrap().len(), 1);
    let store = engine.store().unwrap();
    assert!(
        store
            .peek_pending()
            .unwrap()
            .iter()
            .all(|entry| entry.op().unwrap().stream != "children")
    );
    assert_eq!(
        store
            .peek_snapshot("children", "child")
            .unwrap()
            .unwrap()
            .data["title"],
        text("keep")
    );
    engine.try_close().await.unwrap();
}

#[tokio::test]
async fn held_recreation_keeps_its_predecessor_across_store_reopen() {
    let directory = temp_directory("held-recreation");
    let wire = StubTransport::new();
    let gate = Gate::new(Some("notes"));
    let engine = ReplicaEngine::new(options(&directory, wire.clone(), gate.clone()));
    engine.open(42).await.unwrap();
    engine
        .save_row("notes", "n", None, &ReplicaFields::new())
        .await
        .unwrap();
    engine.drain().await.unwrap();
    let first = wire.pushed_ops()[0].clone();
    engine.delete_row("notes", "n").await.unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("blob", text("upload"))]))
        .await
        .unwrap();
    assert_eq!(engine.held_rows().unwrap().len(), 1);
    engine.try_close().await.unwrap();
    drop(engine);

    let reopened = ReplicaEngine::new(options(&directory, wire.clone(), gate.clone()));
    reopened.open(42).await.unwrap();
    assert_eq!(reopened.held_rows().unwrap().len(), 1);
    gate.open.store(true, Ordering::SeqCst);
    reopened.refresh_sync_gates(None).await.unwrap();
    reopened.drain().await.unwrap();
    let sent = wire.pushed_ops();
    let birth = sent.last().unwrap();
    assert_eq!(birth.verb, verb::ROW_CREATE);
    assert_eq!(birth.replaces, first.incarnation);
    assert!(birth.replaces.is_some());
    reopened.try_close().await.unwrap();
}
