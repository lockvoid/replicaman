use super::support::*;
use crate::*;

const CHILD: &str = "pmck/Element/é/offline";
const EXPECTED: &str = "derived:eeaecc490e5bd06da25475507ce21c4cefd406e00f06c9f3bdd4ed03e2eb9720";

fn schema() -> ReplicaSchema {
    ReplicaSchema::new(streams()).with_identity("refs", 1)
}

fn streams() -> Vec<ReplicaStreamSpec> {
    let mut child = ReplicaStreamSpec::row("children");
    child.references = vec![ReplicaReferenceSpec {
        name: "parent".into(),
        stream: "parents".into(),
        field: None,
        key_segment: Some(2),
        key_prefix: Some("pmck/Element/".into()),
        optional: false,
    }];
    child.lifetime_from = Some("parent".into());
    vec![ReplicaStreamSpec::row("parents"), child]
}

#[tokio::test]
async fn birth_and_patch_carry_the_parent_lifetime_and_cross_language_identity() {
    let store = store("reference-birth");
    let mut options = options(engine_directory(), StubTransport::new());
    options.schema = schema();
    let engine = engine_with(store.clone(), OWNER, options);
    engine
        .create_row("parents", "é", None, &ReplicaFields::new())
        .await
        .unwrap();
    store
        .pool()
        .write(|ctx| store.set_incarnation(&ctx.tx, "parents", "é", "user", "parent-life"))
        .unwrap();
    engine
        .create_row(
            "children",
            CHILD,
            None,
            &fields(&[("value", text("first"))]),
        )
        .await
        .unwrap();
    engine
        .update_row(
            "children",
            CHILD,
            None,
            &fields(&[("value", text("second"))]),
        )
        .await
        .unwrap();
    let operations: Vec<_> = store
        .peek_pending()
        .unwrap()
        .iter()
        .map(|entry| entry.op().unwrap())
        .filter(|op| op.stream == "children")
        .collect();
    assert_eq!(operations.len(), 2);
    for op in operations {
        assert_eq!(op.incarnation.as_deref(), Some(EXPECTED));
        assert_eq!(
            op.references,
            vec![ReplicaReference {
                name: "parent".into(),
                stream: "parents".into(),
                id: "é".into(),
                incarnation: "parent-life".into(),
            }]
        );
    }
}

#[tokio::test]
async fn a_refusal_of_a_reborn_derived_lifetime_is_kept_after_the_rebirth_is_shown() {
    let store = store("reference-reborn");
    let transport = StubTransport::new();
    let mut options = options(engine_directory(), transport.clone());
    options.schema = ReplicaSchema::new(streams());
    let engine = engine_with(store.clone(), OWNER, options);
    engine
        .create_row("parents", "é", None, &ReplicaFields::new())
        .await
        .unwrap();
    transport.script_push(|ops| {
        ops.iter()
            .map(|op| {
                if op.stream == "children" {
                    ReplicaVerdict::rejected(&op.id, "first birth refused")
                } else {
                    ReplicaVerdict::accepted(&op.id)
                }
            })
            .collect()
    });
    engine
        .create_row(
            "children",
            CHILD,
            None,
            &fields(&[("value", text("first"))]),
        )
        .await
        .unwrap();
    engine.drain().await.unwrap();

    transport.script_push(|ops| {
        ops.iter()
            .map(|op| {
                if op.verb == verb::ROW_PATCH {
                    ReplicaVerdict::rejected(&op.id, "patch refused")
                } else {
                    ReplicaVerdict::accepted(&op.id)
                }
            })
            .collect()
    });
    engine
        .create_row(
            "children",
            CHILD,
            None,
            &fields(&[("value", text("again"))]),
        )
        .await
        .unwrap();
    engine.drain().await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                ScriptedFrame::RowSet {
                    stream: "parents".into(),
                    id: "é".into(),
                    row_type: None,
                    data: ReplicaFields::new(),
                },
                ScriptedFrame::RowSet {
                    stream: "children".into(),
                    id: CHILD.into(),
                    row_type: None,
                    data: fields(&[("value", text("again"))]),
                },
            ],
            "c1",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    engine
        .update_row(
            "children",
            CHILD,
            None,
            &fields(&[("value", text("edited"))]),
        )
        .await
        .unwrap();
    engine.drain().await.unwrap();

    let mut refusals: Vec<String> = engine
        .parked_ops()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|row| row.parked)
        .collect();
    refusals.sort();
    assert_eq!(refusals, vec!["first birth refused", "patch refused"]);
}

#[tokio::test]
async fn missing_parent_rolls_back_the_child_and_its_journal() {
    let store = store("reference-missing");
    let mut options = options(engine_directory(), StubTransport::new());
    options.schema = schema();
    let engine = engine_with(store.clone(), OWNER, options);
    assert!(matches!(
        engine
            .create_row("children", CHILD, None, &ReplicaFields::new())
            .await,
        Err(ReplicaError::Storage(_))
    ));
    assert!(store.peek_snapshot("children", CHILD).unwrap().is_none());
    assert!(store.peek_pending().unwrap().is_empty());
}

#[tokio::test]
async fn replacement_parent_cannot_retarget_existing_child_authoring() {
    let store = store("reference-replacement");
    let mut options = options(engine_directory(), StubTransport::new());
    options.schema = schema();
    let engine = engine_with(store.clone(), OWNER, options);
    engine
        .create_row("parents", "é", None, &ReplicaFields::new())
        .await
        .unwrap();
    engine
        .create_row("children", CHILD, None, &fields(&[("value", text("keep"))]))
        .await
        .unwrap();
    let pending = store.peek_pending().unwrap();
    store
        .pool()
        .write(|ctx| store.set_incarnation(&ctx.tx, "parents", "é", "user", "replacement"))
        .unwrap();
    assert!(matches!(
        engine
            .update_row(
                "children",
                CHILD,
                None,
                &fields(&[("value", text("wrong parent"))])
            )
            .await,
        Err(ReplicaError::Storage(_))
    ));
    assert_eq!(store.peek_pending().unwrap(), pending);
    assert_eq!(
        store
            .peek_snapshot("children", CHILD)
            .unwrap()
            .unwrap()
            .data["value"],
        text("keep")
    );
}

struct DeleteReferenceGate {
    held_birth: bool,
    released: std::sync::atomic::AtomicBool,
}

impl SyncGate for DeleteReferenceGate {
    fn id(&self) -> &str {
        "reference-delete"
    }
    fn stream(&self) -> Option<&str> {
        Some("children")
    }
    fn judge(&self, change: &SyncChange) -> SyncGateDecision {
        if self.released.load(std::sync::atomic::Ordering::SeqCst) {
            SyncGateDecision::Push
        } else if self.held_birth || change.kind == SyncChangeKind::Delete {
            SyncGateDecision::Hold("waiting".into())
        } else {
            SyncGateDecision::Push
        }
    }
}

#[tokio::test]
async fn deleting_held_and_delivered_rows_retains_required_field_references() {
    for held_birth in [true, false] {
        let store = store("reference-delete");
        let gate = std::sync::Arc::new(DeleteReferenceGate {
            held_birth,
            released: std::sync::atomic::AtomicBool::new(false),
        });
        let mut child = ReplicaStreamSpec::row("children");
        child.references = vec![ReplicaReferenceSpec {
            name: "parent".into(),
            stream: "parents".into(),
            field: Some("parentId".into()),
            key_segment: None,
            key_prefix: None,
            optional: false,
        }];
        let mut options = options(engine_directory(), StubTransport::new());
        options.schema = ReplicaSchema::new(vec![ReplicaStreamSpec::row("parents"), child]);
        options.sync_gates = vec![gate.clone()];
        let engine = engine_with(store.clone(), OWNER, options);
        engine
            .create_row("parents", "parent", None, &ReplicaFields::new())
            .await
            .unwrap();
        engine
            .create_row(
                "children",
                "child",
                None,
                &fields(&[("parentId", text("parent"))]),
            )
            .await
            .unwrap();
        if !held_birth {
            engine.drain().await.unwrap();
        }

        engine.delete_row("children", "child").await.unwrap();
        assert!(store.peek_snapshot("children", "child").unwrap().is_none());
        gate.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        engine.refresh_sync_gates(None).await.unwrap();
        assert!(engine.held_rows().unwrap().is_empty());
        let deletes: Vec<_> = store
            .peek_pending()
            .unwrap()
            .into_iter()
            .map(|entry| entry.op().unwrap())
            .filter(|op| op.stream == "children")
            .collect();
        if held_birth {
            assert!(deletes.is_empty());
        } else {
            assert_eq!(deletes.len(), 1);
            assert_eq!(deletes[0].verb, verb::ROW_DELETE);
            assert_eq!(deletes[0].references[0].id, "parent");
        }
    }
}
