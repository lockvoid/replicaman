use super::support::*;
use crate::*;
use std::sync::Arc;

#[tokio::test]
async fn a_group_remains_one_submission_with_its_minted_ids_after_reopen() {
    let directory = temp_directory("atomic-reopen");
    let wire = StubTransport::new();
    let engine = unopened_engine(directory.to_path_buf(), wire.clone());
    engine.open(42).await.unwrap();
    engine
        .write_atomically(|tx| {
            for index in 0..60 {
                tx.create(
                    "notes",
                    &format!("n-{index}"),
                    None,
                    &fields(&[("title", text("group"))]),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let store = engine.store().unwrap();
    let saved = store
        .pool()
        .read(|db| store.frozen_submissions(db, 100))
        .unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].entries.len(), 60);
    let members: Vec<ReplicaOp> = saved[0]
        .operations
        .iter()
        .map(|value| ReplicaOp::from_value(value).unwrap())
        .collect();
    let group = members[0]
        .group
        .clone()
        .expect("an atomic action names its group");
    assert!(members.iter().all(|op| op.group.as_ref() == Some(&group)));
    assert_eq!(
        members.iter().map(|op| op.id.clone()).collect::<Vec<_>>(),
        saved[0].ids
    );
    assert!(
        saved[0]
            .ids
            .iter()
            .all(|id| id.len() == 36 && &id[14..15] == "7")
    );
    engine.close().await.unwrap();

    let reopened = unopened_engine(directory.to_path_buf(), wire.clone());
    reopened.open(42).await.unwrap();
    let store = reopened.store().unwrap();
    let retained = store
        .pool()
        .read(|db| store.frozen_submissions(db, 100))
        .unwrap();
    assert_eq!(retained[0].sequence, saved[0].sequence);
    assert_eq!(retained[0].operations, saved[0].operations);
    reopened.drain().await.unwrap();
    assert_eq!(
        wire.pushed_ops()
            .iter()
            .map(|op| op.id.clone())
            .collect::<Vec<_>>(),
        saved[0].ids
    );
    assert_eq!(
        wire.pushed_ops()
            .iter()
            .map(|op| op.row_id.clone())
            .collect::<Vec<_>>(),
        (0..60)
            .map(|index| format!("n-{index}"))
            .collect::<Vec<_>>()
    );
    assert!(store.pending_ops().unwrap().is_empty());
    reopened.close().await.unwrap();
}

struct MemberGate {
    discard: bool,
}
impl SyncGate for MemberGate {
    fn id(&self) -> &str {
        "member"
    }
    fn stream(&self) -> Option<&str> {
        Some("notes")
    }
    fn judge(&self, change: &SyncChange) -> SyncGateDecision {
        if change.row_id != "blocked" {
            return SyncGateDecision::Push;
        }
        if self.discard {
            SyncGateDecision::Discard
        } else {
            SyncGateDecision::Hold("not uploaded".into())
        }
    }
}

#[tokio::test]
async fn one_held_or_discarded_member_rolls_back_every_member() {
    for discard in [false, true] {
        let store = store("atomic-gate");
        let mut options = options(engine_directory(), StubTransport::new());
        options.sync_gates = vec![Arc::new(MemberGate { discard })];
        let engine = engine_with(store.clone(), OWNER, options);
        let result = engine
            .write_atomically(|tx| {
                tx.create("notes", "ready", None, &ReplicaFields::new())?;
                tx.create("notes", "blocked", None, &ReplicaFields::new())
            })
            .await;
        assert!(matches!(result, Err(ReplicaError::AtomicWriteBlocked(_))));
        assert!(store.peek_snapshot("notes", "ready").unwrap().is_none());
        assert!(store.peek_snapshot("notes", "blocked").unwrap().is_none());
        assert!(store.pending_ops().unwrap().is_empty());
        assert!(engine.held_rows().unwrap().is_empty());
        assert_eq!(store.pool().read(|db| store.meta(db)).unwrap().next, 1);
    }
}

#[tokio::test]
async fn unsubmitted_dependency_and_oversized_action_leave_original_work_intact() {
    let store = store("atomic-prior");
    let engine = engine(store.clone(), StubTransport::new());
    engine
        .create_row(
            "notes",
            "earlier",
            None,
            &fields(&[("title", text("original"))]),
        )
        .await
        .unwrap();
    let original = store.pending_ops().unwrap();
    let result = engine
        .write_atomically(|tx| {
            tx.create("notes", "new", None, &ReplicaFields::new())?;
            tx.update("notes", "earlier", &fields(&[("title", text("overtaken"))]))
        })
        .await;
    assert!(matches!(result, Err(ReplicaError::AtomicWriteBlocked(_))));
    assert_eq!(store.pending_ops().unwrap(), original);
    assert!(store.peek_snapshot("notes", "new").unwrap().is_none());
    let result = engine
        .write_atomically(|tx| {
            for index in 0..101 {
                tx.create(
                    "notes",
                    &format!("limit-{index}"),
                    None,
                    &ReplicaFields::new(),
                )?;
            }
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(ReplicaError::AtomicWriteBlocked(_))));
    assert_eq!(store.pending_ops().unwrap(), original);
    assert!(store.peek_snapshot("notes", "limit-0").unwrap().is_none());
}

#[tokio::test]
async fn refusal_reverts_whole_action_and_retains_both_reasons() {
    let store = store("atomic-refused");
    let wire = StubTransport::new();
    reject_all(&wire, "action refused");
    let engine = engine(store.clone(), wire);
    engine
        .write_atomically(|tx| {
            tx.create("notes", "a", None, &ReplicaFields::new())?;
            tx.create("notes", "b", None, &ReplicaFields::new())
        })
        .await
        .unwrap();
    engine.drain().await.unwrap();
    assert!(store.peek_snapshot("notes", "a").unwrap().is_none());
    assert!(store.peek_snapshot("notes", "b").unwrap().is_none());
    assert_eq!(
        store
            .parked_ops()
            .unwrap()
            .iter()
            .map(|row| row.parked.as_deref())
            .collect::<Vec<_>>(),
        [Some("action refused"), Some("action refused")]
    );
    assert_eq!(store.recovery_records(None, 100).unwrap().len(), 2);
}

#[tokio::test]
async fn later_edits_cannot_change_frozen_group_bytes() {
    let store = store("atomic-later");
    let wire = StubTransport::new();
    let engine = engine(store.clone(), wire.clone());
    engine
        .write_atomically(|tx| {
            tx.create("notes", "a", None, &fields(&[("title", text("first"))]))?;
            tx.create("notes", "b", None, &fields(&[("title", text("second"))]))
        })
        .await
        .unwrap();
    let frozen = store
        .pool()
        .read(|db| store.frozen_submissions(db, 100))
        .unwrap();
    engine
        .update_row("notes", "a", None, &fields(&[("title", text("later"))]))
        .await
        .unwrap();
    engine.delete_row("notes", "b").await.unwrap();
    let retained = store
        .pool()
        .read(|db| store.frozen_submissions(db, 100))
        .unwrap();
    assert_eq!(retained[0].operations, frozen[0].operations);
    engine.drain().await.unwrap();
    assert_eq!(
        wire.pushed_ops()
            .iter()
            .map(|op| op.verb.as_str())
            .collect::<Vec<_>>(),
        ["row.create", "row.create", "row.patch", "row.delete"]
    );
    assert_eq!(
        store
            .peek_snapshot("notes", "a")
            .unwrap()
            .unwrap()
            .data
            .get("title"),
        Some(&text("later"))
    );
    assert!(store.peek_snapshot("notes", "b").unwrap().is_none());
}

#[tokio::test]
async fn group_can_create_parent_and_child_and_edit_parent_before_freezing() {
    let store = store("atomic-family");
    let mut notes = ReplicaStreamSpec::row("notes");
    notes.references = vec![ReplicaReferenceSpec {
        name: "parent".into(),
        stream: "notes".into(),
        field: None,
        key_segment: Some(1),
        key_prefix: Some("child/".into()),
        optional: true,
    }];
    let mut options = options(engine_directory(), StubTransport::new());
    options.schema = ReplicaSchema::new(vec![notes]);
    let engine = engine_with(store.clone(), OWNER, options);
    engine
        .write_atomically(|tx| {
            tx.create(
                "notes",
                "parent",
                None,
                &fields(&[("title", text("First"))]),
            )?;
            tx.create(
                "notes",
                "child/parent",
                None,
                &fields(&[("title", text("Child"))]),
            )?;
            tx.update("notes", "parent", &fields(&[("title", text("Last"))]))
        })
        .await
        .unwrap();
    let groups = store
        .pool()
        .read(|db| store.frozen_submissions(db, 100))
        .unwrap();
    assert_eq!(groups.len(), 1);
    let operations: Vec<_> = groups[0]
        .operations
        .iter()
        .map(|value| ReplicaOp::from_value(value).unwrap())
        .collect();
    assert_eq!(
        operations
            .iter()
            .map(|op| op.row_id.as_str())
            .collect::<Vec<_>>(),
        ["parent", "child/parent", "parent"]
    );
    assert_eq!(operations[0].incarnation, operations[2].incarnation);
    assert_eq!(
        Some(&operations[1].references[0].incarnation),
        operations[0].incarnation.as_ref()
    );
    assert_eq!(operations[0].data.as_ref().unwrap()["title"], text("First"));
    assert_eq!(operations[2].data.as_ref().unwrap()["title"], text("Last"));

    engine.drain().await.unwrap();
    assert!(store.peek_pending().unwrap().is_empty());
    assert_eq!(
        store
            .peek_snapshot("notes", "parent")
            .unwrap()
            .unwrap()
            .data["title"],
        text("Last")
    );
    assert_eq!(
        store
            .peek_snapshot("notes", "child/parent")
            .unwrap()
            .unwrap()
            .data["title"],
        text("Child")
    );
}
