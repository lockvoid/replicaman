use super::support::*;
use crate::*;

#[tokio::test]
async fn uncertain_submissions_keep_their_exact_bytes_and_never_cross_principals() {
    let directory = temp_directory("adopt-frozen");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    engine.open(7).await.unwrap();
    engine
        .save_row(
            "notes",
            "offline",
            None,
            &fields(&[
                ("userId", ReplicaValue::signed_integer(7)),
                ("title", text("only copy")),
            ]),
        )
        .await
        .unwrap();
    let source = engine.store().unwrap();
    let before = source.peek_pending().unwrap();
    let frozen = source
        .pool()
        .write(|ctx| source.freeze_submissions(&ctx.tx, None, 100))
        .unwrap();
    let identity = source.pool().read(|db| source.meta(db)).unwrap();
    let reader = rusqlite::Connection::open_with_flags(
        source.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let original: Vec<u8> = reader
        .query_row("SELECT content FROM submissions", [], |row| row.get(0))
        .unwrap();
    drop(reader);

    engine.seal().await.unwrap();
    engine.adopt_merged(7, 42).await.unwrap();
    let target = engine.store().unwrap();
    let state = target.pool().read(|db| target.meta(db)).unwrap();
    assert_ne!(state.store, identity.store);
    assert_eq!(state.next, 1);
    assert!(
        target
            .pool()
            .write(|ctx| target.freeze_submissions(&ctx.tx, None, 100))
            .unwrap()
            .is_empty(),
        "no submission crosses into the adopting principal's queue"
    );
    assert!(target.peek_pending().unwrap().is_empty());
    let parked = target.peek_parked().unwrap();
    assert_eq!(parked[0].payload, before[0].payload);
    assert!(parked[0].parked.as_deref().unwrap().contains("recovery"));
    assert_eq!(
        target
            .peek_snapshot("notes", "offline")
            .unwrap()
            .unwrap()
            .data["title"],
        text("only copy")
    );

    let records = target.recovery_records(None, 100).unwrap();
    assert_eq!(records.len(), 1);
    let parts = target.recovery_parts(&records[0].id, None, 100).unwrap();
    let submission = parts
        .iter()
        .find(|part| part.kind == "submission")
        .expect("frozen content is archived");
    let archived = target
        .recovery_chunk(&records[0].id, submission, 0, 262144)
        .unwrap();
    assert_eq!(archived, original);
    assert!(
        String::from_utf8(archived)
            .unwrap()
            .contains(&frozen[0].ids[0]),
        "the archive keeps the operation id the server may already know"
    );
}

#[tokio::test]
async fn crash_before_host_identity_persistence_resumes_the_completed_copy_without_overwriting_it()
{
    let directory = temp_directory("adopt-resume");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    engine.open(7).await.unwrap();
    engine
        .save_row("notes", "guest", None, &fields(&[("title", text("guest"))]))
        .await
        .unwrap();
    engine.seal().await.unwrap();
    engine.adopt_merged(7, 42).await.unwrap();
    engine.unseal().await;
    engine
        .save_row(
            "notes",
            "arrived",
            None,
            &fields(&[("title", text("after adoption"))]),
        )
        .await
        .unwrap();
    engine.try_close().await.unwrap();

    // The simulated host still remembers the source owner after its crash.
    let reboot = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    reboot.open(7).await.unwrap();
    reboot.seal().await.unwrap();
    reboot.adopt_merged(7, 42).await.unwrap();
    let target = reboot.store().unwrap();
    assert_eq!(
        target
            .peek_snapshot("notes", "arrived")
            .unwrap()
            .unwrap()
            .data["title"],
        text("after adoption")
    );
    assert_eq!(target.peek_pending().unwrap().len(), 2);
}

#[tokio::test]
async fn failed_adoption_copy_leaves_the_source_bound_and_retryable() {
    let directory = temp_directory("adopt-failure");
    let engine = unopened_engine(directory.path().to_path_buf(), StubTransport::new());
    engine.open(7).await.unwrap();
    engine
        .save_row(
            "notes",
            "guest",
            None,
            &fields(&[
                ("userId", ReplicaValue::signed_integer(7)),
                ("title", text("unchanged")),
            ]),
        )
        .await
        .unwrap();
    let source = engine.store().unwrap();
    let before = source.peek_pending().unwrap();
    source
        .pool()
        .write(|ctx| {
            ctx.tx.execute_batch(
                "CREATE TRIGGER fail_adoption BEFORE UPDATE OF adopted_from ON meta
            BEGIN SELECT RAISE(ABORT, 'adoption failed'); END",
            )?;
            Ok(())
        })
        .unwrap();
    engine.seal().await.unwrap();
    let error = engine.adopt_merged(7, 42).await.unwrap_err();
    assert!(error.to_string().contains("adoption failed"));
    assert_eq!(engine.owner(), Some(7));
    assert_eq!(source.peek_pending().unwrap(), before);
    assert!(!directory.path().join("replica-42.sqlite").exists());
    source
        .pool()
        .write(|ctx| {
            ctx.tx.execute_batch("DROP TRIGGER fail_adoption")?;
            Ok(())
        })
        .unwrap();
    engine.adopt_merged(7, 42).await.unwrap();
    assert_eq!(engine.owner(), Some(42));
}

struct HoldChildren;
impl SyncGate for HoldChildren {
    fn id(&self) -> &str {
        "held-children"
    }
    fn judge(&self, change: &SyncChange) -> SyncGateDecision {
        if change.stream == "children" || change.stream == "leaves" {
            SyncGateDecision::Hold("upload pending".into())
        } else {
            SyncGateDecision::Push
        }
    }
}

#[tokio::test]
async fn held_descendants_of_uncertain_submissions_remain_recoverable_after_adoption() {
    use std::sync::Arc;
    let directory = temp_directory("adopt-held-descendants");
    let mut options = options(directory.path().to_path_buf(), StubTransport::new());
    let mut child = ReplicaStreamSpec::row("children");
    child.references = vec![ReplicaReferenceSpec {
        name: "parent".into(),
        stream: "parents".into(),
        field: Some("parentId".into()),
        key_segment: None,
        key_prefix: None,
        optional: false,
    }];
    let mut leaf = ReplicaStreamSpec::row("leaves");
    leaf.references = vec![ReplicaReferenceSpec {
        name: "parent".into(),
        stream: "children".into(),
        field: Some("parentId".into()),
        key_segment: None,
        key_prefix: None,
        optional: false,
    }];
    options.schema = ReplicaSchema::new(vec![ReplicaStreamSpec::row("parents"), child, leaf]);
    options.sync_gates = vec![Arc::new(HoldChildren)];
    let engine = ReplicaEngine::new(options);
    engine.open(7).await.unwrap();
    engine
        .create_row(
            "parents",
            "uncertain",
            None,
            &fields(&[("userId", ReplicaValue::signed_integer(7))]),
        )
        .await
        .unwrap();
    let source = engine.store().unwrap();
    source
        .pool()
        .write(|ctx| source.freeze_submissions(&ctx.tx, None, 100))
        .unwrap();

    for (stream, id, parent) in [
        ("children", "child", "uncertain"),
        ("leaves", "leaf", "child"),
    ] {
        engine
            .create_row(
                stream,
                id,
                None,
                &fields(&[
                    ("parentId", text(parent)),
                    ("userId", ReplicaValue::signed_integer(7)),
                ]),
            )
            .await
            .unwrap();
    }
    engine
        .create_row("parents", "unsent", None, &ReplicaFields::new())
        .await
        .unwrap();
    engine
        .create_row(
            "children",
            "unrelated",
            None,
            &fields(&[("parentId", text("unsent"))]),
        )
        .await
        .unwrap();
    assert_eq!(engine.held_rows().unwrap().len(), 3);

    engine.seal().await.unwrap();
    engine.adopt_merged(7, 42).await.unwrap();
    let target = engine.store().unwrap();
    let records = target.recovery_records(None, 100).unwrap();
    let archived: std::collections::BTreeSet<_> = records
        .iter()
        .map(|record| record.row_id.as_str())
        .collect();
    assert_eq!(
        archived,
        ["uncertain", "child", "leaf"].into_iter().collect()
    );
    let held = engine.held_rows().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].row_id, "unrelated");
    assert!(
        target
            .peek_pending()
            .unwrap()
            .iter()
            .all(|entry| entry.op().unwrap().row_id != "child"
                && entry.op().unwrap().row_id != "leaf")
    );

    for record in records.iter().filter(|record| record.stream != "parents") {
        let parts = target.recovery_parts(&record.id, None, 100).unwrap();
        assert!(parts.iter().any(|part| part.kind == "hold.metadata"));
        let reference = parts.iter().find(|part| part.kind == "reference").unwrap();
        let value: serde_json::Value = serde_json::from_slice(
            &target
                .recovery_chunk(&record.id, reference, 0, 262144)
                .unwrap(),
        )
        .unwrap();
        assert!(!value["incarnation"].as_str().unwrap().is_empty());
        let data = parts.iter().find(|part| part.kind == "row.data").unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&target.recovery_chunk(&record.id, data, 0, 262144).unwrap())
                .unwrap();
        assert_eq!(
            value["userId"], 7,
            "archive preserves the outgoing principal's raw data"
        );
    }
}
