use super::support::*;
use crate::*;

#[tokio::test]
async fn delivery_stages_stay_unsettled_until_checkpoint_visibility() {
    let store = store("sync-status");
    let wire = StubTransport::new();
    let engine = engine_with(
        store.clone(),
        OWNER,
        options(engine_directory(), wire.clone()),
    );
    assert!(!store.sync_status().unwrap().has_unsettled_work());
    engine
        .create_row("notes", "n", None, &fields(&[("title", text("saved"))]))
        .await
        .unwrap();
    assert_eq!(store.sync_status().unwrap().queued_operations, 1);
    assert!(store.sync_status().unwrap().oldest_intent_at.is_some());

    store
        .pool()
        .write(|ctx| store.freeze_submissions(&ctx.tx, None, 100))
        .unwrap();
    assert_eq!(store.sync_status().unwrap().submitted_groups, 1);
    engine.drain().await.unwrap();
    assert!(store.peek_pending().unwrap().is_empty());
    let accepted = store.sync_status().unwrap();
    assert_eq!(accepted.submitted_groups, 0);
    assert_eq!(accepted.accepted_operations, 1);
    assert!(accepted.has_unsettled_work());
    assert!(
        store
            .contains_unsettled_operation(|op| Ok(op.row_id == "n"))
            .unwrap()
    );

    wire.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n", "saved", None)], "1:", false),
    );
    engine.pull_once("user").await.unwrap();
    assert!(!store.sync_status().unwrap().has_unsettled_work());
    assert!(
        !store
            .contains_unsettled_operation(|op| Ok(op.row_id == "n"))
            .unwrap()
    );
}

#[tokio::test]
async fn corrupt_intent_cannot_be_mistaken_for_no_work() {
    let store = store("sync-status-corrupt");
    let engine = engine_with(
        store.clone(),
        OWNER,
        options(engine_directory(), StubTransport::new()),
    );
    engine
        .create_row("notes", "n", None, &ReplicaFields::new())
        .await
        .unwrap();
    store
        .pool()
        .write(|ctx| {
            ctx.tx
                .execute("UPDATE intents SET payload = '{broken'", [])?;
            Ok(())
        })
        .unwrap();
    assert!(store.sync_status().unwrap().has_unsettled_work());
    assert!(matches!(
        store.contains_unsettled_operation(|_| Ok(false)),
        Err(ReplicaError::Storage(_))
    ));
    assert_eq!(store.peek_pending().unwrap().len(), 1);
}

#[test]
fn integrity_hashes_match_independent_vectors() {
    use crate::integrity::{IntegrityHash, base_integrity};
    let vector: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/integrity.json")).unwrap();
    let mut hash = IntegrityHash::new("replicaman-view");
    for row in vector["rows"].as_array().unwrap() {
        for field in ["stream", "id", "incarnation", "revision"] {
            hash.append(Some(row[field].as_str().unwrap().as_bytes()));
        }
    }
    assert_eq!(hash.finish(), vector["view_digest"].as_str().unwrap());
    assert_eq!(
        IntegrityHash::new("replicaman-view").finish(),
        vector["empty_view_digest"].as_str().unwrap()
    );
    assert_eq!(
        base_integrity([
            Some(b"notes"),
            Some("é/🙂".as_bytes()),
            Some(b"user"),
            Some(b"life-e"),
            Some(b"2"),
            None,
            Some(br#"{"n":9007199254740993}"#),
            Some(b"loro@1"),
            Some(&[0, 1, 255]),
        ]),
        vector["base_digest"].as_str().unwrap()
    );
}

#[tokio::test]
async fn integrity_failure_preserves_local_intent() {
    let store = store("integrity-corruption");
    let engine = engine_with(
        store.clone(),
        OWNER,
        options(engine_directory(), StubTransport::new()),
    );
    engine
        .create_row(
            "notes",
            "offline",
            None,
            &fields(&[("title", text("keep me"))]),
        )
        .await
        .unwrap();
    let row = crate::sync_store::BaseRow {
        incarnation: "lifetime".into(),
        revision: 2,
        row_type: None,
        data: fields(&[("title", text("server"))]),
        codec: Some("loro@1".into()),
        fold: Some(vec![0, 1, 2]),
    };
    store
        .pool()
        .write(|ctx| {
            store.save_base(&ctx.tx, "notes", "server", "user", &row)?;
            store.set_cursor(ctx, "checkpoint", "user")
        })
        .unwrap();
    let healthy = store
        .pool()
        .read(|db| store.integrity_snapshot(db, "user"))
        .unwrap();
    assert_eq!(healthy.count, 1);
    let before: Vec<_> = store
        .peek_pending()
        .unwrap()
        .into_iter()
        .map(|entry| entry.payload)
        .collect();
    for assignment in [
        "data = '{}'",
        "fold = X'FF'",
        "revision = 3",
        "incarnation = 'other'",
        "type = ''",
        "integrity = NULL",
    ] {
        store
            .pool()
            .write(|ctx| {
                store.save_base(&ctx.tx, "notes", "server", "user", &row)?;
                ctx.tx
                    .execute(&format!("UPDATE base SET {assignment}"), [])?;
                Ok(())
            })
            .unwrap();
        assert!(
            matches!(
                store.pool().read(|db| store.integrity_snapshot(db, "user")),
                Err(ReplicaError::Storage(_))
            ),
            "{assignment}"
        );
        let retained: Vec<_> = store
            .peek_pending()
            .unwrap()
            .into_iter()
            .map(|entry| entry.payload)
            .collect();
        assert_eq!(retained, before);
    }
    store
        .pool()
        .write(|ctx| store.save_base(&ctx.tx, "notes", "server", "user", &row))
        .unwrap();
    assert_eq!(
        store
            .pool()
            .read(|db| store.integrity_snapshot(db, "user"))
            .unwrap()
            .digest,
        healthy.digest
    );
}
