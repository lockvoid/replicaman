use super::support::*;
use crate::*;

#[tokio::test]
async fn damaged_authoring_and_large_folds_export_in_bounded_parts() {
    let store = store("bounded-recovery");
    let engine = engine(store.clone(), StubTransport::new());
    let fold = vec![97; 600_001];
    engine
        .create_doc("boards", "b1", &fold, 7, &ReplicaFields::new(), None)
        .await
        .unwrap();
    store
        .pool()
        .write(|ctx| {
            ctx.tx.execute(
                "UPDATE snapshots SET data = '{damaged' WHERE row_id = 'b1'",
                [],
            )?;
            ctx.tx.execute(
                "UPDATE intents SET payload = '{damaged' WHERE row_id = 'b1'",
                [],
            )?;
            store.archive_entity(&ctx.tx, "boards", "b1", "test export")
        })
        .unwrap();
    let records = store.recovery_records(None, 1).unwrap();
    let record = &records[0];
    let mut parts = Vec::new();
    loop {
        let page = store.recovery_parts(&record.id, parts.last(), 2).unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 2);
        parts.extend(page);
    }
    let document = parts
        .iter()
        .find(|part| part.kind == "document.fold")
        .unwrap();
    let mut exported = Vec::new();
    while (exported.len() as i64) < document.byte_count {
        let chunk = store
            .recovery_chunk(&record.id, document, exported.len() as i64, 65_536)
            .unwrap();
        assert!(!chunk.is_empty() && chunk.len() <= 65_536);
        exported.extend(chunk);
    }
    assert_eq!(exported, fold);
    for kind in ["row.data", "intent"] {
        let part = parts.iter().find(|part| part.kind == kind).unwrap();
        assert_eq!(
            store.recovery_chunk(&record.id, part, 0, 262144).unwrap(),
            b"{damaged"
        );
    }
    assert_eq!(store.fold("boards", "b1").unwrap().unwrap(), fold);

    let mut attempts = 0;
    let error = store
        .export_recovery(&record.id, |_| {
            attempts += 1;
            if attempts == 3 {
                Err(ReplicaError::Storage("export disk full".into()))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(error, ReplicaError::Storage("export disk full".into()));
    assert_eq!(store.recovery_records(None, 100).unwrap().len(), 1);

    let mut lines = Vec::<serde_json::Value>::new();
    store
        .export_recovery(&record.id, |bytes| {
            assert!(bytes.len() < 360_000, "export must stream large folds");
            lines.push(serde_json::from_slice(bytes).unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(lines.first().unwrap()["format"], "replicaman-recovery");
    assert_eq!(lines.last().unwrap()["type"], "complete");
    assert_eq!(lines.last().unwrap()["parts"], parts.len());
    assert_eq!(
        lines.last().unwrap()["bytes"],
        parts
            .iter()
            .map(|part| part.byte_count)
            .sum::<i64>()
            .to_string()
    );
    let mut kind = None;
    let mut reconstructed = Vec::new();
    for line in &lines {
        if line["type"] == "part" {
            kind = line["kind"].as_str();
        }
        if kind == Some("document.fold") && line["type"] == "chunk" {
            assert_eq!(line["offset"], reconstructed.len().to_string());
            use base64::Engine;
            let chunk = base64::engine::general_purpose::STANDARD
                .decode(line["content"].as_str().unwrap())
                .unwrap();
            // SHA-256 constants computed independently from the original fixture bytes.
            let digest = if chunk.len() == 262_144 {
                "dd3dde87623d9a6b354c68c943d189c89c63652d945e7bbdf0986cae91a49521"
            } else {
                "845671f868efb188716917bfc3b8a3c61c74a8a77195edaae9df445de3ec0a45"
            };
            assert_eq!(line["sha256"], digest);
            reconstructed.extend(chunk);
        }
    }
    assert_eq!(reconstructed, fold);

    store.remove_recovery_record(&record.id).unwrap();
    assert!(store.recovery_records(None, 100).unwrap().is_empty());
    assert!(
        store
            .recovery_parts(&record.id, None, 100)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn archive_failure_rolls_back_every_part_and_keeps_the_original() {
    let store = store("recovery-rollback");
    let engine = engine(store.clone(), StubTransport::new());
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("keep"))]))
        .await
        .unwrap();
    let pending = store.peek_pending().unwrap();
    store
        .pool()
        .write(|ctx| {
            ctx.tx.execute_batch(
                "CREATE TRIGGER fail_archive BEFORE INSERT ON recovery_parts
            WHEN NEW.kind = 'intent' BEGIN SELECT RAISE(ABORT, 'archive failed'); END",
            )?;
            Ok(())
        })
        .unwrap();
    assert!(
        store
            .pool()
            .write(|ctx| store.archive_entity(&ctx.tx, "notes", "n1", "must roll back"))
            .is_err()
    );
    assert!(store.recovery_records(None, 100).unwrap().is_empty());
    assert_eq!(
        store
            .pool()
            .read(|db| Ok(
                db.query_row("SELECT count(*) FROM recovery_parts", [], |row| row
                    .get::<_, i64>(0))?
            ))
            .unwrap(),
        0
    );
    assert_eq!(store.peek_pending().unwrap(), pending);
    assert_eq!(
        store.peek_snapshot("notes", "n1").unwrap().unwrap().data["title"],
        text("keep")
    );
}

#[tokio::test]
async fn corrupt_journal_failure_propagates_on_every_pull() {
    let store = store("failed-authoring-pull");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("keep"))]))
        .await
        .unwrap();
    store
        .pool()
        .write(|ctx| {
            ctx.tx
                .execute("UPDATE intents SET payload = '{damaged'", [])?;
            Ok(())
        })
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            engine.pull_once("user").await,
            Err(ReplicaError::Storage(_))
        ));
    }
    assert_eq!(transport.pull_count(), 0);
    assert_eq!(engine.current_cursor("user").await.unwrap(), None);
    assert_eq!(
        store
            .pool()
            .read(
                |db| Ok(db.query_row("SELECT payload FROM intents", [], |row| row
                    .get::<_, String>(0))?)
            )
            .unwrap(),
        "{damaged"
    );
}
