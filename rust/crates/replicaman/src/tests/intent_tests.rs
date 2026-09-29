//! One local write, one `intents` row, from owed to its verdict.

use super::support::*;
use crate::*;

fn intents(store: &ReplicaStateStore) -> Vec<(String, String, Option<i64>)> {
    store
        .pool()
        .read(|db| {
            Ok(db
                .prepare("SELECT row_id, state, sequence FROM intents ORDER BY rowid")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .collect::<rusqlite::Result<_>>()?)
        })
        .unwrap()
}

fn title(store: &ReplicaStateStore, id: &str) -> Option<ReplicaValue> {
    store
        .peek_snapshot("notes", id)
        .unwrap()
        .map(|row| row.data["title"].clone())
}

#[tokio::test]
async fn a_frozen_intent_survives_the_archive_of_its_address_until_its_verdict() {
    let store = store("intent-frozen-archive");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "server", None)], "c1", false),
    );
    engine.pull_once("user").await.unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("local"))]))
        .await
        .unwrap();
    transport.fail_pushes(true);
    assert!(engine.drain().await.is_err());
    assert_eq!(
        intents(&store),
        [("n1".to_owned(), "frozen".to_owned(), Some(1))]
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![row_delete("notes", "n1"), note("n1", "replacement", None)],
            "c2",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(title(&store, "n1"), Some(text("replacement")));
    assert_eq!(
        store.recovery_records(None, 100).unwrap().len(),
        1,
        "the replaced lifetime's branch was archived"
    );
    assert_eq!(
        intents(&store),
        [("n1".to_owned(), "frozen".to_owned(), Some(1))],
        "the archive leaves frozen bytes to their verdict"
    );
    assert_eq!(store.sync_status().unwrap().submitted_groups, 1);

    transport.fail_pushes(false);
    let verdicts = engine.drain().await.unwrap();
    let requests = transport.push_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0], requests[1],
        "the retry resends the frozen operation"
    );
    assert_eq!(
        verdicts
            .iter()
            .map(|verdict| &verdict.id)
            .collect::<Vec<_>>(),
        requests[1].iter().collect::<Vec<_>>()
    );
    assert!(
        intents(&store).is_empty(),
        "its verdict consumed the archived lifetime's intent"
    );
    assert_eq!(store.sync_status().unwrap().submitted_groups, 0);
    assert!(store.peek_parked().unwrap().is_empty());
    assert_eq!(title(&store, "n1"), Some(text("replacement")));
}

#[tokio::test]
async fn an_accepted_intent_stays_until_a_round_that_began_after_it_publishes() {
    let store = store("intent-accepted-visible");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine.pull_once("user").await.unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("first"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();
    assert_eq!(
        intents(&store),
        [("n1".to_owned(), "accepted".to_owned(), Some(1))]
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "first", None)], "c1", true),
    );
    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("second"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();
    assert_eq!(
        intents(&store),
        [
            ("n1".to_owned(), "accepted".to_owned(), Some(1)),
            ("n2".to_owned(), "accepted".to_owned(), Some(2))
        ]
    );

    transport.queue_pull("user", ScriptedPull::new(Vec::new(), "c2", false));
    engine.pull_once("user").await.unwrap();
    assert_eq!(
        intents(&store),
        [("n2".to_owned(), "accepted".to_owned(), Some(2))],
        "the round began after the first acceptance only"
    );
    assert_eq!(title(&store, "n2"), Some(text("second")));

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n2", "second", None)], "c3", false),
    );
    engine.pull_once("user").await.unwrap();
    assert!(intents(&store).is_empty());
    assert_eq!(title(&store, "n1"), Some(text("first")));
    assert_eq!(title(&store, "n2"), Some(text("second")));
}

fn deltas(store: &ReplicaStateStore) -> Vec<(String, String, Vec<u8>)> {
    store
        .pool()
        .read(|db| {
            let rows: Vec<(String, String, String)> = db
                .prepare(
                    "SELECT id, state, payload FROM intents WHERE op = 'doc.delta' ORDER BY rowid",
                )?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .collect::<rusqlite::Result<_>>()?;
            rows.into_iter()
                .map(|(id, state, payload)| {
                    let op = ReplicaOp::from_json(payload.as_bytes())?;
                    Ok((id, state, op.payload.unwrap()))
                })
                .collect()
        })
        .unwrap()
}

#[tokio::test]
async fn an_edit_never_rewrites_frozen_bytes_and_starts_the_next_owed_delta() {
    let store = store("intent-frozen-delta");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine
        .create_doc("boards", "b1", b"SEED", 7, &ReplicaFields::new(), None)
        .await
        .unwrap();
    engine.drain().await.unwrap();
    engine
        .record_doc_delta("boards", "b1", b"+a")
        .await
        .unwrap();
    transport.fail_pushes(true);
    assert!(engine.drain().await.is_err());

    engine
        .record_doc_delta("boards", "b1", b"+b")
        .await
        .unwrap();
    let owed = deltas(&store)[1].0.clone();
    engine
        .record_doc_delta("boards", "b1", b"+c")
        .await
        .unwrap();
    let recorded = deltas(&store);
    assert_eq!(recorded.len(), 2);
    assert_eq!(
        (recorded[0].1.as_str(), recorded[0].2.as_slice()),
        ("frozen", &b"+a"[..]),
        "frozen bytes are never edited"
    );
    assert_eq!(
        (
            recorded[1].0.as_str(),
            recorded[1].1.as_str(),
            recorded[1].2.as_slice()
        ),
        (owed.as_str(), "owed", &b"+a+b+c"[..]),
        "later edits merge into the one owed delta under its id"
    );

    transport.fail_pushes(false);
    engine.drain().await.unwrap();
    let requests = transport.push_requests();
    assert_eq!(
        requests[1], requests[2],
        "the frozen delta is resent as the same operation"
    );
    let payloads: Vec<Vec<u8>> = transport
        .pushed_ops()
        .into_iter()
        .filter(|op| op.verb == verb::DOC_DELTA)
        .map(|op| op.payload.unwrap())
        .collect();
    assert_eq!(payloads, [b"+a".to_vec(), b"+a+b+c".to_vec()]);
    assert!(store.peek_pending().unwrap().is_empty());
}
