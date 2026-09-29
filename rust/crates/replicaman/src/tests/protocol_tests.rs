//! Protocol 2 against the fixture server: the dataset fence, idempotent push
//! retries, verdict validation, push bounds, staged pull rounds and
//! verification. The mixed-client E2E histories repeat these against Rails.

use super::support::*;
use crate::*;

fn meta(store: &ReplicaStateStore) -> crate::sync_store::Meta {
    store.pool().read(|db| store.meta(db)).unwrap()
}

fn staged(store: &ReplicaStateStore) -> (i64, i64) {
    store
        .pool()
        .read(|db| {
            Ok(db.query_row(
                "SELECT (SELECT COUNT(*) FROM downloads), (SELECT COUNT(*) FROM download_pages)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .unwrap()
}

fn pull_cursors(transport: &StubTransport) -> Vec<Option<String>> {
    transport
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WireEvent::Pull { cursor, .. } => Some(cursor),
            WireEvent::Push { .. } => None,
        })
        .collect()
}

fn title(store: &ReplicaStateStore, id: &str) -> Option<ReplicaValue> {
    store
        .peek_snapshot("notes", id)
        .unwrap()
        .map(|row| row.data["title"].clone())
}

fn paged_engine(
    store: &StoreFixture,
    transport: &std::sync::Arc<StubTransport>,
) -> std::sync::Arc<ReplicaEngine> {
    let mut options = options(engine_directory(), transport.clone());
    options.batch_limit = 1;
    engine_with((*store).clone(), OWNER, options)
}

#[tokio::test]
async fn a_store_learns_its_dataset_from_its_first_pull_and_refuses_another() {
    let store = store("protocol-dataset");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    assert_eq!(meta(&store).dataset, None);
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "c1", false),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(meta(&store).dataset.as_deref(), Some("fixture-dataset"));

    engine
        .save_row("notes", "n2", None, &fields(&[("title", text("offline"))]))
        .await
        .unwrap();
    transport.rotate_dataset("restored");
    let rows = store.all_snapshots().unwrap();
    let pending = store.peek_pending().unwrap();
    for refused in [
        engine.drain().await,
        engine.pull_once("user").await.map(|_| Vec::new()),
    ] {
        assert!(
            matches!(refused, Err(ReplicaError::Protocol { ref code, .. }) if code == "DatasetChanged"),
            "{refused:?}"
        );
    }
    assert_eq!(store.all_snapshots().unwrap(), rows);
    assert_eq!(store.peek_pending().unwrap(), pending);
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("c1")
    );
    assert_eq!(meta(&store).dataset.as_deref(), Some("fixture-dataset"));
}

#[tokio::test]
async fn a_store_that_never_synchronized_pulls_before_its_first_push() {
    let store = store("protocol-first-push");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("first"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();

    let events = transport.events();
    assert!(
        matches!(&events[..], [
        WireEvent::Pull { shard, cursor: None },
        WireEvent::Push { ids },
    ] if shard == "user" && ids.len() == 1),
        "{events:?}"
    );
    assert_eq!(meta(&store).dataset.as_deref(), Some("fixture-dataset"));
    assert!(store.peek_pending().unwrap().is_empty());
}

#[tokio::test]
async fn operations_carry_version_7_ids_and_only_an_atomic_action_names_a_group() {
    let store = store("protocol-ids");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine.pull_once("user").await.unwrap();
    engine
        .save_row(
            "notes",
            "single",
            None,
            &fields(&[("title", text("alone"))]),
        )
        .await
        .unwrap();
    engine
        .write_atomically(|tx| {
            tx.create("notes", "a", None, &ReplicaFields::new())?;
            tx.create("notes", "b", None, &ReplicaFields::new())
        })
        .await
        .unwrap();
    engine.drain().await.unwrap();

    let pushed = transport.pushed_ops();
    assert_eq!(pushed.len(), 3);
    assert!(
        pushed
            .iter()
            .all(|op| uuid::Uuid::parse_str(&op.id).unwrap().get_version_num() == 7)
    );
    let grouped: Vec<_> = pushed.iter().filter(|op| op.group.is_some()).collect();
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].group, grouped[1].group);
    assert_eq!(
        pushed
            .iter()
            .find(|op| op.row_id == "single")
            .unwrap()
            .group,
        None
    );
    assert!(pushed.iter().all(|op| op.incarnation.is_some()));
}

#[tokio::test]
async fn a_lost_reply_replays_the_same_operations_and_the_server_answers_from_its_claims() {
    let directory = temp_directory("protocol-lost-reply");
    let transport = StubTransport::new();
    let engine = unopened_engine(directory.path().to_path_buf(), transport.clone());
    engine.open(OWNER).await.unwrap();
    engine.pull_once("user").await.unwrap();
    for id in ["n1", "n2"] {
        engine
            .save_row("notes", id, None, &fields(&[("title", text(id))]))
            .await
            .unwrap();
    }
    transport.lose_push_replies(1);
    assert!(matches!(
        engine.drain().await,
        Err(ReplicaError::Transport(_))
    ));
    let store = engine.store().unwrap();
    assert_eq!(store.peek_pending().unwrap().len(), 2);
    assert_eq!(
        transport.pushed_batches().len(),
        1,
        "the server executed the batch"
    );
    engine.close().await.unwrap();

    let reopened = unopened_engine(directory.path().to_path_buf(), transport.clone());
    reopened.open(OWNER).await.unwrap();
    let verdicts = reopened.drain().await.unwrap();
    let requests = transport.push_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0], requests[1],
        "the retry resends the frozen operations"
    );
    assert_eq!(
        verdicts
            .iter()
            .map(|verdict| verdict.id.clone())
            .collect::<Vec<_>>(),
        requests[0]
    );
    assert_eq!(
        transport.pushed_batches().len(),
        1,
        "stored verdicts; nothing ran twice"
    );
    let store = reopened.store().unwrap();
    assert!(store.peek_pending().unwrap().is_empty());
    assert_eq!(store.sync_status().unwrap().accepted_operations, 2);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn a_malformed_verdict_set_acknowledges_nothing() {
    type Fault = fn(&mut Vec<ReplicaVerdict>);
    let faults: [(&str, Fault); 4] = [
        ("missing", |verdicts| {
            verdicts.pop();
        }),
        ("duplicate", |verdicts| {
            let first = verdicts[0].clone();
            verdicts.insert(0, first);
        }),
        ("foreign", |verdicts| {
            let last = verdicts.len() - 1;
            verdicts[last].id = crate::id::uuid();
        }),
        ("mixed group", |verdicts| {
            let last = verdicts.len() - 1;
            verdicts[last] = ReplicaVerdict::rejected(verdicts[last].id.clone(), "injected");
        }),
    ];
    for (kind, fault) in faults {
        let store = store("protocol-verdicts");
        let transport = StubTransport::new();
        let engine = engine(store.clone(), transport.clone());
        engine.pull_once("user").await.unwrap();
        if kind == "mixed group" {
            engine
                .write_atomically(|tx| {
                    tx.create("notes", "a", None, &ReplicaFields::new())?;
                    tx.create("notes", "b", None, &ReplicaFields::new())
                })
                .await
                .unwrap();
        } else {
            for id in ["a", "b"] {
                engine
                    .save_row("notes", id, None, &ReplicaFields::new())
                    .await
                    .unwrap();
            }
        }
        let pending = store.peek_pending().unwrap();
        transport.corrupt_next_verdicts(fault);
        assert!(
            matches!(engine.drain().await, Err(ReplicaError::Protocol { ref code, .. }) if code == "InvalidResponse"),
            "{kind}"
        );
        assert_eq!(store.peek_pending().unwrap(), pending, "{kind}");
        assert_eq!(
            store.sync_status().unwrap().accepted_operations,
            0,
            "{kind}"
        );
        assert_eq!(
            store.sync_status().unwrap().submitted_groups,
            if kind == "mixed group" { 1 } else { 2 },
            "{kind}"
        );

        engine.drain().await.unwrap();
        assert!(store.peek_pending().unwrap().is_empty(), "{kind}");
        assert!(
            store.peek_snapshot("notes", "a").unwrap().is_some(),
            "{kind}"
        );
        assert_eq!(
            transport.pushed_batches().len(),
            1,
            "{kind}: the retry replayed stored verdicts"
        );
    }
}

#[tokio::test]
async fn mutation_changed_fails_the_drain_and_keeps_the_submission() {
    let store = store("protocol-mutation-changed");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine.pull_once("user").await.unwrap();
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("sent"))]))
        .await
        .unwrap();
    transport.lose_push_replies(1);
    assert!(engine.drain().await.is_err());
    write(&store, |ctx| {
        ctx.tx.execute_batch(
            "UPDATE submissions SET content = CAST(replace(CAST(content AS TEXT), 'sent', 'edit') AS BLOB);
             UPDATE intents SET payload = replace(payload, 'sent', 'edit') WHERE state = 'frozen';",
        )?;
        Ok(())
    })
    .unwrap();

    let pending = store.peek_pending().unwrap();
    assert!(matches!(
        engine.drain().await,
        Err(ReplicaError::Protocol { ref code, .. }) if code == "MutationChanged"
    ));
    assert_eq!(store.peek_pending().unwrap(), pending);
    assert_eq!(store.sync_status().unwrap().submitted_groups, 1);
    assert_eq!(store.sync_status().unwrap().accepted_operations, 0);
}

#[tokio::test]
async fn a_push_carries_at_most_one_hundred_operations_and_never_splits_a_submission() {
    let store = store("protocol-push-bound");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    engine.pull_once("user").await.unwrap();
    for index in 0..98 {
        engine
            .save_row(
                "notes",
                &format!("n{index:02}"),
                None,
                &ReplicaFields::new(),
            )
            .await
            .unwrap();
    }
    transport.fail_pushes(true);
    assert!(engine.drain().await.is_err());
    engine
        .write_atomically(|tx| {
            for index in 0..3 {
                tx.create(
                    "notes",
                    &format!("group-{index}"),
                    None,
                    &ReplicaFields::new(),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    transport.fail_pushes(false);
    engine.drain().await.unwrap();

    let sizes: Vec<usize> = transport.pushed_batches().iter().map(Vec::len).collect();
    assert_eq!(
        sizes,
        [98, 3],
        "the oldest submissions first; the group stays whole"
    );
    let batches = transport.pushed_batches();
    assert!(batches[0].iter().all(|op| op.group.is_none()));
    let group = batches[1][0]
        .group
        .clone()
        .expect("a frozen action names its group");
    assert!(
        batches[1]
            .iter()
            .all(|op| op.group.as_ref() == Some(&group))
    );
    assert!(store.peek_pending().unwrap().is_empty());
}

#[tokio::test]
async fn a_round_is_staged_until_the_server_has_nothing_more() {
    let store = store("protocol-staged-round");
    let transport = StubTransport::new();
    let engine = paged_engine(&store, &transport);
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![
                note("n1", "one", None),
                note("n2", "two", None),
                note("n3", "three", None),
            ],
            "c3",
            false,
        ),
    );

    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    assert!(
        store.all_snapshots().unwrap().is_empty(),
        "a staged page is invisible"
    );
    assert_eq!(engine.current_cursor("user").await.unwrap(), None);
    assert_eq!(staged(&store), (1, 1));
    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    assert_eq!(staged(&store), (1, 2));
    assert_eq!(engine.pull_once("user").await.unwrap(), 3);

    assert_eq!(title(&store, "n3"), Some(text("three")));
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("c3")
    );
    assert_eq!(staged(&store), (0, 0));
    assert_eq!(
        pull_cursors(&transport),
        [None, Some("c3~1".to_owned()), Some("c3~2".to_owned())],
        "each request continues from the cursor of the last"
    );
}

async fn seed_the_board(engine: &ReplicaEngine, transport: &StubTransport) {
    let board = doc_snapshot(
        "boards",
        "b1",
        "stub@1",
        b"SNAP",
        fields(&[("name", text("Plans"))]),
    );
    transport.queue_pull("user", ScriptedPull::new(vec![board], "c1", false));
    engine
        .pull_until_caught_up(Some(&["user".to_owned()]))
        .await
        .unwrap();
}

fn queue_board_edits_and_a_note(transport: &StubTransport) {
    let frames = vec![
        doc_delta("boards", "b1", 1, "stub@1", b"+a"),
        doc_delta("boards", "b1", 2, "stub@1", b"+b"),
        row_set("boards", "b1", None, fields(&[("name", text("Renamed"))])),
        note("n1", "after", None),
    ];
    transport.queue_pull("user", ScriptedPull::new(frames, "c2", false));
}

fn base_fold(store: &ReplicaStateStore) -> Option<Vec<u8>> {
    store
        .pool()
        .read(|db| store.base_row(db, "boards", "b1"))
        .unwrap()
        .unwrap()
        .fold
}

#[tokio::test]
async fn a_documents_deltas_and_row_staged_across_pages_apply_whole() {
    let store = store("protocol-entity-page");
    let transport = StubTransport::new();
    let engine = paged_engine(&store, &transport);
    seed_the_board(&engine, &transport).await;
    queue_board_edits_and_a_note(&transport);

    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    assert_eq!(engine.pull_once("user").await.unwrap(), 4);

    assert_eq!(base_fold(&store).as_deref(), Some(&b"SNAP+a+b"[..]));
    assert_eq!(
        store.peek_snapshot("boards", "b1").unwrap().unwrap().data["name"],
        text("Renamed")
    );
    assert_eq!(title(&store, "n1"), Some(text("after")));
    engine.verify_integrity("user").await.unwrap();
}

#[tokio::test]
async fn a_staged_round_survives_process_death_and_resumes_from_its_cursor() {
    let directory = temp_directory("protocol-resume");
    let transport = StubTransport::new();
    let open = |transport: &std::sync::Arc<StubTransport>| {
        let mut options = options(directory.path().to_path_buf(), transport.clone());
        options.batch_limit = 1;
        ReplicaEngine::new(options)
    };
    let engine = open(&transport);
    engine.open(OWNER).await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "one", None), note("n2", "two", None)],
            "c2",
            false,
        ),
    );
    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    engine.close().await.unwrap();

    let reopened = open(&transport);
    reopened.open(OWNER).await.unwrap();
    assert_eq!(
        reopened
            .pull_until_caught_up(Some(&["user".to_owned()]))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        pull_cursors(&transport),
        [None, Some("c2~1".to_owned())],
        "the reopened store continued its staged round"
    );
    let store = reopened.store().unwrap();
    assert_eq!(title(&store, "n1"), Some(text("one")));
    assert_eq!(title(&store, "n2"), Some(text("two")));
    assert_eq!(
        reopened.current_cursor("user").await.unwrap().as_deref(),
        Some("c2")
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn cursor_invalid_discards_the_staged_round_and_publishes_a_baseline() {
    let store = store("protocol-cursor-invalid");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "one", None)], "c1", false),
    );
    engine.pull_once("user").await.unwrap();
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n2", "staged", None)], "c2", true),
    );
    assert_eq!(engine.pull_once("user").await.unwrap(), 0);

    transport.forget_cursors();
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "restored", None), note("n3", "three", None)],
            "b1",
            false,
        ),
    );
    assert_eq!(
        engine
            .pull_until_caught_up(Some(&["user".to_owned()]))
            .await
            .unwrap(),
        2
    );

    assert_eq!(
        pull_cursors(&transport)[2..],
        [Some("c2".to_owned()), None],
        "the refused cursor is followed by a baseline request"
    );
    assert_eq!(title(&store, "n1"), Some(text("restored")));
    assert_eq!(
        title(&store, "n2"),
        None,
        "the discarded page never published"
    );
    assert_eq!(title(&store, "n3"), Some(text("three")));
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("b1")
    );
    assert_eq!(staged(&store), (0, 0));
}

#[tokio::test]
async fn an_older_round_cannot_erase_authoring_accepted_after_it_began() {
    let store = store("protocol-visible");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "server", None)], "c1", false),
    );
    engine.pull_once("user").await.unwrap();

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "older", None)], "c2", true),
    );
    assert_eq!(engine.pull_once("user").await.unwrap(), 0);
    engine
        .save_row("notes", "n1", None, &fields(&[("title", text("accepted"))]))
        .await
        .unwrap();
    engine.drain().await.unwrap();
    assert_eq!(store.sync_status().unwrap().accepted_operations, 1);

    transport.queue_pull("user", ScriptedPull::new(Vec::new(), "c3", false));
    assert_eq!(engine.pull_once("user").await.unwrap(), 1);
    assert_eq!(title(&store, "n1"), Some(text("accepted")));
    assert_eq!(
        store.sync_status().unwrap().accepted_operations,
        1,
        "the round began before the acceptance and cannot prove it visible"
    );

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n1", "accepted", None)], "c4", false),
    );
    engine.pull_once("user").await.unwrap();
    assert_eq!(title(&store, "n1"), Some(text("accepted")));
    assert_eq!(store.sync_status().unwrap().accepted_operations, 0);
}

#[tokio::test]
async fn verification_behind_the_server_asks_for_a_pull_and_reports_divergence() {
    let store = store("protocol-verify");
    let transport = StubTransport::new();
    let engine = engine(store.clone(), transport.clone());
    transport.queue_pull(
        "user",
        ScriptedPull::new(
            vec![note("n1", "one", None), note("n2", "two", None)],
            "c1",
            false,
        ),
    );
    engine.pull_once("user").await.unwrap();
    engine.verify_integrity("user").await.unwrap();

    transport.queue_pull(
        "user",
        ScriptedPull::new(vec![note("n3", "three", None)], "c2", false),
    );
    assert!(matches!(
        engine.verify_integrity("user").await,
        Err(ReplicaError::Protocol { ref code, .. }) if code == "CursorBehind"
    ));
    assert_eq!(title(&store, "n3"), None);
    engine.pull_once("user").await.unwrap();
    engine.verify_integrity("user").await.unwrap();
    assert_eq!(title(&store, "n3"), Some(text("three")));
    assert_eq!(
        engine.current_cursor("user").await.unwrap().as_deref(),
        Some("c2")
    );

    engine
        .save_row("notes", "n4", None, &fields(&[("title", text("local"))]))
        .await
        .unwrap();
    engine.verify_integrity("user").await.unwrap();
    write(&store, |ctx| {
        ctx.tx.execute("DELETE FROM base WHERE row_id = 'n2'", [])?;
        Ok(())
    })
    .unwrap();
    let rows = store.all_snapshots().unwrap();
    let pending = store.peek_pending().unwrap();
    assert!(matches!(
        engine.verify_integrity("user").await,
        Err(ReplicaError::Protocol { ref code, .. }) if code == "ReplicaDiverged"
    ));
    assert_eq!(store.all_snapshots().unwrap(), rows);
    assert_eq!(store.peek_pending().unwrap(), pending);
}
