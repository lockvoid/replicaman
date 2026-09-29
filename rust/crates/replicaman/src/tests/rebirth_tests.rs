use super::support::*;
use crate::*;

#[tokio::test]
async fn cancelled_first_birth_leaves_no_invented_predecessor() {
    let store = store("cancelled-birth");
    let engine = engine(store.clone(), StubTransport::new());
    engine
        .save_row("notes", "n", None, &ReplicaFields::new())
        .await
        .unwrap();
    assert!(!engine.delete_row("notes", "n").await.unwrap());
    engine
        .save_row("notes", "n", None, &ReplicaFields::new())
        .await
        .unwrap();
    let operations = store.pending_ops().unwrap();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].op().unwrap().replaces, None);
}

#[tokio::test]
async fn cancelling_recreation_preserves_the_previous_lifetimes_delete() {
    let store = store("cancelled-recreation");
    let wire = StubTransport::new();
    let engine = engine(store.clone(), wire.clone());
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("first"))]))
        .await
        .unwrap();
    let first = store.pending_ops().unwrap()[0].op().unwrap();
    engine.drain().await.unwrap();
    engine.delete_row("notes", "n").await.unwrap();
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("cancelled"))]))
        .await
        .unwrap();
    assert!(!engine.delete_row("notes", "n").await.unwrap());
    engine
        .save_row("notes", "n", None, &fields(&[("title", text("third"))]))
        .await
        .unwrap();

    let operations: Vec<_> = store
        .pending_ops()
        .unwrap()
        .into_iter()
        .map(|entry| entry.op().unwrap())
        .collect();
    assert_eq!(
        operations
            .iter()
            .map(|op| op.verb.as_str())
            .collect::<Vec<_>>(),
        [verb::ROW_DELETE, verb::ROW_CREATE]
    );
    assert_eq!(operations[0].incarnation, first.incarnation);
    assert_eq!(operations[1].replaces, first.incarnation);
    assert_ne!(operations[1].incarnation, first.incarnation);
    engine.drain().await.unwrap();
    assert_eq!(
        wire.pushed_ops()
            .iter()
            .map(|op| op.verb.as_str())
            .collect::<Vec<_>>(),
        [verb::ROW_CREATE, verb::ROW_DELETE, verb::ROW_CREATE]
    );
}
