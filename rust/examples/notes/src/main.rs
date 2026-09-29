pub mod generated;
use generated::{Note, NotesReplica};
use replicaman::transport::NoWireTransport;
use replicaman::{ReplicaEngine, ReplicaEngineOptions, ReplicaResult, ReplicaRowModel};
use std::sync::Arc;

#[tokio::main]
async fn main() -> ReplicaResult<()> {
    let home = std::env::temp_dir().join(format!("replicaman-example-{}", replicaman::id::ulid()));
    let mut options = ReplicaEngineOptions::new(
        &home,
        Arc::new(NoWireTransport),
        NotesReplica::schema(),
        Arc::new(|future| {
            tokio::spawn(future);
        }),
    );
    options.automatically_push_writes = false;
    let engine = ReplicaEngine::new(options);
    engine.open(42).await?;
    let replica = NotesReplica::new(engine.clone());
    replica
        .notes()
        .create(&Note::new("first-note", "Draft", 42))
        .await?;
    replica
        .notes()
        .update(&Note::new("first-note", "Saved offline", 42))
        .await?;
    // One writer sequence and one server transaction for both typed rows.
    engine
        .write_atomically(|tx| {
            for note in [
                Note::new("group-a", "First member", 42),
                Note::new("group-b", "Second member", 42),
            ] {
                tx.create(
                    Note::stream_name(),
                    &note.id,
                    note.type_name(),
                    &note.encode(),
                )?;
            }
            Ok(())
        })
        .await?;
    engine.close().await?;
    engine.open(42).await?;
    assert_eq!(
        replica.notes().find("first-note")?.unwrap().title,
        "Saved offline"
    );
    assert_eq!(engine.pending_ops().await?.len(), 4);
    engine.close().await?;
    std::fs::remove_dir_all(home).expect("remove example's own temporary directory");
    println!("PASS typed Rust save, atomic action and durable reopen");
    Ok(())
}
